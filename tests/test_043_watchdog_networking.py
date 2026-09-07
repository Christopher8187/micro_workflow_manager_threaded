from __future__ import annotations

import asyncio
import json
import os
import socket
import threading
import time
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from threading import Event

import httpx
import pytest

import micro_workflow_manager.workflow.supervisor_attempts as supervisor_attempts_module
import micro_workflow_manager.workflow.supervisor_core as supervisor_core_module
import micro_workflow_manager.workflow.supervisor_persistence as supervisor_persistence_module

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.context import JobContext
from micro_workflow_manager.errors import JobFailedError
from micro_workflow_manager.models import Job
from micro_workflow_manager.networking import (
    close_shared_http_transport,
    configure_shared_http_transport,
    shared_http_transport,
)
from micro_workflow_manager.network.transport import (
    SharedHTTPTransport,
    network_attempt_context,
)
from micro_workflow_manager.runners.api import ApiRunner
from micro_workflow_manager.session_liveness import process_identity


def _claimed_runtime_storage(tmp_path):
    workflow = MicroWorkflow(tmp_path, persist_graph=False)
    workflow.graph([("A", "A")])
    storage = workflow.storage
    session_id = "runtime-observation-owner"
    snapshot = workflow.topology.snapshot()
    storage.register_component_topology(snapshot)
    storage.create_execution_session(
        session_id,
        session_kind="main",
        command="runtime observation",
        start_component=("A",),
        selected_components=[("A",)],
        started_at="2026-01-01T00:00:00",
        hostname=socket.gethostname(),
        pid=os.getpid(),
        process_identity=process_identity(os.getpid()),
    )
    storage.reserve_execution_components(session_id, expected_shape=snapshot.shape_json)
    storage.create_job(Job(node_name="A", job_id=1, params={}))
    generation, execution_id = storage.claim_job_execution(
        "A", 1, started_at="2026-01-01", session_id=session_id, component=("A",),
    )
    return storage, generation, execution_id


def test_cooperative_future_result_preserves_periodic_timeout_semantics():
    future: Future[str] = Future()
    timer = threading.Timer(0.12, lambda: future.set_result("done"))
    timer.start()
    ticks = 0

    def job(_):
        nonlocal ticks
        while True:
            try:
                return future.result(timeout=0.02)
            except FutureTimeoutError:
                ticks += 1

    try:
        assert ApiRunner(max_threads=1, poll_interval=0.002).run_jobs("A", [1], job) == ["done"]
    finally:
        timer.join(timeout=1)
    assert ticks >= 3


def test_framework_http_wait_suspends_checkpoint_watchdog(tmp_path):
    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.15)
        return httpx.Response(200, json={"ok": True}, request=request)

    close_shared_http_transport()
    configure_shared_http_transport(transport=httpx.MockTransport(handler))
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "B")])

    @workflow.task("A", runner="api", max_threads=20, timeout=2.0, checkpoint_timeout=0.03)
    def a(ctx):
        ctx.checkpoint("before network", timeout=0.03)
        return shared_http_transport.post_json(
            "https://example.test/chat",
            timeout=0.5,
            json={"x": 1},
            wait_name="mock model request",
        )

    @workflow.task("B")
    def b(ctx):
        return None

    workflow.start("A", job_id=1)
    try:
        workflow.run_node("A", ignore_readiness=True)
    finally:
        close_shared_http_transport()
    assert workflow.storage.job_status_counts("A").get("done") == 1


def test_external_wait_replay_renews_same_per_attempt_transport_lease(
    tmp_path,
    monkeypatch,
):
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([])
    watch = workflow.scheduler_supervisor.create_attempt(
        node_name="A",
        job_id=1,
        task_name="run",
        attempt=1,
        repeat_index=1,
        generation=0,
        execution_id=None,
        cancellation_event=Event(),
        total_timeout=10.0,
        checkpoint_timeout=1.0,
    )
    supervisor = workflow.scheduler_supervisor
    supervisor.begin_external_wait(
        watch,
        name="model request",
        timeout=0.5,
        cleanup_grace=0.1,
    )
    original_deadline = watch.external_wait_deadline
    original_lease = watch.external_wait_timeout
    monkeypatch.setattr(
        supervisor_attempts_module,
        "monotonic",
        lambda: original_deadline - original_lease + 0.001,
    )

    supervisor.renew_external_wait(
        watch,
        reason="cohort_stream_stall",
    )

    assert watch.external_wait_timeout == original_lease == pytest.approx(0.6)
    assert watch.external_wait_deadline > original_deadline
    assert watch.external_wait_attempt == 2
    assert watch.external_wait_renewals == 1
    assert watch.external_wait_last_renewal_reason == "cohort_stream_stall"
    runtime = supervisor._runtime_payload(watch, state="running")
    assert runtime["external_wait_attempt"] == 2
    assert runtime["external_wait_renewals"] == 1
    assert runtime["external_wait_last_renewal_reason"] == "cohort_stream_stall"

    supervisor.end_external_wait(watch)
    supervisor.finish_attempt(watch, state="succeeded")


def test_deferred_external_wait_arms_only_at_first_physical_dispatch(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([])
    watch = workflow.scheduler_supervisor.create_attempt(
        node_name="A",
        job_id=1,
        task_name="run",
        attempt=1,
        repeat_index=1,
        generation=0,
        execution_id=None,
        cancellation_event=Event(),
        total_timeout=10.0,
        checkpoint_timeout=0.01,
    )
    supervisor = workflow.scheduler_supervisor
    supervisor.begin_external_wait(
        watch,
        name="model request",
        timeout=0.5,
        cleanup_grace=0.1,
        defer_lease_start=True,
    )

    assert watch.external_wait_depth == 1
    assert watch.external_wait_attempt == 0
    assert watch.external_wait_deadline is None

    supervisor.renew_external_wait(
        watch,
        reason="initial_transport_attempt",
    )
    initial_deadline = watch.external_wait_deadline
    assert initial_deadline is not None
    assert watch.external_wait_attempt == 1
    assert watch.external_wait_renewals == 0
    assert watch.external_wait_last_renewal_reason is None

    supervisor.renew_external_wait(
        watch,
        reason="transport_error",
    )
    assert watch.external_wait_deadline >= initial_deadline
    assert watch.external_wait_attempt == 2
    assert watch.external_wait_renewals == 1
    assert watch.external_wait_last_renewal_reason == "transport_error"

    supervisor.end_external_wait(watch)
    supervisor.finish_attempt(watch, state="succeeded")


def test_external_wait_completion_renews_checkpoint_after_lock_contention(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner="api")
    supervisor = workflow.scheduler_supervisor
    watch = supervisor.create_attempt(
        node_name="A", job_id=1, task_name="run", attempt=1, repeat_index=1,
        generation=0, execution_id=None, cancellation_event=Event(),
        total_timeout=10.0, checkpoint_timeout=0.02,
    )
    supervisor.begin_external_wait(watch, name="model request", timeout=0.5)
    condition = supervisor._condition
    waiting_for_lock = Event()
    clock = [time.monotonic() + 60.0]
    errors = []

    def finish_wait():
        try:
            supervisor.end_external_wait(watch)
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=finish_wait)

    class ObservedCondition:
        def __enter__(self):
            if threading.current_thread() is worker:
                waiting_for_lock.set()
            return condition.__enter__()

        def __exit__(self, *args):
            return condition.__exit__(*args)

        def __getattr__(self, name):
            return getattr(condition, name)

    monkeypatch.setattr(supervisor_attempts_module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(supervisor, "_condition", ObservedCondition())
    try:
        with condition:
            worker.start()
            assert waiting_for_lock.wait(3)
            # Advance the clock while the real supervisor lock is unavailable.
            # The resumed handler must receive a fresh checkpoint interval.
            clock[0] += 1.0
        worker.join(timeout=3)
        assert not worker.is_alive()
        assert not errors
        assert watch.external_wait_depth == 0
        assert watch.checkpoint_name == "model request completed"
        assert watch.checkpoint_deadline == pytest.approx(clock[0] + 0.02, abs=0.001, rel=0)
    finally:
        worker.join(timeout=3)
        supervisor.finish_attempt(watch, state="completed")


@pytest.mark.parametrize("phase", ["framework", "caller"])
@pytest.mark.parametrize("deadline_kind", ["checkpoint", "total"])
def test_initial_task_deadline_starts_after_framework_condition_delay(
    tmp_path, monkeypatch, phase, deadline_kind,
):
    clock = [time.monotonic()]
    arming_thread = []
    arm_blocked = Event()
    release_arm = Event()
    armed = Event()
    handler_entered = Event()
    release_handler = Event()
    observe_supervisor = Event()
    supervisor_checked = Event()
    run_finished = Event()
    errors = []
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "B")])
    supervisor = workflow.scheduler_supervisor
    condition = supervisor._condition
    original_begin = supervisor.begin_handler_execution

    def begin_handler(watch):
        arming_thread.append(threading.get_ident())
        try:
            return original_begin(watch)
        finally:
            arming_thread.clear()
            armed.set()

    class ObservedCondition:
        def __enter__(self):
            if arming_thread == [threading.get_ident()]:
                arm_blocked.set()
                assert release_arm.wait(10), "Initial deadline arming was not released"
            return condition.__enter__()

        def __exit__(self, *args):
            return condition.__exit__(*args)

        def wait(self, timeout=None):
            if observe_supervisor.is_set():
                supervisor_checked.set()
            return condition.wait(timeout)

        def __getattr__(self, name):
            return getattr(condition, name)

    for module in (supervisor_attempts_module, supervisor_core_module, supervisor_persistence_module):
        monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(supervisor, "_condition", ObservedCondition())
    monkeypatch.setattr(supervisor, "begin_handler_execution", begin_handler)

    @workflow.task(
        "A", runner="api", max_threads=1,
        timeout=0.02 if deadline_kind == "total" else 100,
        checkpoint_timeout=0.02 if deadline_kind == "checkpoint" else 100,
    )
    def a(ctx):
        handler_entered.set()
        assert release_handler.wait(10), "User work was not released"
        return {"started": True}

    @workflow.task("B")
    def b(ctx):
        return None

    workflow.start("A", job_id=1)

    def run():
        try:
            workflow.run_node("A", ignore_readiness=True)
        except BaseException as error:
            errors.append(error)
        finally:
            run_finished.set()

    active = threading.Thread(target=run, daemon=True)
    active.start()
    try:
        assert arm_blocked.wait(10), "Initial arming did not reach the condition"
        with condition:
            if phase == "framework":
                clock[0] += 1.0
            release_arm.set()
        assert armed.wait(10), "Initial deadline arming did not finish"
        if phase == "caller":
            assert handler_entered.wait(10), "User work did not start"
        with condition:
            if phase == "caller":
                clock[0] += 1.0
            observe_supervisor.set()
            condition.notify_all()
        scan_deadline = time.perf_counter() + 10
        while not supervisor_checked.is_set():
            timeouts = [event for event in workflow.storage.read_job_events("A", 1)
                        if event.get("event") == "timeout"]
            if timeouts:
                break
            assert time.perf_counter() < scan_deadline, "Supervisor did not inspect initial deadlines"
            time.sleep(0.01)
        timeouts = [event for event in workflow.storage.read_job_events("A", 1)
                    if event.get("event") == "timeout"]
        assert [event["timeout_kind"] for event in timeouts] == (
            [] if phase == "framework" else [deadline_kind]
        )
        release_handler.set()
        assert run_finished.wait(10), "The API run did not finish"
        output = json.loads(workflow.storage.output_file("A", 1).read_text(encoding="utf-8"))
        owner = workflow.storage.read_job_current_owner("A", 1)
        assert owner is not None
        if phase == "framework":
            assert not errors, errors
            assert handler_entered.is_set()
            assert workflow.storage.job_status_counts("A").get("done") == 1
            assert output == {
                "status": "done", "result_type": "dict", "result_repr": "{'started': True}",
                "generation": owner["generation"], "execution_id": owner["execution_id"],
            }
        else:
            assert len(errors) == 1 and isinstance(errors[0], JobFailedError)
            assert workflow.storage.job_status_counts("A").get("failed") == 1
            assert timeouts[0]["timeout_seconds"] == 0.02
            assert timeouts[0]["checkpoint"] == "task start"
            assert output["status"] == "failed" and output["error"].startswith("JobTimeoutError(")
    finally:
        release_arm.set()
        release_handler.set()
        active.join(timeout=10)
        workflow.storage.db_mutation_barrier()
        cleanup_deadline = time.perf_counter() + 10
        while workflow.storage.mutation_writer_diagnostics()["writer_alive"]:
            assert time.perf_counter() < cleanup_deadline, "Mutation writer did not retire"
            time.sleep(0.01)
        workflow.storage.close_thread_connection()
    assert not active.is_alive()


@pytest.mark.parametrize("phase", [
    "framework", "heartbeat", "equal", "late", "total", "total_same_deadline",
    "renewed", "renewed_same_time",
])
def test_physical_replay_entry_preserves_lease_boundaries(tmp_path, monkeypatch, phase):
    initial_time = time.monotonic()
    clock = [initial_time]
    first_request = Event()
    fail_first_request = Event()
    second_request = Event()
    release_response = Event()
    replay_requested = Event()
    enter_replay = Event()
    replay_condition_blocked = Event()
    release_replay_condition = Event()
    heartbeat_completed = Event()
    pause_supervisor = Event()
    supervisor_paused = Event()
    release_supervisor = Event()
    observe_supervisor = Event()
    supervisor_checked = Event()
    replay_thread = []
    requests = []
    reasons = []
    errors = []
    renewed_phase = phase in {"renewed", "renewed_same_time"}
    total_timeout = 0.02 if phase == "total" else 30.5 if phase == "total_same_deadline" else 100
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "B")])
    supervisor = workflow.scheduler_supervisor
    condition = supervisor._condition
    original_renew = supervisor.renew_external_wait

    def renew(watch, *, reason):
        reasons.append(reason)
        if len(reasons) == 1:
            return original_renew(watch, reason=reason)
        replay_requested.set()
        assert enter_replay.wait(10), "Replay callback was not allowed to enter"
        replay_thread.append(threading.get_ident())
        try:
            return original_renew(watch, reason=reason)
        finally:
            replay_thread.clear()

    class ObservedCondition:
        def __enter__(self):
            if replay_thread == [threading.get_ident()]:
                replay_condition_blocked.set()
                assert release_replay_condition.wait(10), "Replay condition was not released"
            if (threading.current_thread().name == "mwf-scheduler-supervisor"
                    and pause_supervisor.is_set()):
                supervisor_paused.set()
                assert release_supervisor.wait(10), "Deadline scan was not released"
            return condition.__enter__()

        def __exit__(self, *args):
            return condition.__exit__(*args)

        def wait(self, timeout=None):
            if observe_supervisor.is_set():
                supervisor_checked.set()
            return condition.wait(timeout)

        def __getattr__(self, name):
            return getattr(condition, name)

    async def physical_response(request):
        requests.append(request)
        response_deadline = time.perf_counter() + 10
        if len(requests) == 1:
            first_request.set()
            while not fail_first_request.is_set():
                assert time.perf_counter() < response_deadline, "First physical failure was not released"
                await asyncio.sleep(0.001)
            raise httpx.ReadError("controlled physical-attempt failure", request=request)
        second_request.set()
        while not release_response.is_set():
            assert time.perf_counter() < response_deadline, "Second physical response was not released"
            await asyncio.sleep(0.001)
        return httpx.Response(200, json={"received": True}, request=request)

    for module in (supervisor_attempts_module, supervisor_core_module, supervisor_persistence_module):
        monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(supervisor, "_condition", ObservedCondition())
    monkeypatch.setattr(supervisor, "renew_external_wait", renew)
    close_shared_http_transport()
    configure_shared_http_transport(transport=httpx.MockTransport(physical_response))

    @workflow.task("A", runner="api", timeout=total_timeout,
                   checkpoint_timeout=100)
    def a(ctx):
        def heartbeat(elapsed):
            if replay_condition_blocked.is_set() and not heartbeat_completed.is_set():
                ctx.checkpoint(name="replay heartbeat", progress=0.5)
                heartbeat_completed.set()

        return shared_http_transport.post_json(
            "https://example.test/replay-entry", timeout=0.5,
            json={"request": True}, wait_name="replayed request",
            heartbeat_callback=heartbeat if phase == "heartbeat" else None,
            heartbeat_interval=0.1,
        )

    @workflow.task("B")
    def b(ctx):
        return None

    workflow.start("A", job_id=1)

    def run():
        try:
            workflow.run_node("A", ignore_readiness=True)
        except BaseException as error:
            errors.append(error)

    def timeout_events():
        return [event for event in workflow.storage.read_job_events("A", 1)
                if event.get("event") == "timeout"]

    def wait_for_scan_or_timeout():
        scan_deadline = time.perf_counter() + 10
        while not supervisor_checked.is_set() and not timeout_events():
            assert time.perf_counter() < scan_deadline, "Supervisor did not inspect the replay lease"
            time.sleep(0.01)

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    try:
        assert first_request.wait(10), "First physical request did not start"
        with condition:
            pause_supervisor.set()
            condition.notify_all()
        assert supervisor_paused.wait(10), "Supervisor did not pause before replay entry"
        fail_first_request.set()
        assert replay_requested.wait(10), "A real physical retry was not requested"
        entry_offset = (30.5 if phase == "equal" else 31.0 if phase == "late"
                        else 0.0 if phase == "renewed_same_time" else 0.01)
        clock[0] = initial_time + entry_offset
        enter_replay.set()
        assert replay_condition_blocked.wait(10), "Replay did not reach the supervisor condition"
        assert not second_request.is_set()
        if phase == "heartbeat":
            assert heartbeat_completed.wait(10), "The real heartbeat checkpoint did not finish"
        with condition:
            if not renewed_phase:
                clock[0] = initial_time + (1.0 if phase == "total" else 31.0)
            observe_supervisor.set()
            condition.notify_all()
        release_supervisor.set()
        wait_for_scan_or_timeout()
        expected = "total" if phase in {"total", "total_same_deadline"} else "external" if phase in {"equal", "late"} else None
        assert [event["timeout_kind"] for event in timeout_events()] == ([] if expected is None else [expected])
        release_replay_condition.set()
        if expected is None:
            assert second_request.wait(10), "The replay did not receive its own physical attempt"
            assert reasons == ["initial_transport_attempt", "transport_error"]
            assert len(requests) == 2
            if renewed_phase:
                with condition:
                    supervisor_checked.clear()
                    clock[0] = initial_time + 31.0
                    condition.notify_all()
                wait_for_scan_or_timeout()
                expected = "external"
                assert [event["timeout_kind"] for event in timeout_events()] == [expected]
        release_response.set()
        worker.join(timeout=10)
        assert not worker.is_alive(), "The replay run did not finish"
        output = json.loads(workflow.storage.output_file("A", 1).read_text(encoding="utf-8"))
        owner = workflow.storage.read_job_current_owner("A", 1)
        assert owner is not None
        if expected is None:
            assert not errors, errors
            assert workflow.storage.job_status_counts("A").get("done") == 1
            assert output == {
                "status": "done", "result_type": "dict", "result_repr": "{'received': True}",
                "generation": owner["generation"], "execution_id": owner["execution_id"],
            }
            assert not timeout_events()
        else:
            assert len(errors) == 1 and isinstance(errors[0], JobFailedError)
            assert workflow.storage.job_status_counts("A").get("failed") == 1
            assert output["status"] == "failed" and output["error"].startswith("JobTimeoutError(")
            events = timeout_events()
            assert len(events) == 1 and events[0]["timeout_kind"] == expected
            assert events[0]["timeout_seconds"] == (total_timeout if expected == "total" else 30.5)
            assert events[0]["external_wait_attempt"] == (2 if renewed_phase else 1)
            assert events[0]["external_wait_renewals"] == (1 if renewed_phase else 0)
            assert events[0]["external_wait_last_renewal_reason"] == ("transport_error" if renewed_phase else None)
    finally:
        fail_first_request.set()
        enter_replay.set()
        release_replay_condition.set()
        release_supervisor.set()
        release_response.set()
        worker.join(timeout=10)
        workflow.storage.db_mutation_barrier()
        cleanup_deadline = time.perf_counter() + 10
        while workflow.storage.mutation_writer_diagnostics()["writer_alive"]:
            assert time.perf_counter() < cleanup_deadline, "Mutation writer did not retire"
            time.sleep(0.01)
        workflow.storage.close_thread_connection()
        close_shared_http_transport()
    assert not worker.is_alive()


@pytest.mark.parametrize("recovery", ["retry", "fallback"])
def test_delayed_timeout_preserves_a_later_attempt_runtime(tmp_path, monkeypatch, recovery):
    clock = [time.monotonic()]
    first_waiting = Event()
    publication_blocked = Event()
    release_publication = Event()
    publication_finished = Event()
    errors = []
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "B")])
    workflow.active_job_restart_enabled = True
    supervisor = workflow.scheduler_supervisor
    original_publish = supervisor._persist_timeout

    def publish_timeout(*args, **kwargs):
        publication_blocked.set()
        try:
            assert release_publication.wait(10), "Earlier timeout publication was not released"
            return original_publish(*args, **kwargs)
        finally:
            publication_finished.set()

    for module in (supervisor_attempts_module, supervisor_core_module, supervisor_persistence_module):
        monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(supervisor, "_persist_timeout", publish_timeout)

    def succeed(ctx):
        ctx.checkpoint(name="later attempt result", progress=0.75)
        return {"recovered": recovery}

    @workflow.task("A", runner="api", retries=1 if recovery == "retry" else 0,
                   timeout=100, checkpoint_timeout=0.02)
    def primary(ctx):
        if ctx.attempt == 2:
            return succeed(ctx)
        ctx.checkpoint(name="earlier attempt waiting", progress=0.5)
        first_waiting.set()
        ctx.sleep(100)
        return {"unexpected": True}

    if recovery == "fallback":
        @workflow.fallback("A", name="recover", timeout=100, checkpoint_timeout=100)
        def recover(ctx, error=None):
            return succeed(ctx)

    @workflow.task("B")
    def b(ctx):
        return None

    workflow.start("A", job_id=1)

    def run():
        try:
            workflow.run_node("A", ignore_readiness=True)
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    try:
        assert first_waiting.wait(10), "The first attempt did not reach its cooperative wait"
        with supervisor._condition:
            clock[0] += 1
            supervisor._condition.notify_all()
        assert publication_blocked.wait(10), "The timeout publisher did not pause"
        worker.join(timeout=10)
        assert not worker.is_alive(), "The recovery attempt did not finish independently"
        assert not errors, errors
        assert workflow.storage.job_status_counts("A").get("done") == 1
        workflow.storage.db_mutation_barrier()
        before = workflow.storage.read_job_runtime("A", 1)
        assert before["task"] == ("primary" if recovery == "retry" else "recover")
        assert before["attempt"] == (2 if recovery == "retry" else 1)
        assert before["checkpoint_name"] == "later attempt result" and before["progress"] == 0.75
        output = json.loads(workflow.storage.output_file("A", 1).read_text(encoding="utf-8"))
        owner = workflow.storage.read_job_current_owner("A", 1)
        assert owner is not None and owner["generation"] == 0
        assert output == {"status": "done", "result_type": "dict",
                          "result_repr": "{'recovered': '" + recovery + "'}", "generation": 0,
                          "execution_id": owner["execution_id"]}
        release_publication.set()
        assert publication_finished.wait(10), "The earlier timeout publisher did not finish"
        events = [event for event in workflow.storage.read_job_events("A", 1)
                  if event.get("event") == "timeout"]
        assert len(events) == 1 and events[0]["checkpoint"] == "earlier attempt waiting"
        assert events[0]["task"] == "primary" and events[0]["attempt"] == 1
        assert events[0]["timeout_kind"] == "checkpoint" and events[0]["timeout_seconds"] == 0.02
        after = workflow.storage.read_job_runtime("A", 1)
        assert after == before
    finally:
        release_publication.set()
        worker.join(timeout=10)
        if publication_blocked.is_set():
            assert publication_finished.wait(10), "The timeout publisher did not retire"
        workflow.storage.db_mutation_barrier()
        cleanup_deadline = time.perf_counter() + 10
        while workflow.storage.mutation_writer_diagnostics()["writer_alive"]:
            assert time.perf_counter() < cleanup_deadline, "Mutation writer did not retire"
            time.sleep(0.01)
        workflow.storage.close_thread_connection()
    assert not worker.is_alive()


@pytest.mark.parametrize("ordering", ["queued", "grouped", "priority"])
def test_later_runtime_survives_queued_earlier_observations(tmp_path, monkeypatch, ordering):
    from micro_workflow_manager.storage.runtime_observations import RuntimeObservationSequence

    storage, generation, execution_id = _claimed_runtime_storage(tmp_path)
    sequence = RuntimeObservationSequence()
    orders = {name: sequence.register(name) for name in ("first", "middle", "last")}
    rows = {name: dict(state="running", watch_id=name, generation=generation,
                       execution_id=execution_id, checkpoint_name=name)
            for name in orders}
    storage.write_job_runtime("A", 1, rows["first"], _observation_order=orders["first"])
    entered = Event()
    release = Event()

    def block_writer(connection):
        entered.set()
        assert release.wait(10), "Queued runtime writes were not released"

    blocked = storage.submit_db_mutation(block_writer, wait=False)
    pending = []
    try:
        assert entered.wait(10), "The writer did not reach its queue gate"
        for name in ("middle", "last"):
            pending.append(storage.write_job_runtime(
                "A", 1, rows[name], wait=False, priority=20, _observation_order=orders[name],
            ))
        with monkeypatch.context() as gate:
            if ordering == "grouped":
                original_submit = storage.submit_grouped_db_mutation

                def enqueue_without_waiting(*args, **kwargs):
                    # Delay this synchronous caller's wait so its separate
                    # runtime slot joins the same real writer batch.
                    kwargs["wait"] = False
                    return original_submit(*args, **kwargs)

                gate.setattr(storage, "submit_grouped_db_mutation", enqueue_without_waiting)
            pending.append(storage.write_job_runtime(
                "A", 1, rows["first"], wait=ordering == "grouped", priority=20,
                _observation_order=orders["first"],
            ))
        expected = rows["last"]
        if ordering == "priority":
            expected = dict(rows["last"], state="timed_out")
            pending.append(storage.write_job_runtime(
                "A", 1, expected, wait=False, priority=10, _observation_order=orders["last"],
            ))
        release.set()
        blocked.result(timeout=10)
        for future in pending:
            future.result(timeout=10)
        storage.db_mutation_barrier()
        assert storage.read_job_runtime("A", 1) == expected
        storage.write_job_runtime(
            "A", 1, dict(rows["middle"], state="timed_out"),
            _observation_order=orders["middle"],
        )
        assert storage.read_job_runtime("A", 1) == expected
    finally:
        release.set()
        blocked.result(timeout=10)
        storage.db_mutation_barrier()
        cleanup_deadline = time.perf_counter() + 10
        while storage.mutation_writer_diagnostics()["writer_alive"]:
            assert time.perf_counter() < cleanup_deadline, "Mutation writer did not retire"
            time.sleep(0.01)
        storage.close_thread_connection()


@pytest.mark.parametrize("failure", ["serialization", "writer"])
def test_failed_runtime_successor_preserves_the_saved_attempt(tmp_path, failure):
    import sqlite3
    from micro_workflow_manager.storage.runtime_observations import RuntimeObservationSequence

    storage, generation, execution_id = _claimed_runtime_storage(tmp_path)
    sequence = RuntimeObservationSequence()
    orders = {name: sequence.register(name) for name in ("first", "middle", "last")}
    rows = {name: dict(state="running", watch_id=name, generation=generation,
                       execution_id=execution_id, checkpoint_name=name)
            for name in orders}
    try:
        storage.write_job_runtime("A", 1, rows["first"], _observation_order=orders["first"])
        if failure == "serialization":
            with pytest.raises(TypeError):
                storage.write_job_runtime(
                    "A", 1, dict(rows["middle"], unserializable=object()),
                    wait=False, _observation_order=orders["middle"],
                )
        else:
            storage.submit_db_mutation(lambda connection: connection.execute(
                "CREATE TRIGGER reject_middle_runtime BEFORE UPDATE OF runtime_json ON jobs "
                "WHEN json_extract(NEW.runtime_json, '$.watch_id')='middle' "
                "BEGIN SELECT RAISE(ABORT, 'injected runtime write failure'); END"
            ))
            rejected = storage.write_job_runtime(
                "A", 1, rows["middle"], wait=False, _observation_order=orders["middle"],
            )
            with pytest.raises(sqlite3.IntegrityError, match="injected runtime write failure"):
                rejected.result(timeout=10)
            storage.submit_db_mutation(lambda connection: connection.execute(
                "DROP TRIGGER reject_middle_runtime"
            ))
        assert storage.read_job_runtime("A", 1) == rows["first"]
        timed_out = dict(rows["first"], state="timed_out")
        storage.write_job_runtime("A", 1, timed_out, _observation_order=orders["first"])
        assert storage.read_job_runtime("A", 1) == timed_out
        storage.write_job_runtime("A", 1, rows["last"], _observation_order=orders["last"])
        storage.write_job_runtime("A", 1, timed_out, _observation_order=orders["first"])
        assert storage.read_job_runtime("A", 1) == rows["last"]
    finally:
        storage.submit_db_mutation(lambda connection: connection.execute(
            "DROP TRIGGER IF EXISTS reject_middle_runtime"
        ))
        storage.db_mutation_barrier()
        cleanup_deadline = time.perf_counter() + 10
        while storage.mutation_writer_diagnostics()["writer_alive"]:
            assert time.perf_counter() < cleanup_deadline, "Mutation writer did not retire"
            time.sleep(0.01)
        storage.close_thread_connection()


def test_timeout_event_rejects_an_execution_replaced_during_publication(tmp_path, monkeypatch):
    clock = [time.monotonic()]
    old_handler_waiting = Event()
    release_old_handler = Event()
    publication_blocked = Event()
    release_publication = Event()
    errors = []
    results = []
    old_threads = []
    workflow = MicroWorkflow(tmp_path, runner="direct")
    workflow.graph([("A", "B")])
    workflow.active_job_restart_enabled = True
    supervisor = workflow.scheduler_supervisor
    original_append = workflow.storage.append_job_event

    def append_event(node, job_id, event, **data):
        if event == "timeout":
            publication_blocked.set()
            assert release_publication.wait(10), "Old timeout publication was not released"
        return original_append(node, job_id, event, **data)

    for module in (supervisor_attempts_module, supervisor_core_module, supervisor_persistence_module):
        monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(workflow.storage, "append_job_event", append_event)

    @workflow.task("A", timeout=100, checkpoint_timeout=0.02)
    def a(ctx):
        if ctx.execution_generation == 0:
            old_threads.append(threading.current_thread())
            ctx.checkpoint(name="old generation waiting")
            old_handler_waiting.set()
            assert release_old_handler.wait(10), "The stale handler was not released"
            ctx.write_output("stale.txt", "stale")
            return {"generation": 0}
        ctx.checkpoint(name="replacement completed")
        ctx.write_output("fresh.txt", "fresh")
        return {"generation": ctx.execution_generation}

    @workflow.task("B")
    def b(ctx):
        return None

    workflow.start("A", job_id=1)

    def run():
        try:
            results.append(workflow.run_job("A", 1, ignore_readiness=True))
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    try:
        assert old_handler_waiting.wait(10), "The old handler did not start"
        old_owner = workflow.storage.read_job_control("A", 1)
        assert old_owner["generation"] == 0 and old_owner["active_execution_id"]
        with supervisor._condition:
            clock[0] += 1
            supervisor._condition.notify_all()
        assert publication_blocked.wait(10), "The old timeout did not reach event publication"
        workflow.storage.request_active_job_restart("A", 1, reason="restart during timeout publication")
        replacement = workflow.storage.read_job_control("A", 1)
        assert replacement["generation"] == 1 and replacement["active_execution_id"] is None
        release_publication.set()
        worker.join(timeout=10)
        assert not worker.is_alive(), "The original controller did not complete its replacement"
        assert not errors, errors
        assert results == [{"generation": 1}]
        release_old_handler.set()
        for old_thread in old_threads:
            old_thread.join(timeout=10)
            assert not old_thread.is_alive(), "The stale handler did not retire"
        assert workflow.storage.job_status_counts("A").get("done") == 1
        output = json.loads(workflow.storage.output_file("A", 1).read_text(encoding="utf-8"))
        owner = workflow.storage.read_job_current_owner("A", 1)
        assert owner is not None and owner["generation"] == 1
        assert owner["execution_id"] != old_owner["active_execution_id"]
        assert output == {"status": "done", "result_type": "dict",
                          "result_repr": "{'generation': 1}", "generation": 1,
                          "execution_id": owner["execution_id"]}
        assert (tmp_path / "node/A/output/fresh.txt").read_text(encoding="utf-8") == "fresh"
        assert not (tmp_path / "node/A/output/stale.txt").exists()
        runtime = workflow.storage.read_job_runtime("A", 1)
        assert runtime["generation"] == 1 and runtime["state"] == "completed"
        assert runtime["checkpoint_name"] == "replacement completed"
        assert runtime["execution_id"] != old_owner["active_execution_id"]
        events = workflow.storage.read_job_events("A", 1)
        assert len([event for event in events if event["event"] == "restart_requested"]) == 1
        assert [event for event in events if event["event"] == "timeout"] == []
    finally:
        release_publication.set()
        release_old_handler.set()
        worker.join(timeout=10)
        for old_thread in old_threads:
            old_thread.join(timeout=10)
            assert not old_thread.is_alive(), "The stale handler did not retire"
        workflow.storage.db_mutation_barrier()
        cleanup_deadline = time.perf_counter() + 10
        while workflow.storage.mutation_writer_diagnostics()["writer_alive"]:
            assert time.perf_counter() < cleanup_deadline, "Mutation writer did not retire"
            time.sleep(0.01)
        workflow.storage.close_thread_connection()
    assert not worker.is_alive()


@pytest.mark.parametrize("publication_gate", ["publisher", "event"])
def test_timeout_event_survives_same_execution_terminalization(
    tmp_path, monkeypatch, publication_gate,
):
    clock = [time.monotonic()]
    handler_waiting = Event()
    caller_finishing = Event()
    release_finish = Event()
    publication_blocked = Event()
    release_publication = Event()
    publication_finished = Event()
    errors = []
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "B")])
    workflow.active_job_restart_enabled = True
    supervisor = workflow.scheduler_supervisor
    original_finish = supervisor.finish_attempt
    original_publish = supervisor._persist_timeout
    original_append = workflow.storage.append_job_event

    def finish_attempt(watch, *, state, error=None):
        if state == "timed_out":
            caller_finishing.set()
            assert release_finish.wait(10), "Caller terminalization was not released"
        return original_finish(watch, state=state, error=error)

    def pause_publication():
        publication_blocked.set()
        assert release_publication.wait(10), "Timeout publication was not released"

    def publish_timeout(*args, **kwargs):
        try:
            if publication_gate == "publisher":
                pause_publication()
            return original_publish(*args, **kwargs)
        finally:
            publication_finished.set()

    def append_event(node, job_id, event, **data):
        if event == "timeout" and publication_gate == "event":
            pause_publication()
        return original_append(node, job_id, event, **data)

    for module in (supervisor_attempts_module, supervisor_core_module, supervisor_persistence_module):
        monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(supervisor, "finish_attempt", finish_attempt)
    monkeypatch.setattr(supervisor, "_persist_timeout", publish_timeout)
    monkeypatch.setattr(workflow.storage, "append_job_event", append_event)

    @workflow.task("A", runner="api", timeout=100, checkpoint_timeout=0.02)
    def a(ctx):
        ctx.checkpoint(name="awaiting publication", progress=0.5)
        handler_waiting.set()
        ctx.sleep(100)
        return {"unexpected": True}

    @workflow.task("B")
    def b(ctx):
        return None

    workflow.start("A", job_id=1)

    def run():
        try:
            workflow.run_node("A", ignore_readiness=True)
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    try:
        assert handler_waiting.wait(10), "The API handler did not reach its cooperative wait"
        owner = workflow.storage.read_job_control("A", 1)
        assert owner["generation"] == 0 and owner["active_execution_id"]
        with supervisor._condition:
            clock[0] += 1
            supervisor._condition.notify_all()
        assert publication_blocked.wait(10), "The timeout did not reach its publication gate"
        assert caller_finishing.wait(10), "The API caller did not observe timeout cancellation"
        release_finish.set()
        worker.join(timeout=10)
        assert not worker.is_alive(), "The API caller did not terminalize independently"
        assert len(errors) == 1 and isinstance(errors[0], JobFailedError)
        assert workflow.storage.job_status_counts("A").get("failed") == 1
        terminal_owner = workflow.storage.read_job_control("A", 1)
        assert terminal_owner["generation"] == 0
        assert terminal_owner["active_execution_id"] is None
        status = workflow.storage.read_job_status_data("A", 1)
        assert status["execution_id"] == owner["active_execution_id"]
        release_publication.set()
        assert publication_finished.wait(10), "The timeout publisher did not finish"
        events = [event for event in workflow.storage.read_job_events("A", 1)
                  if event.get("event") == "timeout"]
        assert len(events) == 1
        assert events[0]["timeout_kind"] == "checkpoint"
        assert events[0]["timeout_seconds"] == 0.02
        assert events[0]["checkpoint"] == "awaiting publication"
        assert events[0]["progress"] == 0.5
        runtime = workflow.storage.read_job_runtime("A", 1)
        assert runtime["state"] == "timed_out" and runtime["generation"] == 0
        assert runtime["execution_id"] == owner["active_execution_id"]
        assert runtime["checkpoint_name"] == "awaiting publication"
        output = json.loads(workflow.storage.output_file("A", 1).read_text(encoding="utf-8"))
        assert output["status"] == "failed" and output["error"].startswith("JobTimeoutError(")
    finally:
        release_finish.set()
        release_publication.set()
        worker.join(timeout=10)
        if publication_blocked.is_set():
            assert publication_finished.wait(10), "The timeout publisher did not retire"
        workflow.storage.db_mutation_barrier()
        cleanup_deadline = time.perf_counter() + 10
        while workflow.storage.mutation_writer_diagnostics()["writer_alive"]:
            assert time.perf_counter() < cleanup_deadline, "Mutation writer did not retire"
            time.sleep(0.01)
        workflow.storage.close_thread_connection()
    assert not worker.is_alive()


@pytest.mark.parametrize("deadline_kind", ["external", "total"])
@pytest.mark.parametrize("replay", [False, True])
def test_timeout_details_survive_network_cleanup_before_persistence(
    tmp_path, monkeypatch, deadline_kind, replay,
):
    clock = [time.monotonic()]
    physical_started = Event()
    release_response = Event()
    timeout_detected = Event()
    release_timeout = Event()
    wait_ended = Event()
    requests = []
    errors = []
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "B")])
    supervisor = workflow.scheduler_supervisor
    original_persist = supervisor._persist_timeout
    original_end = supervisor.end_external_wait

    def persist_timeout(*args, **kwargs):
        timeout_detected.set()
        assert release_timeout.wait(10), "Timeout persistence was not released"
        return original_persist(*args, **kwargs)

    def end_wait(*args, **kwargs):
        try:
            return original_end(*args, **kwargs)
        finally:
            wait_ended.set()

    async def physical_response(request):
        requests.append(request)
        if replay and len(requests) == 1:
            raise httpx.ReadError("controlled physical-attempt failure", request=request)
        physical_started.set()
        response_deadline = time.perf_counter() + 10
        while not release_response.is_set():
            assert time.perf_counter() < response_deadline, "Physical response was not released"
            await asyncio.sleep(0.001)
        return httpx.Response(200, json={"received": True}, request=request)

    for module in (supervisor_attempts_module, supervisor_core_module, supervisor_persistence_module):
        monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(supervisor, "_persist_timeout", persist_timeout)
    monkeypatch.setattr(supervisor, "end_external_wait", end_wait)
    close_shared_http_transport()
    configure_shared_http_transport(transport=httpx.MockTransport(physical_response))

    @workflow.task("A", runner="api", timeout=0.02 if deadline_kind == "total" else 100,
                   checkpoint_timeout=100)
    def a(ctx):
        return shared_http_transport.post_json(
            "https://example.test/timeout-observation", timeout=0.5,
            json={"request": True}, wait_name="observed request",
        )

    @workflow.task("B")
    def b(ctx):
        return None

    workflow.start("A", job_id=1)

    def run():
        try:
            workflow.run_node("A", ignore_readiness=True)
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    try:
        assert physical_started.wait(10), "The target physical request did not start"
        assert len(requests) == (2 if replay else 1)
        with supervisor._condition:
            clock[0] += 31.0
            supervisor._condition.notify_all()
        assert timeout_detected.wait(10), "Supervisor did not detect the timeout"
        release_response.set()
        assert wait_ended.wait(10), "Network cleanup did not finish before timeout persistence"
        release_timeout.set()
        worker.join(timeout=10)
        assert not worker.is_alive(), "The failed API run did not finish"
        assert len(errors) == 1 and isinstance(errors[0], JobFailedError)
        assert workflow.storage.job_status_counts("A").get("failed") == 1
        timeouts = [event for event in workflow.storage.read_job_events("A", 1)
                    if event.get("event") == "timeout"]
        assert len(timeouts) == 1
        event = timeouts[0]
        assert event["timeout_kind"] == deadline_kind
        assert event["timeout_seconds"] == (30.5 if deadline_kind == "external" else 0.02)
        assert event["checkpoint"] == "task start"
        assert event["external_wait_attempt"] == (2 if replay else 1)
        assert event["external_wait_renewals"] == (1 if replay else 0)
        assert event["external_wait_last_renewal_reason"] == ("transport_error" if replay else None)
        runtime = workflow.storage.read_job_runtime("A", 1)
        assert runtime["state"] == "timed_out"
        assert runtime["timeout_kind"] == deadline_kind
        assert runtime["checkpoint_name"] == "task start"
        assert runtime["external_wait_active"] is True
        assert runtime["external_wait_name"] == "observed request"
        assert runtime["external_wait_timeout_seconds"] == 30.5
        assert runtime["external_wait_attempt"] == (2 if replay else 1)
        assert runtime["external_wait_renewals"] == (1 if replay else 0)
        assert runtime["external_wait_last_renewal_reason"] == ("transport_error" if replay else None)
        assert ("30.5s transport lease" if deadline_kind == "external" else "timeout=0.02s") in runtime["timeout_message"]
        output = json.loads(workflow.storage.output_file("A", 1).read_text(encoding="utf-8"))
        assert output["status"] == "failed" and output["error"].startswith("JobTimeoutError(")
    finally:
        release_response.set()
        release_timeout.set()
        worker.join(timeout=10)
        workflow.storage.db_mutation_barrier()
        cleanup_deadline = time.perf_counter() + 10
        while workflow.storage.mutation_writer_diagnostics()["writer_alive"]:
            assert time.perf_counter() < cleanup_deadline, "Mutation writer did not retire"
            time.sleep(0.01)
        workflow.storage.close_thread_connection()
        close_shared_http_transport()
    assert not worker.is_alive()


@pytest.mark.parametrize("operation, deadline_kind, phase", [
    ("initial", "checkpoint", "framework"),
    ("initial", "checkpoint", "caller"),
    ("initial", "total", "framework"),
    ("initial", "total", "caller"),
    ("checkpoint", "checkpoint", "framework"),
    ("checkpoint", "checkpoint", "caller"),
    ("checkpoint", "total", "framework"),
    ("network_end", "checkpoint", "framework"),
    ("network_end", "checkpoint", "caller"),
    ("network_end", "total", "framework"),
    ("network_renew", "external", "framework"),
    ("network_renew", "external", "condition"),
    ("network_renew", "external", "physical"),
    ("network_renew", "total", "framework"),
    ("network_replay", "external", "framework"),
    ("network_replay", "external", "condition"),
    ("network_replay", "external", "physical"),
    ("network_replay", "total", "framework"),
])
def test_task_intervals_exclude_framework_heap_maintenance(
    tmp_path, monkeypatch, operation, deadline_kind, phase,
):
    clock = [time.monotonic()]
    pause_supervisor = Event()
    supervisor_paused = Event()
    release_supervisor = Event()
    observe_supervisor = Event()
    supervisor_checked = Event()
    all_other_handlers_entered = Event()
    reports_gate = Future()
    other_handlers_gate = Future()
    reports_finished = Event()
    heap_blocked = Event()
    release_heap = Event()
    heap_rebuilt = Event()
    armed = Event()
    handler_entered = Event()
    interval_entered = Event()
    start_checkpoint = Event()
    checkpoint_submission = Event()
    release_checkpoint_submission = Event()
    response_started = Event()
    release_response = Event()
    renewal_ready = Event()
    release_renewal = Event()
    renewal_condition_blocked = Event()
    release_renewal_condition = Event()
    renewal_reasons = []
    physical_requests = []
    is_renewal = operation in {"network_renew", "network_replay"}
    is_network = operation == "network_end" or is_renewal
    expected_physical_attempts = 2 if operation == "network_replay" else 1
    release_handler = Event()
    allow_a = Event()
    arming_thread = []
    entered_jobs = set()
    entered_lock = threading.Lock()
    errors = []
    workers = []
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "Z"), ("B", "Z")])
    supervisor = workflow.scheduler_supervisor
    condition = supervisor._condition
    method_name = {
        "initial": "begin_handler_execution",
        "checkpoint": "report_checkpoint",
        "network_end": "end_external_wait",
        "network_renew": "renew_external_wait",
        "network_replay": "renew_external_wait",
    }[operation]
    original_operation = getattr(supervisor, method_name)
    original_write = workflow.storage.write_job_runtime
    original_heap = supervisor_core_module.heapq

    def observed_operation(watch, *args, **kwargs):
        if watch.node_name != "A":
            return original_operation(watch, *args, **kwargs)
        if is_renewal:
            renewal_reasons.append(kwargs["reason"])
            if len(renewal_reasons) < expected_physical_attempts:
                return original_operation(watch, *args, **kwargs)
            renewal_ready.set()
            assert release_renewal.wait(10), "Physical-attempt renewal was not released"
        arming_thread.append(threading.get_ident())
        try:
            return original_operation(watch, *args, **kwargs)
        finally:
            arming_thread.clear()
            armed.set()

    def write_runtime(node, job_id, payload, **kwargs):
        if (operation == "checkpoint" and node == "A"
                and payload.get("state") == "running"
                and payload.get("checkpoint_name") == "after reports"):
            checkpoint_submission.set()
            assert release_checkpoint_submission.wait(10), "Checkpoint submission was not released"
        return original_write(node, job_id, payload, **kwargs)

    class ObservedCondition:
        def __enter__(self):
            if is_renewal and phase == "condition" and arming_thread == [threading.get_ident()]:
                renewal_condition_blocked.set()
                assert release_renewal_condition.wait(10), "Renewal condition entry was not released"
            if (threading.current_thread().name == "mwf-scheduler-supervisor"
                    and pause_supervisor.is_set()):
                supervisor_paused.set()
                assert release_supervisor.wait(10), "Deadline scan was not released"
            return condition.__enter__()

        def __exit__(self, *args):
            return condition.__exit__(*args)

        def wait(self, timeout=None):
            if observe_supervisor.is_set():
                supervisor_checked.set()
            return condition.wait(timeout)

        def __getattr__(self, name):
            return getattr(condition, name)

    class ObservedHeap:
        def heapify(self, values):
            if arming_thread == [threading.get_ident()]:
                heap_blocked.set()
                assert release_heap.wait(10), "Framework heap maintenance was not released"
                result = original_heap.heapify(values)
                heap_rebuilt.set()
                return result
            return original_heap.heapify(values)

        def __getattr__(self, name):
            return getattr(original_heap, name)

    for module in (supervisor_attempts_module, supervisor_core_module, supervisor_persistence_module):
        monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(supervisor, "_condition", ObservedCondition())
    monkeypatch.setattr(supervisor, method_name, observed_operation)
    monkeypatch.setattr(supervisor_core_module, "heapq", ObservedHeap())
    monkeypatch.setattr(workflow.storage, "write_job_runtime", write_runtime)

    async def physical_response(request):
        physical_requests.append(request)
        if operation == "network_replay" and len(physical_requests) == 1:
            raise httpx.ReadError("controlled physical-attempt failure", request=request)
        response_started.set()
        response_deadline = time.perf_counter() + 10
        while not release_response.is_set():
            assert time.perf_counter() < response_deadline, "Physical response was not released"
            await asyncio.sleep(0.001)
        return httpx.Response(200, json={"received": True}, request=request)

    if is_network:
        close_shared_http_transport()
        configure_shared_http_transport(transport=httpx.MockTransport(physical_response))

    @workflow.task(
        "A", runner="api", max_threads=1,
        timeout=0.02 if deadline_kind == "total" else 100,
        checkpoint_timeout=0.02 if deadline_kind == "checkpoint" else 100,
    )
    def a(ctx):
        handler_entered.set()
        if operation == "checkpoint":
            assert start_checkpoint.wait(10), "Checkpoint was not allowed to start"
            ctx.checkpoint("after reports", timeout=0.02 if deadline_kind == "checkpoint" else 100)
        elif is_network:
            response = shared_http_transport.post_json(
                "https://example.test/heap-maintenance", timeout=0.5,
                json={"request": True}, wait_name="heap request",
            )
            assert response == {"received": True}
        interval_entered.set()
        assert release_handler.wait(10), "User work was not released"
        return {"started": True}

    @workflow.task("B", runner="api", max_threads=40, timeout=100, checkpoint_timeout=100)
    def b(ctx):
        with entered_lock:
            entered_jobs.add(ctx.job_id)
            if len(entered_jobs) == 40:
                all_other_handlers_entered.set()
        reports_gate.result(timeout=10)
        if ctx.job_id == 1:
            # With forty live attempts, these ordinary reports remain below
            # the normal rebuild threshold. Completing them leaves stale heap
            # entries for the next handler's real maintenance pass.
            for value in range(70):
                ctx.checkpoint(f"other task report {value}", timeout=100)
            reports_finished.set()
        other_handlers_gate.result(timeout=10)
        return {"job": ctx.job_id}

    @workflow.task("Z")
    def z(ctx):
        return None

    workflow.start("A", job_id=1)
    for job_id in range(1, 41):
        workflow.start("B", job_id=job_id)

    def run():
        try:
            workflow.run_concurrently(
                ["A", "B"],
                ready_check=lambda node: node != "A" or operation != "initial" or allow_a.is_set(),
            )
        except BaseException as error:
            errors.append(error)

    active = threading.Thread(target=run, daemon=True)
    workers.append(active)
    active.start()
    try:
        if operation == "checkpoint":
            assert handler_entered.wait(10), "User work did not start"
        elif operation == "network_end":
            assert response_started.wait(10), "The physical request did not start"
        elif is_renewal:
            assert renewal_ready.wait(10), "The target physical-attempt callback did not start"
        assert all_other_handlers_entered.wait(10), "Concurrent tasks did not all start"
        with condition:
            pause_supervisor.set()
            condition.notify_all()
        assert supervisor_paused.wait(10), "Supervisor did not pause before reports"
        reports_gate.set_result(None)
        assert reports_finished.wait(10), "Concurrent checkpoint reports did not finish"
        if operation == "checkpoint":
            # Keep the forty watches alive through the report's initial disarm
            # so only its post-submission rearm takes the rebuild branch.
            start_checkpoint.set()
            assert checkpoint_submission.wait(10), "Checkpoint did not reach runtime submission"
        if operation == "initial":
            allow_a.set()
        other_handlers_gate.set_result(None)
        completed_deadline = time.perf_counter() + 10
        while workflow.storage.job_status_counts("B").get("done") != 40:
            assert time.perf_counter() < completed_deadline, "Concurrent tasks did not finish"
            time.sleep(0.01)
        assert not errors, errors
        assert workflow.storage.job_status_counts("B").get("done") == 40
        for job_id in range(1, 41):
            output = json.loads(workflow.storage.output_file("B", job_id).read_text(encoding="utf-8"))
            owner = workflow.storage.read_job_current_owner("B", job_id)
            assert owner is not None
            assert output == {
                "status": "done", "result_type": "dict", "result_repr": str({"job": job_id}),
                "generation": owner["generation"], "execution_id": owner["execution_id"],
            }
        if operation == "checkpoint":
            release_checkpoint_submission.set()
        elif operation == "network_end":
            release_response.set()
        else:
            release_renewal.set()
            if phase == "condition":
                assert renewal_condition_blocked.wait(10), "Physical renewal did not reach its condition"
                clock[0] += 31.0
                release_renewal_condition.set()
        assert heap_blocked.wait(10), "Interval arming did not rebuild the real heap"
        assert not interval_entered.is_set()
        if phase == "framework":
            clock[0] += 31.0 if is_renewal else 1.0
        release_heap.set()
        assert armed.wait(10), "Interval arming did not finish"
        assert heap_rebuilt.is_set()
        if phase == "caller":
            assert interval_entered.wait(10), "The task did not receive its new interval"
        elif is_renewal:
            assert response_started.wait(10), "The renewed physical request did not start"
            assert len(physical_requests) == expected_physical_attempts
            assert renewal_reasons == (["initial_transport_attempt", "transport_error"]
                                       if operation == "network_replay" else ["initial_transport_attempt"])
        with condition:
            if phase == "caller":
                clock[0] += 1.0
            elif phase == "physical":
                clock[0] += 31.0
            observe_supervisor.set()
            condition.notify_all()
        release_supervisor.set()
        scan_deadline = time.perf_counter() + 10
        while not supervisor_checked.is_set():
            timeouts = [event for event in workflow.storage.read_job_events("A", 1)
                        if event.get("event") == "timeout"]
            if timeouts:
                break
            assert time.perf_counter() < scan_deadline, "Supervisor did not inspect initial deadlines"
            time.sleep(0.01)
        timeouts = [event for event in workflow.storage.read_job_events("A", 1)
                    if event.get("event") == "timeout"]
        expected_kind = deadline_kind if (
            phase in {"caller", "physical"} or (operation != "initial" and deadline_kind == "total")
        ) else None
        assert [event["timeout_kind"] for event in timeouts] == ([] if expected_kind is None else [expected_kind])
        release_response.set()
        release_handler.set()
        active.join(timeout=10)
        assert not active.is_alive(), "The API run did not finish"
        output = json.loads(workflow.storage.output_file("A", 1).read_text(encoding="utf-8"))
        owner = workflow.storage.read_job_current_owner("A", 1)
        assert owner is not None
        if expected_kind is None:
            assert not errors, errors
            assert handler_entered.is_set()
            assert workflow.storage.job_status_counts("A").get("done") == 1
            assert output == {
                "status": "done", "result_type": "dict", "result_repr": "{'started': True}",
                "generation": owner["generation"], "execution_id": owner["execution_id"],
            }
        else:
            assert len(errors) == 1 and isinstance(errors[0], JobFailedError)
            assert workflow.storage.job_status_counts("A").get("failed") == 1
            assert timeouts[0]["timeout_seconds"] == (30.5 if deadline_kind == "external" else 0.02)
            assert timeouts[0]["checkpoint"] == {
                "initial": "task start", "checkpoint": "after reports",
                "network_end": "heap request completed",
                "network_renew": "task start", "network_replay": "task start",
            }[operation]
            if is_renewal:
                assert timeouts[0]["external_wait_attempt"] == expected_physical_attempts
                assert timeouts[0]["external_wait_renewals"] == expected_physical_attempts - 1
                assert timeouts[0]["external_wait_last_renewal_reason"] == (
                    "transport_error" if operation == "network_replay" else None
                )
            assert output["status"] == "failed" and output["error"].startswith("JobTimeoutError(")
    finally:
        for gate in (reports_gate, other_handlers_gate):
            if not gate.done():
                gate.set_result(None)
        release_heap.set()
        release_supervisor.set()
        release_handler.set()
        allow_a.set()
        start_checkpoint.set()
        release_checkpoint_submission.set()
        release_response.set()
        release_renewal.set()
        release_renewal_condition.set()
        for worker in workers:
            worker.join(timeout=10)
        workflow.storage.db_mutation_barrier()
        cleanup_deadline = time.perf_counter() + 10
        while workflow.storage.mutation_writer_diagnostics()["writer_alive"]:
            assert time.perf_counter() < cleanup_deadline, "Mutation writer did not retire"
            time.sleep(0.01)
        workflow.storage.close_thread_connection()
        if is_network:
            close_shared_http_transport()
    assert not any(worker.is_alive() for worker in workers)


@pytest.mark.parametrize("deadline_kind", ["checkpoint", "total"])
def test_initial_runtime_persistence_cannot_expose_deadlines_to_concurrent_compaction(
    tmp_path, monkeypatch, deadline_kind,
):
    clock = [time.monotonic()]
    initial_write_blocked = Event()
    release_initial_write = Event()
    pause_supervisor = Event()
    supervisor_paused = Event()
    release_supervisor = Event()
    heap_rebuilt = Event()
    checkpoints_finished = Event()
    release_other_reports = Event()
    release_other_handler = Event()
    first_handler_entered = Event()
    observe_supervisor = Event()
    supervisor_checked = Event()
    errors = []
    workers = []
    initial_payload = []
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "Z"), ("B", "Z")])
    supervisor = workflow.scheduler_supervisor
    condition = supervisor._condition
    original_write = workflow.storage.write_job_runtime
    original_heap = supervisor_core_module.heapq

    def write_runtime(node, job_id, payload, **kwargs):
        if node == "A" and payload.get("state") == "running":
            initial_payload.append(dict(payload))
            initial_write_blocked.set()
            assert release_initial_write.wait(10), "Initial runtime submission was not released"
        return original_write(node, job_id, payload, **kwargs)

    class ObservedCondition:
        def __enter__(self):
            if (threading.current_thread().name == "mwf-scheduler-supervisor"
                    and pause_supervisor.is_set()):
                supervisor_paused.set()
                assert release_supervisor.wait(10), "Deadline scan was not released"
            return condition.__enter__()

        def __exit__(self, *args):
            return condition.__exit__(*args)

        def wait(self, timeout=None):
            if observe_supervisor.is_set():
                supervisor_checked.set()
            return condition.wait(timeout)

        def __getattr__(self, name):
            return getattr(condition, name)

    class ObservedHeap:
        def heapify(self, values):
            result = original_heap.heapify(values)
            heap_rebuilt.set()
            return result

        def __getattr__(self, name):
            return getattr(original_heap, name)

    for module in (supervisor_attempts_module, supervisor_core_module, supervisor_persistence_module):
        monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(supervisor, "_condition", ObservedCondition())
    monkeypatch.setattr(supervisor_core_module, "heapq", ObservedHeap())
    monkeypatch.setattr(workflow.storage, "write_job_runtime", write_runtime)

    @workflow.task(
        "A", runner="api", max_threads=1,
        timeout=0.02 if deadline_kind == "total" else 100,
        checkpoint_timeout=0.02 if deadline_kind == "checkpoint" else 100,
    )
    def a(ctx):
        first_handler_entered.set()
        return {"started": True}

    @workflow.task("B", runner="api", max_threads=1, timeout=100, checkpoint_timeout=100)
    def b(ctx):
        # Accumulate real obsolete checkpoint revisions while the watchdog is
        # paused, crossing the ordinary compaction threshold without changing it.
        assert release_other_reports.wait(10), "Other reports were not released"
        for value in range(300):
            ctx.checkpoint(f"other task report {value}", timeout=100)
        checkpoints_finished.set()
        assert release_other_handler.wait(10), "Other handler was not released"
        return {"reports": 300}

    @workflow.task("Z")
    def z(ctx):
        return None

    workflow.start("A", job_id=1)
    workflow.start("B", job_id=1)

    def run():
        try:
            workflow.run_concurrently(["A", "B"])
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=run, daemon=True)
    workers.append(worker)
    worker.start()
    try:
        assert initial_write_blocked.wait(10), "Initial runtime was not submitted"
        assert len(initial_payload) == 1
        assert initial_payload[0]["checkpoint_name"] == "task start"
        assert initial_payload[0]["total_timeout_seconds"] == (0.02 if deadline_kind == "total" else 100)
        assert initial_payload[0]["checkpoint_timeout_seconds"] == (0.02 if deadline_kind == "checkpoint" else 100)
        assert initial_payload[0]["total_deadline_at"]
        assert initial_payload[0]["checkpoint_deadline_at"]
        with condition:
            pause_supervisor.set()
            condition.notify_all()
        assert supervisor_paused.wait(10), "Supervisor did not pause before compaction"
        with condition:
            clock[0] += 1.0
        release_other_reports.set()
        assert checkpoints_finished.wait(10), "Concurrent checkpoint reports did not finish"
        assert heap_rebuilt.is_set(), "Concurrent reports did not exercise real heap compaction"
        assert not first_handler_entered.is_set()
        observe_supervisor.set()
        release_supervisor.set()
        scan_deadline = time.perf_counter() + 10
        while not supervisor_checked.is_set():
            timeouts = [event for event in workflow.storage.read_job_events("A", 1)
                        if event.get("event") == "timeout"]
            if timeouts:
                break
            assert time.perf_counter() < scan_deadline, "Supervisor did not inspect the rebuilt heap"
            time.sleep(0.01)
        timeouts = [event for event in workflow.storage.read_job_events("A", 1)
                    if event.get("event") == "timeout"]
        assert not timeouts, timeouts
        assert not first_handler_entered.is_set()
        release_initial_write.set()
        release_other_handler.set()
        for worker in workers:
            worker.join(timeout=10)
            assert not worker.is_alive(), "Concurrent API run did not finish"
        assert not errors, errors
        assert first_handler_entered.is_set()
        for node, expected in (("A", "{'started': True}"), ("B", "{'reports': 300}")):
            assert workflow.storage.job_status_counts(node).get("done") == 1
            output = json.loads(workflow.storage.output_file(node, 1).read_text(encoding="utf-8"))
            owner = workflow.storage.read_job_current_owner(node, 1)
            assert owner is not None
            assert output == {
                "status": "done", "result_type": "dict", "result_repr": expected,
                "generation": owner["generation"], "execution_id": owner["execution_id"],
            }
            assert not [event for event in workflow.storage.read_job_events(node, 1)
                        if event.get("event") == "timeout"]
    finally:
        release_initial_write.set()
        release_supervisor.set()
        release_other_reports.set()
        release_other_handler.set()
        for worker in workers:
            worker.join(timeout=10)
        workflow.storage.db_mutation_barrier()
        cleanup_deadline = time.perf_counter() + 10
        while workflow.storage.mutation_writer_diagnostics()["writer_alive"]:
            assert time.perf_counter() < cleanup_deadline, "Mutation writer did not retire"
            time.sleep(0.01)
        workflow.storage.close_thread_connection()
    assert not any(worker.is_alive() for worker in workers)


@pytest.mark.parametrize("delay, timeout_kind", [
    ("condition", None), ("submission", None),
    ("caller", "checkpoint"), ("total", "total"),
])
def test_api_checkpoint_distinguishes_framework_delay_from_task_time(
    tmp_path, monkeypatch, delay, timeout_kind,
):
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "B")])
    supervisor = workflow.scheduler_supervisor
    condition = supervisor._condition
    clock = [time.monotonic()]
    checkpoint_thread = []
    handler_ready = Event()
    start_checkpoint = Event()
    waiting_for_lock = Event()
    submission_started = Event()
    release_submission = Event()
    checkpoint_returned = Event()
    release_caller = Event()
    observe_supervisor = Event()
    supervisor_checked = Event()
    run_finished = Event()
    errors = []

    class ObservedCondition:
        def __enter__(self):
            if checkpoint_thread == [threading.get_ident()]:
                waiting_for_lock.set()
            return condition.__enter__()

        def __exit__(self, *args):
            return condition.__exit__(*args)

        def wait(self, timeout=None):
            # A return to waiting acknowledges a completed deadline scan. The
            # test observes task results, never the supervisor's watch fields.
            if observe_supervisor.is_set():
                supervisor_checked.set()
            return condition.wait(timeout)

        def __getattr__(self, name):
            return getattr(condition, name)

    original_write = workflow.storage.write_job_runtime

    def write_runtime(node, job_id, payload, **kwargs):
        if payload.get("state") == "running" and payload.get("checkpoint_name") == "controlled":
            submission_started.set()
            assert release_submission.wait(10), "Checkpoint submission was not released"
        return original_write(node, job_id, payload, **kwargs)

    for module in (supervisor_attempts_module, supervisor_core_module, supervisor_persistence_module):
        monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(supervisor, "_condition", ObservedCondition())
    monkeypatch.setattr(workflow.storage, "write_job_runtime", write_runtime)

    @workflow.task("A", runner="api", max_threads=1, timeout=100, checkpoint_timeout=5)
    def a(ctx):
        handler_ready.set()
        assert start_checkpoint.wait(10), "Checkpoint was not allowed to start"
        checkpoint_thread.append(threading.get_ident())
        ctx.checkpoint("controlled", timeout=0.02)
        checkpoint_returned.set()
        if delay == "caller":
            assert release_caller.wait(10), "Caller work was not released"
        return "completed"

    @workflow.task("B")
    def b(ctx):
        return None

    workflow.start("A", job_id=1)

    def run():
        try:
            workflow.run_node("A", ignore_readiness=True)
        except Exception as error:
            errors.append(error)
        finally:
            run_finished.set()

    active = threading.Thread(target=run, daemon=True)
    active.start()
    try:
        assert handler_ready.wait(10), "API handler did not start"
        if delay == "condition":
            with condition:
                start_checkpoint.set()
                assert waiting_for_lock.wait(10), "Checkpoint did not reach the held condition"
                clock[0] += 1.0
        else:
            start_checkpoint.set()
        assert submission_started.wait(10), "Checkpoint runtime was not submitted"
        if delay == "caller":
            release_submission.set()
            assert checkpoint_returned.wait(10), "Checkpoint did not return to the caller"
        with condition:
            if delay in {"submission", "caller"}:
                clock[0] += 1.0
            elif delay == "total":
                clock[0] += 101.0
            observe_supervisor.set()
            condition.notify_all()
        assert supervisor_checked.wait(10), "Supervisor did not inspect the checkpoint deadline"
        while_blocked = [event for event in workflow.storage.read_job_events("A", 1)
                         if event.get("event") == "timeout"]
        assert [event["timeout_kind"] for event in while_blocked] == (
            [] if timeout_kind is None else [timeout_kind]
        )
        release_submission.set()
        release_caller.set()
        assert run_finished.wait(10), "API run did not finish"
        timeouts = [event for event in workflow.storage.read_job_events("A", 1)
                    if event.get("event") == "timeout"]
        if timeout_kind is None:
            assert not errors, (errors, timeouts)
            assert workflow.storage.job_status_counts("A").get("done") == 1
            assert not timeouts
            assert checkpoint_returned.is_set()
        else:
            assert len(errors) == 1 and isinstance(errors[0], JobFailedError)
            assert workflow.storage.job_status_counts("A").get("failed") == 1
            assert [event["timeout_kind"] for event in timeouts] == [timeout_kind]
            assert timeouts[0]["timeout_seconds"] == (100 if delay == "total" else 0.02)
            assert timeouts[0]["checkpoint"] == "controlled"
            assert checkpoint_returned.is_set() == (delay == "caller")
        assert workflow.storage.read_job_runtime("A", 1)["checkpoint_name"] == "controlled"
    finally:
        start_checkpoint.set()
        release_submission.set()
        release_caller.set()
        active.join(timeout=10)
        workflow.storage.db_mutation_barrier()
        cleanup_deadline = time.perf_counter() + 10
        while workflow.storage.mutation_writer_diagnostics()["writer_alive"]:
            assert time.perf_counter() < cleanup_deadline, "Mutation writer did not retire"
            time.sleep(0.01)
        workflow.storage.close_thread_connection()
    assert not active.is_alive()


@pytest.mark.parametrize("phase, handler_fails, timeout_kind", [
    ("completion", False, None), ("completion", True, None),
    ("caller", False, "checkpoint"), ("total", False, "total"),
    ("exact_deadline", False, "checkpoint"), ("later_checkpoint", False, "checkpoint"),
])
def test_api_checkpoint_stops_after_handler_exit_before_framework_completion(
    tmp_path, monkeypatch, phase, handler_fails, timeout_kind,
):
    clock = [time.monotonic()]
    handler_returned = Event()
    release_completion = Event()
    network_returned = Event()
    release_caller = Event()
    task_context = []
    observe_supervisor = Event()
    supervisor_checked = Event()
    run_finished = Event()
    errors = []

    async def response(request):
        return httpx.Response(200, json={"ok": True}, request=request)

    close_shared_http_transport()
    configure_shared_http_transport(transport=httpx.MockTransport(response))
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "B")])
    supervisor = workflow.scheduler_supervisor
    condition = supervisor._condition

    class ObservedCondition:
        def __enter__(self):
            return condition.__enter__()

        def __exit__(self, *args):
            return condition.__exit__(*args)

        def wait(self, timeout=None):
            if observe_supervisor.is_set():
                supervisor_checked.set()
            return condition.wait(timeout)

        def __getattr__(self, name):
            return getattr(condition, name)

    original_flush = JobContext.flush_pending_events

    def flush_after_handler(ctx):
        if ctx.system is workflow and ctx.current_node == "A":
            # The framework invokes this after the mounted handler exits.
            handler_returned.set()
            assert release_completion.wait(10), "Framework completion was not released"
        return original_flush(ctx)

    for module in (supervisor_attempts_module, supervisor_core_module, supervisor_persistence_module):
        monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(supervisor, "_condition", ObservedCondition())
    monkeypatch.setattr(JobContext, "flush_pending_events", flush_after_handler)

    @workflow.task("A", runner="api", max_threads=1, timeout=100, checkpoint_timeout=0.02)
    def a(ctx):
        task_context.append(ctx)
        ctx.checkpoint("before model")
        result = shared_http_transport.post_json(
            "https://example.test/model", json={}, timeout=0.5,
            wait_name="model request",
        )
        if phase in {"caller", "exact_deadline"}:
            network_returned.set()
            assert release_caller.wait(10), "Caller processing was not released"
        if handler_fails:
            raise ValueError("model conversion failed")
        return result

    @workflow.task("B")
    def b(ctx):
        return None

    workflow.start("A", job_id=1)

    def run():
        try:
            workflow.run_node("A", ignore_readiness=True)
        except Exception as error:
            errors.append(error)
        finally:
            run_finished.set()

    active = threading.Thread(target=run, daemon=True)
    active.start()
    try:
        if phase in {"caller", "exact_deadline"}:
            assert network_returned.wait(10), "The network result did not reach the caller"
        else:
            assert handler_returned.wait(10), "The API handler did not return"
        if phase == "later_checkpoint":
            task_context[0].checkpoint("after handler exit", timeout=0.02)
        with condition:
            if phase == "exact_deadline":
                clock[0] += 0.02
                release_caller.set()
                assert handler_returned.wait(10), "Handler did not exit at the controlled deadline"
            else:
                clock[0] += 101.0 if phase == "total" else 1.0
            observe_supervisor.set()
            condition.notify_all()
        assert supervisor_checked.wait(10), "Supervisor did not inspect the completed handler"
        timeouts = [event for event in workflow.storage.read_job_events("A", 1)
                    if event.get("event") == "timeout"]
        assert [event["timeout_kind"] for event in timeouts] == (
            [] if timeout_kind is None else [timeout_kind]
        )
        release_caller.set()
        release_completion.set()
        assert run_finished.wait(10), "The API run did not finish"
        checkpoint_name = (
            "before model" if timeout_kind is None
            else "after handler exit" if phase == "later_checkpoint"
            else "model request completed"
        )
        assert workflow.storage.read_job_runtime("A", 1)["checkpoint_name"] == checkpoint_name
        output = json.loads(workflow.storage.output_file("A", 1).read_text(encoding="utf-8"))
        owner = workflow.storage.read_job_current_owner("A", 1)
        assert owner is not None
        if timeout_kind is not None:
            assert len(errors) == 1 and isinstance(errors[0], JobFailedError)
            assert workflow.storage.job_status_counts("A").get("failed") == 1
            assert timeouts[0]["timeout_seconds"] == (100 if phase == "total" else 0.02)
            assert timeouts[0]["checkpoint"] == checkpoint_name
            assert output["status"] == "failed" and output["error"].startswith("JobTimeoutError(")
        elif handler_fails:
            assert len(errors) == 1 and isinstance(errors[0], JobFailedError)
            assert isinstance(errors[0].__cause__, ValueError)
            assert str(errors[0].__cause__) == "model conversion failed"
            assert workflow.storage.job_status_counts("A").get("failed") == 1
            assert output == {
                "status": "failed", "error": "ValueError('model conversion failed')",
                "generation": owner["generation"], "execution_id": owner["execution_id"],
            }
        else:
            assert not errors, errors
            assert workflow.storage.job_status_counts("A").get("done") == 1
            assert output == {
                "status": "done", "result_type": "dict", "result_repr": "{'ok': True}",
                "generation": owner["generation"], "execution_id": owner["execution_id"],
            }
    finally:
        release_caller.set()
        release_completion.set()
        active.join(timeout=10)
        close_shared_http_transport()
        # Closing the transport submits its final network snapshot. Let the
        # writer finish that snapshot and its own connection/file cleanup.
        workflow.storage.db_mutation_barrier()
        cleanup_deadline = time.perf_counter() + 10
        while workflow.storage.mutation_writer_diagnostics()["writer_alive"]:
            assert time.perf_counter() < cleanup_deadline, "Mutation writer did not retire"
            time.sleep(0.01)
        workflow.storage.close_thread_connection()
    assert not active.is_alive()


@pytest.mark.parametrize("phase, timeout_kind", [
    ("entry", None), ("caller", "checkpoint"), ("total", "total"),
    ("exact_deadline", "checkpoint"), ("later_checkpoint", "checkpoint"),
])
def test_framework_network_entry_distinguishes_admission_from_caller_time(
    tmp_path, monkeypatch, phase, timeout_kind,
):
    clock = [time.monotonic()]
    entering_thread = []
    entry_blocked = Event()
    release_entry = Event()
    caller_blocked = Event()
    release_caller = Event()
    task_context = []
    observe_supervisor = Event()
    supervisor_checked = Event()
    physical_dispatch = Event()
    run_finished = Event()
    errors = []

    async def response(request):
        physical_dispatch.set()
        return httpx.Response(200, json={"ok": True}, request=request)

    close_shared_http_transport()
    configure_shared_http_transport(transport=httpx.MockTransport(response))
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "B")])
    supervisor = workflow.scheduler_supervisor
    condition = supervisor._condition
    original_begin = supervisor.begin_external_wait

    def begin_wait(*args, **kwargs):
        entering_thread.append(threading.get_ident())
        try:
            return original_begin(*args, **kwargs)
        finally:
            entering_thread.clear()

    class ObservedCondition:
        def __enter__(self):
            if entering_thread == [threading.get_ident()]:
                entry_blocked.set()
                assert release_entry.wait(10), "Framework network entry was not released"
            return condition.__enter__()

        def __exit__(self, *args):
            return condition.__exit__(*args)

        def wait(self, timeout=None):
            if observe_supervisor.is_set():
                supervisor_checked.set()
            return condition.wait(timeout)

        def __getattr__(self, name):
            return getattr(condition, name)

    for module in (supervisor_attempts_module, supervisor_core_module, supervisor_persistence_module):
        monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(supervisor, "_condition", ObservedCondition())
    monkeypatch.setattr(supervisor, "begin_external_wait", begin_wait)

    @workflow.task("A", runner="api", max_threads=1, timeout=100, checkpoint_timeout=5)
    def a(ctx):
        task_context.append(ctx)
        ctx.checkpoint("before network", timeout=0.02)
        if phase in {"caller", "exact_deadline"}:
            caller_blocked.set()
            assert release_caller.wait(10), "Caller processing was not released"
        return shared_http_transport.post_json(
            "https://example.test/model", json={}, timeout=0.5, wait_name="model request",
        )

    @workflow.task("B")
    def b(ctx):
        return None

    workflow.start("A", job_id=1)

    def run():
        try:
            workflow.run_node("A", ignore_readiness=True)
        except Exception as error:
            errors.append(error)
        finally:
            run_finished.set()

    active = threading.Thread(target=run, daemon=True)
    active.start()
    try:
        if phase in {"caller", "exact_deadline"}:
            assert caller_blocked.wait(10), "Caller processing did not start"
        else:
            assert entry_blocked.wait(10), "Network entry did not reach the condition"
        if phase == "later_checkpoint":
            task_context[0].checkpoint("during network entry", timeout=0.02)
        with condition:
            if phase == "exact_deadline":
                clock[0] += 0.02
                release_caller.set()
                assert entry_blocked.wait(10), "Network entry did not reach exact expiry"
            else:
                clock[0] += 101.0 if phase == "total" else 1.0
            observe_supervisor.set()
            condition.notify_all()
        assert supervisor_checked.wait(10), "Supervisor did not inspect network entry"
        timeouts = [event for event in workflow.storage.read_job_events("A", 1)
                    if event.get("event") == "timeout"]
        assert [event["timeout_kind"] for event in timeouts] == (
            [] if timeout_kind is None else [timeout_kind]
        )
        assert not physical_dispatch.is_set()
        release_caller.set()
        release_entry.set()
        assert run_finished.wait(10), "The API run did not finish"
        output = json.loads(workflow.storage.output_file("A", 1).read_text(encoding="utf-8"))
        owner = workflow.storage.read_job_current_owner("A", 1)
        assert owner is not None
        if timeout_kind is None:
            assert not errors, errors
            assert physical_dispatch.is_set()
            assert workflow.storage.job_status_counts("A").get("done") == 1
            assert output == {
                "status": "done", "result_type": "dict", "result_repr": "{'ok': True}",
                "generation": owner["generation"], "execution_id": owner["execution_id"],
            }
        else:
            assert len(errors) == 1 and isinstance(errors[0], JobFailedError)
            assert not physical_dispatch.is_set()
            assert workflow.storage.job_status_counts("A").get("failed") == 1
            assert timeouts[0]["timeout_seconds"] == (100 if phase == "total" else 0.02)
            assert timeouts[0]["checkpoint"] == (
                "during network entry" if phase == "later_checkpoint" else "before network"
            )
            assert output["status"] == "failed" and output["error"].startswith("JobTimeoutError(")
    finally:
        release_caller.set()
        release_entry.set()
        active.join(timeout=10)
        close_shared_http_transport()
        workflow.storage.db_mutation_barrier()
        cleanup_deadline = time.perf_counter() + 10
        while workflow.storage.mutation_writer_diagnostics()["writer_alive"]:
            assert time.perf_counter() < cleanup_deadline, "Mutation writer did not retire"
            time.sleep(0.01)
        workflow.storage.close_thread_connection()
    assert not active.is_alive()


def test_external_wait_starts_when_physical_dispatch_starts(monkeypatch):
    calls = []

    class Supervisor:
        def begin_external_wait(
            self,
            watch,
            *,
            name,
            timeout,
            defer_lease_start=False,
        ):
            calls.append(
                ("begin", watch, name, timeout, defer_lease_start)
            )

        def renew_external_wait(self, watch, *, reason):
            calls.append(("renew", watch, reason))

        def end_external_wait(self, watch):
            calls.append(("end", watch))

    class Storage:
        project_dir = "probe"
        publish_network_manager_snapshot = None

    class Workflow:
        storage = Storage()
        scheduler_supervisor = Supervisor()

    def submit_request(method, url, **kwargs):
        assert calls == [
            ("begin", watch, "model request", 0.5, True),
        ]
        kwargs["attempt_callback"](1, None)
        future = Future()
        future.set_result(httpx.Response(200, json={"ok": True}))
        return future

    monkeypatch.setattr(
        "micro_workflow_manager.network.transport.network_manager.submit_request",
        submit_request,
    )
    watch = object()
    ctx = type("Context", (), {"current_node": "A", "job_id": 1})()
    with network_attempt_context(Workflow(), ctx, watch):
        response = SharedHTTPTransport().request(
            "POST",
            "https://example.test/model",
            timeout=(0.2, 0.5),
            wait_name="model request",
            json={},
        )

    assert response.status_code == 200
    assert calls == [
        ("begin", watch, "model request", 0.5, True),
        ("renew", watch, "initial_transport_attempt"),
        ("end", watch),
    ]


def test_external_wait_ends_when_network_submission_fails(monkeypatch):
    calls = []

    class Supervisor:
        def begin_external_wait(self, watch, **kwargs):
            calls.append(("begin", watch, kwargs["defer_lease_start"]))

        def end_external_wait(self, watch):
            calls.append(("end", watch))

    class Storage:
        project_dir = "probe"
        publish_network_manager_snapshot = None

    class Workflow:
        storage = Storage()
        scheduler_supervisor = Supervisor()

    monkeypatch.setattr(
        "micro_workflow_manager.network.transport.network_manager.submit_request",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("submit failed")),
    )
    watch = object()
    ctx = type("Context", (), {"current_node": "A", "job_id": 1})()
    with network_attempt_context(Workflow(), ctx, watch):
        with pytest.raises(RuntimeError, match="submit failed"):
            SharedHTTPTransport().request(
                "POST",
                "https://example.test/model",
                timeout=0.5,
                json={},
            )

    assert calls == [
        ("begin", watch, True),
        ("end", watch),
    ]


def test_async_external_wait_uses_physical_dispatch_lifecycle(monkeypatch):
    calls = []

    class Supervisor:
        def begin_external_wait(self, watch, **kwargs):
            calls.append(("begin", watch, kwargs["defer_lease_start"]))

        def renew_external_wait(self, watch, *, reason):
            calls.append(("renew", watch, reason))

        def end_external_wait(self, watch):
            calls.append(("end", watch))

    class Storage:
        project_dir = "probe"
        publish_network_manager_snapshot = None

    class Workflow:
        storage = Storage()
        scheduler_supervisor = Supervisor()

    def submit_request(method, url, **kwargs):
        kwargs["attempt_callback"](1, None)
        future = Future()
        future.set_result(httpx.Response(200, json={"ok": True}))
        return future

    monkeypatch.setattr(
        "micro_workflow_manager.network.transport.network_manager.submit_request",
        submit_request,
    )
    watch = object()
    ctx = type("Context", (), {"current_node": "A", "job_id": 1})()

    async def perform_request():
        with network_attempt_context(Workflow(), ctx, watch):
            return await SharedHTTPTransport().async_request(
                "POST",
                "https://example.test/model",
                timeout=0.5,
                json={},
            )

    response = asyncio.run(perform_request())
    assert response.status_code == 200
    assert calls == [
        ("begin", watch, True),
        ("renew", watch, "initial_transport_attempt"),
        ("end", watch),
    ]


def test_network_wait_does_not_suspend_total_task_timeout(tmp_path):
    request_started = Event()
    release_response = Event()
    response_returned = Event()
    run_finished = Event()
    errors = []

    async def handler(request: httpx.Request) -> httpx.Response:
        request_started.set()
        while not release_response.is_set():
            await asyncio.sleep(0.01)
        response_returned.set()
        return httpx.Response(200, json={"ok": True}, request=request)

    close_shared_http_transport()
    configure_shared_http_transport(transport=httpx.MockTransport(handler))
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "B")])

    @workflow.task("A", runner="api", max_threads=1, timeout=1.0, checkpoint_timeout=0.5)
    def a(ctx):
        ctx.checkpoint("before network", timeout=0.5)
        return shared_http_transport.post_json(
            "https://example.test/slow",
            timeout=30.0,
            json={},
            wait_name="slow model request",
        )

    @workflow.task("B")
    def b(ctx):
        return None

    workflow.start("A", job_id=1)

    def run():
        try:
            workflow.run_node("A", ignore_readiness=True)
        except Exception as error:
            errors.append(error)
        finally:
            run_finished.set()

    active = threading.Thread(target=run, daemon=True)
    active.start()
    try:
        assert request_started.wait(10), "The mock network request never started"
        assert run_finished.wait(10), "Total timeout did not finish the blocked request"
        assert len(errors) == 1
        assert isinstance(errors[0], JobFailedError)
        assert not response_returned.is_set()
        events = workflow.storage.read_job_events("A", 1)
        timeouts = [event for event in events if event.get("event") == "timeout"]
        assert [event["timeout_kind"] for event in timeouts] == ["total"]
        assert timeouts[0]["timeout_seconds"] == 1.0
        assert workflow.storage.node_job_summary("A")["counts"].get("failed") == 1
    finally:
        release_response.set()
        active.join(timeout=10)
        close_shared_http_transport()
    assert not active.is_alive()


def test_many_framework_network_waits_do_not_cascade_checkpoint_cancellations(tmp_path, monkeypatch):
    clock = [time.monotonic()]
    requests_started = set()
    requests_lock = threading.Lock()
    all_requests_started = Event()
    release_responses = Event()
    observe_supervisor = Event()
    supervisor_checked = Event()
    run_finished = Event()
    errors = []

    async def handler(request: httpx.Request) -> httpx.Response:
        job_id = json.loads(request.content)["job"]
        with requests_lock:
            assert job_id not in requests_started
            requests_started.add(job_id)
            if len(requests_started) == 100:
                all_requests_started.set()
        while not release_responses.is_set():
            await asyncio.sleep(0.01)
        return httpx.Response(200, json={"ok": True, "job": job_id}, request=request)

    close_shared_http_transport()
    configure_shared_http_transport(transport=httpx.MockTransport(handler))
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "B")])
    supervisor = workflow.scheduler_supervisor
    condition = supervisor._condition

    class ObservedCondition:
        def __enter__(self):
            return condition.__enter__()

        def __exit__(self, *args):
            return condition.__exit__(*args)

        def wait(self, timeout=None):
            if observe_supervisor.is_set():
                supervisor_checked.set()
            return condition.wait(timeout)

        def __getattr__(self, name):
            return getattr(condition, name)

    for module in (supervisor_attempts_module, supervisor_core_module, supervisor_persistence_module):
        monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(supervisor, "_condition", ObservedCondition())

    @workflow.task("A", runner="api", max_threads=100, timeout=3.0, checkpoint_timeout=0.02)
    def a(ctx):
        ctx.checkpoint("model request started", timeout=0.02)
        return shared_http_transport.post_json(
            "https://example.test/model",
            timeout=0.5,
            json={"job": ctx.job_id},
            wait_name="model request",
        )

    @workflow.task("B")
    def b(ctx):
        return None

    for job_id in range(1, 101):
        workflow.start("A", job_id=job_id)

    def run():
        try:
            workflow.run_node("A", ignore_readiness=True)
        except Exception as error:
            errors.append(error)
        finally:
            run_finished.set()

    active = threading.Thread(target=run, daemon=True)
    active.start()
    try:
        assert all_requests_started.wait(15), "All 100 physical requests did not start"
        with condition:
            # Advance only while all handlers own a framework-managed wait.
            # Caller scheduling is covered by the separate deadline cases.
            clock[0] += 0.08
            observe_supervisor.set()
            condition.notify_all()
        scan_deadline = time.perf_counter() + 10
        while not supervisor_checked.is_set():
            # A broken supervisor can cancel every watch and exit without
            # waiting again. Report its durable timeout instead of a lost ack.
            for job_id in range(1, 101):
                events = workflow.storage.read_job_events("A", job_id)
                timeouts = [event for event in events if event.get("event") == "timeout"]
                assert not timeouts, (job_id, timeouts)
            assert time.perf_counter() < scan_deadline, "Supervisor did not inspect all network waits"
            supervisor_checked.wait(0.01)
        for job_id in range(1, 101):
            events = workflow.storage.read_job_events("A", job_id)
            assert not [event for event in events if event.get("event") == "timeout"], (job_id, events)
        assert not run_finished.is_set()
        release_responses.set()
        assert run_finished.wait(15), "The 100-request API run did not finish"
        assert not errors, errors
        assert requests_started == set(range(1, 101))
        counts = workflow.storage.node_job_summary("A")["counts"]
        assert counts.get("done") == 100 and sum(counts.values()) == 100
        for job_id in range(1, 101):
            output = json.loads(workflow.storage.output_file("A", job_id).read_text(encoding="utf-8"))
            owner = workflow.storage.read_job_current_owner("A", job_id)
            assert owner is not None
            assert output == {
                "status": "done", "result_type": "dict",
                "result_repr": "{'ok': True, 'job': " + str(job_id) + "}",
                "generation": owner["generation"], "execution_id": owner["execution_id"],
            }
    finally:
        release_responses.set()
        active.join(timeout=10)
        close_shared_http_transport()
        workflow.storage.db_mutation_barrier()
        cleanup_deadline = time.perf_counter() + 10
        while workflow.storage.mutation_writer_diagnostics()["writer_alive"]:
            assert time.perf_counter() < cleanup_deadline, "Mutation writer did not retire"
            time.sleep(0.01)
        workflow.storage.close_thread_connection()
    assert not active.is_alive()
