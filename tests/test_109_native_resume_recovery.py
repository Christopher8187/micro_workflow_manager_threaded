"""Recover exact terminal output while preserving execution ownership and lock order."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from queue import Queue
from threading import Event, Thread, current_thread

import pytest

from micro_workflow_manager import MicroWorkflow, cli
from micro_workflow_manager.errors import JobRestartedError
from micro_workflow_manager.cli.run_commands import resume_node
from micro_workflow_manager.storage.input_publication_files import InputFileChange
from micro_workflow_manager.models import CANCELLED, DONE, FAILED, Job, now
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_046_resume_restart_wait import _start_native_session
from tests.test_086_native_owned_restart import _close
from tests.test_090_component_session_settlement import _rows
from tests.test_093_native_cli_readiness import _node_files


def _abandoned_job(tmp_path, request):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "A")])
    storage = workflow.storage
    request.addfinalizer(lambda: _close(storage))
    calls = []

    @workflow.task("A")
    def run_a(ctx):
        calls.append(ctx.job_id)
        return "replacement"

    session_id = "abandoned-output-owner"
    _start_native_session(workflow, session_id, ("A",))
    storage.create_job(Job(node_name="A", job_id=1, params={}))
    started_at = "2026-09-07T10:11:12.345"
    generation, execution_id = storage.claim_job_execution(
        "A", 1, started_at=started_at, session_id=session_id, component=("A",)
    )
    storage.finish_execution_session(session_id, outcome=FAILED, finished_at=now())
    storage.release_execution_components(session_id)
    storage.set_node_status("A", FAILED)
    return workflow, storage, calls, started_at, generation, execution_id


def test_resume_recovers_output_published_after_observation_before_fence(
    tmp_path, request, monkeypatch
):
    workflow, storage, calls, _, generation, execution_id = _abandoned_job(
        tmp_path, request
    )
    fence_held = Event()
    publish = Event()
    fence_requested = Event()
    worker_error = Queue()
    result = Queue()
    original_fence = FileStorage.filesystem_interprocess_lock

    def observe_resume_fence(self, namespace, name):
        if namespace == "execution-fences" and current_thread().name == "resume-caller":
            fence_requested.set()
        return original_fence(self, namespace, name)

    monkeypatch.setattr(
        FileStorage, "filesystem_interprocess_lock", observe_resume_fence
    )

    def publish_terminal_output():
        try:
            with storage.guard_job_execution("A", 1, generation, execution_id):
                fence_held.set()
                assert publish.wait(10)
                storage.write_output(
                    "A",
                    1,
                    {
                        "status": DONE,
                        "generation": generation,
                        "execution_id": execution_id,
                        "result_repr": "'late durable result'",
                    },
                )
        except BaseException as error:
            worker_error.put(error)

    def resume():
        try:
            result.put((True, resume_node(tmp_path, workflow, "A")))
        except BaseException as error:
            result.put((False, error))

    publisher = Thread(target=publish_terminal_output, name="old-terminal-writer")
    publisher.start()
    assert fence_held.wait(10)
    caller = Thread(target=resume, name="resume-caller")
    caller.start()

    # The old implementation scans output before requesting this fence.  The
    # old owner then publishes while resume is blocked on the same exact job.
    assert fence_requested.wait(10)
    publish.set()
    publisher.join(10)
    caller.join(20)
    assert not publisher.is_alive() and not caller.is_alive()
    assert worker_error.empty()
    succeeded, value = result.get_nowait()
    if not succeeded:
        raise value
    assert value == 0
    assert calls == []
    assert storage.get_job_status("A", 1) == DONE
    assert storage.read_job_control("A", 1)["generation"] == generation
    assert storage.read_json(storage.output_file("A", 1))["result_repr"] == (
        "'late durable result'"
    )


def test_resume_takes_execution_fence_before_selected_receiver_locks(
    tmp_path, request, monkeypatch
):
    workflow, storage, calls, _, generation, execution_id = _abandoned_job(
        tmp_path, request
    )
    worker_holds_fence = Event()
    allow_late_publication = Event()
    order_seen = Event()
    first_resume_lock = []
    worker_error = Queue()
    result = Queue()
    original_fence = FileStorage.filesystem_interprocess_lock
    original_node_lock = FileStorage.interprocess_lock

    def observe_fence(self, namespace, name):
        if namespace == "execution-fences" and current_thread().name == "resume-caller":
            if not first_resume_lock:
                first_resume_lock.append("execution")
                order_seen.set()
        return original_fence(self, namespace, name)

    def refuse_inverted_node_lock(self, name):
        if name.startswith("node-") and current_thread().name == "resume-caller":
            if not first_resume_lock:
                first_resume_lock.append("node")
                order_seen.set()
                # Fail promptly on the old order.  Do not acquire the node lock
                # and leave the late owner blocked behind the test itself.
                raise AssertionError("resume requested a node lock before its execution fence")
        return original_node_lock(self, name)

    monkeypatch.setattr(FileStorage, "filesystem_interprocess_lock", observe_fence)
    monkeypatch.setattr(FileStorage, "interprocess_lock", refuse_inverted_node_lock)

    def late_publication():
        try:
            with storage.guard_job_execution("A", 1, generation, execution_id):
                worker_holds_fence.set()
                assert allow_late_publication.wait(10)
                # The terminal session no longer has publication authority, but
                # this path takes the receiver lock before it discovers that.
                with pytest.raises(RuntimeError):
                    storage.publish_managed_inputs(
                        "A",
                        1,
                        generation,
                        execution_id,
                        "A",
                        (InputFileChange("late.txt", "text", "late", False),),
                    )
        except BaseException as error:
            worker_error.put(error)

    def resume():
        try:
            result.put((True, resume_node(tmp_path, workflow, "A")))
        except BaseException as error:
            result.put((False, error))

    publisher = Thread(target=late_publication, name="old-terminal-writer")
    publisher.start()
    assert worker_holds_fence.wait(10)
    caller = Thread(target=resume, name="resume-caller")
    caller.start()

    assert order_seen.wait(10)
    # This release happens for either observed order.  The old implementation
    # therefore fails the ordering assertion without leaving two locks waiting.
    allow_late_publication.set()
    publisher.join(10)
    caller.join(20)
    assert not publisher.is_alive() and not caller.is_alive()
    assert worker_error.empty()
    assert first_resume_lock == ["execution"]
    succeeded, value = result.get_nowait()
    if not succeeded:
        raise value
    assert value == 0
    assert calls == [1]
    assert not (storage.node_input_dir("A") / "late.txt").exists()


def test_resume_recovery_preserves_terminal_timing_metadata(tmp_path, request):
    workflow, storage, calls, started_at, generation, execution_id = _abandoned_job(
        tmp_path, request
    )
    output = storage.output_file("A", 1)
    storage.write_output(
        "A",
        1,
        {
            "status": DONE,
            "generation": generation,
            "execution_id": execution_id,
            "result_repr": "'already finished'",
        },
    )
    expected_finished_at = datetime.fromtimestamp(output.stat().st_mtime).isoformat(
        timespec="milliseconds"
    )
    before = storage.latest_job_event_id()

    assert resume_node(tmp_path, workflow, "A") == 0

    assert calls == []
    recovered = [
        event
        for event in storage.read_job_events_since(before, node_names=("A",))
        if event["event"] == DONE and event.get("recovered_from_output") is True
    ]
    assert len(recovered) == 1
    assert recovered[0]["started_at"] == started_at
    assert recovered[0]["finished_at"] == expected_finished_at
    assert recovered[0]["generation"] == generation
    assert recovered[0]["execution_id"] == execution_id


@pytest.mark.parametrize("terminal_status", [FAILED, CANCELLED])
def test_resume_requeue_records_recovered_terminal_previous_status(
    tmp_path, request, terminal_status
):
    workflow, storage, calls, _, generation, execution_id = _abandoned_job(
        tmp_path, request
    )
    storage.write_output(
        "A",
        1,
        {
            "status": terminal_status,
            "generation": generation,
            "execution_id": execution_id,
            "error": "durable terminal result",
        },
    )
    before = storage.latest_job_event_id()

    assert resume_node(tmp_path, workflow, "A") == 0

    assert calls == [1]
    events = storage.read_job_events_since(before, node_names=("A",))
    recovered_position = next(
        position
        for position, event in enumerate(events)
        if event["event"] == terminal_status
        and event.get("recovered_from_output") is True
    )
    queued = events[recovered_position + 1]
    assert queued["event"] == "queued"
    assert queued["previous_status"] == terminal_status
    assert queued["status"] == "queued"
    restart = events[recovered_position + 2]
    assert restart["event"] == "restart_requested"
    assert restart["previous_generation"] == generation
    assert restart["generation"] == generation + 1


def test_resume_refuses_terminal_owner_from_prior_component_shape(tmp_path, request):
    old = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    old.graph([("A", "A")])
    old_storage = old.storage
    _start_native_session(old, "old-shape-owner", ("A",))
    old_storage.create_job(Job(node_name="A", job_id=1, params={}))
    generation, execution_id = old_storage.claim_job_execution(
        "A",
        1,
        started_at=now(),
        session_id="old-shape-owner",
        component=("A",),
    )
    old_owner = old_storage.read_job_current_owner("A", 1)
    old_storage.finish_execution_session(
        "old-shape-owner", outcome=FAILED, finished_at=now()
    )
    old_storage.release_execution_components("old-shape-owner")
    old_storage.set_node_status("A", FAILED)
    _close(old_storage)

    current = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    current.graph([("A", "B"), ("B", "A")])
    storage = current.storage
    request.addfinalizer(lambda: _close(storage))

    @current.task("A")
    @current.task("B")
    def must_not_run(ctx):
        raise AssertionError("resume admitted work from a prior component shape")

    # Registration records the new definition but preserves active membership
    # while the prior component still owns reusable work.
    storage.register_component_topology(current.topology.snapshot())
    before_rows = _rows(storage)
    before_files = _node_files(tmp_path)
    before_sessions = storage.list_execution_sessions()

    with pytest.raises(RuntimeError) as caught:
        resume_node(tmp_path, current, "A")

    assert str(caught.value) == "Resume requires initialized component ('A', 'B')"
    assert _rows(storage) == before_rows
    assert _node_files(tmp_path) == before_files
    assert storage.list_execution_sessions() == before_sessions
    assert storage.get_component_reservation(("A", "B")) is None
    assert storage.get_job_status("A", 1) == "running"
    assert storage.read_job_control("A", 1)["generation"] == generation
    assert storage.read_job_current_owner("A", 1) == old_owner
    assert storage.read_job_current_owner("A", 1)["execution_id"] == execution_id


@pytest.mark.parametrize("first_status", [DONE, FAILED])
def test_real_job_output_records_execution_and_is_recovered_after_terminal_write_failure(
    tmp_path, request, monkeypatch, first_status
):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "A")])
    storage = workflow.storage
    request.addfinalizer(lambda: _close(storage))
    calls = []

    @workflow.task("A")
    def run_a(ctx):
        calls.append((ctx.job_id, ctx.execution_generation))
        if first_status == FAILED and ctx.execution_generation == 0:
            raise ValueError("expected first-attempt handler failure")
        return "durable handler result"

    workflow.start("A", job_id=1)
    original_finalize = storage.finalize_job_execution
    original_reconcile = storage.reconcile_terminal_outputs
    terminal_error = OSError("terminal database publication interrupted")
    terminal_attempts = []

    def interrupt_terminal(
        node, job_id, expected_generation, expected_execution_id, status, *args, **kwargs
    ):
        if node == "A" and job_id == 1 and expected_generation == 0:
            terminal_attempts.append(status)
            raise terminal_error
        return original_finalize(
            node, job_id, expected_generation, expected_execution_id, status, *args, **kwargs
        )

    def interrupt_reconciliation(*args, **kwargs):
        raise terminal_error

    monkeypatch.setattr(storage, "reconcile_terminal_outputs", interrupt_reconciliation)
    monkeypatch.setattr(storage, "finalize_job_execution", interrupt_terminal)
    with pytest.raises(OSError) as caught:
        workflow.run_job("A", 1)
    assert caught.value is terminal_error
    assert terminal_attempts
    monkeypatch.setattr(storage, "reconcile_terminal_outputs", original_reconcile)
    monkeypatch.setattr(storage, "finalize_job_execution", original_finalize)

    observation = storage.read_job_owner_observation("A", 1)
    owner = observation["owner"]
    assert observation["status"] == "running"
    assert observation["active_execution_id"] == owner["execution_id"]
    assert owner["generation"] == 0
    assert observation["session"]["status"] == "running"
    assert storage.get_component_reservation(("A",))["session_id"] == owner["session_id"]

    # JobLifecycle wrote this payload before its terminal SQLite mutation.
    output = storage.read_json(storage.output_file("A", 1), default=None)
    assert output["status"] == first_status
    assert output["generation"] == owner["generation"]
    assert output["execution_id"] == owner["execution_id"]

    # The active-job exit guard retained the owner. Convert it to the terminal
    # abandoned-owner state that public resume accepts without changing output.
    component = storage.get_component_state(("A",))
    assert storage.fail_running_component(
        owner["session_id"], ("A",), expected_shape=component["shape_json"],
        expected_alignment_generation=owner["alignment_generation"],
    )
    assert storage.submit_db_mutation(lambda connection: connection.execute(
        "DELETE FROM pending_component_executions WHERE session_id=?",
        (owner["session_id"],),
    ).rowcount) == 1
    storage.finish_execution_session(
        owner["session_id"], outcome=FAILED, finished_at=now()
    )
    storage.release_execution_components(owner["session_id"])
    storage.set_node_status("A", FAILED)
    monkeypatch.setattr(storage, "finalize_job_execution", original_finalize)
    assert storage.read_json(storage.output_file("A", 1)) == output
    assert storage.read_job_current_owner("A", 1) == owner
    before = storage.latest_job_event_id()

    assert resume_node(tmp_path, workflow, "A") == 0

    expected_calls = [(1, 0)] if first_status == DONE else [(1, 0), (1, 1)]
    assert calls == expected_calls
    recovered = [
        event
        for event in storage.read_job_events_since(before, node_names=("A",))
        if event["event"] == first_status
        and event.get("recovered_from_output") is True
        and event.get("execution_id") == owner["execution_id"]
    ]
    assert len(recovered) == 1
    assert storage.get_job_status("A", 1) == DONE
    expected_generation = 0 if first_status == DONE else 1
    assert storage.read_job_control("A", 1)["generation"] == expected_generation



@pytest.mark.parametrize("terminal_status", [FAILED, CANCELLED])
def test_resume_refuses_terminal_owner_from_prior_component_membership(
    tmp_path, request, terminal_status,
):
    old, old_storage, _, _, generation, execution_id = _abandoned_job(
        tmp_path, SimpleNamespace(addfinalizer=lambda callback: None)
    )
    old_storage.finalize_job_execution(
        "A", 1, generation, execution_id, terminal_status
    )
    old_owner = old_storage.read_job_current_owner("A", 1)
    _close(old_storage)

    current = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    current.graph([("A", "B"), ("B", "A")])
    storage = current.storage
    request.addfinalizer(lambda: _close(storage))

    @current.task("A")
    @current.task("B")
    def must_not_run(ctx):
        raise AssertionError("resume admitted a terminal owner from a prior component")

    storage.register_component_topology(current.topology.snapshot())
    before_rows = _rows(storage)
    before_files = _node_files(tmp_path)
    before_sessions = storage.list_execution_sessions()

    with pytest.raises(RuntimeError) as caught:
        resume_node(tmp_path, current, "A")
    assert str(caught.value) == "Resume requires initialized component ('A', 'B')"

    assert _rows(storage) == before_rows
    assert _node_files(tmp_path) == before_files
    assert storage.list_execution_sessions() == before_sessions
    assert storage.get_component_reservation(("A", "B")) is None
    assert storage.get_job_status("A", 1) == terminal_status
    assert storage.read_job_control("A", 1)["generation"] == generation
    assert storage.read_job_current_owner("A", 1) == old_owner


def test_resumefrom_repairs_external_failure_retained_across_new_alignment(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'C'), ('Q', 'C')]",
        files={
            "A": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A")
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    ctx.node("C").add(origin="A")
                    return "A"
            """,
            "Q": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("Q")
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    ctx.node("C").add(origin="Q")
                    return "Q"
            """,
            "C": """
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("C")
                @router.task
                def run(ctx, origin):
                    root = Path(ctx.system.storage.project_dir)
                    path = root / f"c-{origin}-calls.txt"
                    count = int(path.read_text() if path.exists() else "0") + 1
                    path.write_text(str(count), encoding="utf-8")
                    if origin == "Q" and not (root / "allow-q.flag").exists():
                        raise RuntimeError("expected retained Q failure")
                    return origin
            """,
        },
    )
    capsys.readouterr()

    # A publishes C/1 first. Q then publishes C/2 and selects C, where A's
    # external job succeeds before Q's job fails.
    assert cli.main(["run", "A"]) == 0
    capsys.readouterr()
    assert cli.main(["runfrom", "Q"]) == 1
    capsys.readouterr()

    storage = FileStorage(tmp_path)
    try:
        q_job = next(
            job_id for job_id in storage.list_job_ids("C")
            if storage.load_job("C", job_id).params["origin"] == "Q"
        )
        assert storage.get_job_status("C", q_job) == FAILED
        old_owner = storage.read_job_current_owner("C", q_job)
        old_alignment = old_owner["alignment_generation"]

        # Fresh A/C preparation deletes A's prior publication, retains the
        # external Q failure, and advances C's component alignment.
        assert cli.main(["runfrom", "A"]) == 1
        capsys.readouterr()
        prepared = storage.get_component_state(("C",))
        assert prepared["lifecycle"] == FAILED
        assert prepared["misaligned"] is False
        assert prepared["alignment_generation"] == old_alignment + 1
        assert storage.get_job_status("C", q_job) == FAILED
        assert storage.read_job_current_owner("C", q_job) == old_owner

        assert (tmp_path / "c-A-calls.txt").read_text(encoding="utf-8") == "2"
        assert (tmp_path / "c-Q-calls.txt").read_text(encoding="utf-8") == "1"
        (tmp_path / "allow-q.flag").write_text("yes", encoding="utf-8")
        assert cli.main(["resumefrom", "A"]) == 0
        capsys.readouterr()

        assert storage.get_job_status("C", q_job) == "done"
        repaired = storage.get_component_state(("C",))
        assert repaired["lifecycle"] == "done"
        assert repaired["misaligned"] is False
        assert repaired["alignment_generation"] == prepared["alignment_generation"]
        assert (tmp_path / "c-A-calls.txt").read_text(encoding="utf-8") == "2"
        assert (tmp_path / "c-Q-calls.txt").read_text(encoding="utf-8") == "2"
        new_owner = storage.read_job_current_owner("C", q_job)
        assert new_owner["generation"] == old_owner["generation"] + 1
        assert new_owner["execution_id"] != old_owner["execution_id"]
        assert new_owner["component"] == old_owner["component"] == ("C",)
        assert new_owner["alignment_generation"] == prepared["alignment_generation"]
        assert storage.get_component_reservation(("A",)) is None
        assert storage.get_component_reservation(("C",)) is None
    finally:
        _close(storage)


@pytest.mark.parametrize("column", ["active_pid", "active_thread_id", "active_started_at"])
def test_resume_refuses_abandoned_running_job_with_missing_activity_metadata(
    tmp_path, request, column,
):
    workflow, storage, calls, _, generation, execution_id = _abandoned_job(tmp_path, request)
    before_control = storage.read_job_control("A", 1)
    assert storage.get_job_status("A", 1) == "running"
    assert before_control["active_execution_id"] == execution_id
    assert before_control[column] is not None
    storage.submit_db_mutation(lambda connection: connection.execute(
        f"UPDATE jobs SET {column}=NULL WHERE node_name='A' AND job_id=1",
    ))
    damaged = storage.read_job_control("A", 1)
    assert damaged[column] is None
    assert damaged["generation"] == generation
    assert damaged["active_execution_id"] == execution_id
    before_rows, before_files = _rows(storage), _node_files(tmp_path)
    before_sessions = storage.list_execution_sessions()

    with pytest.raises(RuntimeError) as caught:
        resume_node(tmp_path, workflow, "A")

    assert "active" in str(caught.value).lower() and "A/1" in str(caught.value)
    assert calls == []
    assert _rows(storage) == before_rows
    assert _node_files(tmp_path) == before_files
    assert storage.list_execution_sessions() == before_sessions
    assert storage.get_component_reservation(("A",)) is None


def test_manual_restart_requeues_exact_native_abandoned_attempt_and_fences_old_owner(
    tmp_path, request,
):
    _, storage, _, _, generation, execution_id = _abandoned_job(tmp_path, request)
    owner = storage.get_job_execution_owner(execution_id)
    assert owner == storage.read_job_current_owner('A', 1)
    assert storage.get_job_status('A', 1) == 'running'
    assert storage.read_job_control('A', 1)['active_execution_id'] == execution_id
    assert storage.list_execution_sessions()[0]['status'] == 'terminal'
    assert storage.get_component_reservation(('A',)) is None
    output = storage.output_file('A', 1)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(b'abandoned attempt output')

    restarted = storage.request_job_restart('A', 1, reason='recover native abandoned attempt')

    assert restarted['previous_generation'] == generation
    assert restarted['generation'] == generation + 1
    assert storage.get_job_status('A', 1) == 'queued'
    control = storage.read_job_control('A', 1)
    assert control['generation'] == generation + 1
    assert control['active_execution_id'] is None
    assert storage.read_job_current_owner('A', 1) == owner
    assert storage.get_job_execution_owner(execution_id) == owner
    assert not output.exists()
    with pytest.raises(JobRestartedError):
        storage.run_guarded_job_side_effect(
            'A', 1, generation, execution_id,
            lambda: pytest.fail('Abandoned execution retained its publication fence'),
        )
    restart = [
        event for event in storage.read_job_events('A', 1)
        if event['event'] == 'restart_requested'
    ][-1]
    assert restart['previous_generation'] == generation
    assert restart['generation'] == generation + 1
    assert restart['reason'] == 'recover native abandoned attempt'



def test_manual_restart_refuses_damaged_native_abandoned_owner_without_mutation(
    tmp_path, request,
):
    _, storage, _, _, generation, execution_id = _abandoned_job(tmp_path, request)
    output = storage.output_file('A', 1)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(b'preserve damaged owner output')
    connection = storage.db_connection()
    connection.execute('PRAGMA foreign_keys=OFF')
    try:
        assert connection.execute(
            'DELETE FROM job_execution_owners WHERE execution_id=?', (execution_id,),
        ).rowcount == 1
    finally:
        connection.execute('PRAGMA foreign_keys=ON')
    storage.db_mutation_barrier()
    before_database = list(connection.iterdump())
    before_events = storage.read_job_events('A', 1)
    before_control = storage.read_job_control('A', 1)

    with pytest.raises(RuntimeError, match='owner|ownership'):
        storage.request_job_restart('A', 1, reason='must refuse damaged owner')

    storage.db_mutation_barrier()
    assert list(connection.iterdump()) == before_database
    assert storage.read_job_events('A', 1) == before_events
    assert storage.read_job_control('A', 1) == before_control
    assert storage.get_job_status('A', 1) == 'running'
    assert storage.read_job_control('A', 1)['generation'] == generation
    assert output.read_bytes() == b'preserve damaged owner output'
