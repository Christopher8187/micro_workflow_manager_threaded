"""API permit writer decisions survive interruption, restart, and task policy."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.fibers import FiberRuntime
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage import api_admission as storage_api_admission
from micro_workflow_manager.workflow.api_admission import (
    ApiPermitAdmissionError,
    ApiPermitReleaseError,
    ApiPermitStateError,
    _PermitController,
    api_execution_permit,
    release_current_api_execution_permit,
)
from tests.test_064_read_only_previews import _initialize_native_project
from tests.test_090_component_session_settlement import _close
from tests.test_146_native_applied_recovery import _create_active_scope


def _active_api_scope(tmp_path, monkeypatch, *, session_id="api-owner"):
    workflow = _initialize_native_project(
        tmp_path,
        monkeypatch,
        edges=[("A", "A")],
    )
    workflow.runner = "api"
    shape = workflow.topology.snapshot().shape_json
    owner = _create_active_scope(
        workflow.storage, shape, "A", session_id, live=True,
    )
    return workflow, owner


def _controller(workflow, owner):
    return _PermitController(
        system=workflow,
        session_id=owner["session_id"],
        node_name="A",
        job_id=1,
        generation=owner["generation"],
        execution_id=owner["execution_id"],
    )


def _permit_rows(storage):
    return [
        tuple(row)
        for row in storage.db_connection().execute(
            "SELECT session_id, node_name, job_id, generation, execution_id "
            "FROM api_execution_permits ORDER BY execution_id"
        )
    ]


def _expected_permit(owner):
    return (
        owner["session_id"],
        "A",
        1,
        owner["generation"],
        owner["execution_id"],
    )


def test_successful_storage_release_returns_true_after_exact_absent_readback(
    tmp_path, monkeypatch,
):
    workflow, owner = _active_api_scope(tmp_path, monkeypatch)
    storage = workflow.storage
    try:
        assert storage.try_acquire_api_execution_permit(*_expected_permit(owner)) is True
        assert _permit_rows(storage) == [_expected_permit(owner)]
        assert storage.release_api_execution_permit(*_expected_permit(owner)) is True
        assert _permit_rows(storage) == []
        assert storage.read_job_current_owner("A", 1) == owner["owner"]
    finally:
        _close(storage)


@pytest.mark.parametrize("release_failure", [False, True])
def test_postcommit_insert_is_resolved_before_concurrent_restart_preference(
    tmp_path, monkeypatch, release_failure,
):
    workflow, owner = _active_api_scope(tmp_path, monkeypatch)
    storage = workflow.storage
    peer = FileStorage(tmp_path)
    controller = _controller(workflow, owner)
    original_notify = storage.notify_state_change
    original_release = storage_api_admission._release_api_execution_permit
    notification_error = OSError("permit insert notification failed after restart")
    restarted = []

    def notify_after_restart():
        if not restarted and _permit_rows(storage):
            restarted.append(
                peer.request_active_job_restart(
                    "A", 1, reason="race after committed permit insert",
                )
            )
            raise notification_error
        return original_notify()

    def maybe_fail_release(connection, identity):
        if release_failure and restarted:
            raise OSError("permit release writer refused after restart")
        return original_release(connection, identity)

    monkeypatch.setattr(storage, "notify_state_change", notify_after_restart)
    monkeypatch.setattr(
        storage_api_admission,
        "_release_api_execution_permit",
        maybe_fail_release,
    )
    try:
        expected_error = ApiPermitReleaseError if release_failure else ApiPermitAdmissionError
        with pytest.raises(expected_error) as observed:
            controller.acquire()

        assert restarted and restarted[0]["generation"] > owner["generation"]
        assert storage.get_job_status("A", 1) == "queued"
        assert storage.get_job_execution_owner(owner["execution_id"]) == owner["owner"]
        if release_failure:
            assert controller.held is True
            assert _permit_rows(storage) == [_expected_permit(owner)]
            assert "release" in str(observed.value).lower()
        else:
            assert controller.held is False
            assert _permit_rows(storage) == []
            assert observed.value.__cause__ is notification_error
    finally:
        monkeypatch.setattr(
            storage_api_admission,
            "_release_api_execution_permit",
            original_release,
        )
        if controller.held:
            assert storage.release_api_execution_permit(
                *_expected_permit(owner)
            ) is True
            controller.held = False
        _close(peer)
        _close(storage)


class _InterruptFirstResult:
    def __init__(self, future, interrupted):
        self.future = future
        self.interrupted = interrupted
        self.first = True

    def result(self, *args, **kwargs):
        if self.first:
            self.first = False
            self.interrupted.set()
            raise KeyboardInterrupt("synthetic API permit wait interruption")
        return self.future.result(*args, **kwargs)

    def done(self):
        return self.future.done()

    def exception(self, *args, **kwargs):
        return self.future.exception(*args, **kwargs)


def test_inflight_acquisition_future_is_drained_before_capacity_state_changes(
    tmp_path, monkeypatch,
):
    workflow, owner = _active_api_scope(tmp_path, monkeypatch)
    storage = workflow.storage
    controller = _controller(workflow, owner)
    writer_entered = Event()
    release_writer = Event()
    interrupted = Event()
    wrapped = []

    def block_writer(_connection):
        writer_entered.set()
        assert release_writer.wait(10)

    blocker = storage.submit_db_mutation(block_writer, wait=False, priority=0)
    assert writer_entered.wait(10)
    original_acquire = storage.try_acquire_api_execution_permit

    def interrupt_first_wait(*args, **kwargs):
        future = original_acquire(*args, **kwargs)
        assert kwargs.get("_wait") is False
        result = _InterruptFirstResult(future, interrupted)
        wrapped.append(result)
        return result

    monkeypatch.setattr(
        storage, "try_acquire_api_execution_permit", interrupt_first_wait,
    )
    pool = ThreadPoolExecutor(max_workers=1)
    attempt = pool.submit(controller.acquire)
    try:
        assert interrupted.wait(10)
        assert not wrapped[0].future.done()
        assert not attempt.done()
        release_writer.set()
        blocker.result(timeout=10)
        with pytest.raises(ApiPermitAdmissionError, match="interrupted"):
            attempt.result(timeout=10)
        assert wrapped[0].future.done()
        assert controller.held is False
        assert _permit_rows(storage) == []
        assert storage.read_job_current_owner("A", 1) == owner["owner"]
    finally:
        release_writer.set()
        pool.shutdown(wait=True, cancel_futures=False)
        _close(storage)


@pytest.mark.parametrize("concurrent_restart", [False, True])
def test_checkpoint_permit_error_bypasses_task_retry_fallback_and_restart_policy(
    tmp_path, monkeypatch, concurrent_restart,
):
    workflow = MicroWorkflow(tmp_path, runner="api", persist_graph=False)
    workflow.graph([("A", "A")])
    calls = []
    router = NodeRouter("A", runner="api")

    @router.task(retries=1)
    def work(ctx, scope=None):
        calls.append("main")
        ctx.checkpoint("release capacity")
        return "unexpected main success"

    @router.fallback(name="repair", retries=1)
    def repair(ctx, scope=None, error=None):
        calls.append("fallback")
        return "unexpected fallback success"

    workflow.include_routers(router)
    storage = workflow.storage
    snapshot = workflow.topology.snapshot()
    storage.register_component_topology(snapshot)
    shape = snapshot.shape_json
    owner = _create_active_scope(storage, shape, "A", "task-policy-owner", live=True)
    peer = FileStorage(tmp_path)
    workflow.execution_session_context = (
        owner["session_id"], {"A": ("A",)}, shape,
    )
    original_release = storage_api_admission._release_api_execution_permit
    restarted = []
    task_errors = []

    def fail_release(_connection, _identity):
        raise OSError("durable permit release is unresolved")

    def release_at_checkpoint(context):
        if concurrent_restart and not restarted:
            restarted.append(
                peer.request_active_job_restart(
                    "A", 1, reason="restart concurrent with failed permit release",
                )
            )
        assert release_current_api_execution_permit(context) is True

    monkeypatch.setattr(
        storage_api_admission, "_release_api_execution_permit", fail_release,
    )
    monkeypatch.setattr(
        "micro_workflow_manager.interrupt_cooperation.wait_for_interrupt_pauses",
        release_at_checkpoint,
    )

    def execute(job):
        with api_execution_permit(
            workflow,
            "A",
            1,
            owner["generation"],
            owner["execution_id"],
        ):
            try:
                return workflow.execute_with_fallbacks(
                    job,
                    execution_generation=owner["generation"],
                    execution_id=owner["execution_id"],
                )
            except BaseException as error:
                task_errors.append(error)
                raise

    try:
        runtime = FiberRuntime(poll_interval=0.01, start_burst=1)
        with pytest.raises(ApiPermitReleaseError, match="released|release"):
            runtime.run_source(
                "A",
                [storage.load_job("A", 1)],
                execute,
                limit_provider=lambda: 1,
            )

        assert len(task_errors) == 1
        assert isinstance(task_errors[0], ApiPermitStateError)
        assert calls == ["main"]
        event_names = [event["event"] for event in storage.read_job_events("A", 1)]
        assert event_names.count("task_started") == 1
        assert "task_failed" not in event_names
        assert "retry_started" not in event_names
        assert "fallback_started" not in event_names
        control = storage.read_job_control("A", 1)
        if concurrent_restart:
            assert len(restarted) == 1
            assert storage.get_job_status("A", 1) == "queued"
            assert control["generation"] == restarted[0]["generation"]
            assert control["generation"] > owner["generation"]
            assert control["active_execution_id"] is None
            observation = storage.read_job_owner_observation("A", 1)
            assert observation["state"] == "last"
            assert observation["active_execution_id"] is None
            assert observation["owner"] == owner["owner"]
            assert storage.read_job_current_owner("A", 1) == owner["owner"]
        else:
            assert restarted == []
            assert storage.get_job_status("A", 1) == "running"
            assert control["generation"] == owner["generation"]
            assert control["active_execution_id"] == owner["execution_id"]
            assert storage.read_job_current_owner("A", 1) == owner["owner"]
        assert storage.get_job_execution_owner(owner["execution_id"]) == owner["owner"]
        assert _permit_rows(storage) == [_expected_permit(owner)]
    finally:
        monkeypatch.setattr(
            storage_api_admission,
            "_release_api_execution_permit",
            original_release,
        )
        permit_rows = _permit_rows(storage)
        if permit_rows:
            assert permit_rows == [_expected_permit(owner)]
            assert storage.release_api_execution_permit(
                *_expected_permit(owner)
            ) is True
        workflow.execution_session_context = None
        _close(peer)
        _close(storage)
