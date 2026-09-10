from __future__ import annotations

import os
import socket

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.cli.restart import restart_active_scope
from micro_workflow_manager.cli.run_commands import resume_node
from micro_workflow_manager.models import CANCELLED, DONE, FAILED, Job, now
from micro_workflow_manager.processes import process_identity
from micro_workflow_manager.storage import FileStorage
from tests.test_086_native_owned_restart import _close


def _start_native_session(workflow, session_id, component):
    storage = workflow.storage
    snapshot = workflow.topology.snapshot()
    storage.register_component_topology(snapshot)
    storage.create_execution_session(
        session_id,
        session_kind="main",
        command="run",
        start_component=component,
        selected_components=[component],
        started_at=now(),
        hostname=socket.gethostname(),
        pid=os.getpid(),
        process_identity=process_identity(os.getpid()),
        expected_shape=snapshot.shape_json,
    )
    storage.reserve_execution_components(session_id, expected_shape=snapshot.shape_json)


def test_resume_registers_output_backed_completion_before_restart_selection(tmp_path, request):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "A")])
    storage = workflow.storage
    request.addfinalizer(lambda: _close(storage))
    calls = []

    @workflow.task("A")
    def run_a(ctx):
        calls.append(ctx.job_id)
        return "should not rerun"

    session_id = "output-recovery"
    _start_native_session(workflow, session_id, ("A",))
    storage.create_job(Job(node_name="A", job_id=1, params={}))
    generation, execution_id = storage.claim_job_execution(
        "A",
        1,
        started_at=now(),
        session_id=session_id,
        component=("A",),
    )
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
    storage.finish_execution_session(session_id, outcome=FAILED, finished_at=now())
    storage.release_execution_components(session_id)
    storage.set_node_status("A", FAILED)

    assert storage.get_job_status("A", 1) == "running"
    assert resume_node(tmp_path, workflow, "A") == 0
    assert calls == []
    assert storage.get_job_status("A", 1) == DONE
    assert storage.get_node_status("A") == DONE


def _active_component_storage(tmp_path, request) -> FileStorage:
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "B"), ("B", "A")])
    storage = workflow.storage
    request.addfinalizer(lambda: _close(storage))
    session_id = "active-component-run"
    component = ("A", "B")
    _start_native_session(workflow, session_id, component)
    for node_name, job_id in [
        ("A", 1),
        ("A", 2),
        ("B", 1),
        ("B", 2),
        ("B", 3),
        ("B", 4),
    ]:
        storage.create_job(Job(node_name=node_name, job_id=job_id, params={}))

    storage.claim_job_execution(
        "A", 1, started_at=now(), session_id=session_id, component=component,
    )
    a_failed = storage.claim_job_execution(
        "A", 2, started_at=now(), session_id=session_id, component=component,
    )
    storage.finalize_job_execution("A", 2, *a_failed, FAILED)
    storage.claim_job_execution(
        "B", 1, started_at=now(), session_id=session_id, component=component,
    )
    b_cancelled = storage.claim_job_execution(
        "B", 2, started_at=now(), session_id=session_id, component=component,
    )
    storage.finalize_job_execution("B", 2, *b_cancelled, CANCELLED)
    b_done = storage.claim_job_execution(
        "B", 3, started_at=now(), session_id=session_id, component=component,
    )
    storage.finalize_job_execution("B", 3, *b_done, DONE)
    return storage


def test_restart_node_restarts_running_and_failed_jobs_in_active_component(tmp_path, request):
    storage = _active_component_storage(tmp_path, request)

    assert restart_active_scope(tmp_path, "A") == 0

    for node_name, job_id in [("A", 1), ("A", 2), ("B", 1), ("B", 2)]:
        assert storage.get_job_status(node_name, job_id) == "queued"
        assert storage.read_job_control(node_name, job_id)["generation"] == 1
    assert storage.get_job_status("B", 3) == DONE
    assert storage.read_job_control("B", 3)["generation"] == 0
    assert storage.get_job_status("B", 4) == "queued"
    assert storage.read_job_control("B", 4)["generation"] == 0


def test_restart_node_failed_leaves_running_jobs_untouched(tmp_path, request):
    storage = _active_component_storage(tmp_path, request)

    assert restart_active_scope(tmp_path, "A", failed_only=True) == 0

    assert storage.get_job_status("A", 1) == "running"
    assert storage.read_job_control("A", 1)["generation"] == 0
    assert storage.get_job_status("B", 1) == "running"
    assert storage.read_job_control("B", 1)["generation"] == 0
    for node_name, job_id in [("A", 2), ("B", 2)]:
        assert storage.get_job_status(node_name, job_id) == "queued"
        assert storage.read_job_control(node_name, job_id)["generation"] == 1


def test_waiting_node_requires_queued_running_and_failed_counts_to_clear(tmp_path, request):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "B"), ("B", "A")])
    storage = workflow.storage
    request.addfinalizer(lambda: _close(storage))
    ran = []

    @workflow.task("A", waiting=True, wait_for=["B"])
    def run_a(ctx):
        if ctx.job_id == 1:
            workflow.start("A", job_id=2)
            workflow.start("B", job_id=1)
            storage.set_job_status("B", 1, FAILED)

            assert workflow.waiting_blockers("A") == {"B"}
            assert workflow.node_waiting_ready("A") is False
            assert workflow.run_component({"A", "B"}, ignore_readiness=True) == []
            assert ran == []
            assert storage.get_job_status("A", 2) == "queued"

            storage.set_job_status("B", 1, DONE)
            assert workflow.waiting_blockers("A") == set()
            workflow.run_component({"A", "B"}, ignore_readiness=True)
            return
        ran.append("A")

    @workflow.task("B")
    def run_b(ctx):
        ran.append("B")

    workflow.start("A", job_id=1)
    workflow.run_component({"A", "B"}, ignore_readiness=True)
    assert ran == ["A"]
