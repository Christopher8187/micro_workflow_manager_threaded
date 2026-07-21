from __future__ import annotations

import os

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.cli.restart import restart_active_scope
from micro_workflow_manager.cli.run_commands import resume_node
from micro_workflow_manager.models import CANCELLED, DONE, FAILED, Job, now
from micro_workflow_manager.storage import FileStorage


def test_resume_registers_output_backed_completion_before_restart_selection(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="direct")
    calls = []

    @workflow.task("A")
    def run_a(ctx):
        calls.append(ctx.job_id)
        return "should not rerun"

    workflow.storage.create_job(Job(node_name="A", job_id=1, params={}))
    generation, execution_id = workflow.storage.claim_job_execution(
        "A",
        1,
        started_at=now(),
    )
    workflow.storage.write_output(
        "A",
        1,
        {
            "status": DONE,
            "generation": generation,
            "execution_id": execution_id,
            "result_repr": "'already finished'",
        },
    )
    workflow.storage.set_node_status("A", FAILED)

    assert workflow.storage.get_job_status("A", 1) == "running"
    assert resume_node(tmp_path, workflow, "A") == 0
    assert calls == []
    assert workflow.storage.get_job_status("A", 1) == DONE
    assert workflow.storage.get_node_status("A") == DONE


def _active_component_storage(tmp_path) -> FileStorage:
    storage = FileStorage(tmp_path)
    for node_name, job_id in [
        ("A", 1),
        ("A", 2),
        ("B", 1),
        ("B", 2),
        ("B", 3),
        ("B", 4),
    ]:
        storage.create_job(Job(node_name=node_name, job_id=job_id, params={}))

    storage.claim_job_execution("A", 1, started_at=now())
    storage.set_job_status("A", 2, FAILED)
    storage.claim_job_execution("B", 1, started_at=now())
    storage.set_job_status("B", 2, CANCELLED)
    storage.set_job_status("B", 3, DONE)
    storage.write_run_state(
        {
            "run_id": "active-component-run",
            "status": "running",
            "command": "run",
            "nodes": ["A", "B"],
            "components": {"A": ["A", "B"], "B": ["A", "B"]},
            "pid": os.getpid(),
        }
    )
    return storage


def test_restart_node_restarts_running_and_failed_jobs_in_active_component(tmp_path):
    storage = _active_component_storage(tmp_path)

    assert restart_active_scope(tmp_path, "A") == 0

    for node_name, job_id in [("A", 1), ("A", 2), ("B", 1), ("B", 2)]:
        assert storage.get_job_status(node_name, job_id) == "queued"
        assert storage.read_job_control(node_name, job_id)["generation"] == 1
    assert storage.get_job_status("B", 3) == DONE
    assert storage.read_job_control("B", 3)["generation"] == 0
    assert storage.get_job_status("B", 4) == "queued"
    assert storage.read_job_control("B", 4)["generation"] == 0


def test_restart_node_failed_leaves_running_jobs_untouched(tmp_path):
    storage = _active_component_storage(tmp_path)

    assert restart_active_scope(tmp_path, "A", failed_only=True) == 0

    assert storage.get_job_status("A", 1) == "running"
    assert storage.read_job_control("A", 1)["generation"] == 0
    assert storage.get_job_status("B", 1) == "running"
    assert storage.read_job_control("B", 1)["generation"] == 0
    for node_name, job_id in [("A", 2), ("B", 2)]:
        assert storage.get_job_status(node_name, job_id) == "queued"
        assert storage.read_job_control(node_name, job_id)["generation"] == 1


def test_waiting_node_requires_queued_running_and_failed_counts_to_clear(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="direct")
    workflow.graph([("A", "B"), ("B", "A")])
    ran = []

    @workflow.task("A", waiting=True, wait_for=["B"])
    def run_a(ctx):
        ran.append("A")

    @workflow.task("B")
    def run_b(ctx):
        ran.append("B")

    workflow.start("A", job_id=1)
    workflow.start("B", job_id=1)
    workflow.storage.set_job_status("B", 1, FAILED)

    assert workflow.waiting_blockers("A") == {"B"}
    assert workflow.node_waiting_ready("A") is False
    assert workflow.run_component({"A", "B"}, ignore_readiness=True) == []
    assert ran == []
    assert workflow.storage.get_job_status("A", 1) == "queued"

    workflow.storage.set_job_status("B", 1, DONE)
    assert workflow.waiting_blockers("A") == set()
    workflow.run_component({"A", "B"}, ignore_readiness=True)
    assert ran == ["A"]
