from __future__ import annotations

import threading
import time

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.models import DONE, FAILED, Job, now
from micro_workflow_manager.storage import FileStorage


def test_output_backed_terminal_reconciliation_is_idempotent(tmp_path):
    storage = FileStorage(tmp_path)
    storage.create_job(Job(node_name="A", job_id=1, params={}))
    generation, execution_id = storage.claim_job_execution(
        "A",
        1,
        started_at=now(),
    )
    storage.write_output(
        "A",
        1,
        {
            "status": DONE,
            "generation": generation,
            "result_repr": "'complete'",
        },
    )

    assert storage.get_job_status("A", 1) == "running"
    assert storage.reconcile_terminal_outputs(["A"]) == 1
    assert storage.get_job_status("A", 1) == DONE

    # The ordinary job finalizer may wake after recovery committed the same
    # lease. Treat that matching terminal state as success rather than a stale
    # execution error.
    storage.finalize_job_execution(
        "A",
        1,
        generation,
        execution_id,
        DONE,
        started_at=now(),
        finished_at=now(),
        duration_seconds=0.0,
        generation=generation,
        execution_id=execution_id,
    )
    assert storage.get_job_status("A", 1) == DONE


def test_component_failure_joins_started_jobs_without_recovery_scan(tmp_path, monkeypatch):
    from concurrent.futures import Future

    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "B"), ("B", "A")])
    a_started = threading.Event()
    b_failed = threading.Event()
    release_a: Future[None] = Future()

    @workflow.task("A", runner="api", max_threads=1)
    def run_a(ctx):
        a_started.set()
        release_a.result()
        return "finished after sibling failure"

    @workflow.task("B", runner="api", max_threads=1)
    def run_b(ctx):
        assert a_started.wait(2)
        b_failed.set()
        raise RuntimeError("stop the component")

    workflow.add_jobs(None, "A", [{}])
    workflow.add_jobs(None, "B", [{}])
    workflow.active_job_restart_enabled = True

    recovery_calls = []
    original_reconcile = workflow.storage.reconcile_terminal_outputs

    def record_reconcile(*args, **kwargs):
        recovery_calls.append((args, kwargs))
        return original_reconcile(*args, **kwargs)

    monkeypatch.setattr(workflow.storage, "reconcile_terminal_outputs", record_reconcile)

    error = []

    def run_component():
        try:
            workflow.run_node("A", ignore_readiness=True)
        except BaseException as exc:
            error.append(exc)

    worker = threading.Thread(target=run_component)
    worker.start()
    assert b_failed.wait(3)

    # The first failure has stopped admission, but the already-started A job is
    # still owned by the active component and must be allowed to finish.
    time.sleep(0.05)
    assert worker.is_alive()
    assert workflow.storage.get_job_status("A", 1) == "running"

    release_a.set_result(None)
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert error
    assert "stop the component" in workflow.storage.read_json(workflow.storage.output_file("B", 1))["error"]
    assert workflow.storage.get_job_status("A", 1) == DONE
    assert workflow.storage.get_job_status("B", 1) == FAILED
    assert workflow.storage.get_node_status("A") == FAILED
    assert workflow.storage.get_node_status("B") == FAILED
    assert recovery_calls == []
