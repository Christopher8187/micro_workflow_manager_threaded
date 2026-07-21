from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.errors import InvalidGraphError
from micro_workflow_manager.monitor import workflow_snapshot


def _wait_for(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not reached before timeout")


def test_waiting_nodes_alternate_router_and_handler_pumps(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="direct")
    workflow.graph([("explode", "handler"), ("handler", "explode")])
    order: list[str] = []

    explode = NodeRouter(
        "explode",
        runner="direct",
        wait_for=["handler"],
    )
    explode.create_job(params={"round": 0})

    @explode.task
    def run_explode(ctx, round):
        order.append(f"explode:{round}")
        if round == 0:
            ctx.node("handler").add(round=0)
        return round

    handler = NodeRouter(
        "handler",
        runner="direct",
        wait_for=["explode"],
    )

    @handler.task
    def run_handler(ctx, round):
        order.append(f"handler:{round}")
        ctx.node("explode").add(round=1)
        return round

    workflow.include_routers(explode, handler)
    workflow.run_component({"explode", "handler"}, ignore_readiness=True)

    assert order == ["explode:0", "handler:0", "explode:1"]
    assert workflow.storage.get_node_status("explode") == "done"
    assert workflow.storage.get_node_status("handler") == "done"


def test_monitor_reports_waiting_instead_of_queued(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([("A", "B"), ("B", "A")])
    first_a_started = threading.Event()
    release_a = threading.Event()

    a = NodeRouter("A", max_threads=1, runner="threaded")
    a.create_job(number=2)

    @a.task
    def run_a(ctx):
        if ctx.job_id == 1:
            first_a_started.set()
            assert release_a.wait(5)
        return ctx.job_id

    b = NodeRouter("B", max_threads=1, runner="threaded", wait_for=["A"])
    b.create_job(number=1)

    @b.task
    def run_b(ctx):
        return ctx.job_id

    workflow.include_routers(a, b)
    result: dict[str, object] = {}
    thread = threading.Thread(
        target=lambda: result.setdefault(
            "ran",
            workflow.run_component({"A", "B"}, ignore_readiness=True),
        )
    )
    thread.start()
    assert first_a_started.wait(3)

    _wait_for(lambda: workflow.storage.has_queued_jobs("A"))
    snapshot = workflow_snapshot(workflow)
    b_row = next(row for row in snapshot["nodes"] if row["node"] == "B")
    assert b_row["status"] == "waiting"
    assert b_row["queued"] == 1
    assert b_row["waiting_on"] == ["A"]
    assert "B" in snapshot["waiting_nodes"]

    release_a.set()
    thread.join(8)
    assert not thread.is_alive()
    assert "ran" in result


def test_wait_for_component_resolves_all_other_vertices_and_persists_schema(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="direct")
    workflow.graph([("A", "B"), ("B", "C"), ("C", "A")])

    @workflow.task("A", waiting=True)
    def a(ctx):
        return None

    @workflow.task("B")
    def b(ctx):
        return None

    @workflow.task("C")
    def c(ctx):
        return None

    assert workflow.waiting_dependencies("A") == {"B", "C"}
    schema = json.loads(workflow.storage.node_schema_file("A").read_text(encoding="utf-8"))
    assert schema["waiting"] is True
    assert schema["wait_for"] is None
    assert schema["resolved_wait_for"] == ["B", "C"]


def test_waiting_target_outside_component_is_rejected(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="direct")
    workflow.graph([("A", "B")])

    with pytest.raises(InvalidGraphError, match="only wait for vertices"):
        @workflow.task("A", wait_for=["B"])
        def a(ctx):
            return None


def test_waiting_singleton_is_trivial_and_records_reminder(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="direct")
    workflow.graph([("A", "B")])

    @workflow.task("A", waiting=True)
    def a(ctx):
        return None

    @workflow.task("B")
    def b(ctx):
        return None

    assert workflow.waiting_dependencies("A") == set()
    assert workflow.node_waiting_ready("A") is True
    assert any(
        "DAG-type nodes have no queue-independent waiting functionality" in item
        for item in workflow.configuration_notices
    )


def test_cli_reminds_that_waiting_is_trivial_for_dag_node(tmp_path, monkeypatch, capsys):
    from micro_workflow_manager import cli

    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text("EDGES = [('A', 'B')]\n", encoding="utf-8")
    (behavior / "A.py").write_text(
        '''from micro_workflow_manager import NodeRouter
router = NodeRouter("A", waiting=True)
@router.task
def run(ctx): return None
''',
        encoding="utf-8",
    )
    (behavior / "B.py").write_text(
        '''from micro_workflow_manager import NodeRouter
router = NodeRouter("B")
@router.task
def run(ctx): return None
''',
        encoding="utf-8",
    )
    assert cli.main(["init"]) == 0
    capsys.readouterr()
    assert cli.main(["graph", "src/graph.py"]) == 0
    captured = capsys.readouterr()
    assert "DAG-type nodes have no queue-independent waiting functionality" in captured.err


def test_waiting_is_node_only_and_jobs_remain_queued(tmp_path):
    from micro_workflow_manager import MicroWorkflow
    from micro_workflow_manager.models import QUEUED, WAITING

    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([("A", "B"), ("B", "A")])

    @workflow.task("A", waiting=True, wait_for=["B"])
    def a(ctx):
        return None

    @workflow.task("B")
    def b(ctx):
        return None

    workflow.start("A", job_id=1)
    workflow.start("B", job_id=1)
    workflow.storage.set_node_status("A", WAITING)

    assert workflow.storage.get_job_status("A", 1) == QUEUED
    counts = workflow.storage.job_status_counts("A")
    assert "waiting" not in counts
    assert counts[QUEUED] == 1

    try:
        workflow.storage.set_job_status("A", 1, WAITING)
    except ValueError as error:
        assert "Invalid job status" in str(error)
    else:
        raise AssertionError("waiting must never be accepted as a job status")
