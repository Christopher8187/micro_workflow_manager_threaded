from __future__ import annotations

import textwrap
import threading
import time
from pathlib import Path

from micro_workflow_manager import MicroWorkflow, NodeRouter, cli
from micro_workflow_manager.monitor import workflow_snapshot
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.base import _resolved_path_is_within


def test_component_polls_new_sibling_queues_while_router_is_still_running(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([("explode", "handler"), ("handler", "explode")])

    lock = threading.Lock()
    explode_done = 0
    handler_observations: list[int] = []

    explode = NodeRouter("explode", max_threads=2, runner="threaded")
    explode.create_job(number=40)

    @explode.task
    def route(ctx):
        nonlocal explode_done
        time.sleep(0.02)
        with lock:
            explode_done += 1
        ctx.node("handler").add(source=ctx.job_id)
        return ctx.job_id

    handler = NodeRouter("handler", max_threads=2, runner="api")

    @handler.task
    def refine(ctx, source):
        with lock:
            handler_observations.append(explode_done)
        return source

    workflow.include_router(explode)
    workflow.include_router(handler)
    workflow.run_node("explode", ignore_readiness=True)

    assert len(handler_observations) == 40
    assert min(handler_observations) < 40, (
        "handler work did not begin until the explode router drained its entire queue"
    )
    assert workflow.storage.get_node_status("explode") == "done"
    assert workflow.storage.get_node_status("handler") == "done"


def test_queued_component_is_not_reported_running_before_execution(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([("explode", "handler"), ("handler", "explode")])

    explode = NodeRouter("explode")
    explode.create_job(params={"value": 1})

    @explode.task
    def route(ctx, value):
        return value

    handler = NodeRouter("handler")

    @handler.task
    def refine(ctx):
        return None

    workflow.include_router(explode)
    workflow.include_router(handler)
    # Simulate the stale component-wide RUNNING state observed after an earlier
    # refresh. Monitor must derive per-node display state from actual job counts.
    workflow.storage.set_node_status("explode", "running")
    workflow.storage.set_node_status("handler", "running")

    snapshot = workflow_snapshot(workflow)
    statuses = {row["node"]: row["status"] for row in snapshot["nodes"]}
    assert statuses["explode"] == "queued"
    assert statuses["handler"] == "queued"
    assert snapshot["running_nodes"] == []



def test_monitor_prefers_actual_running_job_over_stale_queued_node_state(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([("explode", "handler"), ("handler", "explode")])

    explode = NodeRouter("explode")
    handler = NodeRouter("handler")
    handler.create_job(params={"value": 1})

    @explode.task
    def route(ctx):
        return None

    @handler.task
    def refine(ctx, value):
        return value

    workflow.include_router(explode)
    workflow.include_router(handler)
    workflow.storage.set_node_status("handler", "queued")
    workflow.storage.set_job_status("handler", 1, "running")

    snapshot = workflow_snapshot(workflow)
    rows = {row["node"]: row for row in snapshot["nodes"]}
    assert rows["handler"]["status"] == "running"
    assert rows["handler"]["running"] == 1
    assert "handler" in snapshot["running_nodes"]

def test_threaded_lazy_source_failure_terminates_run_and_monitor(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        "EDGES = [('explode', 'handler'), ('handler', 'explode')]\n",
        encoding="utf-8",
    )
    (behavior / "explode.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeRouter

            router = NodeRouter("explode", max_threads=8, runner="threaded")
            router.create_job(number=200)

            @router.task
            def run(ctx):
                if ctx.job_id == 1:
                    raise RuntimeError("synthetic route failure")
                ctx.sleep(0.01)
                return ctx.job_id
            """
        ).strip(),
        encoding="utf-8",
    )
    (behavior / "handler.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeRouter

            router = NodeRouter("handler", max_threads=4, runner="threaded")

            @router.task
            def run(ctx):
                return None
            """
        ).strip(),
        encoding="utf-8",
    )

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "threaded"]) == 0
    capsys.readouterr()

    started = time.monotonic()
    assert cli.main(
        [
            "run",
            "explode",
            "--runner",
            "threaded",
            "--monitor",
            "--monitor-interval",
            "0.02",
        ]
    ) == 1
    elapsed = time.monotonic() - started
    captured = capsys.readouterr()

    assert elapsed < 5.0
    state = FileStorage(tmp_path).get_run_state()
    assert state["status"] == "failed"
    assert FileStorage(tmp_path).get_node_status("explode") == "failed"
    assert FileStorage(tmp_path).get_node_status("handler") == "failed"
    final = captured.err.rsplit("--- mwf final monitor snapshot ---", 1)[1]
    assert "active run: none" in final
    assert "last run: run explode | status=failed" in final


def test_windows_extended_length_descendant_is_safe():
    base = Path(r"C:\Users\Chris\Desktop\Projects\kaicenat\node\explode\output")
    descendant = Path(
        r"\\?\C:\Users\Chris\Desktop\Projects\kaicenat\node\explode\output"
        r"\ArtinAlgebra\0003_chapter_3_vector_spaces\000172_change_of_basis"
        r"\routes\000172_000002.json"
    )
    outside = Path(
        r"\\?\C:\Users\Chris\Desktop\Projects\kaicenat\node\explode\output-old"
        r"\escape.json"
    )

    assert _resolved_path_is_within(base, descendant)
    assert not _resolved_path_is_within(base, outside)


def test_windows_extended_unc_descendant_is_safe():
    base = Path(r"\\server\share\project\node\explode\output")
    descendant = Path(
        r"\\?\UNC\server\share\project\node\explode\output\book\routes\one.json"
    )
    outside = Path(
        r"\\?\UNC\server\share\project\node\explode-elsewhere\one.json"
    )

    assert _resolved_path_is_within(base, descendant)
    assert not _resolved_path_is_within(base, outside)


def test_api_handler_refills_from_jobs_added_after_its_pump_started(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([("explode", "handler"), ("handler", "explode")])

    lock = threading.Lock()
    routed = 0
    routing_done = False
    active_handlers = 0
    max_active_before_routing_done = 0

    explode = NodeRouter("explode", max_threads=1, runner="threaded")
    explode.create_job(number=20)

    @explode.task
    def route(ctx):
        nonlocal routed, routing_done
        time.sleep(0.015)
        ctx.node("handler").add(source=ctx.job_id)
        with lock:
            routed += 1
            if routed == 20:
                routing_done = True
        return ctx.job_id

    handler = NodeRouter("handler", max_threads=20, runner="api")

    @handler.task
    def refine(ctx, source):
        nonlocal active_handlers, max_active_before_routing_done
        with lock:
            active_handlers += 1
            if not routing_done:
                max_active_before_routing_done = max(
                    max_active_before_routing_done,
                    active_handlers,
                )
        # Keep handlers alive long enough to distinguish live-source refills
        # from one initial snapshot even on slower Windows filesystems.
        time.sleep(1.5)
        with lock:
            active_handlers -= 1
        return source

    workflow.include_router(explode)
    workflow.include_router(handler)
    workflow.run_node("explode", ignore_readiness=True)

    # The old snapshot source started the handler pump with only the first few
    # jobs and could not refill until those long API jobs completed. The live
    # source should grow close to the declared limit while routing is ongoing.
    assert max_active_before_routing_done >= 10
    assert workflow.storage.get_node_status("explode") == "done"
    assert workflow.storage.get_node_status("handler") == "done"


def test_refreshable_queued_source_observes_late_lower_job_ids(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="api")
    router = NodeRouter("handler", runner="api")

    @router.task
    def refine(ctx, value):
        return value

    workflow.include_router(router)
    storage = workflow.storage
    source = storage.queued_job_source("handler")

    # Simulate concurrent batch producers: a higher reserved ID can commit
    # before a lower reserved ID. SQLite row insertion order must be the source
    # cursor so the late lower ID is not skipped.
    from micro_workflow_manager.models import Job

    storage.create_job(Job(node_name="handler", job_id=20, params={"value": 20}))
    assert source.pull(10) == [20]
    storage.create_job(Job(node_name="handler", job_id=10, params={"value": 10}))
    assert source.pull(10) == [10]


def test_cli_monitor_shows_api_handler_scaling_during_live_routing(
    tmp_path,
    monkeypatch,
    capsys,
):
    import re

    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        "EDGES = [('explode', 'handler'), ('handler', 'explode')]\n",
        encoding="utf-8",
    )
    (behavior / "explode.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeRouter

            router = NodeRouter("explode", max_threads=1, runner="threaded")
            router.create_job(number=20)

            @router.task
            def run(ctx):
                ctx.sleep(0.015)
                ctx.node("handler").add(source=ctx.job_id)
                return ctx.job_id
            """
        ).strip(),
        encoding="utf-8",
    )
    (behavior / "handler.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeRouter

            router = NodeRouter("handler", max_threads=20, runner="api")

            @router.task
            def run(ctx, source):
                ctx.sleep(1.5)
                return source
            """
        ).strip(),
        encoding="utf-8",
    )

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "threaded"]) == 0
    capsys.readouterr()

    assert cli.main(
        [
            "run",
            "explode",
            "--runner",
            "threaded",
            "--monitor",
            "--monitor-interval",
            "0.05",
        ]
    ) == 0
    captured = capsys.readouterr()
    output = captured.out + captured.err
    running_counts = [
        int(match.group(1))
        for match in re.finditer(
            r"(?m)^handler\s+running\s+\d+\s+\d+\s+\d+\s+(\d+)\s+",
            output,
        )
    ]
    assert running_counts
    assert max(running_counts) >= 10
