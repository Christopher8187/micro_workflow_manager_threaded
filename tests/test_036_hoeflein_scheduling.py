from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from micro_workflow_manager import MicroWorkflow, NodeRouter, cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.storage import FileStorage


def make_project(tmp_path: Path, monkeypatch, *, edges: str, files: dict[str, str], runner: str = "direct"):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(textwrap.dedent(edges).strip() + "\n", encoding="utf-8")
    for name, content in files.items():
        (behavior / f"{name}.py").write_text(textwrap.dedent(content).strip() + "\n", encoding="utf-8")
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", runner]) == 0


def test_hoeflein_component_refuses_until_external_predecessor_is_done(tmp_path, monkeypatch, capsys):
    make_project(
        tmp_path,
        monkeypatch,
        edges='''
        EDGES = [("A", "C"), ("B", "C"), ("C", "B")]
        ''',
        files={
            "A": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A")
                router.create_job(params={"value": "from-A"})
                @router.task
                def run(ctx, value):
                    ctx.node("C").add(source=value, depth=0)
                    return value
            ''',
            "B": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B")
                router.create_job(params={"value": "from-B", "depth": 0})
                @router.task
                def run(ctx, value, depth):
                    if depth == 0:
                        ctx.node("C").add(autostart=True, source=value, depth=0)
                    return value
            ''',
            "C": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("C")
                @router.task
                def run(ctx, source, depth):
                    ctx.write_output(f"{source}_{ctx.job_id}.txt", source)
                    if source == "from-B" and depth == 0:
                        # This ordinary edge is internal to {B,C}, so MWF
                        # upgrades it to component-autostart automatically.
                        ctx.node("B").add(value="internal-return", depth=1)
                    return source
            ''',
        },
    )
    workflow = load_workflow(tmp_path)
    assert workflow.component_for("A") == {"A"}
    assert workflow.component_for("B") == {"B", "C"}
    assert list(workflow.component_dag().edges) == [(('A',), ('B', 'C'))]

    capsys.readouterr()
    assert cli.main(["run", "B"]) == 1
    out = capsys.readouterr().out
    assert "Node B belongs to Hoeflein component {B, C}" in out
    assert "incomplete predecessor components: A" in out

    assert cli.main(["run", "A"]) == 0
    assert cli.main(["run", "B"]) == 0
    storage = FileStorage(tmp_path)
    assert storage.get_node_status("B") == "done"
    assert storage.get_node_status("C") == "done"
    assert len(storage.list_job_ids("B")) == 2
    assert len(storage.list_job_ids("C")) == 2
    internal = storage.read_job_metadata("B", 2)
    assert internal["job_kind"] == "component"
    assert internal["producer_component"] == ("B", "C")


def test_runfrom_preserves_jobs_from_unselected_producer_component(tmp_path, monkeypatch, capsys):
    make_project(
        tmp_path,
        monkeypatch,
        edges='''
        EDGES = [("A", "C"), ("B", "C")]
        ''',
        files={
            "A": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A")
                router.create_job(params={"label": "A"})
                @router.task
                def run(ctx, label):
                    ctx.node("C").add(label=label)
                    return label
            ''',
            "B": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B")
                router.create_job(params={"label": "B"})
                @router.task
                def run(ctx, label):
                    ctx.node("C").add(label=label)
                    return label
            ''',
            "C": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("C")
                @router.task
                def run(ctx, label):
                    ctx.write_output(f"{label}.txt", label)
                    return label
            ''',
        },
    )
    capsys.readouterr()
    assert cli.main(["runfrom", "A"]) == 0
    storage = FileStorage(tmp_path)
    assert storage.list_job_ids("C") == [1]
    first = storage.read_job_metadata("C", 1)
    assert first["producer_component"] == ("A",)
    assert storage.get_job_status("C", 1) == "done"

    assert cli.main(["runfrom", "B"]) == 0
    assert storage.list_job_ids("C") == [1, 2]
    assert storage.read_job_metadata("C", 1)["producer_component"] == ("A",)
    assert storage.read_job_metadata("C", 2)["producer_component"] == ("B",)
    assert storage.get_job_status("C", 1) == "done"
    assert storage.get_job_status("C", 2) == "done"

    # Repeat-use regression: B's previous C job is removed and recreated, while
    # A's original job remains untouched.
    assert cli.main(["runfrom", "B"]) == 0
    assert storage.list_job_ids("C") == [1, 2]
    assert storage.read_job_metadata("C", 1)["producer_component"] == ("A",)
    assert storage.read_job_metadata("C", 2)["producer_component"] == ("B",)


def test_one_job_failure_marks_whole_hoeflein_component_failed(tmp_path, monkeypatch):
    make_project(
        tmp_path,
        monkeypatch,
        edges='''
        EDGES = [("A", "B"), ("B", "A")]
        ''',
        files={
            "A": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A")
                router.create_job(params={"depth": 0})
                @router.task
                def run(ctx, depth):
                    if depth == 0:
                        ctx.node("B").add(autostart=True, depth=1)
                    return depth
            ''',
            "B": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B")
                @router.task
                def run(ctx, depth):
                    raise RuntimeError("component failure")
            ''',
        },
    )
    assert cli.main(["run", "A"]) == 1
    storage = FileStorage(tmp_path)
    assert storage.get_node_status("A") == "failed"
    assert storage.get_node_status("B") == "failed"


def test_quiescence_join_surfaces_late_node_worker_failure(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([("A", "B"), ("B", "A")])
    routers = []
    for node_name in ("A", "B"):
        router = NodeRouter(node_name, runner="threaded")
        router.task(lambda ctx: None)
        workflow.include_router(router)
        routers.append(router)
    workflow.add_job(None, "A")

    def finish_after_quiescence(node_name, _ignore_readiness, **kwargs):
        kwargs["_live_ready_event"].set()
        assert kwargs["_live_start_event"].wait(1)
        if node_name == "A":
            workflow.storage.set_job_status("A", 1, "done")
        assert kwargs["_stop_event"].wait(1)
        if node_name == "A":
            raise RuntimeError("late worker failure")
        return []

    monkeypatch.setattr(workflow, "run_queued_node_jobs", finish_after_quiescence)

    with pytest.raises(RuntimeError, match="late worker failure"):
        workflow.run_component({"A", "B"}, ignore_readiness=True)

    assert workflow.storage.get_node_status("A") == "failed"
    assert workflow.storage.get_node_status("B") == "failed"
