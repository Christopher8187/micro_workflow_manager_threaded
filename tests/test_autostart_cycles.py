
import json
import textwrap
from pathlib import Path

from micro_workflow_manager import cli


def make_cycle_project(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        """
EDGES = [
    ("A", "A"),
    ("A", "B"),
    ("A", "C"),
    ("B", "A"),
    ("C", "A"),
    ("A", "D"),
]
""".strip(),
        encoding="utf-8",
    )
    (behavior / "A.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeRouter

            router = NodeRouter("A")
            router.create_job(params={"kind": "seed", "depth": 0, "trace": "seed"})

            @router.task
            def run(ctx, kind, depth, trace):
                ctx.write(f"A_{ctx.job_id}.txt", trace)
                ctx.node("D").add(from_a_job=ctx.job_id, kind=kind, depth=depth, trace=trace)
                if kind == "seed":
                    ctx.node("A").add(autostart=True, kind="self", depth=depth + 1, trace=f"{trace}->A")
                    ctx.node("B").add(autostart=True, depth=depth + 1, trace=f"{trace}->B")
                    ctx.node("C").add(autostart=True, depth=depth + 1, trace=f"{trace}->C")
                return trace
            """
        ).strip(),
        encoding="utf-8",
    )
    (behavior / "B.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeRouter

            router = NodeRouter("B")

            @router.task
            def run(ctx, depth, trace):
                ctx.write(f"B_{ctx.job_id}.txt", trace)
                ctx.node("A").add(autostart=True, kind="from_B", depth=depth + 1, trace=f"{trace}->A")
                return trace
            """
        ).strip(),
        encoding="utf-8",
    )
    (behavior / "C.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeRouter

            router = NodeRouter("C")

            @router.task
            def run(ctx, depth, trace):
                ctx.write(f"C_{ctx.job_id}.txt", trace)
                ctx.node("A").add(autostart=True, kind="from_C", depth=depth + 1, trace=f"{trace}->A")
                return trace
            """
        ).strip(),
        encoding="utf-8",
    )
    (behavior / "D.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeRouter

            router = NodeRouter("D")

            @router.task
            def run(ctx, from_a_job, kind, depth, trace):
                ctx.write(f"D_{ctx.job_id}.txt", trace)
                return trace
            """
        ).strip(),
        encoding="utf-8",
    )

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0


def node_status(tmp_path: Path, node: str) -> str:
    return json.loads((tmp_path / "node" / node / "node_state.json").read_text())["status"]


def test_runfrom_supports_self_and_mutual_autostart_cycles_before_downstream(tmp_path, monkeypatch, capsys):
    make_cycle_project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main(["runfrom", "A", "--runner", "direct"]) == 0
    out = capsys.readouterr().out

    assert "Ran:" in out
    assert "  A" in out
    assert "  B" in out
    assert "  C" in out
    assert "  D" in out

    assert node_status(tmp_path, "A") == "done"
    assert node_status(tmp_path, "B") == "done"
    assert node_status(tmp_path, "C") == "done"
    assert node_status(tmp_path, "D") == "done"

    assert sorted(path.name for path in (tmp_path / "node" / "A" / "jobs").iterdir()) == ["1", "2", "3", "4"]
    assert sorted(path.name for path in (tmp_path / "node" / "B" / "jobs").iterdir()) == ["1"]
    assert sorted(path.name for path in (tmp_path / "node" / "C" / "jobs").iterdir()) == ["1"]
    assert sorted(path.name for path in (tmp_path / "node" / "D" / "jobs").iterdir()) == ["1", "2", "3", "4"]


def test_same_component_immediate_autostart_is_deferred_not_nested(tmp_path):
    from micro_workflow_manager import MicroWorkflow

    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B"), ("B", "A")])
    events = []

    @workflow.task("A")
    def run_a(ctx):
        events.append("A-start")
        ctx.node("B").add(autostart=True)
        events.append("A-end")
        return "A"

    @workflow.task("B")
    def run_b(ctx):
        events.append("B")
        return "B"

    workflow.start("A")
    workflow.run()

    assert events == ["A-start", "A-end", "B"]
    assert workflow.node_complete("A")
    assert workflow.node_complete("B")


def test_threaded_runfrom_pumps_cyclic_autostart_component_before_downstream(tmp_path, monkeypatch, capsys):
    make_cycle_project(tmp_path, monkeypatch)
    assert cli.main(["graph", "src/graph.py", "--runner", "threaded"]) == 0
    capsys.readouterr()

    assert cli.main(["runfrom", "A", "--runner", "threaded"]) == 0
    out = capsys.readouterr().out

    assert "Ran:" in out
    assert node_status(tmp_path, "A") == "done"
    assert node_status(tmp_path, "B") == "done"
    assert node_status(tmp_path, "C") == "done"
    assert node_status(tmp_path, "D") == "done"

    assert sorted(path.name for path in (tmp_path / "node" / "A" / "jobs").iterdir()) == ["1", "2", "3", "4"]
    assert sorted(path.name for path in (tmp_path / "node" / "D" / "jobs").iterdir()) == ["1", "2", "3", "4"]
