from __future__ import annotations

import textwrap
from pathlib import Path

from micro_workflow_manager import cli
from micro_workflow_manager.cli.parser import build_parser
from micro_workflow_manager.models import Job
from micro_workflow_manager.storage import FileStorage


def _write_project(tmp_path: Path, monkeypatch, *, edges: str, behaviors: dict[str, str], runner: str = "threaded") -> None:
    monkeypatch.chdir(tmp_path)
    behavior_dir = tmp_path / "src" / "node_behavior"
    behavior_dir.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(textwrap.dedent(edges).strip() + "\n", encoding="utf-8")
    for node, source in behaviors.items():
        (behavior_dir / f"{node}.py").write_text(textwrap.dedent(source).strip() + "\n", encoding="utf-8")
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", runner]) == 0


def test_resumefrom_parser_accepts_refuseafter():
    args = build_parser().parse_args(["resumefrom", "A", "refuseafter", "B", "--keeptrace"])
    assert args.command == "resumefrom"
    assert args.node == "A"
    assert args.refuse_mode == "refuseafter"
    assert args.refuse_node == "B"
    assert args.keeptrace is True


def test_resumefrom_refuseafter_resumes_boundary_then_leaves_later_work_queued(tmp_path, monkeypatch, capsys):
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B'), ('B', 'C')]",
        behaviors={
            "A": """
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A", runner="threaded", max_threads=4)
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    marker = Path(ctx.system.storage.project_dir) / "a-count.txt"
                    count = int(marker.read_text() if marker.exists() else "0") + 1
                    marker.write_text(str(count), encoding="utf-8")
                    ctx.node("B").add(value=count)
                    return count
            """,
            "B": """
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B", runner="threaded", max_threads=4)
                @router.task
                def run(ctx, value):
                    root = Path(ctx.system.storage.project_dir)
                    marker = root / "b-count.txt"
                    count = int(marker.read_text() if marker.exists() else "0") + 1
                    marker.write_text(str(count), encoding="utf-8")
                    if (root / "fail-b.txt").exists():
                        raise RuntimeError("intentional B failure")
                    ctx.node("C").add(value=value)
                    return count
            """,
            "C": """
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("C", runner="threaded", max_threads=4)
                @router.task
                def run(ctx, value):
                    marker = Path(ctx.system.storage.project_dir) / "c-count.txt"
                    count = int(marker.read_text() if marker.exists() else "0") + 1
                    marker.write_text(str(count), encoding="utf-8")
                    return count
            """,
        },
    )
    capsys.readouterr()
    (tmp_path / "fail-b.txt").write_text("1", encoding="utf-8")

    assert cli.main(["runfrom", "A"]) == 1
    capsys.readouterr()
    assert (tmp_path / "a-count.txt").read_text() == "1"
    assert (tmp_path / "b-count.txt").read_text() == "1"
    assert not (tmp_path / "c-count.txt").exists()

    (tmp_path / "fail-b.txt").unlink()
    assert cli.main(["resumefrom", "A", "refuseafter", "B"]) == 0
    output = capsys.readouterr().out
    assert "Refused further Hoeflein-component admission after {B} terminated." in output
    assert (tmp_path / "a-count.txt").read_text() == "1"  # done work preserved
    assert (tmp_path / "b-count.txt").read_text() == "2"  # failed boundary resumed
    assert not (tmp_path / "c-count.txt").exists()

    storage = FileStorage(tmp_path)
    assert storage.list_job_ids("C") == [1]
    assert storage.get_job_status("C", 1) == "queued"

    # A later ordinary resumefrom can consume exactly the queued remainder.
    assert cli.main(["resumefrom", "C"]) == 0
    capsys.readouterr()
    assert (tmp_path / "c-count.txt").read_text() == "1"


def test_resumefrom_refuseafter_plan_and_invalid_boundary_are_read_only(tmp_path, monkeypatch, capsys):
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B'), ('B', 'C'), ('X', 'Y')]",
        runner="direct",
        behaviors={
            name: f'''\n                from micro_workflow_manager import NodeRouter\n                router = NodeRouter("{name}")\n                {"router.create_job(number=1)" if name in {"A", "X"} else ""}\n                @router.task\n                def run(ctx): return "{name}"\n            '''
            for name in ("A", "B", "C", "X", "Y")
        },
    )
    capsys.readouterr()

    assert cli.main(["resumefrom", "A", "refuseafter", "B", "--plan"]) == 0
    plan = capsys.readouterr().out
    assert "Plan for: mwf resumefrom A" in plan
    assert "refusal boundary: stop admitting new components after {B} terminates" in plan
    assert "resume scope: unchanged" in plan

    storage = FileStorage(tmp_path)
    before = {node: storage.node_job_summary(node) for node in ("A", "B", "C", "X", "Y")}
    assert cli.main(["resumefrom", "A", "refuseafter", "Y"]) == 1
    error = capsys.readouterr().err
    assert "refuseafter node 'Y' is not in the resumefrom selection starting at 'A'" in error
    after = {node: storage.node_job_summary(node) for node in ("A", "B", "C", "X", "Y")}
    assert after == before


def test_resumefrom_refuseafter_uses_whole_hoeflein_boundary_component(tmp_path, monkeypatch, capsys):
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B'), ('B', 'D'), ('D', 'B'), ('D', 'C')]",
        runner="threaded",
        behaviors={
            "A": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A", runner="threaded", max_threads=2)
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    ctx.node("B").add(step=0)
            """,
            "B": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B", runner="threaded", max_threads=2)
                @router.task
                def run(ctx, step):
                    ctx.node("D").add(step=step)
            """,
            "D": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("D", runner="threaded", max_threads=2)
                @router.task
                def run(ctx, step):
                    if step < 1:
                        ctx.node("B").add(step=step + 1)
                    ctx.node("C").add(step=step)
            """,
            "C": """
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("C", runner="threaded", max_threads=2)
                @router.task
                def run(ctx, step):
                    Path(ctx.system.storage.project_dir, "c-ran.txt").write_text(str(step), encoding="utf-8")
            """,
        },
    )
    capsys.readouterr()

    # Fresh run first, then add fresh queued work at A so resumefrom has work to do.
    assert cli.main(["runfrom", "A"]) == 0
    capsys.readouterr()
    (tmp_path / "c-ran.txt").unlink()
    storage = FileStorage(tmp_path)
    storage.create_job(Job(node_name="A", job_id=2, params={}))

    assert cli.main(["resumefrom", "A", "refuseafter", "B"]) == 0
    output = capsys.readouterr().out
    # B names the SCC {B,D}; the whole component is the inclusive boundary.
    assert "Refused further Hoeflein-component admission after {B, D} terminated." in output
    assert not (tmp_path / "c-ran.txt").exists()
    assert storage.has_queued_jobs("C")
