from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.parser import build_parser
from micro_workflow_manager.storage import FileStorage


def _write_project(
    tmp_path: Path,
    monkeypatch,
    *,
    edges: str,
    behaviors: dict[str, str],
    runner: str = "threaded",
) -> None:
    monkeypatch.chdir(tmp_path)
    behavior_dir = tmp_path / "src" / "node_behavior"
    behavior_dir.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        textwrap.dedent(edges).strip() + "\n",
        encoding="utf-8",
    )
    for node, source in behaviors.items():
        (behavior_dir / f"{node}.py").write_text(
            textwrap.dedent(source).strip() + "\n",
            encoding="utf-8",
        )
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", runner]) == 0


def _linear_behaviors(runner: str = "threaded") -> dict[str, str]:
    return {
        "A": f'''
            from pathlib import Path
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("A", runner="{runner}", max_threads=2)
            router.create_job(number=1)
            @router.task
            def run(ctx):
                Path(ctx.system.storage.project_dir, "a-ran.txt").write_text("yes")
                ctx.node("B").add()
        ''',
        "B": f'''
            from pathlib import Path
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("B", runner="{runner}", max_threads=2)
            @router.task
            def run(ctx):
                Path(ctx.system.storage.project_dir, "b-ran.txt").write_text("yes")
                ctx.node("C").add()
        ''',
        "C": f'''
            from pathlib import Path
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("C", runner="{runner}", max_threads=2)
            @router.task
            def run(ctx):
                Path(ctx.system.storage.project_dir, "c-ran.txt").write_text("yes")
        ''',
    }


@pytest.mark.parametrize("command", ["runfrom", "resumefrom"])
@pytest.mark.parametrize("mode", ["refuse", "refuseafter"])
def test_parser_keeps_exclusive_refuse_distinct_from_inclusive_refuseafter(
    command, mode
):
    args = build_parser().parse_args([command, "A", mode, "B", "--keeptrace"])
    assert args.command == command
    assert args.node == "A"
    assert args.refuse_mode == mode
    assert args.refuse_node == "B"
    assert args.keeptrace is True


@pytest.mark.parametrize("runner", ["direct", "threaded"])
def test_runfrom_refuse_stops_before_boundary_and_preserves_queued_job(
    tmp_path, monkeypatch, capsys, runner
):
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B'), ('B', 'C')]",
        behaviors=_linear_behaviors(runner),
        runner=runner,
    )
    capsys.readouterr()

    assert cli.main(["runfrom", "A", "refuse", "B"]) == 0
    output = capsys.readouterr().out
    assert "Refused Hoeflein-component admission before {B} started." in output
    assert (tmp_path / "a-ran.txt").exists()
    assert not (tmp_path / "b-ran.txt").exists()
    assert not (tmp_path / "c-ran.txt").exists()

    storage = FileStorage(tmp_path)
    assert storage.list_job_ids("B") == [1]
    assert storage.get_job_status("B", 1) == "queued"
    assert storage.list_job_ids("C") == []
    run_state = storage.get_run_state()
    assert run_state["status"] == "done"
    assert run_state["refuse_before_node"] == "B"
    assert run_state["refuse_after_node"] is None

    # An ordinary resume from the boundary consumes the preserved remainder.
    assert cli.main(["resumefrom", "B"]) == 0
    capsys.readouterr()
    assert (tmp_path / "b-ran.txt").exists()
    assert (tmp_path / "c-ran.txt").exists()


def test_runfrom_refuse_is_global_for_components_ready_in_same_wave(
    tmp_path, monkeypatch, capsys
):
    behaviors = {
        "A": '''
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("A", runner="threaded", max_threads=2)
            router.create_job(number=1)
            @router.task
            def run(ctx):
                ctx.node("B").add()
                ctx.node("X").add()
        ''',
        "B": '''
            from pathlib import Path
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("B", runner="threaded", max_threads=2)
            @router.task
            def run(ctx): Path(ctx.system.storage.project_dir, "b-ran.txt").write_text("yes")
        ''',
        "X": '''
            from pathlib import Path
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("X", runner="threaded", max_threads=2)
            @router.task
            def run(ctx):
                Path(ctx.system.storage.project_dir, "x-ran.txt").write_text("yes")
                ctx.node("Y").add()
        ''',
        "Y": '''
            from pathlib import Path
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("Y", runner="threaded", max_threads=2)
            @router.task
            def run(ctx): Path(ctx.system.storage.project_dir, "y-ran.txt").write_text("yes")
        ''',
    }
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B'), ('A', 'X'), ('X', 'Y')]",
        behaviors=behaviors,
    )
    capsys.readouterr()

    assert cli.main(["runfrom", "A", "refuse", "B"]) == 0
    output = capsys.readouterr().out
    assert "Refused Hoeflein-component admission before {B} started." in output
    assert not (tmp_path / "b-ran.txt").exists()
    assert not (tmp_path / "x-ran.txt").exists()
    assert not (tmp_path / "y-ran.txt").exists()
    storage = FileStorage(tmp_path)
    assert storage.has_queued_jobs("B")
    assert storage.has_queued_jobs("X")


def test_refuse_joins_already_running_branch_but_never_admits_its_late_child(
    tmp_path, monkeypatch, capsys
):
    behaviors = {
        "A": '''
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("A", runner="threaded", max_threads=2)
            router.create_job(number=1)
            @router.task
            def run(ctx):
                ctx.node("P").add()
                ctx.node("X").add()
        ''',
        "P": '''
            import time
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("P", runner="threaded", max_threads=2)
            @router.task
            def run(ctx):
                time.sleep(0.05)
                ctx.node("B").add()
        ''',
        "B": '''
            from pathlib import Path
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("B", runner="threaded", max_threads=2)
            @router.task
            def run(ctx): Path(ctx.system.storage.project_dir, "b-ran.txt").write_text("yes")
        ''',
        "X": '''
            import time
            from pathlib import Path
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("X", runner="threaded", max_threads=2)
            @router.task
            def run(ctx):
                time.sleep(0.2)
                Path(ctx.system.storage.project_dir, "x-finished.txt").write_text("yes")
                ctx.node("Y").add()
        ''',
        "Y": '''
            from pathlib import Path
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("Y", runner="threaded", max_threads=2)
            @router.task
            def run(ctx): Path(ctx.system.storage.project_dir, "y-ran.txt").write_text("yes")
        ''',
    }
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'P'), ('P', 'B'), ('A', 'X'), ('X', 'Y')]",
        behaviors=behaviors,
    )
    capsys.readouterr()

    assert cli.main(["runfrom", "A", "refuse", "B"]) == 0
    output = capsys.readouterr().out
    assert "Refused Hoeflein-component admission before {B} started." in output
    assert not (tmp_path / "b-ran.txt").exists()
    assert (tmp_path / "x-finished.txt").exists()  # non-preemptive join
    assert not (tmp_path / "y-ran.txt").exists()
    storage = FileStorage(tmp_path)
    assert storage.has_queued_jobs("B")
    assert storage.has_queued_jobs("Y")


def test_refuse_names_the_whole_hoeflein_component_without_starting_any_member(
    tmp_path, monkeypatch, capsys
):
    behaviors = {
        "A": '''
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("A", runner="threaded", max_threads=2)
            router.create_job(number=1)
            @router.task
            def run(ctx): ctx.node("B").add(step=0)
        ''',
        "B": '''
            from pathlib import Path
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("B", runner="threaded", max_threads=2)
            @router.task
            def run(ctx, step):
                Path(ctx.system.storage.project_dir, "b-ran.txt").write_text("yes")
                ctx.node("D").add(step=step)
        ''',
        "D": '''
            from pathlib import Path
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("D", runner="threaded", max_threads=2)
            @router.task
            def run(ctx, step):
                Path(ctx.system.storage.project_dir, "d-ran.txt").write_text("yes")
                if step < 1: ctx.node("B").add(step=step + 1)
                ctx.node("C").add()
        ''',
        "C": '''
            from pathlib import Path
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("C", runner="threaded", max_threads=2)
            @router.task
            def run(ctx): Path(ctx.system.storage.project_dir, "c-ran.txt").write_text("yes")
        ''',
    }
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B'), ('B', 'D'), ('D', 'B'), ('D', 'C')]",
        behaviors=behaviors,
    )
    capsys.readouterr()

    assert cli.main(["runfrom", "A", "refuse", "D"]) == 0
    output = capsys.readouterr().out
    assert "Refused Hoeflein-component admission before {B, D} started." in output
    assert not (tmp_path / "b-ran.txt").exists()
    assert not (tmp_path / "d-ran.txt").exists()
    assert not (tmp_path / "c-ran.txt").exists()
    storage = FileStorage(tmp_path)
    assert storage.has_queued_jobs("B")


def test_resumefrom_refuse_does_not_run_queued_boundary_or_later_work(
    tmp_path, monkeypatch, capsys
):
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B'), ('B', 'C')]",
        behaviors=_linear_behaviors(),
    )
    capsys.readouterr()
    assert cli.main(["runfrom", "A", "refuse", "B"]) == 0
    capsys.readouterr()

    assert cli.main(["resumefrom", "A", "refuse", "B"]) == 0
    output = capsys.readouterr().out
    assert "Refused Hoeflein-component admission before {B} started." in output
    assert not (tmp_path / "b-ran.txt").exists()
    storage = FileStorage(tmp_path)
    assert storage.get_job_status("B", 1) == "queued"


def test_refuse_can_name_start_component_and_plan_is_read_only(
    tmp_path, monkeypatch, capsys
):
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B'), ('B', 'C')]",
        behaviors=_linear_behaviors("direct"),
        runner="direct",
    )
    capsys.readouterr()

    assert cli.main(["runfrom", "A", "refuse", "A", "--plan"]) == 0
    plan = capsys.readouterr().out
    assert "refusal boundary: stop before admitting {A}" in plan
    assert "the boundary component does not run" in plan
    assert not (tmp_path / "a-ran.txt").exists()

    assert cli.main(["runfrom", "A", "refuse", "A"]) == 0
    output = capsys.readouterr().out
    assert "Refused Hoeflein-component admission before {A} started." in output
    assert not (tmp_path / "a-ran.txt").exists()
    storage = FileStorage(tmp_path)
    assert storage.get_job_status("A", 1) == "queued"


def test_refuse_rejects_boundary_outside_selection_without_mutation(
    tmp_path, monkeypatch, capsys
):
    behaviors = _linear_behaviors("direct")
    behaviors["X"] = '''
        from micro_workflow_manager import NodeRouter
        router = NodeRouter("X")
        router.create_job(number=1)
        @router.task
        def run(ctx): return "X"
    '''
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B'), ('B', 'C'), ('X', 'X')]",
        behaviors=behaviors,
        runner="direct",
    )
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    before = {node: storage.node_job_summary(node) for node in ("A", "B", "C", "X")}

    assert cli.main(["runfrom", "A", "refuse", "X"]) == 1
    error = capsys.readouterr().err
    assert "refuse node 'X' is not in the runfrom selection starting at 'A'" in error
    after = {node: storage.node_job_summary(node) for node in ("A", "B", "C", "X")}
    assert after == before
