from __future__ import annotations

import textwrap
from pathlib import Path

from micro_workflow_manager import MicroWorkflow, NodeRouter, cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.storage import FileStorage


def _make_cli_project(
    tmp_path: Path,
    monkeypatch,
    *,
    runner: str = "direct",
) -> None:
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        "EDGES = [('A', 'B'), ('B', 'C'), ('C', 'A')]\n",
        encoding="utf-8",
    )
    files = {
        "A": '''
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("A", wait_for=["B"])
            router.create_job(number=1)
            @router.task
            def run(ctx):
                ctx.write("A.txt", "A")
                return "A"
        ''',
        "B": '''
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("B", wait_for=["C"])
            router.create_job(number=1)
            @router.task
            def run(ctx):
                ctx.write("B.txt", "B")
                return "B"
        ''',
        "C": '''
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("C", wait_for=["B"])
            router.create_job(number=1)
            @router.task
            def run(ctx):
                ctx.write("C.txt", "C")
                return "C"
        ''',
    }
    for node_name, source in files.items():
        (behavior / f"{node_name}.py").write_text(
            textwrap.dedent(source).strip() + "\n",
            encoding="utf-8",
        )
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", runner]) == 0


def test_run_prompts_again_only_when_recalculation_finds_another_deadlock(
    tmp_path,
    monkeypatch,
    capsys,
):
    _make_cli_project(tmp_path, monkeypatch, runner="direct")
    capsys.readouterr()

    answers = iter(["A", "B"])
    monkeypatch.setattr("builtins.input", lambda _prompt: next(answers))

    assert cli.main(["run", "A"]) == 0
    output = capsys.readouterr().out
    assert output.count("Waiting deadlock in Hoeflein component {A, B, C}") == 2
    assert "Temporarily overriding waiting for node A." in output
    assert "Temporarily overriding waiting for node B." in output

    storage = FileStorage(tmp_path)
    assert storage.get_node_status("A") == "done"
    assert storage.get_node_status("B") == "done"
    assert storage.get_node_status("C") == "done"
    assert storage.get_job_status("A", 1) == "done"
    assert storage.get_job_status("B", 1) == "done"
    assert storage.get_job_status("C", 1) == "done"


def test_runfrom_accepts_numeric_deadlock_choice_and_returns_to_normal_waiting(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        "EDGES = [('A', 'B'), ('B', 'A')]\n",
        encoding="utf-8",
    )
    for node_name, wait_for in (("A", "B"), ("B", "A")):
        (behavior / f"{node_name}.py").write_text(
            textwrap.dedent(
                f'''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("{node_name}", wait_for=["{wait_for}"])
                router.create_job(number=1)
                @router.task
                def run(ctx): return "{node_name}"
                '''
            ).strip()
            + "\n",
            encoding="utf-8",
        )
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0
    capsys.readouterr()

    monkeypatch.setattr("builtins.input", lambda _prompt: "2")
    assert cli.main(["runfrom", "A"]) == 0
    output = capsys.readouterr().out
    assert output.count("Waiting deadlock in Hoeflein component {A, B}") == 1
    assert "Temporarily overriding waiting for node B." in output

    storage = FileStorage(tmp_path)
    assert storage.get_job_status("A", 1) == "done"
    assert storage.get_job_status("B", 1) == "done"


def test_threaded_component_reprompts_after_override_drains_and_deadlock_remains(
    tmp_path,
):
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([("A", "B"), ("B", "C"), ("C", "A")])

    routers = {
        "A": NodeRouter("A", max_threads=1, runner="threaded", wait_for=["B"]),
        "B": NodeRouter("B", max_threads=1, runner="threaded", wait_for=["C"]),
        "C": NodeRouter("C", max_threads=1, runner="threaded", wait_for=["B"]),
    }
    for router in routers.values():
        router.create_job(number=1)

    @routers["A"].task
    def run_a(ctx):
        return "A"

    @routers["B"].task
    def run_b(ctx):
        return "B"

    @routers["C"].task
    def run_c(ctx):
        return "C"

    workflow.include_routers(*routers.values())
    choices = iter(["A", "B"])
    calls: list[tuple[tuple[str, ...], tuple[str, ...], dict[str, tuple[str, ...]]]] = []

    def resolver(component_nodes, queued_nodes, blockers):
        calls.append((component_nodes, queued_nodes, blockers))
        return next(choices)

    ran = workflow.run_component(
        {"A", "B", "C"},
        ignore_readiness=True,
        wait_deadlock_resolver=resolver,
    )

    assert ran == ["A", "B", "C"]
    assert [call[1] for call in calls] == [("A", "B", "C"), ("B", "C")]
    assert calls[0][2] == {"A": ("B",), "B": ("C",), "C": ("B",)}
    assert calls[1][2] == {"B": ("C",), "C": ("B",)}


def test_declining_deadlock_override_stops_without_resubmission_loop(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        "EDGES = [('A', 'B'), ('B', 'A')]\n",
        encoding="utf-8",
    )
    for node_name, wait_for in (("A", "B"), ("B", "A")):
        (behavior / f"{node_name}.py").write_text(
            textwrap.dedent(
                f'''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("{node_name}", runner="threaded", wait_for=["{wait_for}"])
                router.create_job(number=1)
                @router.task
                def run(ctx): return "{node_name}"
                '''
            ).strip()
            + "\n",
            encoding="utf-8",
        )
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "threaded"]) == 0
    capsys.readouterr()

    monkeypatch.setattr("builtins.input", lambda _prompt: "q")
    assert cli.main(["run", "A"]) == 1
    output = capsys.readouterr().out
    assert output.count("Waiting deadlock in Hoeflein component {A, B}") == 1
    assert "Leaving the Hoeflein component blocked." in output
    assert "Stopped before these queued nodes became ready:" in output


def test_resume_uses_same_repeated_deadlock_override_as_run(
    tmp_path,
    monkeypatch,
    capsys,
):
    _make_cli_project(tmp_path, monkeypatch, runner="direct")
    capsys.readouterr()

    answers = iter(["A", "B"])
    monkeypatch.setattr("builtins.input", lambda _prompt: next(answers))

    assert cli.main(["resume", "A"]) == 0
    output = capsys.readouterr().out
    assert output.count("Waiting deadlock in Hoeflein component {A, B, C}") == 2
    assert "Temporarily overriding waiting for node A." in output
    assert "Temporarily overriding waiting for node B." in output

    storage = FileStorage(tmp_path)
    assert storage.get_job_status("A", 1) == "done"
    assert storage.get_job_status("B", 1) == "done"
    assert storage.get_job_status("C", 1) == "done"


def test_resumefrom_accepts_numeric_deadlock_choice_like_runfrom(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        "EDGES = [('A', 'B'), ('B', 'A')]\n",
        encoding="utf-8",
    )
    for node_name, wait_for in (("A", "B"), ("B", "A")):
        (behavior / f"{node_name}.py").write_text(
            textwrap.dedent(
                f'''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("{node_name}", wait_for=["{wait_for}"])
                router.create_job(number=1)
                @router.task
                def run(ctx): return "{node_name}"
                '''
            ).strip()
            + "\n",
            encoding="utf-8",
        )
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0
    capsys.readouterr()

    monkeypatch.setattr("builtins.input", lambda _prompt: "2")
    assert cli.main(["resumefrom", "A"]) == 0
    output = capsys.readouterr().out
    assert output.count("Waiting deadlock in Hoeflein component {A, B}") == 1
    assert "Temporarily overriding waiting for node B." in output

    storage = FileStorage(tmp_path)
    assert storage.get_job_status("A", 1) == "done"
    assert storage.get_job_status("B", 1) == "done"


def test_resume_declining_deadlock_override_leaves_component_blocked_once(
    tmp_path,
    monkeypatch,
    capsys,
):
    _make_cli_project(tmp_path, monkeypatch, runner="threaded")
    capsys.readouterr()

    monkeypatch.setattr("builtins.input", lambda _prompt: "q")
    assert cli.main(["resume", "A"]) == 1
    output = capsys.readouterr().out
    assert output.count("Waiting deadlock in Hoeflein component {A, B, C}") == 1
    assert "Leaving the Hoeflein component blocked." in output
    assert "Stopped before these queued nodes became ready:" in output

def test_unavailable_stdin_leaves_waiting_deadlock_blocked_instead_of_crashing(
    tmp_path,
    monkeypatch,
    capsys,
):
    _make_cli_project(tmp_path, monkeypatch, runner="threaded")
    capsys.readouterr()

    def unavailable(_prompt):
        raise OSError("pytest-style captured stdin is unavailable")

    monkeypatch.setattr("builtins.input", unavailable)
    assert cli.main(["run", "A"]) == 1
    output = capsys.readouterr().out
    assert output.count("Waiting deadlock in Hoeflein component {A, B, C}") == 1
    assert "No interactive input available; leaving the component blocked." in output
    assert "Stopped before these queued nodes became ready:" in output

