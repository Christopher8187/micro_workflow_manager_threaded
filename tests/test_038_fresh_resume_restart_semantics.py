from __future__ import annotations

import os
import textwrap
from pathlib import Path

from micro_workflow_manager import cli
from micro_workflow_manager.storage import FileStorage


def _write_project(
    tmp_path: Path,
    monkeypatch,
    *,
    edges: str,
    behaviors: dict[str, str],
    runner: str = "direct",
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


def _count(path: Path) -> int:
    return int(path.read_text(encoding="utf-8"))


def test_run_resets_parent_created_jobs_before_running_with_monitor(
    tmp_path, monkeypatch, capsys
):
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B')]",
        behaviors={
            "A": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A")
                router.create_job(params={"value": 1})
                @router.task
                def run(ctx, value):
                    ctx.node("B").add(value=value)
                    return value
            """,
            "B": """
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B")
                @router.task
                def run(ctx, value):
                    path = Path(ctx.system.storage.project_dir) / "b-count.txt"
                    count = int(path.read_text() if path.exists() else "0") + 1
                    path.write_text(str(count), encoding="utf-8")
                    ctx.sleep(0.04)
                    return count
            """,
        },
    )
    capsys.readouterr()

    assert cli.main(["runfrom", "A", "--monitor", "--monitor-interval", "0.01"]) == 0
    capsys.readouterr()
    assert _count(tmp_path / "b-count.txt") == 1

    assert cli.main(["run", "B", "--monitor", "--monitor-interval", "0.01"]) == 0
    captured = capsys.readouterr()
    assert _count(tmp_path / "b-count.txt") == 2
    assert "active run: run B" in captured.err
    assert "last run: run B | status=done" in captured.err
    assert "No queued jobs for B" not in captured.out


def test_runfrom_freshens_start_component_and_preserves_other_merge_branch(
    tmp_path, monkeypatch, capsys
):
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('P', 'A'), ('A', 'C'), ('Q', 'C')]",
        behaviors={
            "P": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("P")
                router.create_job(params={"value": "from-P"})
                @router.task
                def run(ctx, value):
                    ctx.node("A").add(value=value)
                    return value
            """,
            "A": """
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A")
                @router.task
                def run(ctx, value):
                    root = Path(ctx.system.storage.project_dir)
                    path = root / "a-count.txt"
                    count = int(path.read_text() if path.exists() else "0") + 1
                    path.write_text(str(count), encoding="utf-8")
                    ctx.node("C").add(label="A")
                    ctx.sleep(0.03)
                    return value
            """,
            "Q": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("Q")
                router.create_job(params={"label": "Q"})
                @router.task
                def run(ctx, label):
                    ctx.node("C").add(label=label)
                    return label
            """,
            "C": """
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("C")
                @router.task
                def run(ctx, label):
                    root = Path(ctx.system.storage.project_dir)
                    path = root / f"c-{label}-count.txt"
                    count = int(path.read_text() if path.exists() else "0") + 1
                    path.write_text(str(count), encoding="utf-8")
                    ctx.sleep(0.03)
                    return label
            """,
        },
    )
    capsys.readouterr()

    assert cli.main(["runfrom", "P"]) == 0
    capsys.readouterr()
    assert cli.main(["runfrom", "Q"]) == 0
    capsys.readouterr()
    assert _count(tmp_path / "a-count.txt") == 1
    assert _count(tmp_path / "c-A-count.txt") == 1
    assert _count(tmp_path / "c-Q-count.txt") == 1

    assert cli.main(["runfrom", "A", "--monitor", "--monitor-interval", "0.01"]) == 0
    captured = capsys.readouterr()
    assert _count(tmp_path / "a-count.txt") == 2
    assert _count(tmp_path / "c-A-count.txt") == 2
    assert _count(tmp_path / "c-Q-count.txt") == 1
    assert "active run: runfrom A" in captured.err
    assert "last run: runfrom A | status=done" in captured.err

    storage = FileStorage(tmp_path)
    c_jobs = [storage.load_job("C", job_id) for job_id in storage.list_job_ids("C")]
    assert sorted(job.params["label"] for job in c_jobs) == ["A", "Q"]
    assert all(storage.get_job_status("C", job.job_id) == "done" for job in c_jobs)


def test_resumefrom_requeues_failed_descendant_without_prior_restart_and_monitors(
    tmp_path, monkeypatch, capsys
):
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B')]",
        behaviors={
            "A": """
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A")
                router.create_job(number=2)
                @router.task
                def run(ctx):
                    root = Path(ctx.system.storage.project_dir)
                    path = root / f"a-{ctx.job_id}-count.txt"
                    count = int(path.read_text() if path.exists() else "0") + 1
                    path.write_text(str(count), encoding="utf-8")
                    ctx.node("B").add(value=ctx.job_id)
                    return ctx.job_id
            """,
            "B": """
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B")
                @router.task
                def run(ctx, value):
                    root = Path(ctx.system.storage.project_dir)
                    path = root / f"b-{value}-attempts.txt"
                    attempts = int(path.read_text() if path.exists() else "0") + 1
                    path.write_text(str(attempts), encoding="utf-8")
                    ctx.sleep(0.03)
                    if value == 2 and not (root / "allow-two.flag").exists():
                        raise RuntimeError("fail value two once")
                    return value
            """,
        },
    )
    capsys.readouterr()

    assert cli.main(["runfrom", "A", "--monitor", "--monitor-interval", "0.01"]) == 1
    first = capsys.readouterr()
    assert "last run: runfrom A | status=failed" in first.err
    storage = FileStorage(tmp_path)
    failed_id = next(
        job_id
        for job_id in storage.list_job_ids("B")
        if storage.get_job_status("B", job_id) == "failed"
    )
    assert storage.load_job("B", failed_id).params["value"] == 2
    generation_before = storage.current_job_generation("B", failed_id)

    (tmp_path / "allow-two.flag").write_text("yes", encoding="utf-8")
    assert cli.main(["resumefrom", "A", "--monitor", "--monitor-interval", "0.01"]) == 0
    resumed = capsys.readouterr()

    assert _count(tmp_path / "a-1-count.txt") == 1
    assert _count(tmp_path / "a-2-count.txt") == 1
    assert _count(tmp_path / "b-1-attempts.txt") == 1
    assert _count(tmp_path / "b-2-attempts.txt") == 2
    assert storage.current_job_generation("B", failed_id) == generation_before + 1
    assert storage.get_job_status("B", failed_id) == "done"
    assert "active run: resumefrom A" in resumed.err
    assert "last run: resumefrom A | status=done" in resumed.err


def test_restart_is_active_run_only_and_resume_is_post_failure_path(
    tmp_path, monkeypatch, capsys
):
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B')]",
        behaviors={
            "A": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A")
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    raise RuntimeError("boom")
            """,
            "B": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B")
                @router.task
                def run(ctx):
                    return None
            """,
        },
    )
    capsys.readouterr()
    assert cli.main(["run", "A", "--monitor", "--monitor-interval", "0.01"]) == 1
    capsys.readouterr()

    storage = FileStorage(tmp_path)
    assert storage.get_job_status("A", 1) == "failed"
    assert cli.main(["restart", "A", "job", "1"]) == 1
    error = capsys.readouterr().err
    assert "second terminal" in error
    assert "resumefrom" in error
    assert storage.get_job_status("A", 1) == "failed"

    storage.write_run_state(
        {
            "run_id": "live-test-run",
            "status": "running",
            "command": "runfrom",
            "start_node": "A",
            "nodes": ["A", "B"],
            "pid": os.getpid(),
        }
    )
    assert cli.main(["restart", "A", "job", "1"]) == 0
    output = capsys.readouterr().out
    assert "failed-job retry" in output
    assert "existing run remains in control" in output
    assert storage.get_job_status("A", 1) == "queued"
