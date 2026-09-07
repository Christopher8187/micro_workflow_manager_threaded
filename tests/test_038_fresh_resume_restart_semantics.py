from __future__ import annotations

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


def _terminal_session(storage, command, start_component, selected_components, outcome):
    matches = [
        session for session in storage.list_execution_sessions()
        if session["command"] == command and session["start_component"] == start_component
    ]
    assert len(matches) == 1
    session = matches[0]
    assert session["session_kind"] == "main"
    assert session["parent_session_id"] is None
    assert session["status"] == "terminal"
    assert session["outcome"] == outcome
    assert session["selected_components"] == selected_components
    assert session["selected_jobs"] == []
    return session


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
    storage = FileStorage(tmp_path)
    _terminal_session(storage, "runfrom", ("A",), [("A",), ("B",)], "done")
    session = _terminal_session(storage, "run", ("B",), [("B",)], "done")
    assert (
        f"session={session['session_id']} kind=main command=run status=running "
        "parent=- components=[B]"
    ) in captured.err
    assert (
        f"session={session['session_id']} kind=main command=run status=terminal "
        "parent=- components=[B] outcome=done"
    ) in captured.err
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

    # Establish Q through a real native component execution. C remains outside
    # this selection but retains the Q-produced job for the later merge.
    assert cli.main(["run", "Q"]) == 0
    capsys.readouterr()
    assert cli.main(["runfrom", "P"]) == 0
    capsys.readouterr()
    assert _count(tmp_path / "a-count.txt") == 1
    assert _count(tmp_path / "c-A-count.txt") == 1
    assert _count(tmp_path / "c-Q-count.txt") == 1

    assert cli.main(["runfrom", "A", "--monitor", "--monitor-interval", "0.01"]) == 0
    captured = capsys.readouterr()
    assert _count(tmp_path / "a-count.txt") == 2
    assert _count(tmp_path / "c-A-count.txt") == 2
    assert _count(tmp_path / "c-Q-count.txt") == 1
    storage = FileStorage(tmp_path)
    _terminal_session(storage, "run", ("Q",), [("Q",)], "done")
    _terminal_session(storage, "runfrom", ("P",), [("P",), ("A",), ("C",)], "done")
    session = _terminal_session(storage, "runfrom", ("A",), [("A",), ("C",)], "done")
    assert (
        f"session={session['session_id']} kind=main command=runfrom status=running "
        "parent=- components=[A; C]"
    ) in captured.err
    assert (
        f"session={session['session_id']} kind=main command=runfrom status=terminal "
        "parent=- components=[A; C] outcome=done"
    ) in captured.err
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
    storage = FileStorage(tmp_path)
    failed_session = _terminal_session(
        storage, "runfrom", ("A",), [("A",), ("B",)], "failed"
    )
    assert (
        f"session={failed_session['session_id']} kind=main command=runfrom status=terminal "
        "parent=- components=[A; B] outcome=failed"
    ) in first.err
    assert any(
        "Job B/2 failed" in failure["error"]
        for failure in failed_session["failures"]
    )
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
    resumed_session = _terminal_session(
        storage, "resumefrom", ("A",), [("A",), ("B",)], "done"
    )
    assert (
        f"session={resumed_session['session_id']} kind=main command=resumefrom status=running "
        "parent=- components=[A; B]"
    ) in resumed.err
    assert (
        f"session={resumed_session['session_id']} kind=main command=resumefrom status=terminal "
        "parent=- components=[A; B] outcome=done"
    ) in resumed.err


def test_restart_is_active_run_only_and_resume_is_post_failure_path(
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
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    root = Path(ctx.system.storage.project_dir)
                    if not (root / "allow-retry.flag").exists():
                        raise RuntimeError("boom")
                    return "recovered"
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
    owner = storage.read_job_current_owner("A", 1)
    failed_session = _terminal_session(storage, "run", ("A",), [("A",)], "failed")
    assert owner["session_id"] == failed_session["session_id"]
    generation_before = storage.current_job_generation("A", 1)
    assert cli.main(["restart", "A", "job", "1"]) == 1
    error = capsys.readouterr().err
    assert f"belongs to session {failed_session['session_id']}" in error
    assert "cannot accept restart: no running sequence is recorded" in error
    assert storage.get_job_status("A", 1) == "failed"

    (tmp_path / "allow-retry.flag").write_text("yes", encoding="utf-8")
    assert cli.main(["resume", "A"]) == 0
    capsys.readouterr()
    assert storage.get_job_status("A", 1) == "done"
    assert storage.current_job_generation("A", 1) == generation_before + 1
    _terminal_session(storage, "resume", ("A",), [("A",)], "done")
