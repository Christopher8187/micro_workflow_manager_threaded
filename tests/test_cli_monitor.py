import json
import textwrap
from datetime import datetime

import pytest
from pathlib import Path

from micro_workflow_manager import cli
from micro_workflow_manager import monitor as monitor_module
from micro_workflow_manager.monitor import InlineMonitorReporter
from micro_workflow_manager.models import Job
from micro_workflow_manager.processes import process_identity
from micro_workflow_manager.storage import FileStorage


def test_bulk_node_summaries_match_individual_summaries(tmp_path):
    storage = FileStorage(tmp_path)
    for node_name in ("A", "B"):
        for job_id in range(1, 4):
            storage.create_job(Job(node_name=node_name, job_id=job_id, params={}))
        storage.set_job_status(
            node_name,
            1,
            "done",
            duration_seconds=2.5,
            finished_at="2999-01-01T00:00:00",
        )
        storage.set_job_status(
            node_name,
            2,
            "failed",
            duration_seconds=1.5,
            finished_at="2999-01-01T00:00:00",
        )

    bulk = storage.node_job_summaries(["A", "B"])

    assert bulk == {
        node_name: storage.node_job_summary(node_name)
        for node_name in ("A", "B")
    }


def make_monitor_project(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        "EDGES = [('A', 'B')]\n",
        encoding="utf-8",
    )
    (behavior / "A.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeRouter

            router = NodeRouter("A")
            router.create_job(number=2, params={"value": "seed"})

            @router.task
            def run(ctx, value):
                ctx.node("B").add(value=value)
                return value
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
            def run(ctx, value):
                return value
            """
        ).strip(),
        encoding="utf-8",
    )

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0


def _sessions(root: Path) -> list[dict]:
    storage = FileStorage(root)
    try:
        return storage.list_execution_sessions()
    finally:
        storage.close_database_connections()


def _new_session(root: Path, previous_ids: set[str]) -> dict:
    added = [
        session for session in _sessions(root)
        if session["session_id"] not in previous_ids
    ]
    assert len(added) == 1
    return added[0]


def _assert_terminal_main_session(
    session: dict,
    *,
    command: str,
    start_component: tuple[str, ...],
    selected_components: list[tuple[str, ...]],
) -> None:
    assert session["session_kind"] == "main"
    assert session["command"] == command
    assert session["start_component"] == start_component
    assert session["selected_components"] == selected_components
    assert session["selection_kind"] == "components"
    assert session["selected_jobs"] == []
    assert session["parent_session_id"] is None
    assert session["status"] == "terminal"
    assert session["outcome"] == "done"
    assert session["failures"] == []
    assert session["details"]["start_node"] == start_component[0]
    started = datetime.fromisoformat(session["started_at"])
    heartbeat = datetime.fromisoformat(session["heartbeat_at"])
    finished = datetime.fromisoformat(session["finished_at"])
    assert started <= heartbeat <= finished
    assert type(session["pid"]) is int and session["pid"] > 0
    assert isinstance(session["hostname"], str) and session["hostname"].strip()
    assert session["process_identity"] == process_identity(session["pid"])


def _rendered_session_prefix(session: dict, status: str) -> str:
    components = "; ".join(",".join(value) for value in session["selected_components"])
    return (
        f"session={session['session_id']} kind=main command={session['command']} "
        f"status={status} parent=- components=[{components}]"
    )


def _assert_inline_session_timeline(text: str, session: dict) -> None:
    assert _rendered_session_prefix(session, "running") in text
    terminal = _rendered_session_prefix(session, "terminal")
    assert (
        f"{terminal} outcome=done finished={session['finished_at']}"
        in text
    )
    assert "active run:" not in text
    assert "last run:" not in text


def test_monitor_once_prints_workflow_counts(tmp_path, monkeypatch, capsys):
    make_monitor_project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main(["monitor", "--once"]) == 0
    out = capsys.readouterr().out

    assert "micro-workflow monitor" in out
    assert "jobs=2" in out
    assert "A" in out
    assert "queued" in out
    assert "mwf threads --api-total" in out
    assert "no workflow-wide aggregate API cap" not in out


def test_run_stats_and_monitor_json_include_timing_metadata(tmp_path, monkeypatch, capsys):
    make_monitor_project(tmp_path, monkeypatch)
    capsys.readouterr()
    before_ids = {session["session_id"] for session in _sessions(tmp_path)}

    assert cli.main(["run", "A", "--runner", "direct", "--stats", "--stats-interval", "0.1"]) == 0
    captured = capsys.readouterr()

    assert "[stats]" in captured.err
    assert "[final stats]" in captured.err

    status = FileStorage(tmp_path).read_job_status_data("A", 1)
    assert status["status"] == "done"
    assert "started_at" in status
    assert "finished_at" in status
    assert isinstance(status["duration_seconds"], int | float)

    session = _new_session(tmp_path, before_ids)
    _assert_terminal_main_session(
        session,
        command="run",
        start_component=("A",),
        selected_components=[("A",)],
    )

    assert cli.main(["monitor", "--once", "--json"]) == 0
    data = json.loads(capsys.readouterr().out)

    assert data["sessions"] == json.loads(json.dumps([session]))
    assert "run_state" not in data
    assert "active_run" not in data
    assert data["totals"]["jobs"] >= 2


def make_slow_monitor_project(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        "EDGES = [('A', 'B')]\n",
        encoding="utf-8",
    )
    (behavior / "A.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeRouter

            router = NodeRouter("A", max_threads=2)
            router.create_job(number=2)

            @router.task(timeout=5)
            def run(ctx):
                ctx.checkpoint("A working", timeout=2, progress=0.25)
                ctx.sleep(0.08)
                ctx.node("B").add(value=ctx.job_id)
                return ctx.job_id
            """
        ).strip(),
        encoding="utf-8",
    )
    (behavior / "B.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeRouter

            router = NodeRouter("B", max_threads=2)

            @router.task(timeout=5)
            def run(ctx, value):
                ctx.checkpoint("B working", timeout=2, progress=0.5)
                ctx.sleep(0.05)
                return value
            """
        ).strip(),
        encoding="utf-8",
    )

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0


def test_run_monitor_prints_timeline_and_terminal_active_none(tmp_path, monkeypatch, capsys):
    make_slow_monitor_project(tmp_path, monkeypatch)
    capsys.readouterr()
    before_ids = {session["session_id"] for session in _sessions(tmp_path)}

    assert cli.main(
        ["run", "A", "--runner", "direct", "--monitor", "--monitor-interval", "0.02"]
    ) == 0
    captured = capsys.readouterr()
    session = _new_session(tmp_path, before_ids)
    _assert_terminal_main_session(
        session,
        command="run",
        start_component=("A",),
        selected_components=[("A",)],
    )

    assert "--- mwf monitor snapshot ---" in captured.err
    assert "micro-workflow monitor |" in captured.err
    _assert_inline_session_timeline(captured.err, session)
    assert "--- mwf final monitor snapshot ---" in captured.err
    final = captured.err.rsplit("--- mwf final monitor snapshot ---", 1)[1]
    assert _rendered_session_prefix(session, "running") not in final
    assert (
        f"{_rendered_session_prefix(session, 'terminal')} "
        f"outcome=done finished={session['finished_at']}"
        in final
    )
    assert "running jobs" in captured.err


def test_runfrom_monitor_observes_descendants_and_can_be_reused(tmp_path, monkeypatch, capsys):
    make_slow_monitor_project(tmp_path, monkeypatch)
    capsys.readouterr()
    seen_ids: set[str] = set()

    for _ in range(2):
        before_ids = {session["session_id"] for session in _sessions(tmp_path)}
        assert cli.main(
            [
                "runfrom",
                "A",
                "--runner",
                "direct",
                "--monitor",
                "--monitor-interval",
                "0.02",
            ]
        ) == 0
        captured = capsys.readouterr()
        session = _new_session(tmp_path, before_ids)
        _assert_terminal_main_session(
            session,
            command="runfrom",
            start_component=("A",),
            selected_components=[("A",), ("B",)],
        )
        assert session["session_id"] not in seen_ids
        _assert_inline_session_timeline(captured.err, session)
        for previous_id in seen_ids:
            assert f"session={previous_id}" in captured.err
        seen_ids.add(session["session_id"])
        assert "Ran:\n  A\n  B\n" in captured.out
        assert "A" in captured.err
        assert "B" in captured.err
        assert FileStorage(tmp_path).get_node_status("A") == "done"
        assert FileStorage(tmp_path).get_node_status("B") == "done"

    # A different execution command after repeated runfrom use must not inherit
    # a stale reporter or previous native session identity.
    before_ids = {session["session_id"] for session in _sessions(tmp_path)}
    assert cli.main(
        ["run", "B", "--runner", "direct", "--monitor", "--monitor-interval", "0.02"]
    ) == 0
    captured = capsys.readouterr()
    session = _new_session(tmp_path, before_ids)
    _assert_terminal_main_session(
        session,
        command="run",
        start_component=("B",),
        selected_components=[("B",)],
    )
    assert session["session_id"] not in seen_ids
    _assert_inline_session_timeline(captured.err, session)
    for previous_id in seen_ids:
        assert f"session={previous_id}" in captured.err
    assert "Ran:\n  B\n" in captured.out

    import threading

    assert not any(
        thread.name in {"mwf-inline-monitor", "mwf-stats"} and thread.is_alive()
        for thread in threading.enumerate()
    )


def test_standalone_monitor_calls_completed_sequence_last_not_active(tmp_path, monkeypatch, capsys):
    make_slow_monitor_project(tmp_path, monkeypatch)
    capsys.readouterr()
    before_ids = {session["session_id"] for session in _sessions(tmp_path)}

    assert cli.main(["run", "A", "--runner", "direct"]) == 0
    session = _new_session(tmp_path, before_ids)
    _assert_terminal_main_session(
        session,
        command="run",
        start_component=("A",),
        selected_components=[("A",)],
    )
    capsys.readouterr()
    assert cli.main(["monitor", "--once"]) == 0
    output = capsys.readouterr().out

    assert "Execution sessions:" in output
    assert (
        f"{_rendered_session_prefix(session, 'terminal')} "
        f"outcome=done finished={session['finished_at']}"
        in output
    )
    assert _rendered_session_prefix(session, "running") not in output
    assert "active run:" not in output
    assert "last run:" not in output



def test_stats_and_full_monitor_can_run_together(tmp_path, monkeypatch, capsys):
    make_slow_monitor_project(tmp_path, monkeypatch)
    capsys.readouterr()
    before_ids = {session["session_id"] for session in _sessions(tmp_path)}

    assert cli.main([
        "run", "A", "--runner", "direct",
        "--stats", "--stats-interval", "0.02",
        "--monitor", "--monitor-interval", "0.02",
    ]) == 0
    captured = capsys.readouterr()
    session = _new_session(tmp_path, before_ids)
    _assert_terminal_main_session(
        session,
        command="run",
        start_component=("A",),
        selected_components=[("A",)],
    )
    assert "[stats]" in captured.err
    assert "[final stats]" in captured.err
    assert "--- mwf monitor snapshot ---" in captured.err
    assert "--- mwf final monitor snapshot ---" in captured.err
    _assert_inline_session_timeline(captured.err, session)
    final = captured.err.rsplit(
        "--- mwf final monitor snapshot ---", 1
    )[1]
    assert _rendered_session_prefix(session, "running") not in final
    assert (
        f"{_rendered_session_prefix(session, 'terminal')} "
        f"outcome=done finished={session['finished_at']}"
        in final
    )


def test_inline_monitor_failure_is_diagnostic_not_a_run_failure(monkeypatch, capsys):
    def broken_snapshot(*args, **kwargs):
        raise RuntimeError("temporary read failure")

    monkeypatch.setattr(monitor_module, "workflow_snapshot", broken_snapshot)
    reporter = InlineMonitorReporter(
        object(), enabled=True, interval=0.01
    ).start()
    import time
    time.sleep(0.03)
    reporter.stop_periodic()
    reporter.print_final()

    errors = capsys.readouterr().err
    assert "[mwf-inline-monitor error] snapshot unavailable" in errors
    assert "temporary read failure" in errors

def test_run_help_documents_inline_monitor(capsys):
    with pytest.raises(SystemExit) as run_help:
        cli.main(["run", "--help"])
    assert run_help.value.code == 0
    output = capsys.readouterr().out
    assert "--monitor" in output
    assert "--monitor-interval" in output

    with pytest.raises(SystemExit) as runfrom_help:
        cli.main(["runfrom", "--help"])
    assert runfrom_help.value.code == 0
    output = capsys.readouterr().out
    assert "--monitor" in output
    assert "--monitor-interval" in output
