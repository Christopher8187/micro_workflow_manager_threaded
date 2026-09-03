import json
import textwrap

import pytest
from pathlib import Path

from micro_workflow_manager import cli
from micro_workflow_manager import monitor as monitor_module
from micro_workflow_manager.monitor import InlineMonitorReporter
from micro_workflow_manager.models import Job
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

    assert cli.main(["run", "A", "--runner", "direct", "--stats", "--stats-interval", "0.1"]) == 0
    captured = capsys.readouterr()

    assert "[stats]" in captured.err
    assert "[final stats]" in captured.err

    status = FileStorage(tmp_path).read_job_status_data("A", 1)
    assert status["status"] == "done"
    assert "started_at" in status
    assert "finished_at" in status
    assert isinstance(status["duration_seconds"], int | float)

    assert cli.main(["monitor", "--once", "--json"]) == 0
    data = json.loads(capsys.readouterr().out)

    assert data["run_state"]["command"] == "run"
    assert data["run_state"]["status"] == "done"
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

    assert cli.main(
        ["run", "A", "--runner", "direct", "--monitor", "--monitor-interval", "0.02"]
    ) == 0
    captured = capsys.readouterr()

    assert "--- mwf monitor snapshot ---" in captured.err
    assert "micro-workflow monitor |" in captured.err
    assert "active run: run A" in captured.err
    assert "--- mwf final monitor snapshot ---" in captured.err
    final = captured.err.rsplit("--- mwf final monitor snapshot ---", 1)[1]
    assert "active run: none" in final
    assert "last run: run A | status=done" in final
    assert "running jobs" in captured.err


def test_runfrom_monitor_observes_descendants_and_can_be_reused(tmp_path, monkeypatch, capsys):
    make_slow_monitor_project(tmp_path, monkeypatch)
    capsys.readouterr()

    for _ in range(2):
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
        assert "active run: runfrom A" in captured.err
        assert "last run: runfrom A | status=done" in captured.err
        assert "A" in captured.err
        assert "B" in captured.err
        assert FileStorage(tmp_path).get_node_status("A") == "done"
        assert FileStorage(tmp_path).get_node_status("B") == "done"

    # A different execution command after repeated runfrom use must not inherit
    # a stale reporter or active-run label.
    assert cli.main(
        ["run", "B", "--runner", "direct", "--monitor", "--monitor-interval", "0.02"]
    ) == 0
    captured = capsys.readouterr()
    assert "active run: run B" in captured.err
    assert "last run: run B | status=done" in captured.err

    import threading

    assert not any(
        thread.name in {"mwf-inline-monitor", "mwf-stats"} and thread.is_alive()
        for thread in threading.enumerate()
    )


def test_standalone_monitor_calls_completed_sequence_last_not_active(tmp_path, monkeypatch, capsys):
    make_slow_monitor_project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main(["run", "A", "--runner", "direct"]) == 0
    capsys.readouterr()
    assert cli.main(["monitor", "--once"]) == 0
    output = capsys.readouterr().out

    assert "active run: none" in output
    assert "last run: run A | status=done" in output
    assert "active run: run A" not in output



def test_stats_and_full_monitor_can_run_together(tmp_path, monkeypatch, capsys):
    make_slow_monitor_project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main([
        "run", "A", "--runner", "direct",
        "--stats", "--stats-interval", "0.02",
        "--monitor", "--monitor-interval", "0.02",
    ]) == 0
    captured = capsys.readouterr()
    assert "[stats]" in captured.err
    assert "[final stats]" in captured.err
    assert "--- mwf monitor snapshot ---" in captured.err
    assert "--- mwf final monitor snapshot ---" in captured.err
    assert "active run: none" in captured.err.rsplit(
        "--- mwf final monitor snapshot ---", 1
    )[1]


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
