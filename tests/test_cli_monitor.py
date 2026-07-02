import json
import textwrap
from pathlib import Path

from micro_workflow_manager import cli


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


def test_run_stats_and_monitor_json_include_timing_metadata(tmp_path, monkeypatch, capsys):
    make_monitor_project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main(["run", "A", "--runner", "direct", "--stats", "--stats-interval", "0.1"]) == 0
    captured = capsys.readouterr()

    assert "[stats]" in captured.err
    assert "[final stats]" in captured.err

    status = json.loads((tmp_path / "node" / "A" / "jobs" / "1" / "status.json").read_text())
    assert status["status"] == "done"
    assert "started_at" in status
    assert "finished_at" in status
    assert isinstance(status["duration_seconds"], int | float)

    assert cli.main(["monitor", "--once", "--json"]) == 0
    data = json.loads(capsys.readouterr().out)

    assert data["run_state"]["command"] == "run"
    assert data["run_state"]["status"] == "done"
    assert data["totals"]["jobs"] >= 2
