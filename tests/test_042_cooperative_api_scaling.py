from __future__ import annotations

import json
import threading
from concurrent.futures import Future

from micro_workflow_manager import cli
from micro_workflow_manager.runners.api import ApiRunner


def test_two_thousand_api_jobs_are_cooperative_not_thread_per_job():
    runner = ApiRunner(max_threads=2000, poll_interval=0.001)
    lock = threading.Lock()
    active = 0
    peak = 0
    baseline = threading.active_count()
    release: Future[None] = Future()

    def run_one(value: int):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            if peak == 2000:
                release.set_result(None)
        release.result()
        with lock:
            active -= 1
        return value

    results = runner.run_jobs("A", list(range(2000)), run_one)
    assert results == list(range(2000))
    assert peak == 2000
    assert threading.active_count() <= baseline + 1


def test_monitor_reports_no_aggregate_api_limit(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text("EDGES = [('A', 'B')]\n", encoding="utf-8")
    (behavior / "A.py").write_text(
        "from micro_workflow_manager import NodeRouter\n"
        "router = NodeRouter('A', runner='api', max_threads=12000)\n"
        "router.create_job(number=1)\n"
        "@router.task\n"
        "def run(ctx): return None\n",
        encoding="utf-8",
    )
    (behavior / "B.py").write_text(
        "from micro_workflow_manager import NodeRouter\n"
        "router = NodeRouter('B')\n"
        "@router.task\n"
        "def run(ctx): return None\n",
        encoding="utf-8",
    )
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "threaded"]) == 0
    capsys.readouterr()

    assert cli.main(["monitor", "--once", "--json"]) == 0
    snapshot = json.loads(capsys.readouterr().out)
    assert snapshot["api_runtime"]["mode"] == "cooperative"
    assert snapshot["api_runtime"]["aggregate_limit"] is None
    assert snapshot["api_runtime"]["declared_capacity"] == 12000


def test_api_runtime_override_can_exceed_os_thread_ceiling(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text("EDGES = [('A', 'B')]\n", encoding="utf-8")
    (behavior / "A.py").write_text(
        "from micro_workflow_manager import NodeRouter\n"
        "router = NodeRouter('A', runner='api', max_threads=10)\n"
        "@router.task\n"
        "def run(ctx): return None\n",
        encoding="utf-8",
    )
    (behavior / "B.py").write_text(
        "from micro_workflow_manager import NodeRouter\n"
        "router = NodeRouter('B', runner='threaded', max_threads=10)\n"
        "@router.task\n"
        "def run(ctx): return None\n",
        encoding="utf-8",
    )
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "threaded"]) == 0
    capsys.readouterr()
    assert cli.main(["threads", "A", "12000"]) == 0
    assert "10 -> 12000" in capsys.readouterr().out
    assert cli.main(["threads", "B", "12000"]) == 1
    assert "cannot exceed" in capsys.readouterr().err


def test_removed_api_total_option_is_rejected(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert cli.main(["init"]) == 0
    try:
        cli.main(["threads", "--api-total", "7"])
    except SystemExit as error:
        assert error.code == 2
    else:
        raise AssertionError("removed --api-total option was unexpectedly accepted")
