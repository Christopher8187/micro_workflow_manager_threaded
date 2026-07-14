from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from micro_workflow_manager import MicroWorkflow, cli
from micro_workflow_manager.runners.threaded import ThreadedRunner


def _wait_for(predicate, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not reached before timeout")


def _make_project(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        "EDGES = [('A', 'B')]\n",
        encoding="utf-8",
    )
    (behavior / "A.py").write_text(
        '''from micro_workflow_manager import NodeRouter
router = NodeRouter("A", max_threads=3)
router.create_job(number=2)
@router.task
def run(ctx):
    return ctx.job_id
''',
        encoding="utf-8",
    )
    (behavior / "B.py").write_text(
        '''from micro_workflow_manager import NodeRouter
router = NodeRouter("B", max_threads=2)
@router.task
def run(ctx):
    return None
''',
        encoding="utf-8",
    )
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "threaded"]) == 0


def test_threads_command_supports_absolute_relative_list_and_reset(
    tmp_path,
    monkeypatch,
    capsys,
):
    _make_project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main(["threads", "A"]) == 0
    assert "declared max_threads: 3" in capsys.readouterr().out

    assert cli.main(["threads", "A", "5"]) == 0
    assert "3 -> 5" in capsys.readouterr().out

    assert cli.main(["threads", "A", "+2"]) == 0
    assert "5 -> 7" in capsys.readouterr().out

    assert cli.main(["threads", "A", "-3"]) == 0
    assert "7 -> 4" in capsys.readouterr().out

    data = json.loads((tmp_path / ".mwf" / "threads.json").read_text(encoding="utf-8"))
    assert data["overrides"] == {"A": 4}

    assert cli.main(["threads"]) == 0
    listing = capsys.readouterr().out
    assert "Runtime max_threads" in listing
    assert "A" in listing and "4" in listing

    assert cli.main(["inspect", "A"]) == 0
    inspected = capsys.readouterr().out
    assert "declared max_threads: 3" in inspected
    assert "runtime max_threads override: 4" in inspected
    assert "effective max_threads: 4" in inspected

    assert cli.main(["monitor", "--once", "--json"]) == 0
    snapshot = json.loads(capsys.readouterr().out)
    a_row = next(row for row in snapshot["nodes"] if row["node"] == "A")
    assert a_row["declared_max_threads"] == 3
    assert a_row["thread_override"] == 4
    assert a_row["max_parallel_jobs"] == 4

    assert cli.main(["threads", "A", "reset"]) == 0
    assert "4 -> 3" in capsys.readouterr().out
    assert not (tmp_path / ".mwf" / "threads.json").exists()


def test_adaptive_threaded_runner_scales_up_and_down_without_cancelling_running_jobs():
    limit = {"value": 1}
    state_lock = threading.Lock()
    gates = [threading.Event() for _ in range(6)]
    started: list[int] = []
    active = {"count": 0, "max": 0}

    def provider() -> int:
        return limit["value"]

    def run_one(item: int) -> int:
        with state_lock:
            started.append(item)
            active["count"] += 1
            active["max"] = max(active["max"], active["count"])
        gates[item].wait(3)
        with state_lock:
            active["count"] -= 1
        return item

    runner = ThreadedRunner(
        max_threads=1,
        limit_provider=provider,
        poll_interval=0.02,
    )
    result_holder: dict[str, object] = {}

    thread = threading.Thread(
        target=lambda: result_holder.setdefault(
            "result", runner.run_jobs("A", list(range(6)), run_one)
        )
    )
    thread.start()

    _wait_for(lambda: len(started) == 1)
    limit["value"] = 3
    _wait_for(lambda: len(started) == 3)
    assert active["max"] == 3

    # Scale down while three jobs are already active. They are not cancelled,
    # but only one replacement may start after they finish.
    limit["value"] = 1
    # Give the manager one short poll to publish the lower desired limit before
    # the current jobs finish. Live commands are documented to apply within the
    # polling interval, not synchronously with the file write.
    time.sleep(0.06)
    for item in [0, 1, 2]:
        gates[item].set()
    _wait_for(lambda: len(started) == 4)
    time.sleep(0.08)
    assert len(started) == 4
    assert active["count"] == 1

    for item in [3, 4, 5]:
        gates[item].set()
        if item < 5:
            _wait_for(lambda item=item: len(started) >= item + 2)

    thread.join(3)
    assert not thread.is_alive()
    assert result_holder["result"] == list(range(6))


def test_workflow_refreshes_override_from_atomic_runtime_file(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="threaded")
    workflow.graph([("A", "B")])

    @workflow.task("A", max_threads=2)
    def a(ctx):
        return None

    @workflow.task("B")
    def b(ctx):
        return None

    assert workflow.effective_max_threads("A") == 2
    workflow.storage.set_thread_override("A", 6)
    assert workflow.effective_max_threads("A") == 6
    workflow.storage.set_thread_override("A", 3)
    assert workflow.effective_max_threads("A") == 3
    workflow.storage.clear_thread_override("A")
    assert workflow.effective_max_threads("A") == 2


def test_threads_unknown_node_does_not_recreate_folder(tmp_path, monkeypatch, capsys):
    _make_project(tmp_path, monkeypatch)
    capsys.readouterr()
    missing = tmp_path / "node" / "missing"
    assert cli.main(["threads", "missing", "4"]) == 1
    assert "No mounted schema" in capsys.readouterr().err
    assert not missing.exists()


def test_large_declared_limit_does_not_create_one_empty_worker_per_slot(monkeypatch):
    import micro_workflow_manager.runners.threaded as threaded_module

    real_thread = threaded_module.Thread
    created = []

    def counting_thread(*args, **kwargs):
        thread = real_thread(*args, **kwargs)
        created.append(thread)
        return thread

    monkeypatch.setattr(threaded_module, "Thread", counting_thread)
    runner = threaded_module.ThreadedRunner(max_threads=1000, poll_interval=0.01)
    assert runner.run_jobs("A", [1, 2], lambda value: value) == [1, 2]
    assert len(created) <= threaded_module.INITIAL_WORKER_BURST
