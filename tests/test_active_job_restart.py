from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.models import now
from micro_workflow_manager.processes import process_identity
from micro_workflow_manager.cli.project import load_workflow


def wait_until(predicate, timeout: float = 30.0, interval: float = 0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError("condition did not become true before timeout")


def make_restart_project(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    src = tmp_path / "src"
    behavior = src / "node_behavior"
    behavior.mkdir(parents=True)

    (src / "graph.py").write_text("EDGES = [('A', 'B')]\n", encoding="utf-8")
    (behavior / "A.py").write_text(
        r'''
import time
from micro_workflow_manager import NodeRouter

router = NodeRouter("A")
router.create_job(number=1)

@router.task
def run(ctx):
    if ctx.execution_generation == 0:
        ctx.input_path("old_started.flag").write_text("started", encoding="utf-8")
        release = ctx.input_path("release_old.flag")
        while not release.exists():
            time.sleep(0.02)

        # These happen only after the replacement generation has completed.
        # Both must be rejected by the execution-generation fence.
        ctx.write_output("stale.txt", "stale")
        ctx.node("B").add(value="stale")
        return "stale"

    ctx.write_output("fresh.txt", "fresh")
    ctx.node("B").add(value="fresh")
    return "fresh"
'''.strip(),
        encoding="utf-8",
    )
    (behavior / "B.py").write_text(
        r'''
from micro_workflow_manager import NodeRouter

router = NodeRouter("B")

@router.task
def run(ctx, value):
    ctx.write_output("received.txt", value)
    return value
'''.strip(),
        encoding="utf-8",
    )

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "threaded"]) == 0


def _reserve_live_main(root):
    workflow = load_workflow(root, 'direct')
    storage = workflow.storage
    shape = workflow.topology.snapshot()
    storage.register_component_topology(shape)
    storage.create_execution_session(
        'reserved-main', session_kind='main', command='runfrom',
        start_component=('A',), selected_components=[('A',), ('B',)],
        started_at=now(), hostname=socket.gethostname(), pid=os.getpid(),
        process_identity=process_identity(os.getpid()), expected_shape=shape.shape_json,
    )
    storage.reserve_execution_components('reserved-main', expected_shape=shape.shape_json)
    return storage


@pytest.mark.parametrize("runner", ["threaded", "direct"])
def test_restart_command_replaces_running_generation_inside_existing_runfrom(
    tmp_path,
    monkeypatch,
    runner,
):
    make_restart_project(tmp_path, monkeypatch)

    run_result: dict[str, int] = {}

    def run_workflow():
        run_result["code"] = cli.main(["runfrom", "A", "--runner", runner])

    active_thread = threading.Thread(target=run_workflow, name="test-active-run")
    active_thread.start()

    started_flag = tmp_path / "node" / "A" / "input" / "old_started.flag"
    state = FileStorage(tmp_path)

    try:
        wait_until(lambda: started_flag.exists())
        wait_until(lambda: state.get_job_status("A", 1) == "running")
        wait_until(lambda: bool(state.read_job_control("A", 1).get("active_execution_id")))

        before = state.get_live_main_session()
        assert before is not None
        assert before["session_id"] == state.read_job_current_owner("A", 1)["session_id"]
        assert before["status"] == "running"

        env = os.environ.copy()
        package_root = str(Path(__file__).resolve().parents[1])
        env["PYTHONPATH"] = package_root + os.pathsep + env.get("PYTHONPATH", "")

        started = time.perf_counter()
        command = subprocess.run(
            [sys.executable, "-m", "micro_workflow_manager", "restart", "A", "job", "1"],
            cwd=tmp_path,
            env=env,
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
        elapsed = time.perf_counter() - started

        assert command.returncode == 0, command.stderr
        assert "generation 0 -> 1" in command.stdout
        assert before["session_id"] in command.stdout
        # Restart returns while the original handler is still blocked.
        assert elapsed < 30.0

        # The original attempt is still blocked, but the scheduler must abandon it,
        # run generation 1, run B, and finish the original runfrom sequence.
        active_thread.join(timeout=30)
        assert not active_thread.is_alive()
        assert run_result == {"code": 0}

        after = state.get_execution_session(before["session_id"])
        assert after["status"] == "terminal"
        assert after["outcome"] == "done"
        assert len(state.list_execution_sessions()) == 1

        a_output = json.loads(
            (tmp_path / "node" / "A" / "jobs" / "1" / "output.json").read_text(encoding="utf-8")
        )
        assert a_output["status"] == "done"
        assert a_output["generation"] == 1
        assert "fresh" in a_output["result_repr"]

        b_jobs = sorted((tmp_path / "node" / "B" / "jobs").glob("[0-9]*"))
        assert len(b_jobs) == 1
        assert json.loads((b_jobs[0] / "input.json").read_text(encoding="utf-8")) == {"value": "fresh"}
        assert (tmp_path / "node" / "B" / "output" / "received.txt").read_text(encoding="utf-8") == "fresh"

        # Let the abandoned Python thread return. Its stale MWF writes and child-job
        # creation must remain rejected even though the larger run has completed.
        (tmp_path / "node" / "A" / "input" / "release_old.flag").write_text("release", encoding="utf-8")
        time.sleep(0.25)
        assert not (tmp_path / "node" / "A" / "output" / "stale.txt").exists()
        assert len(sorted((tmp_path / "node" / "B" / "jobs").glob("[0-9]*"))) == 1
    finally:
        (tmp_path / "node/A/input/release_old.flag").touch(exist_ok=True)
        active_thread.join(timeout=30)
        assert not active_thread.is_alive()
        state.close_database_connections()


def test_restart_refuses_non_running_job_without_queueing_it(tmp_path, monkeypatch, capsys):
    make_restart_project(tmp_path, monkeypatch)

    # Router loading created A/1, but no run owns it and it is only queued.
    assert cli.main(["restart", "A", "job", "1"]) == 1
    error = capsys.readouterr().err
    assert "no recorded execution owner" in error

    state = FileStorage(tmp_path)
    try:
        assert state.get_job_status("A", 1) == "queued"
    finally:
        state.close_database_connections()


def test_restart_refuses_unowned_queued_job_even_with_a_live_native_reservation(
    tmp_path, monkeypatch, capsys
):
    make_restart_project(tmp_path, monkeypatch)
    state = _reserve_live_main(tmp_path)

    try:
        assert cli.main(["restart", "A", "job", "1"]) == 1
        error = capsys.readouterr().err
        assert "no recorded execution owner" in error
        assert state.get_job_status("A", 1) == "queued"
    finally:
        state.close_database_connections()


def test_run_job_command_refuses_to_compete_with_live_sequence(
    tmp_path, monkeypatch, capsys
):
    make_restart_project(tmp_path, monkeypatch)
    state = _reserve_live_main(tmp_path)

    try:
        assert cli.main(["run", "A", "job", "1"]) == 1
        error = capsys.readouterr().err
        assert "already active" in error
        assert "mwf restart <node> job <id>" in error
    finally:
        state.close_database_connections()


def test_restart_fast_path_does_not_import_broken_graph_or_node_code(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert cli.main(["init"]) == 0

    config = json.loads((tmp_path / ".mwf" / "project.json").read_text(encoding="utf-8"))
    config["graph_path"] = "src/graph.py"
    (tmp_path / ".mwf" / "project.json").write_text(json.dumps(config), encoding="utf-8")
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "graph.py").write_text("this is not valid python !!!", encoding="utf-8")

    assert cli.main(["restart", "A", "job", "1"]) == 1
    error = capsys.readouterr().err
    assert "Job does not exist: A/1" in error
    assert "invalid syntax" not in error.lower()


def test_restart_command_replaces_running_generation_in_process_runner(
    tmp_path,
    monkeypatch,
):
    make_restart_project(tmp_path, monkeypatch)

    env = os.environ.copy()
    package_root = str(Path(__file__).resolve().parents[1])
    env["PYTHONPATH"] = package_root + os.pathsep + env.get("PYTHONPATH", "")

    active = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "micro_workflow_manager",
            "runfrom",
            "A",
            "--runner",
            "process",
        ],
        cwd=tmp_path,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        started_flag = tmp_path / "node" / "A" / "input" / "old_started.flag"
        state = FileStorage(tmp_path)
        wait_until(lambda: started_flag.exists(), timeout=20)
        wait_until(
            lambda: bool(state.read_job_control("A", 1).get("active_execution_id")),
            timeout=20,
        )

        command = subprocess.run(
            [sys.executable, "-m", "micro_workflow_manager", "restart", "A", "job", "1"],
            cwd=tmp_path,
            env=env,
            text=True,
            capture_output=True,
            timeout=15,
            check=False,
        )
        assert command.returncode == 0, command.stderr
        assert "generation 0 -> 1" in command.stdout

        stdout, stderr = active.communicate(timeout=20)
        assert active.returncode == 0, stderr or stdout

        a_output = json.loads(
            (tmp_path / "node" / "A" / "jobs" / "1" / "output.json").read_text(
                encoding="utf-8"
            )
        )
        assert a_output["generation"] == 1
        assert "fresh" in a_output["result_repr"]

        b_jobs = sorted((tmp_path / "node" / "B" / "jobs").glob("[0-9]*"))
        assert len(b_jobs) == 1
        assert json.loads((b_jobs[0] / "input.json").read_text(encoding="utf-8")) == {
            "value": "fresh"
        }
    finally:
        if active.poll() is None:
            active.kill()
            active.communicate(timeout=5)


def test_restart_preserves_replacement_runtime_after_old_checkpoint_deadline(
    tmp_path,
    monkeypatch,
):
    make_restart_project(tmp_path, monkeypatch)
    behavior_file = tmp_path / "src" / "node_behavior" / "A.py"
    behavior_file.write_text(
        r'''
import time
from micro_workflow_manager import NodeRouter

router = NodeRouter("A", checkpoint_timeout=0.4)
router.create_job(number=1)

@router.task
def run(ctx):
    if ctx.execution_generation == 0:
        ctx.input_path("old_started.flag").write_text("started", encoding="utf-8")
        old_attempt_finished = ctx.input_path("old_attempt_finished.flag")
        ctx.checkpoint("old generation waiting", progress=0.2)
        time.sleep(0.8)
        try:
            ctx.write_output("stale.txt", "stale")
            return "stale"
        finally:
            old_attempt_finished.write_text("finished", encoding="utf-8")

    # Keep the replacement alive beyond the old watch's deadline without
    # expiring its own checkpoint while it performs the replacement work.
    ctx.checkpoint("replacement generation", progress=1.0, timeout=5.0)
    time.sleep(0.6)
    ctx.write_output("fresh.txt", "fresh")
    ctx.node("B").add(value="fresh")
    return "fresh"
'''.strip(),
        encoding="utf-8",
    )

    run_result: dict[str, int] = {}
    active_thread = threading.Thread(
        target=lambda: run_result.setdefault(
            "code", cli.main(["runfrom", "A", "--runner", "threaded"])
        ),
        daemon=True,
    )
    active_thread.start()

    state = FileStorage(tmp_path)
    wait_until(
        lambda: state.read_job_runtime("A", 1).get("checkpoint_name")
        == "old generation waiting"
    )

    # Invoke the same command implementation directly so this regression test
    # exercises the generation/watch race rather than subprocess startup time.
    # Separate-terminal CLI behavior is covered by the integration tests above.
    from micro_workflow_manager.cli.restart import restart_active_jobs

    assert restart_active_jobs(tmp_path, "A", [1]) == 0

    active_thread.join(timeout=30)
    assert not active_thread.is_alive()
    assert run_result == {"code": 0}

    # Observe after the old handler has crossed its checkpoint deadline and
    # attempted its stale write, even if thread scheduling delays that attempt.
    old_finished = tmp_path / "node" / "A" / "input" / "old_attempt_finished.flag"
    wait_until(old_finished.exists)
    runtime = state.read_job_runtime("A", 1)
    assert runtime["generation"] == 1
    assert runtime["state"] == "completed"
    assert runtime["checkpoint_name"] == "replacement generation"
    assert runtime["progress"] == 1.0

    events = state.read_job_events("A", 1)
    assert not any(event.get("event") == "timeout" for event in events)
    assert not (tmp_path / "node" / "A" / "output" / "stale.txt").exists()
    assert (tmp_path / "node" / "A" / "output" / "fresh.txt").read_text(encoding="utf-8") == "fresh"
    assert (tmp_path / "node" / "B" / "output" / "received.txt").read_text(encoding="utf-8") == "fresh"
