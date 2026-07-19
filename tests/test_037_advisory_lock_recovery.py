from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from micro_workflow_manager.cli.run import active_workflow_run
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.system import MicroWorkflow


CHILD_LOCK_SCRIPT = r"""
import sys
import time
from pathlib import Path
from micro_workflow_manager.storage import FileStorage

root = Path(sys.argv[1])
ready = Path(sys.argv[2])
lease = float(sys.argv[3])
hold = float(sys.argv[4])
storage = FileStorage(root)
with storage.interprocess_lock("thread-overrides", timeout=2.0, lease_seconds=lease):
    ready.write_text("ready", encoding="utf-8")
    time.sleep(hold)
"""


def _start_lock_holder(root: Path, ready: Path, *, lease: float, hold: float):
    env = os.environ.copy()
    repository = str(Path(__file__).resolve().parents[1])
    env["PYTHONPATH"] = repository + os.pathsep + env.get("PYTHONPATH", "")
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            CHILD_LOCK_SCRIPT,
            str(root),
            str(ready),
            str(lease),
            str(hold),
        ],
        env=env,
    )
    deadline = time.monotonic() + 5.0
    while not ready.exists():
        if process.poll() is not None:
            raise RuntimeError(f"lock-holder subprocess exited with {process.returncode}")
        if time.monotonic() >= deadline:
            process.kill()
            process.wait(timeout=5)
            raise TimeoutError("lock-holder subprocess did not acquire the lock")
        time.sleep(0.01)
    return process


def test_thread_override_lock_is_reclaimed_immediately_after_owner_process_dies(tmp_path):
    storage = FileStorage(tmp_path)
    ready = tmp_path / "holder-ready"
    holder = _start_lock_holder(tmp_path, ready, lease=300.0, hold=30.0)

    holder.terminate()
    holder.wait(timeout=5)

    started = time.monotonic()
    storage.set_thread_override("A", 750)
    elapsed = time.monotonic() - started

    assert elapsed < 2.0
    assert storage.read_thread_overrides() == {"A": 750}
    assert storage.db_connection().execute(
        "SELECT COUNT(*) FROM advisory_locks WHERE name='thread-overrides'"
    ).fetchone()[0] == 0


def test_expired_lease_is_not_stolen_while_local_owner_process_is_alive(tmp_path):
    storage = FileStorage(tmp_path)
    ready = tmp_path / "holder-ready"
    holder = _start_lock_holder(tmp_path, ready, lease=0.05, hold=0.8)
    try:
        time.sleep(0.10)
        with pytest.raises(TimeoutError, match="thread-overrides"):
            with storage.interprocess_lock(
                "thread-overrides",
                timeout=0.15,
                lease_seconds=0.05,
            ):
                pass
    finally:
        holder.wait(timeout=5)

    with storage.interprocess_lock("thread-overrides", timeout=0.5):
        pass


def test_legacy_dead_pid_lock_row_is_reclaimed_before_lease_expiry(tmp_path):
    storage = FileStorage(tmp_path)
    dead_pid = max(99_999_999, os.getpid() + 1_000_000)
    now_value = time.time()
    with storage.db_transaction() as connection:
        connection.execute(
            "INSERT INTO advisory_locks(name, owner, acquired_at, expires_at) "
            "VALUES(?, ?, ?, ?)",
            ("thread-overrides", f"{dead_pid}:123:legacy", now_value, now_value + 300),
        )

    with storage.interprocess_lock("thread-overrides", timeout=0.5):
        pass


def test_run_start_does_not_publish_running_state_when_override_binding_fails(
    tmp_path,
    monkeypatch,
):
    workflow = MicroWorkflow(project_dir=tmp_path)

    def fail_bind(_run_id: str):
        raise TimeoutError("synthetic thread-overrides failure")

    monkeypatch.setattr(workflow.storage, "bind_thread_overrides_to_run", fail_bind)

    with pytest.raises(TimeoutError, match="synthetic"):
        with active_workflow_run(
            workflow,
            command="run",
            start_node="A",
            nodes=["A"],
        ):
            pass

    assert workflow.storage.get_run_state() == {}


def test_run_is_marked_terminal_even_when_override_cleanup_fails(
    tmp_path,
    monkeypatch,
    capsys,
):
    workflow = MicroWorkflow(project_dir=tmp_path)

    def fail_cleanup(_run_id: str):
        raise TimeoutError("synthetic cleanup failure")

    monkeypatch.setattr(workflow.storage, "clear_thread_overrides_for_run", fail_cleanup)

    with active_workflow_run(
        workflow,
        command="run",
        start_node="A",
        nodes=["A"],
    ) as finish:
        finish("done")

    state = workflow.storage.get_run_state()
    assert state["status"] == "done"
    assert "temporary thread override could not be removed" in capsys.readouterr().err
