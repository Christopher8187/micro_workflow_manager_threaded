from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from micro_workflow_manager.cli.run import active_workflow_run
from micro_workflow_manager.monitor import now_iso
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.sqlite import advisory
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
    storage.set_node_status("A", "queued")
    ready = tmp_path / "holder-ready"
    holder = _start_lock_holder(tmp_path, ready, lease=300.0, hold=30.0)

    holder.terminate()
    holder.wait(timeout=5)

    started = time.monotonic()
    with storage.interprocess_lock("thread-overrides", timeout=2.0):
        pass
    expected = storage.read_thread_override_observation("A")
    storage.set_thread_override("A", 750, expected=expected)
    elapsed = time.monotonic() - started

    assert elapsed < 2.0
    assert storage.read_thread_override_observation("A") == {
        "node": "A", "value": 750, "session_id": None,
    }
    assert [tuple(row) for row in storage.db_connection().execute(
        "SELECT node_name, value FROM pending_node_thread_overrides ORDER BY node_name"
    )] == [("A", 750)]
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


@pytest.mark.parametrize('owner_form', ['opaque', 'colon-separated'])
def test_unrecognized_lock_owner_waits_for_lease_expiry(tmp_path, monkeypatch, owner_form):
    storage = FileStorage(tmp_path)
    dead_pid = max(99_999_999, os.getpid() + 1_000_000)
    owner = 'opaque-owner' if owner_form == 'opaque' else f'{dead_pid}:123:unrecognized'
    now_value = time.time()
    with storage.db_transaction() as connection:
        connection.execute(
            "INSERT INTO advisory_locks(name, owner, acquired_at, expires_at) "
            "VALUES(?, ?, ?, ?)",
            ('thread-overrides', owner, now_value, now_value + 300),
        )
    before = tuple(storage.db_connection().execute(
        "SELECT * FROM advisory_locks WHERE name='thread-overrides'",
    ).fetchone())

    def refuse_local_process_guess(pid):
        raise AssertionError(f'Unrecognized owner was treated as local process {pid}')

    monkeypatch.setattr(advisory, 'process_is_alive', refuse_local_process_guess)
    with pytest.raises(TimeoutError, match='thread-overrides'):
        with storage.interprocess_lock('thread-overrides', timeout=0):
            pytest.fail('An unexpired unknown owner must retain its lock')
    assert tuple(storage.db_connection().execute(
        "SELECT * FROM advisory_locks WHERE name='thread-overrides'",
    ).fetchone()) == before

    with storage.db_transaction() as connection:
        connection.execute(
            "UPDATE advisory_locks SET expires_at=? WHERE name='thread-overrides'",
            (time.time() - 1,),
        )
    with storage.interprocess_lock('thread-overrides', timeout=0.5):
        replacement = storage.db_connection().execute(
            "SELECT owner FROM advisory_locks WHERE name='thread-overrides'",
        ).fetchone()['owner']
        assert replacement != owner
        assert storage._parse_advisory_owner(replacement)['pid'] == os.getpid()
    assert storage.db_connection().execute(
        "SELECT 1 FROM advisory_locks WHERE name='thread-overrides'",
    ).fetchone() is None


def test_run_start_does_not_publish_running_state_when_override_binding_fails(
    tmp_path,
    monkeypatch,
):
    workflow = MicroWorkflow(project_dir=tmp_path)
    workflow.graph([('A', 'B')])
    expected = workflow.storage.read_thread_override_observation('A')
    workflow.storage.set_thread_override('A', 5, expected=expected)
    original_bind = workflow.storage._bind_pending_thread_overrides

    def fail_bind(connection, session_id, nodes):
        original_bind(connection, session_id, nodes)
        raise TimeoutError("synthetic thread-overrides failure")

    monkeypatch.setattr(workflow.storage, "_bind_pending_thread_overrides", fail_bind)

    with pytest.raises(TimeoutError, match="synthetic"):
        with active_workflow_run(
            workflow,
            command="run",
            start_node="A",
            nodes=["A"],
        ):
            pass

    session, = workflow.storage.list_execution_sessions()
    assert session['status'] == 'terminal' and session['outcome'] == 'failed'
    assert workflow.storage.get_live_main_session() is None
    assert workflow.storage.get_component_reservation(('A',)) is None
    assert workflow.storage.read_thread_override_observation('A') == {
        'node': 'A', 'value': 5, 'session_id': None,
    }
    assert not (tmp_path / '.mwf' / 'run.json').exists()


def test_run_terminal_settlement_rolls_back_when_native_override_cleanup_fails(
    tmp_path,
    monkeypatch,
):
    workflow = MicroWorkflow(project_dir=tmp_path)
    workflow.graph([('A', 'B')])
    expected = workflow.storage.read_thread_override_observation('A')
    workflow.storage.set_thread_override('A', 5, expected=expected)
    original_cleanup = workflow.storage._clear_thread_overrides_for_session

    def fail_cleanup(connection, session_id):
        original_cleanup(connection, session_id)
        raise TimeoutError("synthetic cleanup failure")

    with monkeypatch.context() as patch:
        patch.setattr(
            workflow.storage, "_clear_thread_overrides_for_session", fail_cleanup,
        )
        with pytest.raises(TimeoutError, match="synthetic cleanup failure"):
            with active_workflow_run(
                workflow,
                command="run",
                start_node="A",
                nodes=["A"],
            ) as finish:
                finish("done")

    state, = workflow.storage.list_execution_sessions()
    assert state['status'] == 'running' and state['outcome'] is None
    assert workflow.storage.get_component_reservation(('A',)) == {
        'members': ('A',), 'session_id': state['session_id'],
    }
    assert workflow.storage.read_thread_override_observation('A') == {
        'node': 'A', 'value': 5, 'session_id': state['session_id'],
    }
    workflow.storage.decide_execution_session_exit(
        state['session_id'], outcome='done', finished_at=now_iso(), failures=[],
    )
    terminal = workflow.storage.get_execution_session(state['session_id'])
    assert terminal['status'] == 'terminal' and terminal['outcome'] == 'done'
    assert workflow.storage.get_live_main_session() is None
    assert workflow.storage.get_component_reservation(('A',)) is None
    assert workflow.storage.read_thread_override_observation('A') == {
        'node': 'A', 'value': None, 'session_id': None,
    }
    assert workflow.storage.db_connection().execute(
        'SELECT 1 FROM node_thread_overrides LIMIT 1'
    ).fetchone() is None
    assert not (tmp_path / '.mwf' / 'run.json').exists()
