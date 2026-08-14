from __future__ import annotations

import json
import time

from micro_workflow_manager.cli import top
from micro_workflow_manager.cli import active_run
from micro_workflow_manager.storage.filesystem import FileStorage
from micro_workflow_manager.storage import state_events


def test_state_event_pid_probe_uses_platform_safe_helper(monkeypatch, tmp_path):
    calls = []

    def safe_probe(pid):
        calls.append(pid)
        return True

    monkeypatch.setattr(state_events, "process_is_alive", safe_probe)
    monkeypatch.setattr(
        state_events.os,
        "kill",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("state event broker must not call os.kill(pid, 0)")
        ),
    )

    assert state_events.StateEventStorageMixin._pid_alive(12345) is True
    assert calls == [12345]


def test_subscriber_refresh_never_uses_os_kill(monkeypatch, tmp_path):
    storage = FileStorage(tmp_path)
    directory = storage._subscriber_dir()
    record = directory / "12345-test.json"
    record.write_text(
        json.dumps(
            {
                "pid": 12345,
                "host": "127.0.0.1",
                "port": 54321,
                "database": str(storage.state_database_path()),
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(state_events, "process_is_alive", lambda pid: pid == 12345)
    monkeypatch.setattr(
        state_events.os,
        "kill",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("subscriber refresh must use process_is_alive")
        ),
    )

    storage._refresh_subscriber_cache(time.monotonic() + 1.0)
    assert storage._state_subscriber_cache == (("127.0.0.1", 54321),)


def test_top_pid_snapshot_uses_platform_safe_helper(monkeypatch):
    monkeypatch.setattr(top, "process_is_alive", lambda pid: pid == 999)
    monkeypatch.setattr(
        top.os,
        "kill",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("mwf top must not call os.kill(pid, 0)")
        ),
    )

    assert top._pid_snapshot(999)["alive"] is True
    assert top._pid_snapshot(1000)["alive"] is False


def test_active_run_rejects_recycled_pid_even_when_it_exists(monkeypatch):
    monkeypatch.setattr(active_run, "process_is_alive", lambda _pid: True)
    monkeypatch.setattr(
        active_run,
        "process_identity",
        lambda _pid: "windows-filetime:new-process",
    )
    state = {
        "status": "running",
        "pid": 33248,
        "process_identity": "windows-filetime:old-controller",
        "heartbeat_at": "2020-01-01T00:00:00",
    }

    liveness = active_run.run_state_liveness(state)

    assert liveness["live"] is False
    assert liveness["process_identity_matches"] is False
    assert "different process instance" in liveness["reason"]


def test_legacy_active_run_does_not_trust_recycled_pid_after_stale_heartbeat(
    monkeypatch,
):
    monkeypatch.setattr(active_run, "process_is_alive", lambda _pid: True)
    state = {
        "status": "running",
        "pid": 33248,
        "heartbeat_at": "2020-01-01T00:00:00",
    }

    liveness = active_run.run_state_liveness(state)

    assert liveness["live"] is False
    assert "PID existence alone is ambiguous" in liveness["reason"]
