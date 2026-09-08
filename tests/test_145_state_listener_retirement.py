"""Notification listener shutdown survives transient locks and delayed threads."""

from pathlib import Path
from threading import Event, Lock
import socket
import time

from micro_workflow_manager.storage import FileStorage
from tests.test_090_component_session_settlement import _close


def test_unsubscribe_retires_record_after_transient_windows_sharing_failure(tmp_path, monkeypatch):
    storage = FileStorage(tmp_path)
    unsubscribe = storage.subscribe_state_changes(lambda: None, local=False, cross_process=True)
    record = storage._state_listener_record
    original = Path.unlink
    denied = []
    def unlink(path, *args, **kwargs):
        if path == record and not denied:
            denied.append(path)
            raise PermissionError('Injected transient Windows sharing violation')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'unlink', unlink)
    try:
        unsubscribe()
        deadline = time.monotonic() + 2
        while record.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert denied == [record]
        assert not record.exists(), 'Finished listener left its subscriber file behind'
    finally:
        unsubscribe()
        _close(storage)


def test_delayed_retired_listener_cannot_adopt_replacement_socket(tmp_path, monkeypatch):
    storage = FileStorage(tmp_path)
    original = storage._state_listener_loop
    started, release, woke = Event(), Event(), Event()
    guard = Lock()
    calls = []
    def delayed(*args):
        with guard:
            first = not calls
            calls.append(args)
        if first:
            started.set()
            assert release.wait(5)
        original(*args)
    monkeypatch.setattr(storage, '_state_listener_loop', delayed)
    unsubscribe_old = storage.subscribe_state_changes(lambda: None, local=False, cross_process=True)
    retired = storage._state_listener_thread
    unsubscribe_new = lambda: None
    try:
        assert started.wait(5)
        unsubscribe_old()
        unsubscribe_new = storage.subscribe_state_changes(woke.set, local=False, cross_process=True)
        current = storage._state_listener_thread
        listener = storage._state_listener_socket
        release.set()
        retired.join(2)
        assert not retired.is_alive(), 'Retired listener adopted the replacement socket'
        assert current.is_alive()
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
            sender.sendto(b'changed', listener.getsockname())
        assert woke.wait(2)
    finally:
        release.set()
        unsubscribe_old()
        unsubscribe_new()
        retired.join(2)
        _close(storage)
