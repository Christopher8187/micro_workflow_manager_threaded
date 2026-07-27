from __future__ import annotations

import os
import threading

from micro_workflow_manager.storage import FileStorage


def test_connection_registry_allows_same_thread_storage_finalizer_reentry(tmp_path):
    old_storage = FileStorage(tmp_path / "old")
    old_storage.db_connection()
    old_path = old_storage.state_database_path()
    old_storage._connection_finalizer.detach()

    current_storage = FileStorage(tmp_path / "current")
    original_new_connection = current_storage._new_db_connection
    errors: list[BaseException] = []

    def new_connection_with_finalizer_reentry():
        type(old_storage)._release_storage_path(old_path, os.getpid())
        return original_new_connection()

    current_storage._new_db_connection = new_connection_with_finalizer_reentry

    def connect_and_close():
        try:
            current_storage.db_connection()
            current_storage.close_thread_connection()
        except BaseException as error:  # pragma: no cover - assertion reports it
            errors.append(error)

    worker = threading.Thread(target=connect_and_close)
    worker.start()
    worker.join(timeout=3)

    assert not worker.is_alive(), "connection registry deadlocked during finalizer re-entry"
    assert errors == []
