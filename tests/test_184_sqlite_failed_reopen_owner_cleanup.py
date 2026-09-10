"""A failed native reopen releases its registered SQLite storage ownership."""

from __future__ import annotations

import gc
import sqlite3

import pytest

from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.sqlite.schema import DATABASE_SCHEMA_VERSION


def _set_database_version(root, value):
    database = root / ".mwf" / "state.sqlite3"
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            "UPDATE metadata SET value=? WHERE key='database_schema_version'",
            (value,),
        )
        connection.commit()
    finally:
        connection.close()


def _assert_no_sidecars(root):
    assert not (root / ".mwf" / "state.sqlite3-wal").exists()
    assert not (root / ".mwf" / "state.sqlite3-shm").exists()


def test_failed_reopen_releases_owner_before_a_later_valid_owner_closes(
    tmp_path,
):
    initial = FileStorage._create_new_project_state(tmp_path)
    initial.close_database_connections()
    _assert_no_sidecars(tmp_path)

    _set_database_version(tmp_path, "damaged")
    was_enabled = gc.isenabled()
    gc.disable()
    try:
        # Keep cyclic, failed constructor objects uncollected. Correctness must
        # not depend on a later garbage-collection pass invoking finalizers.
        for _ in range(2):
            with pytest.raises(RuntimeError, match="database_schema_version"):
                FileStorage(tmp_path)

        _set_database_version(tmp_path, str(DATABASE_SCHEMA_VERSION))
        reopened = FileStorage(tmp_path)
        assert reopened.database_integrity_check() == "ok"
        reopened.close_database_connections()
        _assert_no_sidecars(tmp_path)
    finally:
        if was_enabled:
            gc.enable()
        gc.collect()
