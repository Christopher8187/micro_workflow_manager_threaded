from __future__ import annotations

import os
import sqlite3
import stat
from pathlib import Path

from .base import FileStorageBase
from .execution import JobExecutionStorageMixin
from .execution_sessions import ExecutionSessionStorageMixin
from .events import JobEventStorageMixin
from .job_index import JobIndexStorageMixin
from .jobs import JobFileStorageMixin
from .nodes import NodeFileStorageMixin
from .network_state import NetworkStateStorageMixin
from .runtime_config import RuntimeConfigStorageMixin
from .sqlite_state import SQLiteStateMixin
from .state_events import StateEventStorageMixin


def _is_link_or_reparse_point(path: Path) -> bool:
    try:
        entry = path.lstat()
    except FileNotFoundError:
        return False
    return stat.S_ISLNK(entry.st_mode) or bool(
        getattr(entry, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    )


class FileStorage(
    ExecutionSessionStorageMixin,
    NetworkStateStorageMixin,
    StateEventStorageMixin,
    RuntimeConfigStorageMixin,
    JobEventStorageMixin,
    JobExecutionStorageMixin,
    JobFileStorageMixin,
    JobIndexStorageMixin,
    NodeFileStorageMixin,
    SQLiteStateMixin,
    FileStorageBase,
):
    """Hybrid storage: user payload files plus SQLite framework state."""

    def __init__(self, project_dir):
        self._initialize_storage(project_dir)

    def _initialize_storage(self, project_dir, *, initial_schema_version=4):
        super().__init__(project_dir)
        self._init_network_state_publisher()
        self._init_state_event_broker()
        self._init_sqlite_state(initial_schema_version=initial_schema_version)
        self._init_job_execution_state()

    @classmethod
    def _create_new_project_state(cls, project_dir):
        """Internal fresh-store creation; runtime activation is a separate step."""
        root = Path(project_dir).resolve()
        cls._refuse_existing_runtime(root)
        # An ordinary constructor must never upgrade an existing project to
        # session storage. Claim the new state directory before initialization.
        metadata = root / ".mwf"
        metadata.mkdir(parents=True, exist_ok=False)
        directory_identity = metadata.stat()
        storage = cls.__new__(cls)
        storage._fresh_database_owned = False
        try:
            storage._initialize_storage(root, initial_schema_version=5)
        except BaseException as error:
            try:
                finalizer = getattr(storage, "_connection_finalizer", None)
                if finalizer is not None:
                    finalizer()
                path = getattr(storage, "_state_database_path_cached", None)
                if path is not None and storage._fresh_database_owned:
                    cls._initialized_databases.discard((path, os.getpid()))
                current_identity = metadata.lstat()
                if (_is_link_or_reparse_point(metadata)
                    or (directory_identity.st_dev, directory_identity.st_ino)
                    != (current_identity.st_dev, current_identity.st_ino)):
                    raise RuntimeError("The new state directory changed during initialization")
                # A competing ordinary initializer may have committed its own
                # state. Owning the directory does not make that database ours.
                if storage._fresh_database_owned:
                    database = metadata / "state.sqlite3"
                    check = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)
                    try:
                        tables = {row[0] for row in check.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                        version = check.execute(
                            "SELECT value FROM metadata WHERE key='database_schema_version'"
                        ).fetchone() if "metadata" in tables else None
                    finally:
                        check.close()
                    if not tables or (version is not None and version[0] == "5"):
                        for name in ("state.sqlite3-wal", "state.sqlite3-shm", "state.sqlite3"):
                            (metadata / name).unlink(missing_ok=True)
                # Preserve unexpected entries and let rmdir refuse them.
                metadata.rmdir()
            except Exception as cleanup_error:
                raise error from cleanup_error
            raise
        return storage

    @staticmethod
    def _refuse_existing_runtime(root: Path) -> None:
        def refuse(path):
            raise RuntimeError(f"Fresh session storage requires no existing MWF runtime state: {path}")

        for name in (".mwf", ".mwf_run.json", ".mwf_threads.json", ".mwf_locks"):
            path = root / name
            if os.path.lexists(path):
                refuse(path)
        nodes = root / "node"
        if _is_link_or_reparse_point(nodes):
            refuse(nodes)
        if not nodes.is_dir():
            return
        for node in nodes.iterdir():
            if _is_link_or_reparse_point(node):
                refuse(node)
            if not node.is_dir():
                continue
            for name in ("schema.json", "node_state.json", "default_jobs.json",
                         "job_index.json", "job_index.dirty", "queued", "idempotency", "jobs"):
                path = node / name
                if os.path.lexists(path):
                    refuse(path)
