from __future__ import annotations

import os
import sqlite3
import threading
import weakref
from concurrent.futures import Future
from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Any, Callable, Hashable, Iterator, TypeVar

from micro_workflow_manager.paths import state_database_file

from .mutation_writer import SQLiteMutationWriter


T = TypeVar("T")


class SQLiteConnectionMixin:
    """Connections, transactions, queue notifications, and the mutation lane."""

    _db_init_locks: dict[Path, threading.Lock] = {}
    _db_init_locks_guard = threading.Lock()
    _db_write_locks: dict[Path, threading.RLock] = {}
    _db_write_locks_guard = threading.Lock()
    _initialized_databases: set[tuple[Path, int]] = set()
    _connection_registry: dict[tuple[Path, int, int], sqlite3.Connection] = {}
    _connection_threads: dict[tuple[Path, int, int], threading.Thread] = {}
    _connection_registry_guard = threading.Lock()
    _storage_path_refcounts: dict[tuple[Path, int], int] = {}
    _advisory_owner_registry: set[str] = set()
    _advisory_owner_registry_guard = threading.Lock()

    def _init_sqlite_state(self) -> None:
        self._advisory_local = threading.local()
        raw_path = state_database_file(self.project_dir)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        self._state_database_path_cached = raw_path.resolve()
        self._mutation_writer = SQLiteMutationWriter(self)
        path = self._state_database_path_cached
        pid = os.getpid()
        with self._connection_registry_guard:
            ref_key = (path, pid)
            self._storage_path_refcounts[ref_key] = (
                self._storage_path_refcounts.get(ref_key, 0) + 1
            )
        self._connection_finalizer = weakref.finalize(
            self,
            type(self)._release_storage_path,
            path,
            pid,
        )
        with self._db_init_locks_guard:
            lock = self._db_init_locks.setdefault(path, threading.Lock())
        with lock:
            initialized_key = (path, os.getpid())
            if initialized_key not in self._initialized_databases or not path.is_file():
                self.initialize_state_database()
                self._initialized_databases.add(initialized_key)

    def submit_db_mutation(
        self,
        operation: Callable[[sqlite3.Connection], T],
        *,
        wait: bool = True,
        priority: int = 10,
    ) -> T | Future[T]:
        """Run one mutation through the project-local priority writer."""
        return self._mutation_writer.submit(
            operation,
            wait=wait,
            priority=priority,
        )

    def submit_grouped_db_mutation(
        self,
        group_key: Hashable,
        item: Any,
        operation,
        *,
        wait: bool = True,
        priority: int = 10,
        collect_seconds: float = 0.001,
        weight: int | None = None,
    ):
        """Submit one item to a related mutation group in the single writer.

        Items may expose ``mutation_weight`` so existing wrappers that do not
        know about weighted batching remain compatible.
        """
        if weight is None:
            weight = getattr(item, "mutation_weight", 1)
        return self._mutation_writer.submit_grouped(
            group_key,
            item,
            operation,
            wait=wait,
            priority=priority,
            collect_seconds=collect_seconds,
            weight=weight,
        )

    def urgent_state_mutation_pending(self) -> bool:
        return self._mutation_writer.urgent_state_pending()

    def mutation_writer_diagnostics(self) -> dict[str, Any]:
        return self._mutation_writer.diagnostics()

    def persisted_mutation_writer_diagnostics(self) -> dict[str, Any]:
        return self._mutation_writer.persisted_diagnostics()

    def db_mutation_barrier(self) -> None:
        """Wait until every mutation submitted before this call is durable."""
        self._mutation_writer.barrier()

    def flush_db_mutations(self) -> None:
        self.db_mutation_barrier()

    def state_database_path(self) -> Path:
        path = getattr(self, "_state_database_path_cached", None)
        if path is not None:
            return path
        raw_path = state_database_file(self.project_dir)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        path = raw_path.resolve()
        self._state_database_path_cached = path
        return path

    def _new_db_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.state_database_path(),
            timeout=60.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 60000")
        connection.execute("PRAGMA synchronous = NORMAL")
        return connection

    @classmethod
    def _release_storage_path(cls, path: Path, pid: int) -> None:
        """Close a project's connections when its last storage owner is gone."""
        matches: list[sqlite3.Connection] = []
        with cls._connection_registry_guard:
            ref_key = (path, pid)
            remaining = cls._storage_path_refcounts.get(ref_key, 0) - 1
            if remaining > 0:
                cls._storage_path_refcounts[ref_key] = remaining
                return
            cls._storage_path_refcounts.pop(ref_key, None)
            for key, connection in list(cls._connection_registry.items()):
                if key[0] != path or key[1] != pid:
                    continue
                cls._connection_registry.pop(key, None)
                cls._connection_threads.pop(key, None)
                matches.append(connection)
        for connection in matches:
            try:
                connection.close()
            except sqlite3.Error:
                pass

    @classmethod
    def _prune_stale_registered_connections_locked(cls, pid: int) -> list[sqlite3.Connection]:
        """Detach dead-thread and deleted-project handles before FD exhaustion."""
        stale: list[sqlite3.Connection] = []
        for key, connection in list(cls._connection_registry.items()):
            if key[1] != pid:
                continue
            owner = cls._connection_threads.get(key)
            dead_thread = owner is not None and not owner.is_alive()
            deleted_project = not key[0].exists()
            if not dead_thread and not deleted_project:
                continue
            cls._connection_registry.pop(key, None)
            cls._connection_threads.pop(key, None)
            stale.append(connection)
        return stale

    def _connection_key(self) -> tuple[Path, int, int]:
        return (self.state_database_path(), os.getpid(), threading.get_ident())

    def db_connection(self) -> sqlite3.Connection:
        key = self._connection_key()
        current_thread = threading.current_thread()
        stale_connection = None
        stale_registry_connections: list[sqlite3.Connection] = []
        with self._connection_registry_guard:
            if len(self._connection_registry) >= 256:
                stale_registry_connections = self._prune_stale_registered_connections_locked(
                    os.getpid()
                )
            connection = self._connection_registry.get(key)
            owner_thread = self._connection_threads.get(key)
            # Python may reuse a dead thread's integer identifier. Never hand
            # its persistent SQLite connection to a new worker merely because
            # the recycled ident produced the same registry key.
            if connection is not None and owner_thread is not current_thread:
                stale_connection = self._connection_registry.pop(key, None)
                self._connection_threads.pop(key, None)
                connection = None
            if connection is None:
                connection = self._new_db_connection()
                self._connection_registry[key] = connection
                self._connection_threads[key] = current_thread
        for stale in stale_registry_connections:
            try:
                stale.close()
            except sqlite3.Error:
                pass
        if stale_connection is not None:
            try:
                stale_connection.close()
            except sqlite3.Error:
                pass
        return connection

    def close_thread_connection(self) -> None:
        key = self._connection_key()
        with self._connection_registry_guard:
            connection = self._connection_registry.pop(key, None)
            self._connection_threads.pop(key, None)
        if connection is not None:
            try:
                connection.close()
            except sqlite3.Error:
                pass

    def prune_dead_thread_connections(self) -> int:
        """Close database connections owned by framework threads that exited.

        Normal runners now close their own connection explicitly. This sweep is
        a defensive boundary for interrupted tests, custom runner extensions,
        and any older thread path that exits before its cleanup callback runs.
        """
        path = self.state_database_path()
        pid = os.getpid()
        current = threading.current_thread()
        stale: list[sqlite3.Connection] = []
        with self._connection_registry_guard:
            for key, connection in list(self._connection_registry.items()):
                if key[0] != path or key[1] != pid:
                    continue
                owner = self._connection_threads.get(key)
                if owner is current or (owner is not None and owner.is_alive()):
                    continue
                self._connection_registry.pop(key, None)
                self._connection_threads.pop(key, None)
                stale.append(connection)
        for connection in stale:
            try:
                connection.close()
            except sqlite3.Error:
                pass
        return len(stale)

    def close_database_connections(self) -> None:
        path = self.state_database_path()
        pid = os.getpid()
        with self._connection_registry_guard:
            matches = [
                (key, connection)
                for key, connection in self._connection_registry.items()
                if key[0] == path and key[1] == pid
            ]
            for key, _ in matches:
                self._connection_registry.pop(key, None)
                self._connection_threads.pop(key, None)
        for _, connection in matches:
            try:
                connection.close()
            except sqlite3.Error:
                pass

    def _database_write_lock(self) -> threading.RLock:
        path = self.state_database_path()
        with self._db_write_locks_guard:
            return self._db_write_locks.setdefault(path, threading.RLock())

    @contextmanager
    def db_transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self.db_connection()
        committed = False
        # SQLite has one writer even in WAL mode. Serialize same-process
        # writers before they enter SQLite instead of allowing hundreds of
        # worker connections to sit inside busy_timeout simultaneously.
        lock = self._database_write_lock() if immediate else nullcontext()
        with lock:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            try:
                yield connection
                connection.commit()
                committed = True
            except BaseException:
                # A commit failure must also roll back. Leaving a transaction
                # open on a persistent worker connection is enough to poison
                # every later round with "database is locked" failures.
                try:
                    connection.rollback()
                except sqlite3.Error:
                    pass
                raise
        if committed:
            notify = getattr(self, "notify_state_change", None)
            if callable(notify):
                notify()
