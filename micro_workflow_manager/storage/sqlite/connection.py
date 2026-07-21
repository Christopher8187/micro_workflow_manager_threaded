from __future__ import annotations

import os
import sqlite3
import threading
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
    _advisory_owner_registry: set[str] = set()
    _advisory_owner_registry_guard = threading.Lock()
    _queue_listeners: dict[Path, set[Callable[[], None]]] = {}
    _queue_listeners_guard = threading.Lock()

    def _init_sqlite_state(self) -> None:
        self._advisory_local = threading.local()
        self._mutation_writer = SQLiteMutationWriter(self)
        path = self.state_database_path().resolve()
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
    ):
        """Submit one item to a related mutation group in the single writer."""
        return self._mutation_writer.submit_grouped(
            group_key,
            item,
            operation,
            wait=wait,
            priority=priority,
            collect_seconds=collect_seconds,
        )

    def db_mutation_barrier(self) -> None:
        """Wait until every mutation submitted before this call is durable."""
        self._mutation_writer.barrier()

    def flush_db_mutations(self) -> None:
        self.db_mutation_barrier()

    def subscribe_queue_changes(self, callback: Callable[[], None]) -> Callable[[], None]:
        """Subscribe an in-process scheduler wakeup to durable queue changes."""
        path = self.state_database_path().resolve()
        with self._queue_listeners_guard:
            self._queue_listeners.setdefault(path, set()).add(callback)

        def unsubscribe() -> None:
            with self._queue_listeners_guard:
                listeners = self._queue_listeners.get(path)
                if listeners is None:
                    return
                listeners.discard(callback)
                if not listeners:
                    self._queue_listeners.pop(path, None)

        return unsubscribe

    def notify_queue_change(self) -> None:
        path = self.state_database_path().resolve()
        with self._queue_listeners_guard:
            listeners = tuple(self._queue_listeners.get(path, ()))
        for callback in listeners:
            callback()

    def state_database_path(self) -> Path:
        path = state_database_file(self.project_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
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

    def _connection_key(self) -> tuple[Path, int, int]:
        return (self.state_database_path().resolve(), os.getpid(), threading.get_ident())

    def db_connection(self) -> sqlite3.Connection:
        key = self._connection_key()
        current_thread = threading.current_thread()
        stale_connection = None
        with self._connection_registry_guard:
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
        if stale_connection is not None:
            try:
                stale_connection.close()
            except sqlite3.Error:
                pass
            return connection
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
        path = self.state_database_path().resolve()
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
        path = self.state_database_path().resolve()
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
        path = self.state_database_path().resolve()
        with self._db_write_locks_guard:
            return self._db_write_locks.setdefault(path, threading.RLock())

    @contextmanager
    def db_transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self.db_connection()
        # SQLite has one writer even in WAL mode. Serialize same-process
        # writers before they enter SQLite instead of allowing hundreds of
        # worker connections to sit inside busy_timeout simultaneously.
        lock = self._database_write_lock() if immediate else nullcontext()
        with lock:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            try:
                yield connection
                connection.commit()
            except BaseException:
                # A commit failure must also roll back. Leaving a transaction
                # open on a persistent worker connection is enough to poison
                # every later round with "database is locked" failures.
                try:
                    connection.rollback()
                except sqlite3.Error:
                    pass
                raise
