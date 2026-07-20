from __future__ import annotations

import json
import os
import shutil
import socket
import sqlite3
import threading
import time
from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

from micro_workflow_manager.models import QUEUED, RUNNING, VALID_STATUSES, now
from micro_workflow_manager.paths import state_database_file
from micro_workflow_manager.processes import process_is_alive


DATABASE_SCHEMA_VERSION = 2


class SQLiteStateMixin:
    """SQLite-backed framework metadata.

    User payloads remain ordinary files. SQLite stores only framework-owned,
    high-churn state: jobs, statuses, queue membership, execution leases,
    checkpoints, events, idempotency keys, default-job declarations, node
    statuses, and advisory locks.
    """

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

    def _init_sqlite_state(self) -> None:
        self._advisory_local = threading.local()
        path = self.state_database_path().resolve()
        with self._db_init_locks_guard:
            lock = self._db_init_locks.setdefault(path, threading.Lock())
        with lock:
            initialized_key = (path, os.getpid())
            if initialized_key not in self._initialized_databases or not path.is_file():
                self.initialize_state_database()
                self._initialized_databases.add(initialized_key)

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

    def initialize_state_database(self) -> Path:
        connection = self._new_db_connection()
        required_tables = {
            "metadata",
            "nodes",
            "jobs",
            "job_events",
            "idempotency",
            "default_job_specs",
            "advisory_locks",
            "job_sequences",
        }
        try:
            # WAL keeps monitor/inspect readers from blocking the scheduler's
            # short metadata writes. The WAL and SHM files are SQLite internals,
            # not per-job filesystem state.
            connection.execute("PRAGMA journal_mode = WAL")

            existing_tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            existing_version: int | None = None
            if "metadata" in existing_tables:
                row = connection.execute(
                    "SELECT value FROM metadata WHERE key='database_schema_version'"
                ).fetchone()
                if row is not None:
                    try:
                        existing_version = int(row[0])
                    except (TypeError, ValueError) as error:
                        raise RuntimeError(
                            "Invalid SQLite database_schema_version in .mwf/state.sqlite3"
                        ) from error
                    if existing_version > DATABASE_SCHEMA_VERSION:
                        raise RuntimeError(
                            "SQLite workflow state was written by a newer MWF schema "
                            f"({existing_version} > {DATABASE_SCHEMA_VERSION}). "
                            "Install a compatible newer package instead of downgrading."
                        )

            schema_is_current = (
                existing_version == DATABASE_SCHEMA_VERSION
                and required_tables.issubset(existing_tables)
            )
            if not schema_is_current:
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS metadata (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS nodes (
                        node_name TEXT PRIMARY KEY,
                        status TEXT,
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE TABLE IF NOT EXISTS jobs (
                        node_name TEXT NOT NULL,
                        job_id INTEGER NOT NULL,
                        parent_json TEXT,
                        created_at TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'queued',
                        status_json TEXT NOT NULL DEFAULT '{}',
                        generation INTEGER NOT NULL DEFAULT 0,
                        active_execution_id TEXT,
                        active_pid INTEGER,
                        active_thread_id INTEGER,
                        active_started_at TEXT,
                        restart_requested_at TEXT,
                        restart_requested_by_pid INTEGER,
                        restart_reason TEXT,
                        runtime_json TEXT,
                        PRIMARY KEY (node_name, job_id)
                    );

                    CREATE INDEX IF NOT EXISTS jobs_node_status_idx
                        ON jobs(node_name, status, job_id);
                    CREATE INDEX IF NOT EXISTS jobs_status_idx
                        ON jobs(status, node_name, job_id);

                    CREATE TABLE IF NOT EXISTS job_events (
                        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        node_name TEXT NOT NULL,
                        job_id INTEGER NOT NULL,
                        time TEXT NOT NULL,
                        event TEXT NOT NULL,
                        data_json TEXT NOT NULL DEFAULT '{}'
                    );

                    CREATE INDEX IF NOT EXISTS job_events_job_idx
                        ON job_events(node_name, job_id, event_id);

                    CREATE TABLE IF NOT EXISTS idempotency (
                        node_name TEXT NOT NULL,
                        key_hash TEXT NOT NULL,
                        key_text TEXT NOT NULL,
                        job_id INTEGER NOT NULL,
                        PRIMARY KEY (node_name, key_hash)
                    );

                    CREATE TABLE IF NOT EXISTS default_job_specs (
                        node_name TEXT NOT NULL,
                        spec_key TEXT NOT NULL,
                        start_job_id INTEGER NOT NULL,
                        number INTEGER NOT NULL,
                        params_signature TEXT NOT NULL,
                        PRIMARY KEY (node_name, spec_key)
                    );

                    CREATE TABLE IF NOT EXISTS advisory_locks (
                        name TEXT PRIMARY KEY,
                        owner TEXT NOT NULL,
                        acquired_at REAL NOT NULL,
                        expires_at REAL NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS job_sequences (
                        node_name TEXT PRIMARY KEY,
                        next_job_id INTEGER NOT NULL
                    );
                    """
                )
                connection.execute(
                    "INSERT INTO job_sequences(node_name, next_job_id) "
                    "SELECT node_name, COALESCE(MAX(job_id), 0) + 1 FROM jobs GROUP BY node_name "
                    "ON CONFLICT(node_name) DO UPDATE SET "
                    "next_job_id=MAX(job_sequences.next_job_id, excluded.next_job_id)"
                )
                connection.execute(
                    "INSERT INTO metadata(key, value) VALUES('database_schema_version', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (str(DATABASE_SCHEMA_VERSION),),
                )
                connection.commit()
        finally:
            connection.close()

        self._migrate_legacy_metadata_once()
        # 0.3.4 advisory locks are database rows. Reusable legacy lock files
        # are safe to remove after the database is ready.
        obsolete_locks = self.project_dir / ".mwf" / "locks"
        if obsolete_locks.exists():
            if obsolete_locks.is_dir():
                shutil.rmtree(obsolete_locks)
            else:
                obsolete_locks.unlink()
        legacy_locks = self.project_dir / ".mwf_locks"
        if legacy_locks.exists():
            if legacy_locks.is_dir():
                shutil.rmtree(legacy_locks)
            else:
                legacy_locks.unlink()
        return self.state_database_path()

    def database_integrity_check(self) -> str:
        row = self.db_connection().execute("PRAGMA quick_check").fetchone()
        return str(row[0]) if row is not None else "unknown"

    def _metadata_value(self, key: str) -> str | None:
        row = self.db_connection().execute(
            "SELECT value FROM metadata WHERE key = ?", (key,)
        ).fetchone()
        return None if row is None else str(row[0])

    def _set_metadata_value(self, key: str, value: str) -> None:
        with self.db_transaction() as connection:
            connection.execute(
                "INSERT INTO metadata(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    # ------------------------------------------------------------------
    # SQLite advisory locks replace .mwf/locks/*.lock files.
    # ------------------------------------------------------------------
    def _new_advisory_owner(self) -> str:
        return json.dumps(
            {
                "version": 2,
                "hostname": socket.gethostname(),
                "pid": os.getpid(),
                "thread_id": threading.get_ident(),
                "nonce": uuid4().hex,
            },
            separators=(",", ":"),
            sort_keys=True,
        )

    def _parse_advisory_owner(self, owner: str) -> dict[str, Any] | None:
        if not isinstance(owner, str) or not owner:
            return None
        try:
            data = json.loads(owner)
        except (TypeError, json.JSONDecodeError):
            data = None
        if isinstance(data, dict):
            pid = data.get("pid")
            hostname = data.get("hostname")
            if type(pid) is int and pid > 0 and isinstance(hostname, str):
                return {"pid": pid, "hostname": hostname}

        # MWF 0.3.4-0.3.6 stored ``pid:thread_id:uuid``. Those databases are
        # local project state, so an owner without a hostname is treated as a
        # local legacy owner for immediate dead-process recovery.
        parts = owner.split(":", 2)
        if len(parts) == 3:
            try:
                pid = int(parts[0])
            except ValueError:
                return None
            if pid > 0:
                return {"pid": pid, "hostname": None}
        return None

    @classmethod
    def _register_advisory_owner(cls, owner: str) -> None:
        with cls._advisory_owner_registry_guard:
            cls._advisory_owner_registry.add(owner)

    @classmethod
    def _unregister_advisory_owner(cls, owner: str) -> None:
        with cls._advisory_owner_registry_guard:
            cls._advisory_owner_registry.discard(owner)

    @classmethod
    def _advisory_owner_is_registered(cls, owner: str) -> bool:
        with cls._advisory_owner_registry_guard:
            return owner in cls._advisory_owner_registry

    def _advisory_owner_liveness(self, owner: str) -> bool | None:
        parsed = self._parse_advisory_owner(owner)
        if parsed is None:
            return None

        hostname = parsed["hostname"]
        if hostname not in {None, "", socket.gethostname()}:
            # A shared project directory may be visible from another host. We
            # cannot query that process locally, so its lease remains the
            # fallback authority.
            return None

        pid = parsed["pid"]
        if pid == os.getpid():
            # The process-local registry distinguishes a genuinely held lock
            # from a row orphaned by a terminated owner thread/storage object.
            return self._advisory_owner_is_registered(owner)
        return process_is_alive(pid)

    def _advisory_lock_is_reclaimable(
        self,
        owner: str,
        expires_at: float,
        now_value: float,
    ) -> bool:
        liveness = self._advisory_owner_liveness(owner)
        if liveness is False:
            return True
        if liveness is True:
            # Do not steal a live local lock merely because a long critical
            # section exceeded its nominal lease.
            return False
        return expires_at <= now_value

    @contextmanager
    def interprocess_lock(
        self,
        name: str,
        *,
        timeout: float = 120.0,
        lease_seconds: float = 300.0,
    ):
        safe_name = str(name)
        thread_lock = self.thread_lock_for(
            self.state_database_path().parent / "logical-locks" / safe_name
        )
        with thread_lock:
            held = getattr(self._advisory_local, "held", None)
            if held is None:
                held = {}
                self._advisory_local.held = held
            existing = held.get(safe_name)
            if existing is not None:
                existing["count"] += 1
                try:
                    yield
                finally:
                    existing["count"] -= 1
                return

            owner = self._new_advisory_owner()
            deadline = time.monotonic() + timeout
            delay = 0.005
            while True:
                acquired = False
                now_value = time.time()
                with self.db_transaction() as connection:
                    row = connection.execute(
                        "SELECT owner, expires_at FROM advisory_locks WHERE name = ?",
                        (safe_name,),
                    ).fetchone()
                    reclaimable = row is None
                    if row is not None:
                        reclaimable = self._advisory_lock_is_reclaimable(
                            str(row["owner"]),
                            float(row["expires_at"]),
                            now_value,
                        )
                    if reclaimable:
                        connection.execute(
                            "INSERT INTO advisory_locks(name, owner, acquired_at, expires_at) "
                            "VALUES(?, ?, ?, ?) "
                            "ON CONFLICT(name) DO UPDATE SET "
                            "owner=excluded.owner, acquired_at=excluded.acquired_at, "
                            "expires_at=excluded.expires_at",
                            (safe_name, owner, now_value, now_value + lease_seconds),
                        )
                        acquired = True
                if acquired:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Timed out acquiring MWF lock {safe_name!r}")
                time.sleep(delay)
                delay = min(0.2, delay * 1.5)

            self._register_advisory_owner(owner)
            held[safe_name] = {"owner": owner, "count": 1}
            try:
                yield
            finally:
                state = held.get(safe_name)
                if state is not None:
                    state["count"] -= 1
                    if state["count"] <= 0:
                        held.pop(safe_name, None)
                        try:
                            with self.db_transaction() as connection:
                                connection.execute(
                                    "DELETE FROM advisory_locks WHERE name = ? AND owner = ?",
                                    (safe_name, owner),
                                )
                        finally:
                            self._unregister_advisory_owner(owner)

    # ------------------------------------------------------------------
    # One-time 0.3.3-and-earlier metadata import.
    # ------------------------------------------------------------------
    def _migrate_legacy_metadata_once(self) -> None:
        if self._metadata_value("legacy_file_metadata_imported") == "1":
            return

        node_root = self.project_dir / "node"
        if node_root.is_dir():
            for node_dir in sorted(path for path in node_root.iterdir() if path.is_dir()):
                self._import_legacy_node(node_dir.name, node_dir)
        self._set_metadata_value("legacy_file_metadata_imported", "1")

    def _import_legacy_node(self, node_name: str, node_dir: Path) -> None:
        node_state = self.read_json(node_dir / "node_state.json", default=None)
        if isinstance(node_state, dict) and node_state.get("status") in VALID_STATUSES:
            with self.db_transaction() as connection:
                connection.execute(
                    "INSERT INTO nodes(node_name, status) VALUES(?, ?) "
                    "ON CONFLICT(node_name) DO UPDATE SET status=excluded.status, "
                    "updated_at=CURRENT_TIMESTAMP",
                    (node_name, node_state["status"]),
                )

        jobs_dir = node_dir / "jobs"
        if jobs_dir.is_dir():
            for job_dir in sorted(
                (path for path in jobs_dir.iterdir() if path.is_dir() and path.name.isdigit()),
                key=lambda path: int(path.name),
            ):
                self._import_legacy_job(node_name, int(job_dir.name), job_dir)

        idempotency_dir = node_dir / "idempotency"
        if idempotency_dir.is_dir():
            for path in idempotency_dir.glob("*.json"):
                data = self.read_json(path, default=None)
                if not isinstance(data, dict):
                    continue
                key = data.get("key")
                job_id = data.get("job_id")
                if isinstance(key, str) and type(job_id) is int:
                    with self.db_transaction() as connection:
                        connection.execute(
                            "INSERT OR IGNORE INTO idempotency"
                            "(node_name, key_hash, key_text, job_id) VALUES(?, ?, ?, ?)",
                            (node_name, path.stem, key, job_id),
                        )

        manifest = self.read_json(node_dir / "default_jobs.json", default={})
        if isinstance(manifest, dict):
            with self.db_transaction() as connection:
                for spec_key, spec in manifest.items():
                    if spec_key == "schema_version" or not isinstance(spec, dict):
                        continue
                    start = spec.get("start_job_id")
                    number = spec.get("number")
                    signature = spec.get("params_signature")
                    if type(start) is int and type(number) is int and isinstance(signature, str):
                        connection.execute(
                            "INSERT OR IGNORE INTO default_job_specs"
                            "(node_name, spec_key, start_job_id, number, params_signature) "
                            "VALUES(?, ?, ?, ?, ?)",
                            (node_name, spec_key, start, number, signature),
                        )

        # Delete only framework-owned metadata after it is durable in SQLite.
        for name in (
            "node_state.json",
            "default_jobs.json",
            "job_index.json",
            "job_index.dirty",
        ):
            self.remove_if_exists(node_dir / name)
        for directory in (node_dir / "queued", node_dir / "idempotency"):
            if directory.exists():
                shutil.rmtree(directory)

    def _import_legacy_job(self, node_name: str, job_id: int, job_dir: Path) -> None:
        job_data = self.read_json(job_dir / "job.json", default=None)
        if not isinstance(job_data, dict):
            return
        status_data = self.read_json(job_dir / "status.json", default=None)
        status_data = status_data if isinstance(status_data, dict) else {}
        status = status_data.get("status", QUEUED)
        if status not in VALID_STATUSES:
            status = QUEUED
        control = self.read_json(job_dir / "execution.json", default=None)
        control = control if isinstance(control, dict) else {}
        runtime = self.read_json(job_dir / "runtime.json", default=None)
        runtime_json = json.dumps(runtime, ensure_ascii=False) if isinstance(runtime, dict) else None
        parent = job_data.get("parent")
        parent_json = json.dumps(parent, ensure_ascii=False) if parent is not None else None
        created_at = str(job_data.get("created_at") or "")
        status_extra = {
            key: value for key, value in status_data.items()
            if key not in {"schema_version", "job_id", "node_name", "status"}
        }
        with self.db_transaction() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO jobs(" 
                "node_name, job_id, parent_json, created_at, status, status_json, "
                "generation, active_execution_id, active_pid, active_thread_id, "
                "active_started_at, restart_requested_at, restart_requested_by_pid, "
                "restart_reason, runtime_json" 
                ") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    node_name,
                    job_id,
                    parent_json,
                    created_at,
                    status,
                    json.dumps(status_extra, ensure_ascii=False),
                    int(control.get("generation", 0) or 0),
                    control.get("active_execution_id"),
                    control.get("active_pid"),
                    control.get("active_thread_id"),
                    control.get("active_started_at"),
                    control.get("restart_requested_at"),
                    control.get("restart_requested_by_pid"),
                    control.get("restart_reason"),
                    runtime_json,
                ),
            )
            count = connection.execute(
                "SELECT COUNT(*) FROM job_events WHERE node_name=? AND job_id=?",
                (node_name, job_id),
            ).fetchone()[0]
            if count == 0:
                events_path = job_dir / "events.jsonl"
                if events_path.is_file():
                    for raw in events_path.read_text(encoding="utf-8").splitlines():
                        try:
                            event_row = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(event_row, dict):
                            continue
                        event_name = str(event_row.pop("event", "event"))
                        event_time = str(event_row.pop("time", ""))
                        connection.execute(
                            "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
                            "VALUES(?, ?, ?, ?, ?)",
                            (
                                node_name,
                                job_id,
                                event_time,
                                event_name,
                                json.dumps(event_row, ensure_ascii=False, separators=(",", ":")),
                            ),
                        )

        for name in ("job.json", "status.json", "execution.json", "runtime.json", "events.jsonl"):
            self.remove_if_exists(job_dir / name)

    def delete_node_state(self, node_name: str) -> None:
        node_name = self.validate_node_name(node_name)
        with self.db_transaction() as connection:
            connection.execute("DELETE FROM idempotency WHERE node_name=?", (node_name,))
            connection.execute("DELETE FROM default_job_specs WHERE node_name=?", (node_name,))
            connection.execute("DELETE FROM job_events WHERE node_name=?", (node_name,))
            connection.execute("DELETE FROM jobs WHERE node_name=?", (node_name,))
            connection.execute("DELETE FROM job_sequences WHERE node_name=?", (node_name,))
            connection.execute("DELETE FROM nodes WHERE node_name=?", (node_name,))

    def export_node_state(self, node_name: str, destination: Path) -> Path:
        """Write a cold SQLite snapshot used by ``mwf copy``/``mwf paste``."""
        node_name = self.validate_node_name(node_name)
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.remove_if_exists(destination)
        snapshot = sqlite3.connect(destination)
        snapshot.row_factory = sqlite3.Row
        try:
            snapshot.executescript(
                """
                CREATE TABLE node_state (node_name TEXT PRIMARY KEY, status TEXT);
                CREATE TABLE jobs AS SELECT * FROM (SELECT
                    '' AS node_name, 0 AS job_id, NULL AS parent_json, '' AS created_at,
                    'queued' AS status, '{}' AS status_json, 0 AS generation,
                    NULL AS active_execution_id, NULL AS active_pid,
                    NULL AS active_thread_id, NULL AS active_started_at,
                    NULL AS restart_requested_at, NULL AS restart_requested_by_pid,
                    NULL AS restart_reason, NULL AS runtime_json) WHERE 0;
                CREATE TABLE job_events AS SELECT * FROM (SELECT
                    0 AS event_id, '' AS node_name, 0 AS job_id, '' AS time,
                    '' AS event, '{}' AS data_json) WHERE 0;
                CREATE TABLE idempotency AS SELECT * FROM (SELECT
                    '' AS node_name, '' AS key_hash, '' AS key_text, 0 AS job_id) WHERE 0;
                CREATE TABLE default_job_specs AS SELECT * FROM (SELECT
                    '' AS node_name, '' AS spec_key, 0 AS start_job_id,
                    0 AS number, '' AS params_signature) WHERE 0;
                """
            )
            source = self.db_connection()
            node_row = source.execute(
                "SELECT node_name, status FROM nodes WHERE node_name=?", (node_name,)
            ).fetchone()
            if node_row is not None:
                snapshot.execute(
                    "INSERT INTO node_state(node_name, status) VALUES(?, ?)",
                    tuple(node_row),
                )
            table_columns = {
                "jobs": [
                    "node_name", "job_id", "parent_json", "created_at", "status",
                    "status_json", "generation", "active_execution_id", "active_pid",
                    "active_thread_id", "active_started_at", "restart_requested_at",
                    "restart_requested_by_pid", "restart_reason", "runtime_json",
                ],
                "job_events": ["event_id", "node_name", "job_id", "time", "event", "data_json"],
                "idempotency": ["node_name", "key_hash", "key_text", "job_id"],
                "default_job_specs": [
                    "node_name", "spec_key", "start_job_id", "number", "params_signature"
                ],
            }
            for table, columns in table_columns.items():
                rows = source.execute(
                    f"SELECT {', '.join(columns)} FROM {table} WHERE node_name=?",
                    (node_name,),
                ).fetchall()
                if rows:
                    placeholders = ", ".join("?" for _ in columns)
                    snapshot.executemany(
                        f"INSERT INTO {table}({', '.join(columns)}) VALUES({placeholders})",
                        [tuple(row[column] for column in columns) for row in rows],
                    )
            snapshot.commit()
        finally:
            snapshot.close()
        return destination

    def reconcile_pasted_node_state(self, node_name: str) -> dict[str, int]:
        """Make pasted payload folders and SQLite metadata immediately consistent.

        Older clipboard snapshots may not contain a SQLite export, while a snapshot
        captured during execution may contain stale ``running`` leases. Pasting is a
        cold restore: missing metadata rows are rebuilt as queued jobs and running
        rows are requeued with their execution leases cleared.
        """
        node_name = self.validate_node_name(node_name)
        jobs_root = self.project_dir / "node" / node_name / "jobs"
        payload_ids: set[int] = set()
        if jobs_root.is_dir():
            for child in jobs_root.iterdir():
                if not child.is_dir() or not child.name.isdigit():
                    continue
                job_id = int(child.name)
                if job_id < 1 or not (child / "input.json").is_file():
                    continue
                payload_ids.add(job_id)

        created = 0
        requeued = 0
        removed = 0
        with self.db_transaction() as connection:
            existing_rows = connection.execute(
                "SELECT job_id, status FROM jobs WHERE node_name=?", (node_name,)
            ).fetchall()
            existing = {int(row["job_id"]): str(row["status"]) for row in existing_rows}

            for job_id in sorted(set(existing) - payload_ids):
                connection.execute(
                    "DELETE FROM job_events WHERE node_name=? AND job_id=?",
                    (node_name, job_id),
                )
                connection.execute(
                    "DELETE FROM jobs WHERE node_name=? AND job_id=?",
                    (node_name, job_id),
                )
                connection.execute(
                    "DELETE FROM idempotency WHERE node_name=? AND job_id=?",
                    (node_name, job_id),
                )
                removed += 1

            for job_id in sorted(payload_ids - set(existing)):
                connection.execute(
                    "INSERT INTO jobs(node_name, job_id, parent_json, created_at, status, status_json) "
                    "VALUES(?, ?, NULL, ?, ?, '{}')",
                    (node_name, job_id, now(), QUEUED),
                )
                connection.execute(
                    "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
                    "VALUES(?, ?, ?, 'clipboard_restored', ?)",
                    (node_name, job_id, now(), json.dumps({"status": QUEUED})),
                )
                created += 1

            connection.execute(
                "INSERT INTO job_sequences(node_name, next_job_id) VALUES(?, ?) "
                "ON CONFLICT(node_name) DO UPDATE SET next_job_id=excluded.next_job_id",
                (node_name, max(payload_ids, default=0) + 1),
            )

            running_ids = [job_id for job_id, status in existing.items() if job_id in payload_ids and status == RUNNING]
            for job_id in running_ids:
                connection.execute(
                    "UPDATE jobs SET status=?, status_json='{}', active_execution_id=NULL, "
                    "active_pid=NULL, active_thread_id=NULL, active_started_at=NULL, "
                    "restart_requested_at=NULL, restart_requested_by_pid=NULL, restart_reason=NULL, "
                    "runtime_json=NULL WHERE node_name=? AND job_id=?",
                    (QUEUED, node_name, job_id),
                )
                connection.execute(
                    "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
                    "VALUES(?, ?, ?, 'clipboard_requeued', ?)",
                    (node_name, job_id, now(), json.dumps({"previous_status": RUNNING, "status": QUEUED})),
                )
                requeued += 1

            queued_count = connection.execute(
                "SELECT COUNT(*) FROM jobs WHERE node_name=? AND status=?",
                (node_name, QUEUED),
            ).fetchone()[0]
            if queued_count:
                connection.execute(
                    "INSERT INTO nodes(node_name, status) VALUES(?, ?) "
                    "ON CONFLICT(node_name) DO UPDATE SET status=excluded.status",
                    (node_name, QUEUED),
                )
            elif payload_ids and connection.execute(
                "SELECT 1 FROM nodes WHERE node_name=?", (node_name,)
            ).fetchone() is None:
                connection.execute(
                    "INSERT INTO nodes(node_name, status) VALUES(?, ?)",
                    (node_name, QUEUED),
                )

        return {"created": created, "requeued": requeued, "removed": removed, "jobs": len(payload_ids)}

    def import_node_state(self, node_name: str, source_path: Path) -> None:
        node_name = self.validate_node_name(node_name)
        source_path = Path(source_path)
        if not source_path.is_file():
            # A clipboard made before 0.3.4 has no database snapshot. Preserve
            # its payload files and initialize an empty queued node.
            self.delete_node_state(node_name)
            return
        snapshot = sqlite3.connect(source_path)
        snapshot.row_factory = sqlite3.Row
        try:
            node_row = snapshot.execute("SELECT node_name, status FROM node_state LIMIT 1").fetchone()
            rows = {
                table: snapshot.execute(f"SELECT * FROM {table}").fetchall()
                for table in ("jobs", "job_events", "idempotency", "default_job_specs")
            }
        finally:
            snapshot.close()

        self.delete_node_state(node_name)
        with self.db_transaction() as connection:
            if node_row is not None:
                connection.execute(
                    "INSERT INTO nodes(node_name, status) VALUES(?, ?)",
                    (node_name, node_row["status"]),
                )
            for row in rows["jobs"]:
                connection.execute(
                    "INSERT INTO jobs(node_name, job_id, parent_json, created_at, status, "
                    "status_json, generation, active_execution_id, active_pid, active_thread_id, "
                    "active_started_at, restart_requested_at, restart_requested_by_pid, "
                    "restart_reason, runtime_json) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        node_name, row["job_id"], row["parent_json"], row["created_at"],
                        row["status"], row["status_json"], row["generation"],
                        row["active_execution_id"], row["active_pid"], row["active_thread_id"],
                        row["active_started_at"], row["restart_requested_at"],
                        row["restart_requested_by_pid"], row["restart_reason"], row["runtime_json"],
                    ),
                )
            for row in rows["job_events"]:
                connection.execute(
                    "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
                    "VALUES(?, ?, ?, ?, ?)",
                    (node_name, row["job_id"], row["time"], row["event"], row["data_json"]),
                )
            for row in rows["idempotency"]:
                connection.execute(
                    "INSERT INTO idempotency(node_name, key_hash, key_text, job_id) VALUES(?, ?, ?, ?)",
                    (node_name, row["key_hash"], row["key_text"], row["job_id"]),
                )
            for row in rows["default_job_specs"]:
                connection.execute(
                    "INSERT INTO default_job_specs(node_name, spec_key, start_job_id, number, params_signature) "
                    "VALUES(?, ?, ?, ?, ?)",
                    (
                        node_name, row["spec_key"], row["start_job_id"],
                        row["number"], row["params_signature"],
                    ),
                )
