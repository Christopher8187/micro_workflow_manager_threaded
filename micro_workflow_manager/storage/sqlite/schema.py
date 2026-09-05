from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path


DATABASE_SCHEMA_VERSION = 5
AUTOMATIC_SCHEMA_VERSION = 4


class SQLiteSchemaMixin:
    """Schema creation, migration metadata, and integrity checks."""

    def initialize_state_database(self, *, initial_schema_version: int = AUTOMATIC_SCHEMA_VERSION) -> Path:
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
            "network_state",
        }
        try:
            # WAL keeps monitor/inspect readers from blocking the scheduler's
            # short metadata writes. The WAL and SHM files are SQLite internals,
            # not per-job filesystem state.
            connection.execute("PRAGMA journal_mode = WAL")
            # All processes must decide the schema under the same SQLite write
            # transaction. A process must not publish a stale pre-lock version.
            connection.execute("BEGIN IMMEDIATE")

            existing_tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if initial_schema_version == 5:
                if existing_tables:
                    raise RuntimeError("Fresh session storage found an already initialized database")
                self._fresh_database_owned = True
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

            session_tables = {"execution_sessions", "session_components", "session_jobs"}
            if existing_tables.intersection(session_tables) and existing_version != 5:
                raise RuntimeError("Incomplete SQLite execution-session schema: missing version 5 marker")
            target_version = max(existing_version or 0, initial_schema_version)
            if target_version == 5:
                required_tables.update(session_tables)
                if existing_version == 5 and not required_tables.issubset(existing_tables):
                    raise RuntimeError("Incomplete SQLite execution-session schema")
                if existing_version == 5:
                    self._validate_execution_session_schema(connection)
            schema_is_current = (
                existing_version == target_version
                and required_tables.issubset(existing_tables)
            )
            if not schema_is_current:
                self._execute_schema_statements(connection,
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
                    CREATE INDEX IF NOT EXISTS jobs_active_execution_idx
                        ON jobs(node_name, active_execution_id)
                        WHERE active_execution_id IS NOT NULL;

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

                    CREATE TABLE IF NOT EXISTS network_state (
                        node_name TEXT PRIMARY KEY,
                        submitted INTEGER NOT NULL DEFAULT 0,
                        dispatched INTEGER NOT NULL DEFAULT 0,
                        completed INTEGER NOT NULL DEFAULT 0,
                        failed INTEGER NOT NULL DEFAULT 0,
                        bytes_received INTEGER NOT NULL DEFAULT 0,
                        in_flight INTEGER NOT NULL DEFAULT 0,
                        peak_in_flight INTEGER NOT NULL DEFAULT 0,
                        max_ingress_delay_seconds REAL NOT NULL DEFAULT 0,
                        max_request_seconds REAL NOT NULL DEFAULT 0,
                        average_request_seconds REAL NOT NULL DEFAULT 0,
                        last_error TEXT,
                        updated_at REAL NOT NULL DEFAULT 0
                    );
                    """
                )
                if target_version == 5:
                    self._create_execution_session_tables(connection)
                    connection.execute(
                        "INSERT INTO metadata(key, value) VALUES('legacy_file_metadata_imported', '1')"
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
                    (str(target_version),),
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
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

    @staticmethod
    def _execute_schema_statements(connection, script: str) -> None:
        # executescript commits an existing transaction before executing its
        # input. Execute these fixed declarations without releasing our lock.
        statement = ""
        for line in script.splitlines(keepends=True):
            statement += line
            if sqlite3.complete_statement(statement):
                connection.execute(statement)
                statement = ""
        if statement.strip():
            raise RuntimeError("Incomplete internal SQLite schema declaration")

    @staticmethod
    def _validate_execution_session_schema(connection) -> None:
        marker = connection.execute(
            "SELECT value FROM metadata WHERE key='legacy_file_metadata_imported'"
        ).fetchone()
        if marker is None or marker[0] != "1":
            raise RuntimeError("Incomplete SQLite execution-session schema: missing fresh-state marker")

        def objects(database):
            return {
                (row[0], row[1]): " ".join(row[2].split()) if row[2] is not None else None
                for row in database.execute(
                    "SELECT type, name, sql FROM sqlite_master "
                    "WHERE tbl_name IN ('execution_sessions', 'session_components', 'session_jobs')"
                )
            }

        # Version 5 has one complete shape. Compare its declarations, including
        # column checks, foreign keys and the partial main-slot uniqueness rule.
        reference = sqlite3.connect(":memory:")
        try:
            SQLiteSchemaMixin._create_execution_session_tables(reference)
            expected = objects(reference)
        finally:
            reference.close()
        actual = objects(connection)
        if actual != expected:
            raise RuntimeError("Incomplete SQLite execution-session schema: declarations differ")

    @staticmethod
    def _create_execution_session_tables(connection) -> None:
        connection.execute("""
            CREATE TABLE execution_sessions (
                session_id TEXT PRIMARY KEY,
                session_kind TEXT NOT NULL CHECK(session_kind IN ('main', 'interrupt')),
                parent_session_id TEXT REFERENCES execution_sessions(session_id)
                    CHECK(parent_session_id IS NULL OR
                          (session_kind='interrupt' AND parent_session_id<>session_id)),
                command TEXT NOT NULL,
                start_component TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('running', 'terminal')),
                started_at TEXT NOT NULL,
                heartbeat_at TEXT NOT NULL,
                finished_at TEXT,
                hostname TEXT NOT NULL,
                pid INTEGER NOT NULL,
                process_identity TEXT,
                outcome TEXT,
                failures_json TEXT NOT NULL DEFAULT '[]',
                details_json TEXT NOT NULL DEFAULT '{}'
            )
        """)
        connection.execute("""
            CREATE UNIQUE INDEX one_running_main_session
                ON execution_sessions(session_kind)
                WHERE session_kind='main' AND status='running'
        """)
        connection.execute("""
            CREATE TABLE session_components (
                session_id TEXT NOT NULL REFERENCES execution_sessions(session_id),
                position INTEGER NOT NULL,
                component_key TEXT NOT NULL,
                PRIMARY KEY(session_id, component_key),
                UNIQUE(session_id, position)
            )
        """)
        connection.execute("""
            CREATE TABLE session_jobs (
                session_id TEXT NOT NULL REFERENCES execution_sessions(session_id),
                position INTEGER NOT NULL,
                node_name TEXT NOT NULL,
                job_id INTEGER NOT NULL,
                PRIMARY KEY(session_id, node_name, job_id),
                UNIQUE(session_id, position)
            )
        """)

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
