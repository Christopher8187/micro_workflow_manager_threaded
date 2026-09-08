from __future__ import annotations

import sqlite3
from pathlib import Path

from .input_schema import INPUT_TABLES, create_input_tables
from .membership_schema import MEMBERSHIP_TABLES, create_membership_tables
from .component_schema import create_component_tables
from .misalignment_schema import MISALIGNMENT_TABLES, create_misalignment_tables
from .preparation_schema import PREPARATION_TABLES, create_preparation_tables


DATABASE_SCHEMA_VERSION = 6
SESSION_TABLES = frozenset({
    "execution_sessions", "session_components", "session_jobs",
    "graph_shapes", "component_definitions", "component_reservations", "component_holds",
    "job_execution_owners", "component_states", "job_instances",
    "pending_component_executions", "component_successful_results",
})
SESSION_TRIGGERS = frozenset({"create_job_instance"})
CORE_TABLES = frozenset({
    "metadata", "nodes", "jobs", "job_events", "idempotency",
    "default_job_specs", "advisory_locks", "job_sequences", "network_state",
})
NATIVE_TABLES = (CORE_TABLES | SESSION_TABLES | INPUT_TABLES | MISALIGNMENT_TABLES
                 | PREPARATION_TABLES | MEMBERSHIP_TABLES)


class SQLiteSchemaMixin:
    """Native schema creation and integrity checks."""

    def initialize_state_database(self, *, create: bool = False) -> Path:
        connection = self._new_db_connection()
        try:
            # WAL keeps monitor/inspect readers from blocking the scheduler's
            # short metadata writes. The WAL and SHM files are SQLite internals,
            # not per-job filesystem state.
            if create:
                connection.execute("PRAGMA journal_mode = WAL")
            # All processes must decide the schema under the same SQLite write
            # transaction. A process must not publish a stale pre-lock version.
            connection.execute("BEGIN IMMEDIATE")

            if create:
                existing_tables = {
                    str(row[0]) for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                if existing_tables:
                    raise RuntimeError("Fresh session storage found an already initialized database")
                self._create_core_tables(connection)
                self._create_execution_session_tables(connection)
                connection.execute(
                    "INSERT INTO metadata(key, value) VALUES('database_schema_version', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (str(DATABASE_SCHEMA_VERSION),),
                )
            else:
                self.validate_native_database(connection)
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

        return self.state_database_path()

    @staticmethod
    def validate_native_database(connection) -> None:
        """Validate a caller-owned snapshot without modifying its database."""
        existing_tables = {
            str(row[0]) for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        existing_version = None
        if "metadata" in existing_tables:
            row = connection.execute(
                "SELECT value FROM metadata WHERE key='database_schema_version'"
            ).fetchone()
            if row is not None:
                try:
                    existing_version = int(row[0])
                except (TypeError, ValueError) as error:
                    raise RuntimeError("Invalid SQLite database_schema_version in .mwf/state.sqlite3") from error
                if type(row[0]) is not str or row[0] != str(existing_version):
                    raise RuntimeError("Invalid SQLite database_schema_version in .mwf/state.sqlite3")
                if existing_version > DATABASE_SCHEMA_VERSION:
                    raise RuntimeError(
                        "SQLite workflow state was written by a newer MWF schema "
                        f"({existing_version} > {DATABASE_SCHEMA_VERSION}). "
                        "Install a compatible newer package instead of downgrading."
                    )
        if existing_version is not None and existing_version < DATABASE_SCHEMA_VERSION:
            raise RuntimeError("Unsupported MWF project format. Use migration.md to prepare a separate fresh project.")
        existing_triggers = {
            str(row[0]) for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        }
        if (existing_tables.intersection(SESSION_TABLES)
                or existing_triggers.intersection(SESSION_TRIGGERS)) and existing_version != DATABASE_SCHEMA_VERSION:
            raise RuntimeError("Incomplete SQLite execution-session schema: missing current format marker")
        if existing_version != DATABASE_SCHEMA_VERSION:
            raise RuntimeError("Unsupported MWF project format. Use migration.md to prepare a separate fresh project.")
        if not NATIVE_TABLES.issubset(existing_tables):
            raise RuntimeError("Incomplete SQLite execution-session schema")
        SQLiteSchemaMixin._validate_execution_session_schema(connection)

    @staticmethod
    def _create_core_tables(connection) -> None:
        SQLiteSchemaMixin._execute_schema_statements(connection,
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
        def objects(database):
            placeholders = ",".join("?" for _ in NATIVE_TABLES)
            trigger_placeholders = ",".join("?" for _ in SESSION_TRIGGERS)
            return {
                (row[0], row[1]): " ".join(row[2].split()) if row[2] is not None else None
                for row in database.execute(
                    "SELECT type, name, sql FROM sqlite_master "
                    f"WHERE tbl_name IN ({placeholders}) "
                    f"OR (type='trigger' AND name IN ({trigger_placeholders}))",
                    tuple(sorted(NATIVE_TABLES)) + tuple(sorted(SESSION_TRIGGERS)),
                )
            }

        # The current format has one complete shape. Compare declarations, including
        # column checks, foreign keys and the partial main-slot uniqueness rule.
        reference = sqlite3.connect(":memory:")
        try:
            SQLiteSchemaMixin._create_core_tables(reference)
            SQLiteSchemaMixin._create_execution_session_tables(reference)
            expected = objects(reference)
        finally:
            reference.close()
        actual = objects(connection)
        if actual != expected:
            raise RuntimeError("Incomplete SQLite execution-session schema: declarations differ")
        missing_identity = connection.execute(
            "SELECT 1 FROM jobs AS j LEFT JOIN job_instances AS i "
            "ON i.node_name=j.node_name AND i.job_id=j.job_id "
            "WHERE i.node_name IS NULL LIMIT 1"
        ).fetchone()
        orphan_identity = connection.execute(
            "SELECT 1 FROM job_instances AS i LEFT JOIN jobs AS j "
            "ON j.node_name=i.node_name AND j.job_id=i.job_id "
            "WHERE j.node_name IS NULL LIMIT 1"
        ).fetchone()
        invalid_identity = connection.execute(
            "SELECT 1 FROM job_instances WHERE typeof(instance_id)<>'text' "
            "OR length(instance_id)<>32 OR length(CAST(instance_id AS BLOB))<>32 "
            "OR instance_id GLOB '*[^0-9a-f]*' LIMIT 1"
        ).fetchone()
        if missing_identity or orphan_identity or invalid_identity:
            raise RuntimeError("Incomplete SQLite execution-session schema: invalid job identities")

    @staticmethod
    def _create_execution_session_tables(connection) -> None:
        connection.execute("""
            CREATE TABLE job_instances (
                node_name TEXT NOT NULL,
                job_id INTEGER NOT NULL,
                instance_id TEXT NOT NULL UNIQUE
                    CHECK(typeof(instance_id)='text' AND length(instance_id)=32
                          AND length(CAST(instance_id AS BLOB))=32
                          AND instance_id NOT GLOB '*[^0-9a-f]*'),
                last_execution_id TEXT REFERENCES job_execution_owners(execution_id),
                created_by_execution_id TEXT REFERENCES job_execution_owners(execution_id),
                PRIMARY KEY(node_name, job_id),
                FOREIGN KEY(node_name, job_id) REFERENCES jobs(node_name, job_id)
                    ON DELETE CASCADE
            )
        """)
        connection.execute("""
            CREATE TRIGGER create_job_instance AFTER INSERT ON jobs
            BEGIN
                INSERT INTO job_instances(node_name, job_id, instance_id)
                VALUES(NEW.node_name, NEW.job_id, lower(hex(randomblob(16))));
                SELECT RAISE(ABORT, 'Job instance identity was not created')
                WHERE NOT EXISTS(SELECT 1 FROM job_instances
                                 WHERE node_name=NEW.node_name AND job_id=NEW.job_id);
            END
        """)
        connection.execute("""
            CREATE TABLE execution_sessions (
                session_id TEXT PRIMARY KEY,
                session_kind TEXT NOT NULL CHECK(session_kind IN ('main', 'interrupt')),
                parent_session_id TEXT REFERENCES execution_sessions(session_id)
                    CHECK(parent_session_id IS NULL OR
                          (session_kind='interrupt' AND parent_session_id<>session_id)),
                command TEXT NOT NULL,
                selection_kind TEXT NOT NULL CHECK(selection_kind IN ('components', 'jobs')),
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
                details_json TEXT NOT NULL DEFAULT '{}',
                admitted_shape_id INTEGER NOT NULL REFERENCES graph_shapes(shape_id),
                partition_revision INTEGER NOT NULL
                    CHECK(typeof(partition_revision)='integer' AND partition_revision>=0)
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
                job_instance_id TEXT NOT NULL
                    CHECK(typeof(job_instance_id)='text' AND length(job_instance_id)=32
                          AND length(CAST(job_instance_id AS BLOB))=32
                          AND job_instance_id NOT GLOB '*[^0-9a-f]*'),
                PRIMARY KEY(session_id, node_name, job_id),
                UNIQUE(session_id, position)
            )
        """)
        create_component_tables(connection)
        create_membership_tables(connection)

        create_input_tables(connection)
        create_preparation_tables(connection)
        create_misalignment_tables(connection)

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
