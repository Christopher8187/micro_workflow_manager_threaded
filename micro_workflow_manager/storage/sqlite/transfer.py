from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from micro_workflow_manager.project_format import is_link_or_reparse_point

from micro_workflow_manager.models import (
    QUEUED,
    RUNNING,
    now,
)


class SQLiteStateTransferMixin:
    """Native clipboard state transfer."""

    def delete_node_state(self, node_name: str) -> None:
        node_name = self.validate_node_name(node_name)
        with self.db_transaction() as connection:
            self._delete_node_state(connection, node_name)

    @staticmethod
    def _delete_node_state(connection, node_name):
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

        A native snapshot must describe every pasted job. A snapshot captured
        during execution may contain stale running leases, which are cleared
        when those jobs are requeued for the destination.
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

        requeued = 0
        removed = 0
        with self.db_transaction() as connection:
            existing_rows = connection.execute(
                "SELECT job_id, status FROM jobs WHERE node_name=?", (node_name,)
            ).fetchall()
            existing = {int(row["job_id"]): str(row["status"]) for row in existing_rows}

            missing = sorted(payload_ids - set(existing))
            if missing:
                addresses = ', '.join(f'{node_name}/{job_id}' for job_id in missing)
                raise RuntimeError(f'Clipboard payloads have no native job state: {addresses}')

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

        return {"requeued": requeued, "removed": removed, "jobs": len(payload_ids)}

    def import_node_state(self, node_name: str, source_path: Path) -> None:
        node_name = self.validate_node_name(node_name)
        source_path = Path(source_path)
        if is_link_or_reparse_point(source_path) or not source_path.is_file():
            raise RuntimeError("Native clipboard state snapshot is missing or is not an ordinary file")
        snapshot = sqlite3.connect(source_path.resolve().as_uri() + '?mode=ro', uri=True)
        snapshot.row_factory = sqlite3.Row
        try:
            node_row = snapshot.execute("SELECT node_name, status FROM node_state LIMIT 1").fetchone()
            rows = {
                table: snapshot.execute(f"SELECT * FROM {table}").fetchall()
                for table in ("jobs", "job_events", "idempotency", "default_job_specs")
            }
        finally:
            snapshot.close()

        with self.db_transaction() as connection:
            self._delete_node_state(connection, node_name)
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
