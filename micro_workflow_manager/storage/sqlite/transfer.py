from __future__ import annotations

import json
import sqlite3
import shutil
from pathlib import Path

from micro_workflow_manager.models import (
    JOB_VALID_STATUSES,
    NODE_VALID_STATUSES,
    QUEUED,
    RUNNING,
    now,
)


class SQLiteStateTransferMixin:
    """Legacy migration plus clipboard/export/import state transfer."""

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
        if isinstance(node_state, dict) and node_state.get("status") in NODE_VALID_STATUSES:
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
        if status not in JOB_VALID_STATUSES:
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
