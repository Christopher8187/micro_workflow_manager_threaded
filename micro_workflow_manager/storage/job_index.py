from __future__ import annotations

import json
from typing import Any

from micro_workflow_manager.models import CANCELLED, DONE, FAILED, JOB_VALID_STATUSES, QUEUED, RUNNING, SKIPPED


class JobIndexStorageMixin:
    """SQL-backed summaries; retained method names preserve the public API."""

    def job_index_dirty(self, node_name: str) -> bool:
        return False

    def mark_job_index_dirty(self, node_name: str, reason: str | None = None):
        return None

    def clear_job_index_dirty(self, node_name: str):
        return None

    def empty_job_index(self, node_name: str) -> dict[str, Any]:
        return {
            "node": node_name,
            "last_job_id": 0,
            "counts": {status: 0 for status in sorted(JOB_VALID_STATUSES)},
            "running_jobs": {},
            "duration_total": 0.0,
            "duration_count": 0,
        }

    def normalize_job_index(self, node_name: str, data: Any) -> dict[str, Any] | None:
        return self.read_job_index(node_name)

    def read_job_index(self, node_name: str) -> dict[str, Any]:
        index = self.empty_job_index(node_name)
        connection = self.db_connection()
        summary = connection.execute(
            "SELECT COALESCE(MAX(job_id), 0) AS last_job_id, "
            "COALESCE(SUM(CASE WHEN status IN ('done','failed','skipped','cancelled') "
            "THEN CAST(json_extract(status_json, '$.duration_seconds') AS REAL) ELSE 0 END), 0) "
            "AS duration_total, "
            "SUM(CASE WHEN status IN ('done','failed','skipped','cancelled') "
            "AND json_type(status_json, '$.duration_seconds') IN ('integer','real') "
            "THEN 1 ELSE 0 END) AS duration_count "
            "FROM jobs WHERE node_name=?",
            (node_name,),
        ).fetchone()
        if summary is not None:
            index["last_job_id"] = int(summary["last_job_id"] or 0)
            index["duration_total"] = float(summary["duration_total"] or 0.0)
            index["duration_count"] = int(summary["duration_count"] or 0)

        for row in connection.execute(
            "SELECT status, COUNT(*) AS count FROM jobs WHERE node_name=? GROUP BY status",
            (node_name,),
        ).fetchall():
            status = row["status"]
            if status in index["counts"]:
                index["counts"][status] = int(row["count"])

        for row in connection.execute(
            "SELECT job_id, json_extract(status_json, '$.started_at') AS started_at "
            "FROM jobs WHERE node_name=? AND status=? ORDER BY job_id",
            (node_name, RUNNING),
        ).fetchall():
            index["running_jobs"][str(int(row["job_id"]))] = {
                "started_at": row["started_at"]
            }
        return index

    def write_job_index(self, node_name: str, index: dict[str, Any]):
        # SQLite is authoritative and indexed; there is no rebuildable sidecar.
        return None

    def rebuild_job_index_unlocked(self, node_name: str) -> dict[str, Any]:
        return self.read_job_index(node_name)

    def rebuild_job_index(self, node_name: str) -> dict[str, Any]:
        return self.read_job_index(node_name)

    def job_status_counts(self, node_name: str) -> dict[str, int]:
        counts = {status: 0 for status in sorted(JOB_VALID_STATUSES)}
        rows = self.db_connection().execute(
            "SELECT status, COUNT(*) AS count FROM jobs WHERE node_name=? GROUP BY status",
            (node_name,),
        ).fetchall()
        for row in rows:
            if row["status"] in counts:
                counts[row["status"]] = int(row["count"])
        return counts

    def node_job_summary(self, node_name: str) -> dict[str, Any]:
        index = self.read_job_index(node_name)
        counts = dict(index["counts"])
        duration_count = int(index["duration_count"])
        avg_duration = (
            float(index["duration_total"]) / duration_count if duration_count else None
        )
        recent = self.db_connection().execute(
            "SELECT COUNT(*) AS count FROM jobs WHERE node_name=? "
            "AND status IN ('done','skipped') "
            "AND julianday(json_extract(status_json, '$.finished_at')) "
            ">= julianday('now', '-60 seconds')",
            (node_name,),
        ).fetchone()
        return {
            "total": sum(counts.values()),
            "counts": counts,
            "running_jobs": dict(index["running_jobs"]),
            "avg_duration_seconds": avg_duration,
            "completed_last_60_seconds": int(recent["count"] or 0),
        }

    def register_job_created(self, node_name: str, job_id: int, status: str = QUEUED):
        return None

    def update_job_index_status(
        self,
        node_name: str,
        job_id: int,
        old_status: str | None,
        new_status: str,
        old_data: dict[str, Any] | None = None,
        new_data: dict[str, Any] | None = None,
    ):
        return None
