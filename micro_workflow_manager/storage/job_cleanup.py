from __future__ import annotations

import hashlib
import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from micro_workflow_manager.models import Job, QUEUED


class JobCleanupStorageMixin:
    """Job deletion and fresh-run reset operations."""

    def delete_job(self, node_name: str, job_id: int, *, remove_payload: bool = True) -> bool:
        job_id = self.validate_job_id(job_id)
        with self.db_transaction() as connection:
            existed = connection.execute(
                "SELECT 1 FROM jobs WHERE node_name=? AND job_id=?",
                (node_name, job_id),
            ).fetchone() is not None
            connection.execute(
                "DELETE FROM idempotency WHERE node_name=? AND job_id=?",
                (node_name, job_id),
            )
            connection.execute(
                "DELETE FROM job_events WHERE node_name=? AND job_id=?",
                (node_name, job_id),
            )
            connection.execute(
                "DELETE FROM jobs WHERE node_name=? AND job_id=?",
                (node_name, job_id),
            )
        if remove_payload:
            shutil.rmtree(self.job_base_dir(node_name, job_id), ignore_errors=True)
        return existed

    def delete_jobs_batch(
        self,
        node_name: str,
        job_ids: list[int],
        *,
        remove_payload: bool = True,
    ) -> int:
        """Delete selected jobs with one transaction and bulk directory removal."""
        node_name = self.validate_node_name(node_name)
        normalized = sorted({self.validate_job_id(job_id) for job_id in job_ids})
        if not normalized:
            return 0
        existing_ids = self.list_job_ids(node_name)
        targets = sorted(set(existing_ids).intersection(normalized))
        if not targets:
            return 0
        remove_whole_jobs_dir = targets == existing_ids

        with self.db_transaction() as connection:
            if remove_whole_jobs_dir:
                connection.execute("DELETE FROM idempotency WHERE node_name=?", (node_name,))
                connection.execute("DELETE FROM job_events WHERE node_name=?", (node_name,))
                connection.execute("DELETE FROM jobs WHERE node_name=?", (node_name,))
            else:
                for offset in range(0, len(targets), 500):
                    chunk = targets[offset:offset + 500]
                    placeholders = ",".join("?" for _ in chunk)
                    args = [node_name, *chunk]
                    connection.execute(
                        f"DELETE FROM idempotency WHERE node_name=? AND job_id IN ({placeholders})",
                        args,
                    )
                    connection.execute(
                        f"DELETE FROM job_events WHERE node_name=? AND job_id IN ({placeholders})",
                        args,
                    )
                    connection.execute(
                        f"DELETE FROM jobs WHERE node_name=? AND job_id IN ({placeholders})",
                        args,
                    )

        if remove_payload:
            jobs_dir = self.jobs_dir(node_name)

            def remove_job_payload(job_id: int) -> None:
                shutil.rmtree(
                    self.safe_join(jobs_dir, str(job_id)),
                    ignore_errors=True,
                )

            # Windows directory removal is disproportionately expensive and was
            # still serial after the SQLite deletion became batched. Job payload
            # trees are independent, so remove a bounded number concurrently.
            if os.name != "nt" or len(targets) < 8:
                for job_id in targets:
                    remove_job_payload(job_id)
            else:
                with ThreadPoolExecutor(
                    max_workers=min(32, len(targets)),
                    thread_name_prefix="mwf-job-cleanup",
                ) as executor:
                    list(executor.map(remove_job_payload, targets))
            if remove_whole_jobs_dir:
                # Clear abandoned unpublished payloads too, then recreate the
                # conventional directory expected by filesystem integrations.
                shutil.rmtree(jobs_dir, ignore_errors=True)
                self.jobs_dir(node_name)
        return len(targets)

    def reset_jobs_for_run_batch(self, node_name: str, job_ids: list[int]) -> int:
        """Requeue retained jobs with one state/event transaction."""
        node_name = self.validate_node_name(node_name)
        normalized = sorted({self.validate_job_id(job_id) for job_id in job_ids})
        if not normalized:
            return 0
        event_time = datetime.now().isoformat(timespec="milliseconds")

        with self.db_transaction() as connection:
            rows = []
            for offset in range(0, len(normalized), 500):
                chunk = normalized[offset:offset + 500]
                placeholders = ",".join("?" for _ in chunk)
                rows.extend(
                    connection.execute(
                        f"SELECT job_id, status FROM jobs WHERE node_name=? "
                        f"AND job_id IN ({placeholders})",
                        [node_name, *chunk],
                    ).fetchall()
                )
            if not rows:
                return 0
            existing = [int(row["job_id"]) for row in rows]
            for offset in range(0, len(existing), 500):
                chunk = existing[offset:offset + 500]
                placeholders = ",".join("?" for _ in chunk)
                connection.execute(
                    "UPDATE jobs SET status=?, status_json='{}', runtime_json=NULL, "
                    "active_execution_id=NULL, active_pid=NULL, active_thread_id=NULL, "
                    "active_started_at=NULL, restart_requested_at=NULL, "
                    "restart_requested_by_pid=NULL, restart_reason=NULL "
                    f"WHERE node_name=? AND job_id IN ({placeholders})",
                    [QUEUED, node_name, *chunk],
                )
            previous = {int(row["job_id"]): str(row["status"]) for row in rows}
            connection.executemany(
                "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
                "VALUES(?, ?, ?, 'queued', ?)",
                [
                    (
                        node_name,
                        job_id,
                        event_time,
                        json.dumps(
                            {
                                "previous_status": previous[job_id],
                                "status": QUEUED,
                            },
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                    )
                    for job_id in existing
                ],
            )
        self.notify_queue_change()
        return len(existing)

    def delete_node_jobs(self, node_name: str, *, remove_payload: bool = True) -> int:
        ids = self.list_job_ids(node_name)
        with self.db_transaction() as connection:
            connection.execute("DELETE FROM idempotency WHERE node_name=?", (node_name,))
            connection.execute("DELETE FROM job_events WHERE node_name=?", (node_name,))
            connection.execute("DELETE FROM default_job_specs WHERE node_name=?", (node_name,))
            connection.execute("DELETE FROM jobs WHERE node_name=?", (node_name,))
            connection.execute(
                "INSERT INTO job_sequences(node_name, next_job_id) VALUES(?, 1) "
                "ON CONFLICT(node_name) DO UPDATE SET next_job_id=1",
                (node_name,),
            )
        if remove_payload:
            shutil.rmtree(self.jobs_dir(node_name), ignore_errors=True)
            self.jobs_dir(node_name)
        return len(ids)
