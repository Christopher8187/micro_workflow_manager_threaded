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

from .job_sources import (
    QueuedJobObjectSource,
    RefreshableQueuedJobObjectSource,
    RefreshableQueuedJobSource,
)


class JobQueryStorageMixin:
    """Job loading, statuses, listings, and queue sources."""

    def load_job(self, node_name: str, job_id: int) -> Job:
        metadata = self.read_job_metadata(node_name, job_id)
        if not metadata:
            raise FileNotFoundError(f"Job does not exist: {node_name}/{job_id}")
        params = self.read_json(self.input_file(node_name, job_id), default={})
        return Job(
            job_id=metadata["job_id"],
            node_name=metadata["node_name"],
            params=params,
            parent=metadata.get("parent"),
            producer_component=metadata.get("producer_component"),
            job_kind=metadata.get("job_kind"),
            created_at=metadata["created_at"],
        )

    def load_jobs_batch(self, node_name: str, job_ids: list[int]) -> list[Job]:
        """Load one admission burst without per-job metadata queries.

        API admission used to alternate one filesystem read with one execution
        claim. On Windows that spacing prevented the group-commit lane from
        seeing a useful claim burst. Loading metadata and payloads first keeps
        queue semantics unchanged while allowing following claims to coalesce.
        """
        node_name = self.validate_node_name(node_name)
        normalized = [self.validate_job_id(job_id) for job_id in job_ids]
        if not normalized:
            return []
        if len(normalized) != len(set(normalized)):
            raise ValueError("job_ids contains duplicates")

        rows = []
        connection = self.db_connection()
        for offset in range(0, len(normalized), 500):
            chunk = normalized[offset:offset + 500]
            placeholders = ",".join("?" for _ in chunk)
            rows.extend(
                connection.execute(
                    "SELECT job_id, node_name, parent_json, created_at FROM jobs "
                    f"WHERE node_name=? AND job_id IN ({placeholders})",
                    [node_name, *chunk],
                ).fetchall()
            )
        rows_by_id = {int(row["job_id"]): row for row in rows}
        missing = [job_id for job_id in normalized if job_id not in rows_by_id]
        if missing:
            raise FileNotFoundError(f"Job does not exist: {node_name}/{missing[0]}")

        def load_params(job_id: int) -> dict[str, Any]:
            return self.read_json(self.input_file(node_name, job_id), default={})

        if os.name == "nt" and len(normalized) >= 8:
            with ThreadPoolExecutor(
                max_workers=min(32, len(normalized)),
                thread_name_prefix="mwf-job-prefetch",
            ) as executor:
                params_list = list(executor.map(load_params, normalized))
        else:
            params_list = [load_params(job_id) for job_id in normalized]

        jobs: list[Job] = []
        for job_id, params in zip(normalized, params_list):
            row = rows_by_id[job_id]
            raw_parent = json.loads(row["parent_json"]) if row["parent_json"] else None
            producer_component = None
            job_kind = None
            parent = raw_parent
            if isinstance(raw_parent, dict):
                parent = dict(raw_parent)
                producer_component = parent.pop(
                    "_mwf_from_component",
                    parent.pop("from_component", None),
                )
                job_kind = parent.pop(
                    "_mwf_job_kind",
                    parent.pop("job_kind", None),
                )
                parent = parent or None
            jobs.append(
                Job(
                    job_id=job_id,
                    node_name=str(row["node_name"]),
                    params=params,
                    parent=parent,
                    producer_component=(
                        tuple(producer_component)
                        if isinstance(producer_component, list)
                        else None
                    ),
                    job_kind=job_kind,
                    created_at=str(row["created_at"]),
                )
            )
        return jobs

    def read_job_status_data(self, node_name: str, job_id: int) -> dict[str, Any]:
        row = self.db_connection().execute(
            "SELECT status, status_json FROM jobs WHERE node_name=? AND job_id=?",
            (node_name, self.validate_job_id(job_id)),
        ).fetchone()
        if row is None:
            return {}
        try:
            extra = json.loads(row["status_json"] or "{}")
        except json.JSONDecodeError:
            extra = {}
        if not isinstance(extra, dict):
            extra = {}
        return {
            "job_id": job_id,
            "node_name": node_name,
            "status": row["status"],
            **extra,
        }

    def set_job_status(self, node_name: str, job_id: int, status: str, **extra):
        job_id = self.validate_job_id(job_id)
        status = self.validate_status(status)
        terminal = status in {"done", "failed", "cancelled", "skipped"}
        status_json = json.dumps(extra, ensure_ascii=False, separators=(",", ":"))
        event_time = datetime.now().isoformat(timespec="milliseconds")

        def update(connection):
            row = connection.execute(
                "SELECT status FROM jobs WHERE node_name=? AND job_id=?",
                (node_name, job_id),
            ).fetchone()
            if row is None:
                raise FileNotFoundError(f"Job does not exist: {node_name}/{job_id}")
            old_status = str(row["status"])
            if terminal:
                connection.execute(
                    "UPDATE jobs SET status=?, status_json=?, active_execution_id=NULL, "
                    "active_pid=NULL, active_thread_id=NULL, active_started_at=NULL "
                    "WHERE node_name=? AND job_id=?",
                    (
                        status,
                        status_json,
                        node_name,
                        job_id,
                    ),
                )
            else:
                connection.execute(
                    "UPDATE jobs SET status=?, status_json=? WHERE node_name=? AND job_id=?",
                    (
                        status,
                        status_json,
                        node_name,
                        job_id,
                    ),
                )
            if old_status != status or extra:
                event_name = {
                    "queued": "queued",
                    "running": "started",
                    "done": "done",
                    "failed": "failed",
                    "cancelled": "cancelled",
                    "skipped": "skipped",
                }.get(status, "status_changed")
                event_data = json.dumps(
                    {"previous_status": old_status, "status": status, **extra},
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                connection.execute(
                    "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
                    "VALUES(?, ?, ?, ?, ?)",
                    (node_name, job_id, event_time, event_name, event_data),
                )

        self.submit_db_mutation(update)
        if status == QUEUED:
            self.notify_queue_change()

    def get_job_status(self, node_name: str, job_id: int) -> str | None:
        row = self.db_connection().execute(
            "SELECT status FROM jobs WHERE node_name=? AND job_id=?",
            (node_name, self.validate_job_id(job_id)),
        ).fetchone()
        return None if row is None else str(row["status"])

    def list_job_ids(self, node_name: str) -> list[int]:
        rows = self.db_connection().execute(
            "SELECT job_id FROM jobs WHERE node_name=? ORDER BY job_id",
            (node_name,),
        ).fetchall()
        return [int(row["job_id"]) for row in rows]

    def list_jobs(self, node_name: str, status: str | None = None) -> list[dict]:
        sql = "SELECT job_id FROM jobs WHERE node_name=?"
        args: list[Any] = [node_name]
        if status is not None:
            sql += " AND status=?"
            args.append(status)
        sql += " ORDER BY job_id"
        rows = self.db_connection().execute(sql, args).fetchall()
        result = []
        for row in rows:
            job_id = int(row["job_id"])
            result.append({
                **self.read_job_metadata(node_name, job_id),
                **self.read_job_status_data(node_name, job_id),
            })
        return result

    def list_job_parent_metadata(
        self,
        node_names: list[str] | tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        """Read producer metadata for many jobs with one SQLite snapshot."""
        args: list[Any] = []
        sql = "SELECT node_name, job_id, parent_json FROM jobs"
        if node_names is not None:
            normalized = [self.validate_node_name(name) for name in node_names]
            if not normalized:
                return []
            placeholders = ",".join("?" for _ in normalized)
            sql += f" WHERE node_name IN ({placeholders})"
            args.extend(normalized)
        sql += " ORDER BY node_name, job_id"
        rows = self.db_connection().execute(sql, args).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            raw_parent = json.loads(row["parent_json"]) if row["parent_json"] else None
            producer_component = None
            parent = raw_parent
            if isinstance(raw_parent, dict):
                parent = dict(raw_parent)
                producer_component = parent.pop(
                    "_mwf_from_component",
                    parent.pop("from_component", None),
                )
                parent.pop("_mwf_job_kind", parent.pop("job_kind", None))
                parent = parent or None
            result.append(
                {
                    "node_name": str(row["node_name"]),
                    "job_id": int(row["job_id"]),
                    "parent": parent,
                    "producer_component": (
                        tuple(producer_component)
                        if isinstance(producer_component, list)
                        else None
                    ),
                }
            )
        return result

    def job_is_queued(self, node_name: str, job_id: int) -> bool:
        return self.get_job_status(node_name, job_id) == QUEUED

    def iter_job_ids(self, node_name: str):
        yield from self.list_job_ids(node_name)

    def queued_job_source(self, node_name: str) -> RefreshableQueuedJobSource:
        return RefreshableQueuedJobSource(self, node_name)

    def queued_job_object_source(
        self,
        node_name: str,
        *,
        refreshable: bool,
    ) -> QueuedJobObjectSource | RefreshableQueuedJobObjectSource:
        if refreshable:
            return RefreshableQueuedJobObjectSource(self, node_name)
        return QueuedJobObjectSource(
            self,
            node_name,
            self.iter_queued_job_ids(node_name),
        )

    def iter_queued_job_ids(self, node_name: str):
        # Preserve the public snapshot iterator and deterministic job-id order.
        # Component API pumps use ``queued_job_source`` so they can refill from
        # jobs inserted after the pump starts.
        rows = self.db_connection().execute(
            "SELECT job_id FROM jobs WHERE node_name=? AND status=? ORDER BY job_id",
            (node_name, QUEUED),
        ).fetchall()
        for row in rows:
            yield int(row["job_id"])

    def queued_job_ids(self, node_name: str) -> list[int]:
        return list(self.iter_queued_job_ids(node_name))

    def has_queued_jobs(self, node_name: str) -> bool:
        row = self.db_connection().execute(
            "SELECT 1 FROM jobs WHERE node_name=? AND status=? LIMIT 1",
            (node_name, QUEUED),
        ).fetchone()
        return row is not None

    def nodes_with_job_statuses(
        self,
        node_names: list[str] | tuple[str, ...] | set[str],
        statuses: set[str] | tuple[str, ...] | list[str],
    ) -> set[str]:
        """Return selected nodes having at least one job in any given status."""
        normalized_nodes = [self.validate_node_name(name) for name in node_names]
        normalized_statuses = [self.validate_status(status) for status in statuses]
        if not normalized_nodes or not normalized_statuses:
            return set()
        node_placeholders = ",".join("?" for _ in normalized_nodes)
        status_placeholders = ",".join("?" for _ in normalized_statuses)
        rows = self.db_connection().execute(
            f"SELECT DISTINCT node_name FROM jobs "
            f"WHERE status IN ({status_placeholders}) "
            f"AND node_name IN ({node_placeholders})",
            [*normalized_statuses, *normalized_nodes],
        ).fetchall()
        return {str(row["node_name"]) for row in rows}

    def queued_nodes(self, node_names: list[str] | tuple[str, ...]) -> set[str]:
        """Return queued members of a component with one indexed query."""
        return self.nodes_with_job_statuses(node_names, {QUEUED})

    def queued_jobs(self, node_name: str) -> list[Job]:
        return [self.load_job(node_name, job_id) for job_id in self.iter_queued_job_ids(node_name)]
