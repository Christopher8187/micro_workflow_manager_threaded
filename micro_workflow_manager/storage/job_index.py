from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from micro_workflow_manager.schema import CURRENT_STATE_SCHEMA_VERSION
from micro_workflow_manager.models import CANCELLED, DONE, FAILED, QUEUED, RUNNING, SKIPPED, VALID_STATUSES


class JobIndexStorageMixin:
    """Rebuildable scheduler index and queue-marker bookkeeping."""

    def job_index_dirty(self, node_name: str) -> bool:
        return self.job_index_dirty_file(node_name).exists()

    def mark_job_index_dirty(self, node_name: str, reason: str | None = None):
        # The index is a rebuildable cache. If a high-contention index update
        # fails after retries, do not fail the job spawn/status transition. Mark
        # the node dirty so the next reader rebuilds from per-job truth.
        try:
            self.atomic_write_text(
                self.job_index_dirty_file(node_name),
                (reason or "dirty") + "\n",
            )
        except OSError:
            # If even the dirty marker is temporarily blocked, leave recovery to
            # normal index validation/rebuild on the next read. The underlying
            # job/status files have already been written by the caller.
            pass

    def clear_job_index_dirty(self, node_name: str):
        self.remove_if_exists(self.job_index_dirty_file(node_name))

    def empty_job_index(self, node_name: str) -> dict[str, Any]:
        return {
            "schema_version": CURRENT_STATE_SCHEMA_VERSION,
            "version": 1,
            "node": node_name,
            "last_job_id": 0,
            "counts": {status: 0 for status in VALID_STATUSES},
            "running_jobs": {},
            "duration_total": 0.0,
            "duration_count": 0,
        }

    def normalize_job_index(self, node_name: str, data: Any) -> dict[str, Any] | None:
        if not isinstance(data, dict) or data.get("version") != 1:
            return None

        index = self.empty_job_index(node_name)
        index.update(data)
        counts = index.get("counts")
        if not isinstance(counts, dict):
            counts = {}
        index["counts"] = {
            status: int(counts.get(status, 0) or 0)
            for status in VALID_STATUSES
        }
        running_jobs = index.get("running_jobs")
        index["running_jobs"] = running_jobs if isinstance(running_jobs, dict) else {}
        index["last_job_id"] = int(index.get("last_job_id") or 0)
        index["duration_total"] = float(index.get("duration_total") or 0.0)
        index["duration_count"] = int(index.get("duration_count") or 0)
        return index

    def read_job_index(self, node_name: str) -> dict[str, Any]:
        # Fast path: a valid clean index is returned without taking the OS lock.
        # If the file is missing/corrupt/dirty/temporarily unreadable, rebuild
        # under the node index lock from the authoritative job folders and
        # status files.
        path = self.job_index_file(node_name)
        if not self.job_index_dirty(node_name):
            try:
                index = self.normalize_job_index(node_name, self.read_json(path, default=None))
                if index is not None:
                    return index
            except OSError as error:
                if not self.retryable_errno(error):
                    raise
            except json.JSONDecodeError:
                pass

        return self.rebuild_job_index(node_name)

    def write_job_index(self, node_name: str, index: dict[str, Any]):
        self.atomic_write_json(self.job_index_file(node_name), index)

    def rebuild_job_index_unlocked(self, node_name: str) -> dict[str, Any]:
        """Rebuild the per-node job index. Caller must hold node index lock."""
        index = self.empty_job_index(node_name)
        queued = self.queued_dir(node_name)

        for marker in list(queued.glob("*.queued")):
            self.remove_if_exists(marker)

        jobs_root = self.jobs_dir(node_name)
        with os.scandir(jobs_root) as entries:
            for entry in entries:
                if not entry.is_dir() or not entry.name.isdigit():
                    continue

                job_id = int(entry.name)
                job_file = Path(entry.path) / "job.json"
                if not job_file.is_file():
                    continue

                index["last_job_id"] = max(index["last_job_id"], job_id)
                status_path = Path(entry.path) / "status.json"
                status_data = self.read_json(status_path, default=None)
                if isinstance(status_data, dict):
                    status = status_data.get("status") or QUEUED
                else:
                    status = QUEUED

                if status not in VALID_STATUSES:
                    status = QUEUED

                index["counts"][status] += 1

                if status == QUEUED:
                    self.atomic_write_text(self.queued_marker_file(node_name, job_id), "")

                if status == RUNNING:
                    index["running_jobs"][str(job_id)] = {
                        "started_at": status_data.get("started_at") if isinstance(status_data, dict) else None,
                    }

                duration = None
                if isinstance(status_data, dict):
                    duration = status_data.get("duration_seconds")
                if status in {DONE, FAILED, SKIPPED, CANCELLED} and isinstance(duration, int | float):
                    index["duration_total"] += float(duration)
                    index["duration_count"] += 1

        self.write_job_index(node_name, index)
        self.clear_job_index_dirty(node_name)
        return index

    def rebuild_job_index(self, node_name: str) -> dict[str, Any]:
        """Rebuild the per-node job index from existing job folders.

        This is the compatibility path for projects created before job_index.json
        existed. It is intentionally the only place that scans every job in a
        node for scheduler bookkeeping. Normal runs maintain this index
        incrementally as jobs are created and statuses change.
        """
        with self.interprocess_lock(f"node-{node_name}-index"):
            return self.rebuild_job_index_unlocked(node_name)

    def job_status_counts(self, node_name: str) -> dict[str, int]:
        return dict(self.read_job_index(node_name)["counts"])

    def node_job_summary(self, node_name: str) -> dict[str, Any]:
        index = self.read_job_index(node_name)
        counts = dict(index["counts"])
        total = sum(counts.values())
        duration_count = int(index.get("duration_count") or 0)
        avg_duration = None
        if duration_count:
            avg_duration = float(index.get("duration_total") or 0.0) / duration_count

        return {
            "total": total,
            "counts": counts,
            "running_jobs": dict(index.get("running_jobs") or {}),
            "avg_duration_seconds": avg_duration,
        }

    def register_job_created(self, node_name: str, job_id: int, status: str = QUEUED):
        job_id = self.validate_job_id(job_id)
        status = self.validate_status(status)

        # The job folder/job.json/input.json are the source of truth. The queued
        # marker is the scheduler's cheap queue source. job_index.json is only a
        # rebuildable summary cache; if its update fails under Windows file
        # contention, mark it dirty and let the job spawn succeed.
        if status == QUEUED:
            self.atomic_write_text(self.queued_marker_file(node_name, job_id), "")

        try:
            with self.interprocess_lock(f"node-{node_name}-index"):
                index = self.normalize_job_index(node_name, self.read_json(self.job_index_file(node_name), default=None))
                if index is None:
                    # The job was already written before registration, so rebuilding
                    # will include it. Do not increment a second time.
                    self.rebuild_job_index_unlocked(node_name)
                    return
                index["last_job_id"] = max(index["last_job_id"], job_id)
                index["counts"][status] += 1
                self.write_job_index(node_name, index)
        except OSError as error:
            if not self.retryable_errno(error):
                raise
            self.mark_job_index_dirty(node_name, f"create {job_id}: {error}")

    def update_job_index_status(
        self,
        node_name: str,
        job_id: int,
        old_status: str | None,
        new_status: str,
        old_data: dict[str, Any] | None = None,
        new_data: dict[str, Any] | None = None,
    ):
        job_id = self.validate_job_id(job_id)
        new_status = self.validate_status(new_status)

        # Keep the queued marker correct before touching the summary cache. The
        # scheduler can still see queued work even if the index update is later
        # marked dirty and rebuilt.
        marker = self.queued_marker_file(node_name, job_id)
        if new_status == QUEUED:
            self.atomic_write_text(marker, "")
        else:
            self.remove_if_exists(marker)

        try:
            with self.interprocess_lock(f"node-{node_name}-index"):
                index = self.normalize_job_index(node_name, self.read_json(self.job_index_file(node_name), default=None))
                if index is None:
                    # The status file has already been written/removed, so rebuilding
                    # now captures the new state exactly.
                    self.rebuild_job_index_unlocked(node_name)
                    return
                index["last_job_id"] = max(index["last_job_id"], job_id)

                if old_status in VALID_STATUSES and index["counts"].get(old_status, 0) > 0:
                    index["counts"][old_status] -= 1

                index["counts"][new_status] += 1

                running = index.setdefault("running_jobs", {})
                running.pop(str(job_id), None)
                if new_status == RUNNING:
                    running[str(job_id)] = {
                        "started_at": (new_data or {}).get("started_at"),
                    }

                terminal = {DONE, FAILED, SKIPPED, CANCELLED}
                if old_status in terminal and isinstance((old_data or {}).get("duration_seconds"), int | float):
                    index["duration_total"] = max(0.0, index["duration_total"] - float(old_data["duration_seconds"]))
                    index["duration_count"] = max(0, int(index["duration_count"]) - 1)

                if new_status in terminal and isinstance((new_data or {}).get("duration_seconds"), int | float):
                    index["duration_total"] += float(new_data["duration_seconds"])
                    index["duration_count"] = int(index["duration_count"]) + 1

                self.write_job_index(node_name, index)
        except OSError as error:
            if not self.retryable_errno(error):
                raise
            self.mark_job_index_dirty(node_name, f"status {job_id}: {error}")
