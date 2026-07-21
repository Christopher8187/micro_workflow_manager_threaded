from __future__ import annotations

import json
import os
import shutil
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Callable, Iterator, TypeVar

from micro_workflow_manager.errors import JobRestartedError
from micro_workflow_manager.models import QUEUED, RUNNING


T = TypeVar("T")


class JobRestartStorageMixin:
    """Execution fencing and manual restart operations."""

    def job_execution_is_current(
        self,
        node_name: str,
        job_id: int,
        generation: int,
        execution_id: str | None = None,
    ) -> bool:
        row = self.db_connection().execute(
            "SELECT generation, active_execution_id FROM jobs WHERE node_name=? AND job_id=?",
            (node_name, self.validate_job_id(job_id)),
        ).fetchone()
        if row is None or int(row["generation"]) != int(generation):
            return False
        if execution_id is not None and row["active_execution_id"] != execution_id:
            return False
        return True

    def active_job_executions(self) -> dict[tuple[str, int], tuple[int, str]]:
        """Return all active execution leases with one indexed SQLite scan."""
        rows = self.db_connection().execute(
            "SELECT node_name, job_id, generation, active_execution_id FROM jobs "
            "WHERE active_execution_id IS NOT NULL"
        ).fetchall()
        return {
            (str(row["node_name"]), int(row["job_id"])): (
                int(row["generation"]),
                str(row["active_execution_id"]),
            )
            for row in rows
        }

    @contextmanager
    def guard_job_execution(
        self,
        node_name: str,
        job_id: int,
        generation: int,
        execution_id: str | None = None,
    ) -> Iterator[None]:
        with self.filesystem_interprocess_lock(
            "execution-fences",
            self.job_execution_lock_name(node_name, job_id),
        ):
            if not self.job_execution_is_current(node_name, job_id, generation, execution_id):
                raise JobRestartedError(
                    f"Job {node_name}/{job_id} generation {generation} was restarted"
                )
            yield

    def run_guarded_job_side_effect(
        self,
        node_name: str,
        job_id: int,
        generation: int,
        execution_id: str,
        action: Callable[[], T],
    ) -> T:
        with self.guard_job_execution(node_name, job_id, generation, execution_id):
            return action()

    def _remove_restart_artifact(self, path):
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            self.remove_if_exists(path)

    def _request_job_restart_locked(
        self,
        node_name: str,
        job_id: int,
        *,
        requested_by_pid: int | None,
        reason: str,
        require_running_execution: bool,
    ) -> dict[str, Any]:
        control = self.read_job_control(node_name, job_id)
        previous_status = self.get_job_status(node_name, job_id) or QUEUED
        if require_running_execution:
            if previous_status != RUNNING or not control.get("active_execution_id"):
                raise RuntimeError(
                    f"Job {node_name}/{job_id} is not currently running. Only a live "
                    "running attempt can be restarted inside an existing run/runfrom sequence."
                )

        requested_at = datetime.now().isoformat(timespec="seconds")
        previous_generation = int(control["generation"])
        generation = previous_generation + 1
        runtime = self.read_job_runtime(node_name, job_id)
        if runtime:
            runtime = {
                **runtime,
                "state": "restarted",
                "updated_at": requested_at,
                "restart_reason": reason,
                "previous_generation": previous_generation,
                "generation": generation,
            }
        status_extra: dict[str, Any] = {}
        with self.db_transaction() as connection:
            connection.execute(
                "UPDATE jobs SET generation=?, active_execution_id=NULL, active_pid=NULL, "
                "active_thread_id=NULL, active_started_at=NULL, restart_requested_at=?, "
                "restart_requested_by_pid=?, restart_reason=?, status=?, status_json=?, runtime_json=? "
                "WHERE node_name=? AND job_id=?",
                (
                    generation,
                    requested_at,
                    requested_by_pid or os.getpid(),
                    reason,
                    QUEUED,
                    json.dumps(status_extra),
                    json.dumps(runtime, ensure_ascii=False, separators=(",", ":")) if runtime else None,
                    node_name,
                    job_id,
                ),
            )
        self.append_job_event(
            node_name,
            job_id,
            "queued",
            previous_status=previous_status,
            status=QUEUED,
        )
        self.append_job_event(
            node_name,
            job_id,
            "restart_requested",
            previous_generation=previous_generation,
            generation=generation,
            reason=reason,
            requested_by_pid=requested_by_pid or os.getpid(),
        )

        base = self.job_base_dir(node_name, job_id)
        self._remove_restart_artifact(base / "output.json")
        self._remove_restart_artifact(base / "files")
        return {
            "node": node_name,
            "job_id": job_id,
            "previous_generation": previous_generation,
            "generation": generation,
            "requested_at": requested_at,
        }

    def request_job_restart(
        self,
        node_name: str,
        job_id: int,
        *,
        requested_by_pid: int | None = None,
        reason: str = "manual restart",
    ) -> dict[str, Any]:
        if not self.job_exists(node_name, job_id):
            raise FileNotFoundError(f"Job does not exist: {node_name}/{job_id}")
        with self.filesystem_interprocess_lock(
            "execution-fences",
            self.job_execution_lock_name(node_name, job_id),
        ):
            return self._request_job_restart_locked(
                node_name,
                job_id,
                requested_by_pid=requested_by_pid,
                reason=reason,
                require_running_execution=False,
            )

    def request_active_job_restart(
        self,
        node_name: str,
        job_id: int,
        *,
        requested_by_pid: int | None = None,
        reason: str = "second-terminal active-job restart",
    ) -> dict[str, Any]:
        if not self.job_exists(node_name, job_id):
            raise FileNotFoundError(f"Job does not exist: {node_name}/{job_id}")
        with self.filesystem_interprocess_lock(
            "execution-fences",
            self.job_execution_lock_name(node_name, job_id),
        ):
            return self._request_job_restart_locked(
                node_name,
                job_id,
                requested_by_pid=requested_by_pid,
                reason=reason,
                require_running_execution=True,
            )
