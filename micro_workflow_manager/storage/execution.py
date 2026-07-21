from __future__ import annotations

import json
import os
import shutil
from contextlib import contextmanager
from datetime import datetime
from threading import get_ident
from typing import Any, Callable, Iterator, TypeVar
from uuid import uuid4

from micro_workflow_manager.errors import JobRestartedError
from micro_workflow_manager.models import QUEUED, RUNNING


T = TypeVar("T")


class JobExecutionStorageMixin:
    """SQLite execution generations, restart fencing, and checkpoint state."""

    def read_job_runtime(self, node_name: str, job_id: int) -> dict[str, Any]:
        row = self.db_connection().execute(
            "SELECT runtime_json FROM jobs WHERE node_name=? AND job_id=?",
            (node_name, self.validate_job_id(job_id)),
        ).fetchone()
        if row is None or not row["runtime_json"]:
            return {}
        try:
            data = json.loads(row["runtime_json"])
        except json.JSONDecodeError:
            return {}
        return data if isinstance(data, dict) else {}

    def write_job_runtime(
        self,
        node_name: str,
        job_id: int,
        data: dict[str, Any],
        *,
        wait: bool = True,
    ):
        job_id = self.validate_job_id(job_id)
        serialized = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        # One atomic conditional UPDATE replaces the old advisory-lock acquire,
        # runtime UPDATE, and advisory-lock release. The WHERE clause preserves
        # terminal runtime state when a late checkpoint from the same handler
        # races with timeout/restart completion.
        def update(connection):
            connection.execute(
                "UPDATE jobs SET runtime_json=? WHERE node_name=? AND job_id=? "
                "AND NOT ("
                "?='running' AND runtime_json IS NOT NULL "
                "AND json_valid(runtime_json)=1 "
                "AND json_extract(runtime_json, '$.watch_id')=? "
                "AND json_extract(runtime_json, '$.state') IN "
                "('timed_out','completed','failed','restarted','recovered')"
                ")",
                (
                    serialized,
                    node_name,
                    job_id,
                    data.get("state"),
                    data.get("watch_id"),
                ),
            )

        return self.submit_db_mutation(update, wait=wait)

    def job_execution_lock_name(self, node_name: str, job_id: int) -> str:
        self.validate_node_name(node_name)
        self.validate_job_id(job_id)
        return f"job-{node_name}-{job_id}-execution"

    def normalize_job_control(self, data: Any) -> dict[str, Any]:
        if not isinstance(data, dict):
            data = {}
        try:
            generation = int(data.get("generation", 0) or 0)
        except (TypeError, ValueError):
            generation = 0
        return {**data, "version": 1, "generation": max(0, generation)}

    def read_job_control(self, node_name: str, job_id: int) -> dict[str, Any]:
        row = self.db_connection().execute(
            "SELECT generation, active_execution_id, active_pid, active_thread_id, "
            "active_started_at, restart_requested_at, restart_requested_by_pid, restart_reason "
            "FROM jobs WHERE node_name=? AND job_id=?",
            (node_name, self.validate_job_id(job_id)),
        ).fetchone()
        if row is None:
            return self.normalize_job_control({})
        return self.normalize_job_control(dict(row))

    def current_job_generation(self, node_name: str, job_id: int) -> int:
        return int(self.read_job_control(node_name, job_id)["generation"])

    def claim_job_execution(self, node_name: str, job_id: int, *, started_at: str) -> tuple[int, str]:
        job_id = self.validate_job_id(job_id)
        execution_id = uuid4().hex
        pid = os.getpid()
        thread_id = get_ident()
        event_time = datetime.now().isoformat(timespec="milliseconds")

        def claim(connection):
            row = connection.execute(
                "SELECT generation, status FROM jobs WHERE node_name=? AND job_id=?",
                (node_name, job_id),
            ).fetchone()
            if row is None:
                raise FileNotFoundError(f"Job does not exist: {node_name}/{job_id}")
            generation = int(row["generation"])
            previous_status = str(row["status"])
            status_extra = {
                "started_at": started_at,
                "generation": generation,
                "execution_id": execution_id,
                "pid": pid,
            }
            connection.execute(
                "UPDATE jobs SET active_execution_id=?, active_pid=?, active_thread_id=?, "
                "active_started_at=?, restart_requested_at=NULL, restart_requested_by_pid=NULL, "
                "restart_reason=NULL, runtime_json=NULL, status=?, status_json=? "
                "WHERE node_name=? AND job_id=? AND generation=?",
                (
                    execution_id,
                    pid,
                    thread_id,
                    started_at,
                    RUNNING,
                    json.dumps(status_extra, ensure_ascii=False, separators=(",", ":")),
                    node_name,
                    job_id,
                    generation,
                ),
            )
            event_data = json.dumps(
                {
                    "previous_status": previous_status,
                    "status": RUNNING,
                    **status_extra,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
            connection.execute(
                "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
                "VALUES(?, ?, ?, 'started', ?)",
                (node_name, job_id, event_time, event_data),
            )
            return generation, execution_id

        # Claiming is itself an atomic database transition. The filesystem lock
        # remains reserved for side effects/final publication, where a restart
        # must fence an actual file mutation.
        return self.submit_db_mutation(claim)

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
