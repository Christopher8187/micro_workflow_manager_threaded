from __future__ import annotations

import os
import shutil
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from threading import get_ident
from typing import Any, Callable, Iterator, TypeVar
from uuid import uuid4

from micro_workflow_manager.errors import JobRestartedError
from micro_workflow_manager.models import QUEUED, RUNNING


T = TypeVar("T")


class JobExecutionStorageMixin:
    """Cross-process execution generations and restart fencing for jobs.

    A generation is an execution lease. Restarting a job increments the lease
    before any output cleanup occurs. An older attempt may still be inside an
    uninterruptible third-party call, but it can no longer commit status/output
    or use the guarded JobContext side-effect helpers.
    """

    def job_control_file(self, node_name: str, job_id: int) -> Path:
        return self.job_base_dir(node_name, job_id) / "execution.json"

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
        if generation < 0:
            generation = 0
        return {
            **data,
            "version": 1,
            "generation": generation,
        }

    def read_job_control(self, node_name: str, job_id: int) -> dict[str, Any]:
        self.validate_job_id(job_id)
        return self.normalize_job_control(
            self.read_json(self.job_control_file(node_name, job_id), default={})
        )

    def current_job_generation(self, node_name: str, job_id: int) -> int:
        return int(self.read_job_control(node_name, job_id)["generation"])

    def claim_job_execution(
        self,
        node_name: str,
        job_id: int,
        *,
        started_at: str,
    ) -> tuple[int, str]:
        """Claim the current generation and mark it running atomically."""
        if not self.job_exists(node_name, job_id):
            raise FileNotFoundError(f"Job does not exist: {node_name}/{job_id}")

        with self.interprocess_lock(self.job_execution_lock_name(node_name, job_id)):
            control = self.read_job_control(node_name, job_id)
            generation = int(control["generation"])
            execution_id = uuid4().hex
            self.atomic_write_json(
                self.job_control_file(node_name, job_id),
                {
                    **control,
                    "generation": generation,
                    "active_execution_id": execution_id,
                    "active_pid": os.getpid(),
                    "active_thread_id": get_ident(),
                    "active_started_at": started_at,
                    "restart_requested_at": None,
                },
            )
            self.set_job_status(
                node_name,
                job_id,
                RUNNING,
                started_at=started_at,
                generation=generation,
                execution_id=execution_id,
                pid=os.getpid(),
            )
            return generation, execution_id

    def job_execution_is_current(
        self,
        node_name: str,
        job_id: int,
        generation: int,
        execution_id: str | None = None,
    ) -> bool:
        control = self.read_job_control(node_name, job_id)
        if int(control["generation"]) != int(generation):
            return False
        if execution_id is not None:
            active = control.get("active_execution_id")
            if active not in {None, execution_id}:
                return False
        return True

    @contextmanager
    def guard_job_execution(
        self,
        node_name: str,
        job_id: int,
        generation: int,
        execution_id: str | None = None,
    ) -> Iterator[None]:
        """Hold the execution lock while performing one fenced side effect."""
        with self.interprocess_lock(self.job_execution_lock_name(node_name, job_id)):
            control = self.read_job_control(node_name, job_id)
            current_generation = int(control["generation"])
            active_execution_id = control.get("active_execution_id")
            if current_generation != int(generation) or (
                execution_id is not None
                and active_execution_id not in {None, execution_id}
            ):
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
        with self.guard_job_execution(
            node_name,
            job_id,
            generation,
            execution_id,
        ):
            return action()

    def _remove_restart_artifact(self, path: Path):
        if path.is_dir():
            def remove_tree():
                try:
                    shutil.rmtree(path)
                except FileNotFoundError:
                    pass
            self.retry_fs(remove_tree)
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

        if require_running_execution:
            status = self.get_job_status(node_name, job_id)
            active_execution_id = control.get("active_execution_id")
            if status != RUNNING or not active_execution_id:
                raise RuntimeError(
                    f"Job {node_name}/{job_id} is not currently running. "
                    "Only a live running attempt can be restarted inside an "
                    "existing run/runfrom sequence."
                )

        requested_at = datetime.now().isoformat(timespec="seconds")
        previous_generation = int(control["generation"])
        generation = previous_generation + 1

        # Fence first. Any old execution that reaches a guarded operation or
        # final commit after this write is rejected before it can mutate the
        # restarted job. This is intentionally the first persistent mutation.
        self.atomic_write_json(
            self.job_control_file(node_name, job_id),
            {
                **control,
                "generation": generation,
                "active_execution_id": None,
                "active_pid": None,
                "active_thread_id": None,
                "active_started_at": None,
                "restart_requested_at": requested_at,
                "restart_requested_by_pid": requested_by_pid or os.getpid(),
                "restart_reason": reason,
            },
        )

        self.set_job_status(node_name, job_id, QUEUED)

        # Cleanup happens only after the old generation is invalid. This
        # ordering prevents an old fast-finishing attempt from recreating a
        # successful status between reset steps.
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
        """Fence any old attempt, requeue the job, and clear job-local output."""
        if not self.job_exists(node_name, job_id):
            raise FileNotFoundError(f"Job does not exist: {node_name}/{job_id}")

        with self.interprocess_lock(self.job_execution_lock_name(node_name, job_id)):
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
        """Atomically replace one currently running execution generation.

        The running-status check, generation increment, queue transition, and
        job-local cleanup all happen under the same cross-process execution
        lock used by final job commits. If the old attempt already completed,
        this method refuses instead of leaving an orphan queued job that the
        active scheduler no longer owns.
        """
        if not self.job_exists(node_name, job_id):
            raise FileNotFoundError(f"Job does not exist: {node_name}/{job_id}")

        with self.interprocess_lock(self.job_execution_lock_name(node_name, job_id)):
            return self._request_job_restart_locked(
                node_name,
                job_id,
                requested_by_pid=requested_by_pid,
                reason=reason,
                require_running_execution=True,
            )
