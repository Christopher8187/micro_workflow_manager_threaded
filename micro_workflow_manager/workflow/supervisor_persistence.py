from __future__ import annotations

import heapq
from datetime import datetime
from threading import Condition, Thread
from time import monotonic
from typing import Any

from ..errors import JobTimeoutError
from ..fibers import in_fiber_runtime
from ..monitor import now_iso
from .supervisor_watch import AttemptWatch, _deadline_iso, _validate_progress, _validate_timeout


class SupervisorPersistenceMixin:
    """Runtime payload and timeout persistence for inspect/recovery."""

    def _runtime_payload(
        self,
        watch: AttemptWatch,
        *,
        state: str,
        error: str | None = None,
        checkpoint_remaining_override: float | None = None,
    ) -> dict[str, Any]:
        now_value = monotonic()
        total_remaining = (
            max(0.0, watch.total_deadline - now_value)
            if watch.total_deadline is not None and state == "running"
            else None
        )
        checkpoint_remaining = checkpoint_remaining_override
        if checkpoint_remaining is None:
            checkpoint_remaining = (
                max(0.0, watch.checkpoint_deadline - now_value)
                if watch.checkpoint_deadline is not None and state == "running"
                else None
            )
        payload = {
            "state": state,
            "watch_id": watch.watch_id,
            "node": watch.node_name,
            "job_id": watch.job_id,
            "task": watch.task_name,
            "attempt": watch.attempt,
            "repeat_index": watch.repeat_index,
            "generation": watch.generation,
            "execution_id": watch.execution_id,
            "started_at": watch.started_at,
            "updated_at": datetime.now().astimezone().isoformat(timespec="milliseconds"),
            "total_timeout_seconds": watch.total_timeout,
            "total_deadline_at": _deadline_iso(total_remaining),
            "checkpoint_timeout_seconds": watch.checkpoint_timeout,
            "checkpoint_at": watch.checkpoint_at,
            "checkpoint_name": watch.checkpoint_name,
            "progress": watch.progress,
            "progress_detail": watch.progress_detail,
            "checkpoint_deadline_at": _deadline_iso(checkpoint_remaining),
            "timeout_kind": watch.timeout_kind,
            "timeout_message": watch.timeout_message,
            "external_wait_active": watch.external_wait_depth > 0,
            "external_wait_name": watch.external_wait_name,
            "external_wait_timeout_seconds": watch.external_wait_timeout,
        }
        if error is not None:
            payload["error"] = error
        return payload

    def _persist_runtime(
        self,
        watch: AttemptWatch,
        *,
        state: str,
        error: str | None = None,
        wait: bool = True,
        priority: int = 10,
        checkpoint_remaining_override: float | None = None,
    ):
        payload = self._runtime_payload(
            watch,
            state=state,
            error=error,
            checkpoint_remaining_override=checkpoint_remaining_override,
        )
        self.storage.write_job_runtime(
            watch.node_name,
            watch.job_id,
            payload,
            wait=wait,
            priority=priority,
        )
        watch.runtime_written = True

    def _persist_timeout(self, watch: AttemptWatch, kind: str):
        if watch.execution_id is not None and not self.storage.job_execution_is_current(
            watch.node_name,
            watch.job_id,
            watch.generation,
            watch.execution_id,
        ):
            return
        self._persist_runtime(watch, state="timed_out")
        seconds = (
            watch.checkpoint_timeout if kind == "checkpoint"
            else watch.external_wait_timeout if kind == "external"
            else watch.total_timeout
        )
        self.storage.append_job_event(
            watch.node_name,
            watch.job_id,
            "timeout",
            task=watch.task_name,
            timeout_kind=kind,
            timeout_seconds=seconds,
            checkpoint=watch.checkpoint_name,
            progress=watch.progress,
            progress_detail=watch.progress_detail,
            attempt=watch.attempt,
            repeat_index=watch.repeat_index,
        )
