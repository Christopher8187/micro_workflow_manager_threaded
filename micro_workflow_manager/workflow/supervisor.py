from __future__ import annotations

import heapq
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from threading import Condition, Event, Thread
from time import monotonic
from typing import Any
from uuid import uuid4

from ..errors import JobTimeoutError
from ..monitor import now_iso


def _deadline_iso(seconds: float | None) -> str | None:
    if seconds is None:
        return None
    return (datetime.now().astimezone() + timedelta(seconds=seconds)).isoformat(
        timespec="milliseconds"
    )


def _validate_timeout(value: float | int | None, *, name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a positive number or None")
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive finite number or None")
    return value


def _validate_progress(value: float | int | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("progress must be a number from 0 to 1 or None")
    value = float(value)
    if not math.isfinite(value) or not 0 <= value <= 1:
        raise ValueError("progress must be a finite number from 0 to 1")
    return value


@dataclass
class AttemptWatch:
    """One scheduler-owned handler attempt and its progress deadline.

    A watch may be passive when neither timeout is configured. Passive watches
    do not start the supervisor thread or write runtime state until the task
    explicitly reports a checkpoint, preserving the old fast path for ordinary
    jobs that do not use progress reporting or timeout supervision.
    """

    node_name: str
    job_id: int
    task_name: str
    attempt: int
    repeat_index: int
    generation: int
    execution_id: str | None
    cancellation_event: Event
    total_timeout: float | None
    default_checkpoint_timeout: float | None
    watch_id: str = field(default_factory=lambda: uuid4().hex)
    wake_event: Event = field(default_factory=Event)
    started_at: str = field(default_factory=lambda: datetime.now().astimezone().isoformat(timespec="milliseconds"))
    started_monotonic: float = field(default_factory=monotonic)
    total_deadline: float | None = None
    checkpoint_deadline: float | None = None
    checkpoint_timeout: float | None = None
    checkpoint_at: str | None = None
    checkpoint_name: str | None = None
    progress: float | None = None
    progress_detail: str | None = None
    revision: int = 0
    state: str = "active"
    timeout_kind: str | None = None
    timeout_message: str | None = None
    cancel_message: str | None = None
    runtime_written: bool = False

    @property
    def supervised(self) -> bool:
        return self.total_timeout is not None or self.default_checkpoint_timeout is not None

    @property
    def key(self) -> str:
        return self.watch_id


class SchedulerSupervisor:
    """One lightweight scheduler supervisor for all active attempts.

    The supervisor owns a single daemon thread and a deadline heap. It replaces
    per-handler timeout timer threads and also owns the CLI run heartbeat. Only
    attempts with an actual deadline enter the heap; normal untimed jobs remain
    on the direct execution path.
    """

    def __init__(self, workflow):
        self.workflow = workflow
        self.storage = workflow.storage
        self._condition = Condition()
        self._thread: Thread | None = None
        self._watches: dict[str, AttemptWatch] = {}
        self._deadlines: list[tuple[float, int, str, str, int]] = []
        self._serial = 0
        self._run_heartbeat: dict[str, Any] | None = None

    # ------------------------------------------------------------------
    # Thread lifecycle and deadline scheduling
    # ------------------------------------------------------------------
    def _ensure_thread_locked(self):
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = Thread(
            target=self._loop,
            name="mwf-scheduler-supervisor",
            daemon=True,
        )
        self._thread.start()

    def _push_deadline_locked(
        self,
        watch: AttemptWatch,
        kind: str,
        deadline: float | None,
    ):
        if deadline is None:
            return
        self._serial += 1
        heapq.heappush(
            self._deadlines,
            (deadline, self._serial, watch.key, kind, watch.revision),
        )

    def _compact_deadlines_locked(self):
        limit = max(256, len(self._watches) * 8)
        if len(self._deadlines) <= limit:
            return
        rebuilt: list[tuple[float, int, str, str, int]] = []
        for watch in self._watches.values():
            if watch.state != "active":
                continue
            for kind, deadline in (
                ("total", watch.total_deadline),
                ("checkpoint", watch.checkpoint_deadline),
            ):
                if deadline is None:
                    continue
                self._serial += 1
                rebuilt.append((deadline, self._serial, watch.key, kind, watch.revision))
        heapq.heapify(rebuilt)
        self._deadlines = rebuilt

    def _next_valid_deadline_locked(self) -> float | None:
        while self._deadlines:
            deadline, _, key, kind, revision = self._deadlines[0]
            watch = self._watches.get(key)
            if watch is None or watch.state != "active" or watch.revision != revision:
                heapq.heappop(self._deadlines)
                continue
            current = watch.total_deadline if kind == "total" else watch.checkpoint_deadline
            if current is None or abs(current - deadline) > 1e-9:
                heapq.heappop(self._deadlines)
                continue
            return deadline
        return None

    def _loop(self):
        while True:
            expired: list[tuple[AttemptWatch, str]] = []
            heartbeat: dict[str, Any] | None = None

            with self._condition:
                now_value = monotonic()

                while self._deadlines:
                    deadline, _, key, kind, revision = self._deadlines[0]
                    if deadline > now_value:
                        break
                    heapq.heappop(self._deadlines)
                    watch = self._watches.get(key)
                    if watch is None or watch.state != "active" or watch.revision != revision:
                        continue
                    current = watch.total_deadline if kind == "total" else watch.checkpoint_deadline
                    if current is None or abs(current - deadline) > 1e-9:
                        continue
                    watch.state = "timed_out"
                    watch.timeout_kind = kind
                    if kind == "checkpoint":
                        seconds = watch.checkpoint_timeout
                        checkpoint = watch.checkpoint_name or "task start"
                        watch.timeout_message = (
                            f"{watch.node_name}.{watch.task_name} made no checkpoint progress "
                            f"for {seconds:g}s after {checkpoint!r}"
                        )
                    else:
                        seconds = watch.total_timeout
                        watch.timeout_message = (
                            f"{watch.node_name}.{watch.task_name} exceeded timeout={seconds:g}s"
                        )
                    watch.cancellation_event.set()
                    watch.revision += 1
                    expired.append((watch, kind))

                run = self._run_heartbeat
                if run is not None and run["next_at"] <= now_value:
                    heartbeat = dict(run)
                    run["next_at"] = now_value + run["interval"]

                if not expired and heartbeat is None:
                    deadline = self._next_valid_deadline_locked()
                    if self._run_heartbeat is not None:
                        heartbeat_deadline = self._run_heartbeat["next_at"]
                        deadline = heartbeat_deadline if deadline is None else min(deadline, heartbeat_deadline)

                    if deadline is None:
                        # No active deadline and no run heartbeat. End the idle
                        # daemon so many short-lived programmatic workflows do
                        # not accumulate sleeping threads.
                        self._thread = None
                        return

                    self._condition.wait(max(0.0, deadline - monotonic()))
                    continue

            for watch, kind in expired:
                try:
                    self._persist_timeout(watch, kind)
                except Exception as error:
                    try:
                        self.storage.write_debug(
                            watch.node_name,
                            f"scheduler watchdog could not persist timeout state for "
                            f"job {watch.job_id}: {error}",
                        )
                    except Exception:
                        pass
                finally:
                    watch.wake_event.set()
            if heartbeat is not None:
                try:
                    self._write_run_heartbeat(heartbeat)
                except Exception:
                    # A transient heartbeat write must not stop checkpoint
                    # supervision. The next scheduled heartbeat retries.
                    pass

    # ------------------------------------------------------------------
    # CLI run heartbeat
    # ------------------------------------------------------------------
    def start_run_heartbeat(self, run_id: str, *, interval: float = 2.0):
        interval = _validate_timeout(interval, name="heartbeat interval")
        assert interval is not None
        with self._condition:
            self._run_heartbeat = {
                "run_id": run_id,
                "interval": interval,
                "next_at": monotonic() + interval,
            }
            self._ensure_thread_locked()
            self._condition.notify_all()

    def stop_run_heartbeat(self, run_id: str):
        with self._condition:
            current = self._run_heartbeat
            if current is not None and current.get("run_id") == run_id:
                self._run_heartbeat = None
                self._condition.notify_all()

    def _write_run_heartbeat(self, heartbeat: dict[str, Any]):
        run_id = heartbeat["run_id"]
        with self.storage.interprocess_lock("active-run-state"):
            current = self.storage.get_run_state()
            if current.get("run_id") != run_id or current.get("status") != "running":
                with self._condition:
                    if self._run_heartbeat is not None and self._run_heartbeat.get("run_id") == run_id:
                        self._run_heartbeat = None
                        self._condition.notify_all()
                return
            self.storage.update_run_state(heartbeat_at=now_iso())

    # ------------------------------------------------------------------
    # Handler attempts and checkpoints
    # ------------------------------------------------------------------
    def create_attempt(
        self,
        *,
        node_name: str,
        job_id: int,
        task_name: str,
        attempt: int,
        repeat_index: int,
        generation: int,
        execution_id: str | None,
        cancellation_event: Event,
        total_timeout: float | None,
        checkpoint_timeout: float | None,
    ) -> AttemptWatch:
        total_timeout = _validate_timeout(total_timeout, name="timeout")
        checkpoint_timeout = _validate_timeout(
            checkpoint_timeout,
            name="checkpoint_timeout",
        )
        watch = AttemptWatch(
            node_name=node_name,
            job_id=job_id,
            task_name=task_name,
            attempt=attempt,
            repeat_index=repeat_index,
            generation=generation,
            execution_id=execution_id,
            cancellation_event=cancellation_event,
            total_timeout=total_timeout,
            default_checkpoint_timeout=checkpoint_timeout,
        )
        if total_timeout is not None:
            watch.total_deadline = watch.started_monotonic + total_timeout
        if checkpoint_timeout is not None:
            watch.checkpoint_timeout = checkpoint_timeout
            watch.checkpoint_at = watch.started_at
            watch.checkpoint_name = "task start"
            watch.checkpoint_deadline = watch.started_monotonic + checkpoint_timeout

        with self._condition:
            self._watches[watch.key] = watch
            if watch.supervised:
                self._push_deadline_locked(watch, "total", watch.total_deadline)
                self._push_deadline_locked(watch, "checkpoint", watch.checkpoint_deadline)
                self._compact_deadlines_locked()
                self._ensure_thread_locked()
                self._condition.notify_all()
        if watch.supervised:
            self._persist_runtime(watch, state="running")
        return watch

    def report_checkpoint(
        self,
        watch: AttemptWatch,
        *,
        name: str | None = None,
        progress: float | int | None = None,
        detail: str | None = None,
        timeout: float | int | None = None,
    ):
        if name is not None and (not isinstance(name, str) or not name.strip()):
            raise ValueError("checkpoint name must be a non-empty string or None")
        if detail is not None and not isinstance(detail, str):
            raise ValueError("checkpoint detail must be a string or None")
        progress_value = _validate_progress(progress)
        timeout_value = _validate_timeout(timeout, name="checkpoint timeout")

        if timeout_value is not None and not watch.supervised:
            raise RuntimeError(
                "A dynamic checkpoint timeout requires the task or router to set "
                "checkpoint_timeout (or timeout) so MWF can run the handler under "
                "the centralized scheduler watchdog."
            )

        now_text = datetime.now().astimezone().isoformat(timespec="milliseconds")
        now_value = monotonic()
        with self._condition:
            if watch.state == "timed_out":
                raise JobTimeoutError(watch.timeout_message or "The task attempt timed out")
            if watch.state == "superseded":
                from ..errors import JobRestartedError
                raise JobRestartedError(watch.cancel_message or "The task attempt was restarted")
            if watch.state != "active":
                return

            watch.checkpoint_at = now_text
            if name is not None:
                watch.checkpoint_name = name.strip()
            if progress_value is not None:
                watch.progress = progress_value
            if detail is not None:
                watch.progress_detail = detail

            effective = timeout_value
            if effective is None:
                effective = watch.default_checkpoint_timeout
            if effective is not None:
                if not watch.supervised:
                    raise RuntimeError("Checkpoint timeout supervision is not enabled")
                watch.checkpoint_timeout = effective
                watch.checkpoint_deadline = now_value + effective
                watch.revision += 1
                self._push_deadline_locked(watch, "total", watch.total_deadline)
                self._push_deadline_locked(watch, "checkpoint", watch.checkpoint_deadline)
                self._compact_deadlines_locked()
                self._ensure_thread_locked()
                self._condition.notify_all()

        self._persist_runtime(watch, state="running")

    def signal_handler_complete(self, watch: AttemptWatch):
        if not watch.supervised:
            watch.state = "handler_done"
            watch.wake_event.set()
            return
        with self._condition:
            if watch.state == "active":
                watch.state = "handler_done"
                watch.revision += 1
                watch.wake_event.set()
                self._condition.notify_all()
            elif watch.state == "timed_out":
                # Timeout already won the race. The supervisor persists the
                # timeout event/runtime before waking the fallback path.
                return

    def finish_attempt(
        self,
        watch: AttemptWatch,
        *,
        state: str,
        error: BaseException | None = None,
    ):
        with self._condition:
            self._watches.pop(watch.key, None)
            if watch.state not in {"timed_out", "superseded"}:
                watch.state = state
            watch.revision += 1
            self._condition.notify_all()

        if watch.state == "superseded":
            return
        if (watch.runtime_written or watch.supervised) and watch.execution_id is not None:
            if not self.storage.job_execution_is_current(
                watch.node_name,
                watch.job_id,
                watch.generation,
                watch.execution_id,
            ):
                return
        if watch.runtime_written or watch.supervised:
            self._persist_runtime(
                watch,
                state=watch.state,
                error=repr(error) if error is not None else None,
            )

    def cancel_execution(
        self,
        node_name: str,
        job_id: int,
        generation: int,
        execution_id: str | None,
        *,
        reason: str = "job execution was restarted",
    ):
        """Wake and discard every attempt watch owned by a stale execution."""
        with self._condition:
            for watch in list(self._watches.values()):
                if (
                    watch.node_name != node_name
                    or watch.job_id != job_id
                    or watch.generation != generation
                    or watch.execution_id != execution_id
                    or watch.state not in {"active", "handler_done"}
                ):
                    continue
                watch.state = "superseded"
                watch.cancel_message = reason
                watch.cancellation_event.set()
                watch.revision += 1
                watch.wake_event.set()
            self._condition.notify_all()

    def execution_cancel_error(self, watch: AttemptWatch):
        if watch.state != "superseded":
            return None
        from ..errors import JobRestartedError
        return JobRestartedError(watch.cancel_message or "The task attempt was restarted")

    def timeout_error(self, watch: AttemptWatch) -> JobTimeoutError | None:
        if watch.state != "timed_out":
            return None
        return JobTimeoutError(watch.timeout_message or "The task attempt timed out")

    # ------------------------------------------------------------------
    # Runtime persistence for inspect/doctor/recovery
    # ------------------------------------------------------------------
    def _runtime_payload(
        self,
        watch: AttemptWatch,
        *,
        state: str,
        error: str | None = None,
    ) -> dict[str, Any]:
        now_value = monotonic()
        total_remaining = (
            max(0.0, watch.total_deadline - now_value)
            if watch.total_deadline is not None and state == "running"
            else None
        )
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
    ):
        payload = self._runtime_payload(watch, state=state, error=error)
        self.storage.write_job_runtime(watch.node_name, watch.job_id, payload)
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
        seconds = watch.checkpoint_timeout if kind == "checkpoint" else watch.total_timeout
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
