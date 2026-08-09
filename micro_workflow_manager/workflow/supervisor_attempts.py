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


class SupervisorAttemptMixin:
    """Attempt registration, progress updates, cancellation, and completion."""

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
        force_abandonable: bool = False,
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
            force_abandonable=bool(force_abandonable),
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
            if watch.execution_id is not None:
                self._restartable_keys.add(watch.key)
                self._ensure_restart_event_subscription_locked()
        if watch.supervised:
            # API controllers share one cooperative pump. Waiting synchronously
            # for one runtime-row write per job serializes admission and lets
            # already-completed provider responses become invisible "ghosts"
            # until an entire legacy dense admission wave has started. Runtime metadata is
            # generation-fenced in storage, so API writes can be grouped and
            # asynchronous while direct/thread/process inspection stays durable.
            self._persist_runtime(
                watch,
                state="running",
                wait=not in_fiber_runtime(),
                priority=20 if in_fiber_runtime() else 10,
            )
            watch.started_monotonic = monotonic()
            if watch.total_timeout is not None:
                watch.total_deadline = watch.started_monotonic + watch.total_timeout
            if watch.checkpoint_timeout is not None:
                watch.checkpoint_deadline = (
                    watch.started_monotonic + watch.checkpoint_timeout
                )
        with self._condition:
            if watch.supervised:
                self._schedule_watch_locked(watch)
                self._compact_deadlines_locked()
            if watch.supervised or watch.force_abandonable:
                self._ensure_thread_locked()
                self._condition.notify_all()
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
                "A dynamic checkpoint timeout requires the task or fallback to declare "
                "timeout=... (or the legacy checkpoint_timeout setting) so MWF can "
                "run the handler under the centralized scheduler watchdog."
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
                self._schedule_watch_locked(watch)
                self._compact_deadlines_locked()
                self._ensure_thread_locked()
                self._condition.notify_all()

        # A cooperative API fiber must not charge group-commit latency against
        # a very short checkpoint deadline. Direct/thread/process callers keep
        # synchronous checkpoint visibility for inspect and recovery.
        self._persist_runtime(
            watch,
            state="running",
            wait=not in_fiber_runtime(),
            priority=20 if in_fiber_runtime() else 10,
        )

    def begin_external_wait(
        self,
        watch: AttemptWatch,
        *,
        name: str,
        timeout: float | int,
        cleanup_grace: float = 30.0,
        fatal_timeout: bool = True,
    ) -> None:
        """Suspend checkpoint expiry while a framework-managed network call is live.

        The transport timeout remains bounded and the task's total timeout is
        never suspended. This removes the dependency on user-space heartbeat
        callbacks arriving exactly on schedule under extreme concurrency.
        """
        timeout_value = _validate_timeout(timeout, name="external wait timeout")
        assert timeout_value is not None
        grace = _validate_timeout(cleanup_grace, name="external wait cleanup grace")
        assert grace is not None
        if type(fatal_timeout) is not bool:
            raise ValueError("fatal_timeout must be a bool")
        now_value = monotonic()
        with self._condition:
            if watch.state != "active":
                error = self.timeout_error(watch) or self.execution_cancel_error(watch)
                if error is not None:
                    raise error
                return
            watch.external_wait_depth += 1
            watch.external_wait_name = str(name)
            watch.external_wait_timeout = timeout_value + grace
            if fatal_timeout:
                deadline = now_value + timeout_value + grace
                watch.external_wait_deadline = max(
                    watch.external_wait_deadline or 0.0,
                    deadline,
                )
            elif watch.external_wait_depth == 1:
                # The transport itself owns this deadline and will raise back
                # into user code. Keeping the watch in external-wait mode still
                # suspends checkpoint expiry while preserving the task's total
                # timeout and restart fencing.
                watch.external_wait_deadline = None
            watch.revision += 1
            self._schedule_watch_locked(watch)
            self._compact_deadlines_locked()
            self._ensure_thread_locked()
            self._condition.notify_all()

    def end_external_wait(self, watch: AttemptWatch) -> None:
        now_value = monotonic()
        with self._condition:
            if watch.external_wait_depth <= 0:
                return
            watch.external_wait_depth -= 1
            if watch.external_wait_depth == 0:
                completed_name = watch.external_wait_name or "network request"
                watch.external_wait_name = None
                watch.external_wait_timeout = None
                watch.external_wait_deadline = None
                watch.checkpoint_at = datetime.now().astimezone().isoformat(timespec="milliseconds")
                watch.checkpoint_name = f"{completed_name} completed"
                effective = watch.checkpoint_timeout or watch.default_checkpoint_timeout
                watch.checkpoint_deadline = now_value + effective if effective is not None else None
            watch.revision += 1
            if watch.state == "active":
                self._schedule_watch_locked(watch)
                self._compact_deadlines_locked()
                self._condition.notify_all()

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
            self._restartable_keys.discard(watch.key)
            self._stop_restart_event_subscription_locked()
            if watch.state not in {"timed_out", "superseded"}:
                watch.state = state
            watch.revision += 1
            self._condition.notify_all()

        if watch.state == "superseded":
            return
        if (
            (watch.runtime_written or watch.supervised)
            and watch.execution_id is not None
            and not in_fiber_runtime()
        ):
            if not self.storage.job_execution_is_current(
                watch.node_name,
                watch.job_id,
                watch.generation,
                watch.execution_id,
            ):
                return
        if watch.runtime_written or watch.supervised:
            # A cooperative API attempt publishes a durable terminal job event
            # immediately after this method returns. Writing an additional
            # completed/failed runtime snapshot for every job only creates a
            # low-priority backlog that can hold the SQLite writer inside a large
            # non-preemptible transaction. Running/checkpoint/timeout metadata is
            # still persisted; terminal status_json and job_events are authoritative.
            if in_fiber_runtime() and watch.state in {"completed", "failed"}:
                return
            self._persist_runtime(
                watch,
                state=watch.state,
                error=repr(error) if error is not None else None,
                wait=not in_fiber_runtime(),
                priority=20 if in_fiber_runtime() else 10,
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
                self._restartable_keys.discard(watch.key)
                watch.cancel_message = reason
                watch.cancellation_event.set()
                watch.revision += 1
                watch.wake_event.set()
            self._stop_restart_event_subscription_locked()
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
