from __future__ import annotations

import heapq
from datetime import datetime
from threading import Condition, Thread
from time import monotonic
from typing import Any

from ..errors import JobTimeoutError, safe_exception_repr
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
        if checkpoint_timeout is not None:
            watch.checkpoint_timeout = checkpoint_timeout
            watch.checkpoint_name = "task start"

        with self._condition:
            self._watches[watch.key] = watch
            if watch.execution_id is not None:
                self._restartable_keys.add(watch.key)
                self._ensure_restart_event_subscription_locked()
        # Do not arm timeout/checkpoint deadlines until the user handler is
        # actually about to execute. Dense admission can spend measurable time
        # in framework-owned trace/runtime bookkeeping; charging that time to a
        # short user checkpoint budget creates false timeouts before line 1.
        with self._condition:
            if watch.supervised or watch.force_abandonable:
                self._ensure_thread_locked()
                self._condition.notify_all()
        return watch

    def begin_handler_execution(self, watch: AttemptWatch) -> None:
        """Arm attempt deadlines immediately before invoking user code."""
        if watch.state != "active":
            error = self.timeout_error(watch) or self.execution_cancel_error(watch)
            if error is not None:
                raise error
            return

        # Prepare/persist the initial runtime row before the deadline is exposed
        # to the supervisor. The grouped API write is asynchronous in a fiber;
        # direct/thread/process callers retain synchronous inspect visibility.
        if watch.supervised:
            provisional = monotonic()
            watch.started_monotonic = provisional
            if watch.total_timeout is not None:
                watch.total_deadline = provisional + watch.total_timeout
            if watch.checkpoint_timeout is not None:
                watch.checkpoint_at = datetime.now().astimezone().isoformat(timespec="milliseconds")
                watch.checkpoint_name = "task start"
                watch.checkpoint_deadline = provisional + watch.checkpoint_timeout
            self._persist_runtime(
                watch,
                state="running",
                wait=not in_fiber_runtime(),
                priority=20 if in_fiber_runtime() else 10,
            )

        now_value = monotonic()
        now_text = datetime.now().astimezone().isoformat(timespec="milliseconds")
        with self._condition:
            if watch.state != "active":
                error = self.timeout_error(watch) or self.execution_cancel_error(watch)
                if error is not None:
                    raise error
                return
            watch.started_monotonic = now_value
            watch.started_at = now_text
            if watch.total_timeout is not None:
                watch.total_deadline = now_value + watch.total_timeout
            if watch.checkpoint_timeout is not None:
                watch.checkpoint_at = now_text
                watch.checkpoint_name = "task start"
                watch.checkpoint_deadline = now_value + watch.checkpoint_timeout
            watch.revision += 1
            if watch.supervised:
                self._schedule_watch_locked(watch)
                self._compact_deadlines_locked()
                self._ensure_thread_locked()
                self._condition.notify_all()

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

        effective = timeout_value
        if effective is None:
            effective = watch.default_checkpoint_timeout
        fiber_runtime = in_fiber_runtime()
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

            if effective is not None:
                if not watch.supervised:
                    raise RuntimeError("Checkpoint timeout supervision is not enabled")
                watch.checkpoint_timeout = effective
                # A direct/thread/process checkpoint persists synchronously for
                # immediate inspect/recovery visibility. Temporarily disarm its
                # checkpoint deadline so framework-owned SQLite time is not
                # charged to the user's interval. API fibers persist without
                # waiting and retain the ordinary deadline path.
                watch.checkpoint_deadline = (
                    now_value + effective if fiber_runtime else None
                )
                watch.revision += 1
                self._schedule_watch_locked(watch)
                self._compact_deadlines_locked()
                self._ensure_thread_locked()
                self._condition.notify_all()

        # A cooperative API fiber must not charge group-commit latency against
        # a very short checkpoint deadline. Direct/thread/process callers keep
        # synchronous checkpoint visibility for inspect and recovery.
        try:
            self._persist_runtime(
                watch,
                state="running",
                wait=not fiber_runtime,
                priority=20 if fiber_runtime else 10,
                checkpoint_remaining_override=(
                    effective if not fiber_runtime else None
                ),
            )
        finally:
            if effective is not None and not fiber_runtime:
                with self._condition:
                    if watch.state == "active":
                        watch.checkpoint_deadline = monotonic() + effective
                        watch.revision += 1
                        self._schedule_watch_locked(watch)
                        self._compact_deadlines_locked()
                        self._ensure_thread_locked()
                        self._condition.notify_all()

    def begin_external_wait(
        self,
        watch: AttemptWatch,
        *,
        name: str,
        timeout: float | int,
        cleanup_grace: float = 30.0,
        defer_lease_start: bool = False,
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
        now_value = monotonic()
        with self._condition:
            if watch.state != "active":
                error = self.timeout_error(watch) or self.execution_cancel_error(watch)
                if error is not None:
                    raise error
                return
            outermost_wait = watch.external_wait_depth == 0
            if outermost_wait:
                watch.external_wait_attempt = 1
                watch.external_wait_renewals = 0
                watch.external_wait_last_renewal_reason = None
            watch.external_wait_depth += 1
            watch.external_wait_name = str(name)
            watch.external_wait_timeout = timeout_value + grace
            if defer_lease_start and outermost_wait:
                watch.external_wait_attempt = 0
                watch.external_wait_deadline = None
            else:
                watch.external_wait_attempt = 1
                deadline = now_value + timeout_value + grace
                watch.external_wait_deadline = max(
                    watch.external_wait_deadline or 0.0,
                    deadline,
                )
            watch.revision += 1
            self._schedule_watch_locked(watch)
            self._compact_deadlines_locked()
            self._ensure_thread_locked()
            self._condition.notify_all()

    def renew_external_wait(
        self,
        watch: AttemptWatch,
        *,
        reason: str,
    ) -> None:
        """Start a fresh bounded lease for a framework-level physical replay.

        The configured per-attempt lease value is unchanged. Only an actual
        transport replay may renew its deadline; user-space heartbeats cannot.
        The task's total timeout remains active across every renewal.
        """
        now_value = monotonic()
        with self._condition:
            if watch.state != "active":
                error = self.timeout_error(watch) or self.execution_cancel_error(watch)
                if error is not None:
                    raise error
                return
            if watch.external_wait_depth <= 0 or watch.external_wait_timeout is None:
                raise RuntimeError("cannot renew an inactive external wait")
            first_physical_attempt = watch.external_wait_attempt == 0
            watch.external_wait_attempt += 1
            if not first_physical_attempt:
                watch.external_wait_renewals += 1
                watch.external_wait_last_renewal_reason = str(reason)
            watch.external_wait_deadline = now_value + watch.external_wait_timeout
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
                error=safe_exception_repr(error) if error is not None else None,
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
