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


from .supervisor_attempts import SupervisorAttemptMixin
from .supervisor_persistence import SupervisorPersistenceMixin


class SchedulerSupervisor(
    SupervisorAttemptMixin,
    SupervisorPersistenceMixin,
):
    """One scheduler thread for deadlines, restarts, and run heartbeats."""

    def __init__(self, workflow):
        self.workflow = workflow
        self.storage = workflow.storage
        self._condition = Condition()
        self._thread: Thread | None = None
        self._watches: dict[str, AttemptWatch] = {}
        self._restartable_keys: set[str] = set()
        self._deadlines: list[tuple[float, int, str, str, int]] = []
        self._serial = 0
        self._run_heartbeat: dict[str, Any] | None = None
        self._restart_fallback_interval = 5.0
        self._next_restart_fallback = monotonic() + self._restart_fallback_interval
        self._restart_event_pending = False
        self._restart_revision = self.storage.job_restart_revision()
        # Cross-process wakeups are installed lazily only while restartable
        # executions exist. Programmatic workflows with no active jobs therefore
        # do not retain an idle socket/thread merely because a supervisor object
        # was constructed.
        self._unsubscribe_restart_events = None
        self._restart_unsubscribe_at: float | None = None
        self._restart_listener_idle_seconds = 0.5

    def _ensure_restart_event_subscription_locked(self) -> None:
        # Dense API waves can momentarily have zero watches between a completion
        # and the next admission. Cancel a pending idle shutdown instead of
        # creating one UDP listener thread per job.
        self._restart_unsubscribe_at = None
        if self._unsubscribe_restart_events is not None:
            return
        self._unsubscribe_restart_events = self.storage.subscribe_state_changes(
            self._on_external_state_change,
            local=False,
            cross_process=True,
        )

    def _stop_restart_event_subscription_locked(self) -> None:
        if self._restartable_keys or self._unsubscribe_restart_events is None:
            return
        if self._restart_unsubscribe_at is None:
            self._restart_unsubscribe_at = monotonic() + self._restart_listener_idle_seconds
        self._condition.notify_all()

    def _finalize_restart_event_subscription_locked(self, now_value: float) -> None:
        if (
            self._restartable_keys
            or self._unsubscribe_restart_events is None
            or self._restart_unsubscribe_at is None
            or now_value < self._restart_unsubscribe_at
        ):
            return
        unsubscribe = self._unsubscribe_restart_events
        self._unsubscribe_restart_events = None
        self._restart_unsubscribe_at = None
        unsubscribe()

    def _on_external_state_change(self) -> None:
        with self._condition:
            self._restart_event_pending = True
            self._condition.notify_all()

    def _ensure_thread_locked(self):
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = Thread(
            target=self._thread_entry,
            name="mwf-scheduler-supervisor",
            daemon=True,
        )
        self._thread.start()

    def _thread_entry(self):
        try:
            self._loop()
        finally:
            self.storage.close_thread_connection()

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

    def _schedule_watch_locked(self, watch: AttemptWatch):
        self._push_deadline_locked(watch, "total", watch.total_deadline)
        if watch.external_wait_depth > 0:
            self._push_deadline_locked(watch, "external", watch.external_wait_deadline)
        else:
            self._push_deadline_locked(watch, "checkpoint", watch.checkpoint_deadline)

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
                (
                    "external" if watch.external_wait_depth > 0 else "checkpoint",
                    watch.external_wait_deadline if watch.external_wait_depth > 0 else watch.checkpoint_deadline,
                ),
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
            current = (
                watch.total_deadline if kind == "total"
                else watch.external_wait_deadline if kind == "external"
                else watch.checkpoint_deadline
            )
            if current is None or abs(current - deadline) > 1e-9:
                heapq.heappop(self._deadlines)
                continue
            return deadline
        return None

    def _loop(self):
        while True:
            expired: list[tuple[AttemptWatch, str]] = []
            heartbeat: dict[str, Any] | None = None
            check_restart_revision = False

            with self._condition:
                now_value = monotonic()

                while self._deadlines:
                    deadline, _, key, kind, revision = self._deadlines[0]
                    if deadline > now_value:
                        break
                    heapq.heappop(self._deadlines)
                    watch = self._watches.get(key)
                    if (
                        watch is None
                        or watch.state != "active"
                        or watch.revision != revision
                    ):
                        continue
                    current = (
                        watch.total_deadline
                        if kind == "total"
                        else watch.external_wait_deadline
                        if kind == "external"
                        else watch.checkpoint_deadline
                    )
                    if current is None or abs(current - deadline) > 1e-9:
                        continue
                    watch.state = "timed_out"
                    self._restartable_keys.discard(watch.key)
                    self._stop_restart_event_subscription_locked()
                    watch.timeout_kind = kind
                    if kind == "checkpoint":
                        seconds = watch.checkpoint_timeout
                        checkpoint = watch.checkpoint_name or "task start"
                        watch.timeout_message = (
                            f"{watch.node_name}.{watch.task_name} made no checkpoint progress "
                            f"for {seconds:g}s after {checkpoint!r}"
                        )
                    elif kind == "external":
                        seconds = watch.external_wait_timeout
                        operation = (
                            watch.external_wait_name
                            or "framework-managed network request"
                        )
                        watch.timeout_message = (
                            f"{watch.node_name}.{watch.task_name} network wait "
                            f"{operation!r} exceeded its {seconds:g}s transport lease"
                        )
                    else:
                        seconds = watch.total_timeout
                        watch.timeout_message = (
                            f"{watch.node_name}.{watch.task_name} exceeded "
                            f"timeout={seconds:g}s"
                        )
                    watch.cancellation_event.set()
                    watch.revision += 1
                    expired.append((watch, kind))

                run = self._run_heartbeat
                if run is not None and run["next_at"] <= now_value:
                    heartbeat = dict(run)
                    run["next_at"] = now_value + run["interval"]

                self._finalize_restart_event_subscription_locked(now_value)
                has_restartable = bool(self._restartable_keys)
                if has_restartable and (
                    self._restart_event_pending
                    or self._next_restart_fallback <= now_value
                ):
                    check_restart_revision = True
                    self._restart_event_pending = False
                    self._next_restart_fallback = (
                        now_value + self._restart_fallback_interval
                    )

                if not expired and heartbeat is None and not check_restart_revision:
                    deadline = self._next_valid_deadline_locked()
                    if self._run_heartbeat is not None:
                        heartbeat_deadline = self._run_heartbeat["next_at"]
                        deadline = (
                            heartbeat_deadline
                            if deadline is None
                            else min(deadline, heartbeat_deadline)
                        )
                    if has_restartable:
                        deadline = (
                            self._next_restart_fallback
                            if deadline is None
                            else min(deadline, self._next_restart_fallback)
                        )
                    if self._restart_unsubscribe_at is not None:
                        deadline = (
                            self._restart_unsubscribe_at
                            if deadline is None
                            else min(deadline, self._restart_unsubscribe_at)
                        )

                    if deadline is None:
                        if self._watches:
                            # A direct/thread/process checkpoint briefly disarms
                            # its deadline while the synchronous framework write
                            # is in progress. All active watches can therefore
                            # have no heap deadline for a small window. Keep the
                            # one central watchdog alive until those writes rearm
                            # the watches instead of mistaking that window for an
                            # idle supervisor and exiting.
                            self._condition.wait()
                            continue
                        # No active watch, deadline, or run heartbeat. End the
                        # idle daemon so many short-lived programmatic workflows
                        # do not accumulate sleeping threads.
                        self._thread = None
                        return

                    # The cross-process restart listener deliberately remains
                    # warm for a short idle grace so dense API waves do not
                    # create one listener per job. That grace must not retain a
                    # SQLite connection after the final watch has gone away.
                    # A later wake reopens a connection lazily if it needs one.
                    if not self._watches and self._run_heartbeat is None:
                        self.storage.close_thread_connection()
                    self._condition.wait(max(0.0, deadline - monotonic()))
                    continue

            if check_restart_revision:
                try:
                    current_revision = self.storage.job_restart_revision()
                    if current_revision != self._restart_revision:
                        self._restart_revision = current_revision
                        with self._condition:
                            restart_watches = [
                                watch
                                for key in tuple(self._restartable_keys)
                                if (watch := self._watches.get(key)) is not None
                                and watch.state in {"active", "handler_done"}
                            ]
                        current = self.storage.active_job_executions()
                        for watch in restart_watches:
                            lease = current.get((watch.node_name, watch.job_id))
                            if lease == (watch.generation, watch.execution_id):
                                continue
                            self.cancel_execution(
                                watch.node_name,
                                watch.job_id,
                                watch.generation,
                                watch.execution_id,
                                reason=(
                                    f"Job {watch.node_name}/{watch.job_id} generation "
                                    f"{watch.generation} was restarted"
                                ),
                            )
                except Exception:
                    # A transient read failure is retried on the next centralized
                    # poll. The common no-restart path reads one metadata row;
                    # active leases are materialized only after the revision moves.
                    pass

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
