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
        self._deadlines: list[tuple[float, int, str, str, int]] = []
        self._serial = 0
        self._run_heartbeat: dict[str, Any] | None = None
        self._restart_poll_interval = 0.05
        self._next_restart_poll = monotonic()

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
            restart_watches: list[AttemptWatch] = []

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
                    current = (
                watch.total_deadline if kind == "total"
                else watch.external_wait_deadline if kind == "external"
                else watch.checkpoint_deadline
            )
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
                    elif kind == "external":
                        seconds = watch.external_wait_timeout
                        operation = watch.external_wait_name or "framework-managed network request"
                        watch.timeout_message = (
                            f"{watch.node_name}.{watch.task_name} network wait {operation!r} "
                            f"exceeded its {seconds:g}s transport lease"
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

                restartable = [
                    watch
                    for watch in self._watches.values()
                    if watch.execution_id is not None
                    and watch.state in {"active", "handler_done"}
                ]
                if restartable and self._next_restart_poll <= now_value:
                    restart_watches = restartable
                    self._next_restart_poll = now_value + self._restart_poll_interval

                if not expired and heartbeat is None and not restart_watches:
                    deadline = self._next_valid_deadline_locked()
                    if self._run_heartbeat is not None:
                        heartbeat_deadline = self._run_heartbeat["next_at"]
                        deadline = heartbeat_deadline if deadline is None else min(deadline, heartbeat_deadline)
                    if restartable:
                        deadline = (
                            self._next_restart_poll
                            if deadline is None
                            else min(deadline, self._next_restart_poll)
                        )

                    if deadline is None:
                        # No active deadline and no run heartbeat. End the idle
                        # daemon so many short-lived programmatic workflows do
                        # not accumulate sleeping threads.
                        self._thread = None
                        return

                    self._condition.wait(max(0.0, deadline - monotonic()))
                    continue

            if restart_watches:
                try:
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
                    # poll; individual job controllers never stampede SQLite.
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
