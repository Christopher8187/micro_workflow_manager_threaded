from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import ceil
from threading import Event, Lock
from typing import Callable, Iterable

from .base import BaseRunner
from ..fibers import FiberRuntime


class _SharedRefreshableSource:
    """Serialize cursor pulls while allowing several fiber pumps to start work."""

    def __init__(self, source) -> None:
        self.source = source
        self.lock = Lock()
        self.stopped = Event()

    def pull(self, max_items: int):
        if max_items <= 0 or self.stopped.is_set():
            return []
        if self.stopped.is_set():
            return []
        # RefreshableQueuedJobSource reserves rowids under its own short lock;
        # payload reads and grouped claims then proceed concurrently per lane.
        return self.source.pull(max_items)

    def stop(self) -> None:
        self.stopped.set()

    def remaining_hint(self):
        hint = getattr(self.source, "remaining_hint", None)
        return None if not callable(hint) else hint()


class _LaneCoordinator:
    """Split a live node limit exactly across the fiber pumps still running."""

    def __init__(self, lanes: int, limit_provider: Callable[[], int]) -> None:
        self.limit_provider = limit_provider
        self.lock = Lock()
        self.active = set(range(lanes))

    def limit_for(self, lane: int) -> int:
        total = self.limit_provider()
        if type(total) is not int or total < 1:
            raise ValueError("runtime API concurrency must be an integer >= 1")
        with self.lock:
            ordered = sorted(self.active)
            if lane not in self.active or not ordered:
                return 0
            position = ordered.index(lane)
            base, remainder = divmod(total, len(ordered))
            return base + (1 if position < remainder else 0)

    def unregister(self, lane: int) -> None:
        with self.lock:
            self.active.discard(lane)


class ApiRunner(BaseRunner):
    """Cooperative API runner with event-prioritized terminal publication.

    The default strategy uses one pump for small queues and two coordinated
    pumps for dense queues. Admission windows are sized from the remaining
    queue and terminal-state pressure preempts ordinary claim work. Experimental
    strategies remain available for benchmarking, but are not production defaults.
    """

    supports_refreshable_job_source = True
    prefers_preloaded_jobs = True
    preclaims_job_bursts = True
    prefetches_job_bursts = True

    def __init__(
        self,
        max_threads: int,
        *,
        limit_provider: Callable[[], int] | None = None,
        worker_cleanup: Callable[[], None] | None = None,
        poll_interval: float = 0.05,
        startup_strategy: str | None = None,
        admission_pressure_provider: Callable[[], bool] | None = None,
        **_ignored,
    ):
        if type(max_threads) is not int or max_threads < 1:
            raise ValueError("max_threads must be an integer >= 1")
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        self.max_threads = max_threads
        self.limit_provider = limit_provider
        self.worker_cleanup = worker_cleanup
        self.poll_interval = float(poll_interval)
        self.admission_pressure_provider = admission_pressure_provider
        self.prefetches_job_bursts = os.environ.get(
            "MWF_API_PREFETCH", "0"
        ).strip().lower() not in {"0", "false", "no", "off"}
        self.startup_strategy = (
            startup_strategy
            or os.environ.get("MWF_API_STARTUP_STRATEGY")
            or "balanced"
        ).strip().lower()

    def effective_limit(self) -> int:
        value = self.limit_provider() if self.limit_provider is not None else self.max_threads
        if type(value) is not int or value < 1:
            raise ValueError("runtime API concurrency must be an integer >= 1")
        return value

    def startup_lanes(self, items=None) -> int:
        limit = self.effective_limit()
        strategy = self.startup_strategy
        if strategy in {"event", "single", "serial", "legacy", "latency"}:
            return 1
        if strategy == "balanced":
            hint = getattr(items, "remaining_hint", None)
            remaining = hint() if callable(hint) else None
            if type(remaining) is not int or remaining < 128:
                return 1
            return min(2, limit)
        if strategy == "elastic":
            hint = getattr(items, "remaining_hint", None)
            remaining = hint() if callable(hint) else None
            if type(remaining) is not int or remaining <= 0:
                return 1
            if remaining < 256:
                return 1
            jobs_per_lane = int(os.environ.get("MWF_API_JOBS_PER_STARTUP_LANE", "1500"))
            if jobs_per_lane < 256:
                raise ValueError("MWF_API_JOBS_PER_STARTUP_LANE must be >= 256")
            # One base lane plus additional startup help only for genuinely
            # dense nodes. This avoids eight-way completion waves on a 500-job
            # node while allowing 3k-10k queues to use three or four loaders.
            return min(4, limit, 1 + ceil(remaining / jobs_per_lane))
        if strategy == "adaptive":
            # One lane per 128 desired in-flight jobs gives 256/512/1024 jobs
            # two/four/eight startup controllers without creating a thread per
            # job or allowing an unbounded admission wave.
            return min(8, max(1, (limit + 127) // 128))
        if strategy.startswith("lanes:"):
            try:
                requested = int(strategy.split(":", 1)[1])
            except ValueError as error:
                raise ValueError(
                    "MWF_API_STARTUP_STRATEGY lanes value must be an integer"
                ) from error
            if requested < 1:
                raise ValueError("API startup lanes must be >= 1")
            return min(requested, limit)
        raise ValueError(
            "API startup strategy must be elastic, balanced, event, latency, adaptive, single, or lanes:<count>"
        )

    def _run_single_source(self, node_name: str, items, run_one: Callable):
        strategy = self.startup_strategy
        service_interval = 8 if strategy == "latency" else 12 if strategy in {"event", "balanced"} else 16
        pressure = (
            self.admission_pressure_provider
            if strategy in {"event", "balanced", "latency"}
            else None
        )
        pressure_waits = {
            "event": 0.010,
            "balanced": 0.010,
            "latency": 0.050,
        }
        configured_wait = os.environ.get("MWF_API_EVENT_DRAIN_SECONDS")
        pressure_wait = pressure_waits.get(strategy, 0.0)
        if configured_wait is not None and pressure is not None:
            try:
                pressure_wait = float(configured_wait)
            except ValueError as error:
                raise ValueError(
                    "MWF_API_EVENT_DRAIN_SECONDS must be a non-negative number"
                ) from error
            if pressure_wait < 0:
                raise ValueError(
                    "MWF_API_EVENT_DRAIN_SECONDS must be a non-negative number"
                )
        configured_microbatch = os.environ.get("MWF_API_TERMINAL_MICROBATCH")
        state_writer_service_interval = 1
        if configured_microbatch is not None:
            try:
                state_writer_service_interval = int(configured_microbatch)
            except ValueError as error:
                raise ValueError(
                    "MWF_API_TERMINAL_MICROBATCH must be an integer >= 1"
                ) from error
            if state_writer_service_interval < 1:
                raise ValueError(
                    "MWF_API_TERMINAL_MICROBATCH must be an integer >= 1"
                )
        configured_max_burst = os.environ.get("MWF_API_MAX_ADMISSION_BURST")
        max_admission_burst = 512
        if configured_max_burst is not None:
            try:
                max_admission_burst = int(configured_max_burst)
            except ValueError as error:
                raise ValueError(
                    "MWF_API_MAX_ADMISSION_BURST must be an integer >= 16"
                ) from error
            if max_admission_burst < 16:
                raise ValueError(
                    "MWF_API_MAX_ADMISSION_BURST must be an integer >= 16"
                )
        configured_rounds = os.environ.get("MWF_API_ADMISSION_TARGET_ROUNDS")
        target_rounds = 4
        if configured_rounds is not None:
            try:
                target_rounds = int(configured_rounds)
            except ValueError as error:
                raise ValueError(
                    "MWF_API_ADMISSION_TARGET_ROUNDS must be an integer >= 1"
                ) from error
            if target_rounds < 1:
                raise ValueError(
                    "MWF_API_ADMISSION_TARGET_ROUNDS must be an integer >= 1"
                )

        limit = self.effective_limit()
        remaining_hint = getattr(items, "remaining_hint", None)
        remaining = remaining_hint() if callable(remaining_hint) else None
        if type(remaining) is int and remaining > 0:
            start_burst = min(
                limit,
                remaining,
                max_admission_burst,
                max(64, ceil(remaining / target_rounds)),
            )
        else:
            start_burst = min(64, max_admission_burst)
        runtime = FiberRuntime(
            poll_interval=self.poll_interval,
            start_burst=start_burst,
            max_admission_burst=max_admission_burst,
            service_interval=service_interval,
            state_writer_service_interval=state_writer_service_interval,
            admission_pressure_provider=pressure,
            admission_pressure_wait=pressure_wait,
        )
        return runtime.run_source(
            node_name,
            items,
            run_one,
            limit_provider=self.effective_limit,
        )

    def _run_sharded_source(self, node_name: str, items, run_one: Callable, lanes: int):
        pull = getattr(items, "pull", None)
        if not callable(pull):
            # Snapshot iterables cannot safely be consumed by multiple pumps.
            return self._run_single_source(node_name, items, run_one)

        shared = _SharedRefreshableSource(items)
        coordinator = _LaneCoordinator(lanes, self.effective_limit)
        hint = getattr(shared, "remaining_hint", None)
        remaining = hint() if callable(hint) else None
        per_lane = (
            ceil(remaining / lanes)
            if type(remaining) is int and remaining > 0
            else 128
        )
        lane_max_burst = min(512, max(64, per_lane))
        lane_start_burst = min(lane_max_burst, max(64, ceil(per_lane / 2)))
        pressure = self.admission_pressure_provider
        configured_wait = float(os.environ.get("MWF_API_EVENT_DRAIN_SECONDS", "0.010"))
        configured_microbatch = int(os.environ.get("MWF_API_TERMINAL_MICROBATCH", "1"))

        def run_lane(lane: int):
            runtime = FiberRuntime(
                poll_interval=self.poll_interval,
                start_burst=lane_start_burst,
                max_admission_burst=lane_max_burst,
                service_interval=max(4, 12 // min(lanes, 3)),
                state_writer_service_interval=configured_microbatch,
                admission_pressure_provider=pressure,
                admission_pressure_wait=configured_wait,
            )

            def guarded_run_one(item):
                try:
                    return run_one(item)
                except BaseException:
                    shared.stop()
                    raise

            try:
                return runtime.run_source(
                    node_name,
                    shared,
                    guarded_run_one,
                    limit_provider=lambda: coordinator.limit_for(lane),
                )
            finally:
                coordinator.unregister(lane)
                if self.worker_cleanup is not None:
                    self.worker_cleanup()

        results: list = []
        first_error: BaseException | None = None
        with ThreadPoolExecutor(
            max_workers=lanes,
            thread_name_prefix=f"mwf-api-start-{node_name}",
        ) as executor:
            futures = [executor.submit(run_lane, lane) for lane in range(lanes)]
            for future in as_completed(futures):
                try:
                    results.extend(future.result())
                except BaseException as error:
                    shared.stop()
                    first_error = first_error or error
        if first_error is not None:
            raise first_error
        return results

    def _run_source(self, node_name: str, items: Iterable, run_one: Callable):
        lanes = self.startup_lanes(items)
        try:
            if lanes == 1:
                return self._run_single_source(node_name, items, run_one)
            return self._run_sharded_source(node_name, items, run_one, lanes)
        finally:
            if self.worker_cleanup is not None and lanes == 1:
                self.worker_cleanup()

    def run_jobs(self, node_name: str, jobs: list, run_one: Callable):
        if not jobs:
            return []
        return self._run_source(node_name, jobs, run_one)

    def run_job_source(self, node_name: str, job_source, run_one: Callable):
        return self._run_source(node_name, job_source, run_one)
