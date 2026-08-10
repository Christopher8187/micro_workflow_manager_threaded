from __future__ import annotations

from collections import deque
from threading import Condition, Lock, Thread
from typing import Callable, Iterable

from .base import BaseRunner


MAX_RUNTIME_THREADS = 4096
HIGH_RUNTIME_THREAD_WARNING = 256
THREAD_LIMIT_POLL_SECONDS = 0.20
INITIAL_WORKER_BURST = 8


class ThreadedRunner(BaseRunner):
    """Adaptive local thread runner for jobs inside one node.

    ``max_threads`` is the router-declared default. ``limit_provider`` may
    return a runtime override written by ``mwf threads``. Scale-up starts more
    worker loops within the polling interval; scale-down never kills a running
    job and lets surplus workers retire after their current job.

    Workers pull jobs lazily and remain alive for multiple jobs. This preserves
    the low scheduling overhead of the original runner and avoids creating one
    future per job or one empty worker per very large declared limit.
    """

    prefers_preloaded_jobs = True
    supports_refreshable_job_source = True
    refreshable_only_when_live = True
    # The scheduler wraps queued job objects in a bounded background prefetch
    # source. Payload I/O therefore happens outside the worker source lock.
    prefetches_job_bursts = True

    def job_prefetch_workers(self) -> int:
        import os
        configured = os.environ.get("MWF_THREADED_JOB_PREFETCH_WORKERS")
        if configured is None:
            return 2
        try:
            value = int(configured)
        except ValueError as error:
            raise ValueError(
                "MWF_THREADED_JOB_PREFETCH_WORKERS must be an integer >= 1"
            ) from error
        if value < 1:
            raise ValueError(
                "MWF_THREADED_JOB_PREFETCH_WORKERS must be an integer >= 1"
            )
        return min(value, 32)

    def job_prefetch_batches(self) -> int:
        import os
        configured = os.environ.get("MWF_THREADED_JOB_PREFETCH_BATCHES")
        if configured is None:
            return self.job_prefetch_workers()
        try:
            value = int(configured)
        except ValueError as error:
            raise ValueError(
                "MWF_THREADED_JOB_PREFETCH_BATCHES must be an integer >= 1"
            ) from error
        if value < 1:
            raise ValueError(
                "MWF_THREADED_JOB_PREFETCH_BATCHES must be an integer >= 1"
            )
        return min(value, 32)

    def __init__(
        self,
        max_threads: int,
        *,
        limit_provider: Callable[[], int] | None = None,
        worker_cleanup: Callable[[], None] | None = None,
        poll_interval: float = THREAD_LIMIT_POLL_SECONDS,
    ):
        if type(max_threads) is not int or max_threads < 1:
            raise ValueError("max_threads must be an integer >= 1")
        if max_threads > MAX_RUNTIME_THREADS:
            raise ValueError(f"max_threads must be <= {MAX_RUNTIME_THREADS}")
        if poll_interval <= 0:
            raise ValueError("poll_interval must be > 0")
        self.max_threads = max_threads
        self.limit_provider = limit_provider
        self.worker_cleanup = worker_cleanup
        self.poll_interval = float(poll_interval)

    def effective_limit(self) -> int:
        value = self.max_threads
        if self.limit_provider is not None:
            value = self.limit_provider()
        if type(value) is not int or value < 1:
            raise ValueError("runtime max_threads must be an integer >= 1")
        if value > MAX_RUNTIME_THREADS:
            raise ValueError(
                f"runtime max_threads must be <= {MAX_RUNTIME_THREADS}"
            )
        return value

    def _run_adaptive(
        self,
        node_name: str,
        items: Iterable,
        run_one: Callable,
        *,
        known_count: int | None = None,
    ):
        iterator = iter(items)
        source_pull = getattr(items, "pull", None)
        source_wait = getattr(items, "wait_for_change", None)
        source_condition = Condition()
        condition = Condition()

        desired = self.effective_limit()
        source_exhausted = False
        source_loading = False
        source_buffer = deque()
        stop = False
        next_item_index = 0
        next_worker_id = 0
        workers: dict[int, Thread] = {}
        results: dict[int, object] = {}
        first_error: BaseException | None = None

        def take_item():
            nonlocal source_exhausted, source_loading, next_item_index
            while True:
                with source_condition:
                    if stop:
                        return None
                    if source_buffer:
                        item = source_buffer.popleft()
                        pair = (next_item_index, item)
                        next_item_index += 1
                        return pair
                    if source_exhausted:
                        return None
                    if source_loading:
                        source_condition.wait(self.poll_interval)
                        continue
                    source_loading = True

                try:
                    if callable(source_pull):
                        # Pull one admission-sized batch outside the shared source
                        # condition, then hand those already-loaded objects to
                        # workers from a deque. This pays synchronization once per
                        # batch rather than once per ``next()`` while still never
                        # holding peers behind slow payload/filesystem I/O.
                        loaded_batch = list(source_pull(64))
                        while not loaded_batch and callable(source_wait) and not stop:
                            if not source_wait(self.poll_interval):
                                break
                            if stop:
                                break
                            loaded_batch = list(source_pull(64))
                        exhausted = not loaded_batch
                    else:
                        try:
                            loaded_batch = [next(iterator)]
                            exhausted = False
                        except StopIteration:
                            loaded_batch = []
                            exhausted = True
                except BaseException:
                    # Never turn a payload-loader failure (notably EMFILE) into
                    # a phantom job. Release peers waiting on the source
                    # condition, then propagate the original exception so the
                    # component scheduler can stop admission and join siblings.
                    with source_condition:
                        source_loading = False
                        source_condition.notify_all()
                    raise

                with source_condition:
                    source_loading = False
                    if loaded_batch:
                        source_buffer.extend(loaded_batch)
                    elif exhausted:
                        source_exhausted = True
                    source_condition.notify_all()
                    if source_buffer:
                        continue
                    if source_exhausted:
                        return None

        def worker_loop(worker_id: int) -> None:
            nonlocal stop, first_error
            try:
                while True:
                    with condition:
                        if stop:
                            return
                        # Surplus workers retire only between jobs. Running jobs
                        # are never interrupted by a scale-down command.
                        if len(workers) > desired:
                            return

                    try:
                        pair = take_item()
                    except BaseException as error:
                        with condition:
                            if first_error is None:
                                first_error = error
                            stop = True
                            condition.notify_all()
                        with source_condition:
                            source_condition.notify_all()
                        return
                    if pair is None:
                        with condition:
                            condition.notify_all()
                        return
                    index, item = pair

                    try:
                        value = run_one(item)
                    except BaseException as error:
                        with condition:
                            if first_error is None:
                                first_error = error
                            stop = True
                            condition.notify_all()
                        with source_condition:
                            source_condition.notify_all()
                        return

                    with condition:
                        results[index] = value
                        condition.notify_all()
            finally:
                if self.worker_cleanup is not None:
                    self.worker_cleanup()
                with condition:
                    workers.pop(worker_id, None)
                    condition.notify_all()

        def spawn_worker() -> None:
            nonlocal next_worker_id
            worker_id = next_worker_id
            next_worker_id += 1
            thread = Thread(
                target=worker_loop,
                args=(worker_id,),
                name=f"mwf-job-{node_name}-{worker_id}",
            )
            workers[worker_id] = thread
            thread.start()

        with condition:
            # Preserve fast startup for normal nodes, while preventing a node
            # declared with max_threads=1000 from creating 1000 empty workers
            # before the lazy source has revealed how much work exists.
            initial_workers = min(desired, INITIAL_WORKER_BURST)
            if known_count is not None:
                initial_workers = min(initial_workers, known_count)
            for _ in range(initial_workers):
                spawn_worker()

            while workers or (not source_exhausted and first_error is None):
                condition.wait(self.poll_interval)

                if first_error is not None:
                    stop = True
                    condition.notify_all()
                    # A lazy source may still contain thousands of unclaimed
                    # jobs when one worker fails. Once the remaining workers
                    # have retired, do not wait forever for a source that no
                    # worker is allowed to consume.
                    if not workers:
                        break
                    continue

                desired = self.effective_limit()

                if not source_exhausted and len(workers) < desired:
                    needed = desired - len(workers)
                    if known_count is not None:
                        # For an in-memory list, never create workers after all
                        # items have already been claimed. This avoids a fast
                        # two-item run waking the manager between completion
                        # and StopIteration and triggering a second empty burst.
                        needed = min(needed, max(0, known_count - next_item_index))
                    # Grow geometrically so large requested limits become
                    # available quickly, but stop as soon as one worker proves
                    # the lazy source is exhausted.
                    growth = max(1, len(workers))
                    for _ in range(min(needed, growth)):
                        spawn_worker()

                # Workers observe a lower desired value after their current job
                # and retire themselves. The manager only needs to wake them.
                condition.notify_all()

        if first_error is not None:
            raise first_error

        return [results[index] for index in sorted(results)]

    def run_jobs(self, node_name: str, jobs: list, run_one: Callable):
        if not jobs:
            return []
        return self._run_adaptive(
            node_name, jobs, run_one, known_count=len(jobs)
        )

    def run_job_source(self, node_name: str, job_source, run_one: Callable):
        """Run a lazy job source with a live-adjustable concurrency ceiling."""
        return self._run_adaptive(node_name, job_source, run_one)
