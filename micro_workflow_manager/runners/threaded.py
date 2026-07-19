from __future__ import annotations

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

    def _run_adaptive(self, node_name: str, items: Iterable, run_one: Callable):
        iterator = iter(items)
        source_lock = Lock()
        condition = Condition()

        desired = self.effective_limit()
        source_exhausted = False
        stop = False
        next_item_index = 0
        next_worker_id = 0
        workers: dict[int, Thread] = {}
        results: dict[int, object] = {}
        first_error: BaseException | None = None

        def take_item():
            nonlocal source_exhausted, next_item_index
            with source_lock:
                if source_exhausted:
                    return None
                try:
                    item = next(iterator)
                except StopIteration:
                    source_exhausted = True
                    return None
                pair = (next_item_index, item)
                next_item_index += 1
                return pair

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

                    pair = take_item()
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
            for _ in range(initial_workers):
                spawn_worker()

            while workers or not source_exhausted:
                condition.wait(self.poll_interval)

                if first_error is not None:
                    stop = True
                    condition.notify_all()
                    continue

                desired = self.effective_limit()

                if not source_exhausted and len(workers) < desired:
                    needed = desired - len(workers)
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
        return self._run_adaptive(node_name, jobs, run_one)

    def run_job_source(self, node_name: str, job_source, run_one: Callable):
        """Run a lazy job source with a live-adjustable concurrency ceiling."""
        return self._run_adaptive(node_name, job_source, run_one)
