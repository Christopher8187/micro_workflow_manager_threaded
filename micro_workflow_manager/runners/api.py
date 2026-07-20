from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Callable, Iterable

from .base import BaseRunner
from .threaded import MAX_RUNTIME_THREADS, THREAD_LIMIT_POLL_SECONDS


class ApiRunner(BaseRunner):
    """Eager bounded-concurrency runner for high-latency API/I/O jobs.

    ``max_threads`` is intentionally retained as the public setting even though
    it acts as the maximum number of in-flight API job controllers. Executor
    threads are created lazily only when work is submitted. Unlike the adaptive
    threaded runner, this runner fills the requested concurrency immediately,
    which is useful when most wall time is network wait rather than local CPU.
    """

    supports_refreshable_job_source = True

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
        value = self.limit_provider() if self.limit_provider is not None else self.max_threads
        if type(value) is not int or value < 1:
            raise ValueError("runtime max_threads must be an integer >= 1")
        if value > MAX_RUNTIME_THREADS:
            raise ValueError(f"runtime max_threads must be <= {MAX_RUNTIME_THREADS}")
        return value

    def _run_source(self, node_name: str, items: Iterable, run_one: Callable):
        pull = getattr(items, "pull", None)
        refreshable = callable(pull)
        iterator = None if refreshable else iter(items)
        source_exhausted = False
        next_index = 0
        futures = {}
        results: dict[int, object] = {}

        # A large executor ceiling is cheap: ThreadPoolExecutor creates worker
        # threads lazily. Submission is still bounded by effective_limit().
        with ThreadPoolExecutor(
            max_workers=MAX_RUNTIME_THREADS,
            thread_name_prefix=f"mwf-api-{node_name}",
        ) as executor:
            def submit_item(item) -> None:
                nonlocal next_index
                index = next_index
                next_index += 1

                def run_with_cleanup(value=item):
                    try:
                        return run_one(value)
                    finally:
                        if self.worker_cleanup is not None:
                            self.worker_cleanup()

                futures[executor.submit(run_with_cleanup)] = index

            def fill() -> int:
                nonlocal source_exhausted
                limit = self.effective_limit()
                capacity = max(0, limit - len(futures))
                if capacity == 0:
                    return 0

                if refreshable:
                    new_items = pull(capacity)
                    for item in new_items:
                        submit_item(item)
                    return len(new_items)

                added = 0
                while not source_exhausted and added < capacity:
                    try:
                        item = next(iterator)
                    except StopIteration:
                        source_exhausted = True
                        break
                    submit_item(item)
                    added += 1
                return added

            while True:
                fill()
                if not futures:
                    # A refreshable source is empty *now*. If a sibling node
                    # publishes more work after this pump exits, the component
                    # scheduler will start a new pump. While any API jobs remain
                    # in flight, however, every poll refills from newly queued
                    # rows so the node can grow to its configured concurrency.
                    break

                done, _ = wait(
                    futures,
                    timeout=self.poll_interval,
                    return_when=FIRST_COMPLETED,
                )
                if not done:
                    # Observe runtime max_threads changes and jobs appended after
                    # this node pump started, without waiting for a long API call
                    # to complete.
                    continue
                for future in done:
                    index = futures.pop(future)
                    try:
                        results[index] = future.result()
                    except BaseException:
                        for pending in futures:
                            pending.cancel()
                        raise

        return [results[index] for index in sorted(results)]

    def run_jobs(self, node_name: str, jobs: list, run_one: Callable):
        if not jobs:
            return []
        return self._run_source(node_name, jobs, run_one)

    def run_job_source(self, node_name: str, job_source, run_one: Callable):
        return self._run_source(node_name, job_source, run_one)
