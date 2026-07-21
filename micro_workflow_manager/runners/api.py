from __future__ import annotations

from typing import Callable, Iterable

from .base import BaseRunner
from ..fibers import FiberRuntime


class ApiRunner(BaseRunner):
    """Unbounded-by-framework cooperative runner for API/I/O jobs.

    ``max_threads`` is the node's requested number of in-flight job fibers. It
    is not translated into OS threads and there is no workflow-wide API cap.
    """

    supports_refreshable_job_source = True
    prefers_preloaded_jobs = True
    preclaims_job_bursts = True

    def __init__(
        self,
        max_threads: int,
        *,
        limit_provider: Callable[[], int] | None = None,
        worker_cleanup: Callable[[], None] | None = None,
        poll_interval: float = 0.05,
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

    def effective_limit(self) -> int:
        value = self.limit_provider() if self.limit_provider is not None else self.max_threads
        if type(value) is not int or value < 1:
            raise ValueError("runtime API concurrency must be an integer >= 1")
        return value

    def _run_source(self, node_name: str, items: Iterable, run_one: Callable):
        runtime = FiberRuntime(poll_interval=self.poll_interval)
        try:
            return runtime.run_source(
                node_name,
                items,
                run_one,
                limit_provider=self.effective_limit,
            )
        finally:
            if self.worker_cleanup is not None:
                self.worker_cleanup()

    def run_jobs(self, node_name: str, jobs: list, run_one: Callable):
        if not jobs:
            return []
        return self._run_source(node_name, jobs, run_one)

    def run_job_source(self, node_name: str, job_source, run_one: Callable):
        return self._run_source(node_name, job_source, run_one)
