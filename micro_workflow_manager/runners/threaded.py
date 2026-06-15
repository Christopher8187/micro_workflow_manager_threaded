from __future__ import annotations

from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from typing import Callable

from .base import BaseRunner


class ThreadedRunner(BaseRunner):
    """Dependency-free thread pool runner for jobs inside one node.

    This is the local replacement for the Prefect ThreadPoolTaskRunner used by
    PrefectThreadRunner. It runs multiple queued jobs for the same node in
    parallel, capped by that node's max_threads setting.
    """

    def __init__(self, max_threads: int):
        if type(max_threads) is not int or max_threads < 1:
            raise ValueError("max_threads must be an integer >= 1")
        self.max_threads = max_threads

    def run_jobs(self, node_name: str, jobs: list, run_one: Callable):
        if not jobs:
            return []

        max_workers = min(self.max_threads, len(jobs))
        results_by_index = [None] * len(jobs)

        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=f"mwf-job-{node_name}",
        ) as executor:
            futures = {
                executor.submit(run_one, job): index
                for index, job in enumerate(jobs)
            }

            done, not_done = wait(futures, return_when=FIRST_EXCEPTION)

            first_error = None
            for future in done:
                index = futures[future]
                try:
                    results_by_index[index] = future.result()
                except Exception as error:  # keep original traceback via raise below
                    first_error = error
                    break

            if first_error is not None:
                for future in not_done:
                    future.cancel()

                # Running futures cannot be force-stopped safely, so wait for any
                # already-started jobs to finish writing their status/output files.
                wait(not_done)
                raise first_error

            # No exception happened before the first wait returned. Collect every
            # result, preserving input job order instead of completion order.
            for future in not_done:
                index = futures[future]
                results_by_index[index] = future.result()

        return results_by_index
