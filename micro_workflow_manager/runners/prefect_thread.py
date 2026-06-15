from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner

from .base import BaseRunner


class PrefectThreadRunner(BaseRunner):
    def __init__(self, max_threads: int):
        self.max_threads = max_threads

    def run_jobs(self, node_name: str, jobs: list, run_one):
        @task
        def run_one_job(job):
            return run_one(job)

        @flow(
            name=f"run-{node_name}",
            task_runner=ThreadPoolTaskRunner(max_workers=self.max_threads),
        )
        def run_flow():
            futures = [run_one_job.submit(job) for job in jobs]
            return [future.result() for future in futures]

        return run_flow()