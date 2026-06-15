from .base import BaseRunner


class DirectRunner(BaseRunner):
    def run_jobs(self, node_name: str, jobs: list, run_one):
        return [run_one(job) for job in jobs]