from .base import BaseRunner


class DirectRunner(BaseRunner):
    def run_jobs(self, node_name: str, jobs: list, run_one):
        return [run_one(job) for job in jobs]

    def run_job_source(self, node_name: str, job_source, run_one):
        return [run_one(job) for job in job_source]
