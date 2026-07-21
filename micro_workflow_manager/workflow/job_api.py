from pathlib import Path
from queue import Queue
from threading import Event, Thread
from time import perf_counter
from typing import Callable, TypeVar

from ..context import JobContext
from ..errors import (
    InvalidGraphError,
    InvalidJobError,
    JobFailedError,
    JobRestartedError,
    JobTimeoutError,
)
from ..models import CANCELLED, DONE, FAILED, QUEUED, RUNNING, SKIPPED, Job, now
from ..fibers import cancellation_scope, in_fiber_runtime
from ..networking import network_attempt_context


T = TypeVar("T")


class JobExecutionApiMixin:
    """Small public job-control convenience methods."""

    def run_one(self, node_name: str, **params):
        job = self.start(
            node_name,
            autostart=False,
            **params,
        )

        result = self.run_job(
            node_name=node_name,
            job_id=job.job_id,
            ignore_readiness=True,
        )
        self.refresh_node_status(node_name, allow_complete=True)
        return result

    def run_node_once(self, node_name: str):
        return self.run_node(
            node_name,
            ignore_readiness=True,
        )

    def list_jobs(self, node_name: str, status: str | None = None):
        return self.storage.list_jobs(node_name, status=status)

    def cancel_job(self, node_name: str, job_id: int):
        self.storage.set_job_status(node_name, job_id, CANCELLED)
        self.refresh_node_status(node_name, allow_complete=False)

    def retry_job(self, node_name: str, job_id: int):
        self.storage.request_job_restart(
            node_name,
            job_id,
            reason="retry_job API",
        )
        self.storage.set_node_status(node_name, QUEUED)

    def skip_node(self, node_name: str):
        self.storage.set_node_status(node_name, SKIPPED)

    def mark_node_done(self, node_name: str):
        self.storage.set_node_status(node_name, DONE)

    def input_dir(self, node_name: str) -> Path:
        return self.storage.node_input_dir(node_name)

    def output_dir(self, node_name: str) -> Path:
        return self.storage.node_output_dir(node_name)
