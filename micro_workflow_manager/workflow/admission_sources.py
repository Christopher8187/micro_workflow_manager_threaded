from __future__ import annotations

from dataclasses import dataclass
from threading import Event
from time import perf_counter

from ..models import Job, now


@dataclass(slots=True)
class ClaimedJob:
    storage: object
    job: Job
    generation: int
    execution_id: str
    started_at: str
    started_perf: float
    task_started_recorded: bool = False

    def abandon_unstarted(self) -> None:
        self.storage.release_unstarted_job_execution(
            self.job.node_name,
            self.job.job_id,
            self.generation,
            self.execution_id,
        )


class ClaimedQueuedJobSource:
    """Preload and claim each refreshable API admission burst atomically."""

    def __init__(
        self,
        storage,
        node_name: str,
        source,
        *,
        task_started_data=None,
        required_params=None,
        allowed_params=None,
    ):
        self.storage = storage
        self.node_name = node_name
        self.source = source
        self.task_started_data = task_started_data
        self.required_params = set(required_params or ())
        self.allowed_params = set(allowed_params or ())

    def pull(self, max_items: int) -> list[ClaimedJob]:
        jobs = self.source.pull(max_items)
        if not jobs:
            return []
        started_at = now()
        started_perf = perf_counter()
        task_started_mask = None
        if self.task_started_data is not None:
            task_started_mask = []
            for job in jobs:
                present = {key for key in job.params if key in self.allowed_params}
                if "error" in self.allowed_params:
                    present.add("error")
                task_started_mask.append(not (self.required_params - present))
        leases = self.storage.claim_job_executions_batch(
            self.node_name,
            [job.job_id for job in jobs],
            started_at=started_at,
            task_started_data=self.task_started_data,
            task_started_mask=task_started_mask,
        )
        return [
            ClaimedJob(
                storage=self.storage,
                job=job,
                generation=generation,
                execution_id=execution_id,
                started_at=started_at,
                started_perf=started_perf,
                task_started_recorded=(
                    self.task_started_data is not None
                    and (task_started_mask is None or task_started_mask[index])
                ),
            )
            for index, (job, (generation, execution_id)) in enumerate(zip(jobs, leases))
        ]

    def close(self):
        close = getattr(self.source, "close", None)
        if callable(close):
            close()

    def remaining_hint(self):
        hint = getattr(self.source, "remaining_hint", None)
        return None if not callable(hint) else hint()

    def wait_for_change(self, timeout: float = 5.0) -> bool:
        waiter = getattr(self.source, "wait_for_change", None)
        return False if not callable(waiter) else bool(waiter(timeout))


class StoppingJobSource:
    """Stop a node pump from admitting more jobs after a component failure."""

    def __init__(self, source, stop_event: Event):
        self.source = source
        self.stop_event = stop_event

    def pull(self, max_items: int):
        if self.stop_event.is_set():
            return []
        pull = getattr(self.source, "pull", None)
        if not callable(pull):
            raise TypeError("wrapped source does not support pull")
        return pull(max_items)

    def __iter__(self):
        for item in self.source:
            if self.stop_event.is_set():
                return
            yield item

    def close(self):
        close = getattr(self.source, "close", None)
        if callable(close):
            close()

    def remaining_hint(self):
        if self.stop_event.is_set():
            return 0
        hint = getattr(self.source, "remaining_hint", None)
        return None if not callable(hint) else hint()

    def wait_for_change(self, timeout: float = 5.0) -> bool:
        if self.stop_event.is_set():
            return False
        waiter = getattr(self.source, "wait_for_change", None)
        return False if not callable(waiter) else bool(waiter(timeout))
