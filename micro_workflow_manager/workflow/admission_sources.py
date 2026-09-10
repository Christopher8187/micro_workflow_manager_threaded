from __future__ import annotations

from dataclasses import dataclass
from itertools import islice
from threading import Event, Lock
from time import perf_counter
from typing import Callable

from ..models import Job, now
from ..storage import InterruptAdmissionPaused


@dataclass(slots=True)
class ClaimedJob:
    storage: object
    job: Job
    generation: int
    execution_id: str
    started_at: str
    started_perf: float
    task_started_recorded: bool = False
    on_abandoned: Callable[[str, int, int, str, bool], None] | None = None

    def abandon_unstarted(self) -> None:
        released = False
        try:
            released = self.storage.release_unstarted_job_execution(
                self.job.node_name, self.job.job_id, self.generation, self.execution_id,
            )
        finally:
            if self.on_abandoned is not None:
                self.on_abandoned(
                    self.job.node_name, self.job.job_id, self.generation, self.execution_id, released,
                )


class ClaimedQueuedJobSource:
    """Preload and claim each refreshable API admission burst atomically."""

    def __init__(
        self,
        storage,
        node_name: str,
        source,
        *,
        session_id: str,
        component: tuple[str, ...],
        task_started_data=None,
        required_params=None,
        allowed_params=None,
        on_abandoned=None,
    ):
        self.storage = storage
        self.node_name = node_name
        self.source = source
        self.session_id = session_id
        self.component = component
        self.task_started_data = task_started_data
        self.required_params = set(required_params or ())
        self.allowed_params = set(allowed_params or ())
        self.on_abandoned = on_abandoned
        self._pending = []
        self._pending_lock = Lock()
        self._closed = False

    def pull(self, max_items: int) -> list[ClaimedJob]:
        if type(max_items) is not int or max_items < 0:
            raise ValueError('max_items must be an integer >= 0')
        if max_items == 0 or self._closed:
            return []
        with self._pending_lock:
            jobs = self._pending[:max_items]
            del self._pending[:len(jobs)]
        if not jobs:
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
                present.update({
                    name for name in ("error", "errors")
                    if name in self.allowed_params
                })
                task_started_mask.append(not (self.required_params - present))
        try:
            leases = self.storage.claim_job_executions_batch(
                self.node_name,
                [job.job_id for job in jobs],
                started_at=started_at,
                task_started_data=self.task_started_data,
                task_started_mask=task_started_mask,
                session_id=self.session_id,
                component=self.component,
            )
        except InterruptAdmissionPaused:
            # Cursor pulls have already consumed these jobs. Retain them until
            # admission resumes, including across concurrent API startup lanes.
            with self._pending_lock:
                self._pending.extend(jobs)
            return []
        return [
            ClaimedJob(
                storage=self.storage,
                job=job,
                generation=generation,
                execution_id=execution_id,
                started_at=started_at,
                started_perf=started_perf,
                on_abandoned=self.on_abandoned,
                task_started_recorded=(
                    self.task_started_data is not None
                    and (task_started_mask is None or task_started_mask[index])
                ),
            )
            for index, (job, (generation, execution_id)) in enumerate(zip(jobs, leases))
        ]

    def close(self):
        self._closed = True
        close = getattr(self.source, "close", None)
        if callable(close):
            close()

    def remaining_hint(self):
        hint = getattr(self.source, "remaining_hint", None)
        remaining = None if not callable(hint) else hint()
        with self._pending_lock:
            pending = len(self._pending)
        return None if remaining is None else remaining + pending

    def wait_for_change(self, timeout: float = 5.0) -> bool:
        if self._closed:
            return False
        with self._pending_lock:
            pending = bool(self._pending)
        if pending:
            blockers = self.storage.interrupt_component_admission_blockers(self.session_id, self.component)
            if blockers and timeout > 0:
                from time import sleep
                sleep(min(timeout, 0.05))
            return not self._closed
        waiter = getattr(self.source, "wait_for_change", None)
        return False if not callable(waiter) else bool(waiter(timeout))


class StoppingJobSource:
    """Stop a node pump from admitting more jobs after a component failure."""

    def __init__(self, source, stop_event: Event):
        self.source = source
        self.iterator = None
        self.iterator_lock = Lock()
        self.stop_event = stop_event

    def pull(self, max_items: int):
        if self.stop_event.is_set():
            return []
        pull = getattr(self.source, "pull", None)
        if not callable(pull):
            with self.iterator_lock:
                if self.iterator is None:
                    self.iterator = iter(self.source)
                return list(islice(self.iterator, max_items))
        return pull(max_items)

    def __iter__(self):
        with self.iterator_lock:
            if self.iterator is None:
                self.iterator = iter(self.source)
        for item in self.iterator:
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
