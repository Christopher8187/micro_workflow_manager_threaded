from __future__ import annotations

from collections import deque
from dataclasses import dataclass, replace
from itertools import islice
from threading import Lock

from ..errors import JobFailedError
from ..models import Job, QUEUED
from ..runners.process import ProcessPoolRunner
from ..storage.execution_claims import RestartClaimChanged
from ..storage.component_states import ComponentExecutionIncomplete


@dataclass
class _Slot:
    item: object
    complete: bool = False
    value: object = None
    error: BaseException | None = None
    expected_restart: dict | None = None
    failed_attempt: tuple | None = None


class NodeExecutionGroup:
    """Keep one node call's values and selection across explicit replacements.

    Runners still stop admission on failure and join started jobs. This group
    retains their completed Python values, retries only accepted successors,
    and continues the original source without losing prefetched items.
    """

    def __init__(self, storage, node_name, execution_context, source, *, component_completion=None):
        self.storage = storage
        self.node_name = node_name
        self.session_id = execution_context[0]
        self.component = execution_context[1][node_name]
        self.source = source
        self.iterator = iter(source)
        self.slots: dict[int, _Slot] = {}
        self.pending = deque()
        self.source_paused = False
        self.stop_admission = False
        self.replacement_epoch = False
        self.lock = Lock()
        self.component_completion = component_completion

    @staticmethod
    def _job_id(item):
        return item.job_id if isinstance(item, Job) else item

    def pull(self, max_items):
        if self.pending:
            return [self.pending.popleft() for _ in range(min(max_items, len(self.pending)))]
        if self.source_paused:
            return []
        pull = getattr(self.source, 'pull', None)
        items = list(pull(max_items) if callable(pull) else islice(self.iterator, max_items))
        with self.lock:
            for item in items:
                job_id = self._job_id(item)
                if job_id in self.slots:
                    raise RuntimeError(f'Job {self.node_name}/{job_id} was admitted twice')
                self.slots[job_id] = _Slot(item)
        return items

    def __iter__(self):
        while items := self.pull(64):
            yield from items

    def _prepare_replacements(self):
        failed = [slot for slot in self.slots.values() if slot.error is not None]
        if not failed:
            return False
        replacements = []
        for slot in failed:
            error = slot.error
            if not isinstance(error, JobFailedError):
                return False
            attempt = getattr(error, 'execution_attempt', None)
            if attempt is None:
                return False
            node, job_id, generation, execution_id = attempt
            observed = self.storage.read_job_owner_observation(node, job_id)
            owner = None if observed is None else observed['owner']
            if (owner is None or node != self.node_name
                    or owner['session_id'] != self.session_id
                    or owner['component'] != self.component
                    or owner['execution_id'] != execution_id
                    or observed['status'] != QUEUED
                    or observed['active_execution_id'] is not None
                    or observed['generation'] <= generation):
                continue
            control = self.storage.read_job_control(node, job_id)
            if (control['generation'] != observed['generation']
                    or control['active_execution_id'] is not None
                    or control['restart_requested_at'] is None):
                continue
            expected = {'job_instance_id': observed['job_instance_id'],
                        'generation': observed['generation'], 'owner': owner,
                        'restart_requested_at': control['restart_requested_at']}
            replacements.append((slot, self.storage.load_job(node, job_id)
                                 if isinstance(slot.item, Job) else job_id, expected))
        if not replacements:
            return False
        for slot, item, expected in replacements:
            slot.item, slot.error, slot.expected_restart = item, None, expected
        # Join every accepted successor before admitting ordinary work. Keep
        # prefetched ordinary items in their slots for the following epoch.
        self.replacement_epoch = self.source_paused = True
        self.pending.clear()
        self.pending.extend(item for _, item, _ in replacements)
        return True

    def _record_outcome(self, item, *, value=None, error=None):
        if (self.component_completion is not None and isinstance(error, ComponentExecutionIncomplete)
                and getattr(error, 'execution_attempt', None) is None):
            return
        with self.lock:
            slot = self.slots[self._job_id(item)]
            if error is not None:
                slot.error = error
                attempt = getattr(error, 'execution_attempt', None)
                if attempt is not None:
                    slot.failed_attempt = attempt
            else:
                slot.value, slot.complete = value, True

    def failed_component_outcomes(self):
        if self.component_completion is None:
            return ()
        return (replace(self.component_completion, lifecycle='failed', stability=None, instability_origin=None),)

    def failed_attempts(self):
        failed = [slot for slot in self.slots.values()
                  if slot.error is not None]
        return tuple(slot.failed_attempt for slot in failed if slot.failed_attempt is not None)

    def failed_nodes(self):
        return (self.node_name,)

    def primary_error(self, error):
        if not isinstance(error, (JobFailedError, RestartClaimChanged)):
            return error
        return next((slot.error for slot in self.slots.values()
                     if slot.error is not None
                     and not isinstance(slot.error, (JobFailedError, RestartClaimChanged))), error)

    def rejected_restarts(self):
        return {
            (self.node_name, job_id): slot.expected_restart
            for job_id, slot in self.slots.items()
            if isinstance(slot.error, RestartClaimChanged) and slot.expected_restart is not None
        }

    def exhausted_restarts(self):
        return {}

    def resume(self, replacements, *, stop_admission=False):
        """Reload only the successors chosen by the session's writer decision."""
        resumed = []
        for (node, job_id), expected in replacements.items():
            if node != self.node_name or job_id not in self.slots:
                raise RuntimeError('Session continuation lies outside this node operation')
            slot = self.slots[job_id]
            item = self.storage.load_job(node, job_id) if isinstance(slot.item, Job) else job_id
            resumed.append((slot, item, expected))
        for slot, item, expected in resumed:
            slot.item, slot.error, slot.expected_restart = item, None, expected
        self.stop_admission = stop_admission
        self.replacement_epoch = bool(resumed)
        self.source_paused = (stop_admission or self.replacement_epoch
                              or any(slot.error is not None for slot in self.slots.values()))
        self.pending.clear()
        if resumed:
            self.pending.extend(item for _, item, _ in resumed)
        elif not self.source_paused:
            self.pending.extend(slot.item for slot in self.slots.values() if not slot.complete)

    def run(self, runner, run_one):
        def expected_restart(item):
            return self.slots[self._job_id(item)].expected_restart

        def execute(item):
            try:
                value = run_one(item, _expected_restart=expected_restart(item))
            except BaseException as error:
                self._record_outcome(item, error=error)
                raise
            self._record_outcome(item, value=value)
            return value

        while True:
            paused = False
            try:
                if isinstance(runner, ProcessPoolRunner):
                    runner.run_job_source(
                        self.node_name, self, execute, on_outcome=self._record_outcome,
                        get_restart_expectation=expected_restart,
                    )
                else:
                    runner.run_job_source(self.node_name, self, execute)
            except ComponentExecutionIncomplete as error:
                if self.component_completion is None or getattr(error, 'execution_attempt', None) is not None:
                    raise
                paused = True
            except JobFailedError:
                if self._prepare_replacements():
                    continue
                raise
            errors = [slot.error for slot in self.slots.values() if slot.error is not None]
            if errors:
                if self._prepare_replacements():
                    continue
                raise errors[0]
            if self.replacement_epoch and not paused and not self.stop_admission:
                self.resume({})
                continue
            # A no-attempt pause hands the pending component back to the
            # full-call boundary after preserving any actual attempt failure.
            return [slot.value for slot in self.slots.values() if slot.complete]
