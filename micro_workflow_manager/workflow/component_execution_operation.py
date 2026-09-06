from __future__ import annotations

from dataclasses import replace
from threading import Lock

from ..errors import JobFailedError
from ..storage.component_states import ComponentExecutionIncomplete
from ..storage.execution_claims import RestartClaimChanged


class ComponentExecutionOperation:
    """Retain one component across joined pump epochs and owner decisions."""

    def __init__(self, workflow, component, execution_context, wait_deadlock_resolver, api_pump_allocations, *, task_parent=None):
        self.workflow = workflow
        self.component = tuple(sorted(component))
        self.execution_context = execution_context
        self.wait_deadlock_resolver = wait_deadlock_resolver
        self.api_pump_allocations = api_pump_allocations
        self.ran = []
        self.work_nodes = set()
        self.values = {}
        self.errors = {}
        self.attempts = {}
        self.abandoned = {}
        self.cleanup_attempts = {}
        self.expectations = {}
        self.retried_expectations = {}
        self.unrelated_error = None
        self.stop_admission = False
        self.replacement_epoch = False
        self.lock = Lock()
        self.completion = None
        self.completed = False
        self.started = False
        self.task_parent = task_parent

    @classmethod
    def from_pending_component(cls, workflow, completion, execution_context, replacements=None):
        """Retain a component that already began in the current session."""
        replacements = {} if replacements is None else replacements
        operation = cls(workflow, completion.component, execution_context, None, None)
        operation.completion = completion
        operation.started = True
        operation.stop_admission = True
        operation.expectations = dict(replacements)
        operation.attempts = {
            key: (*key, expected['owner']['generation'], expected['owner']['execution_id'])
            for key, expected in replacements.items()
        }
        return operation

    def record_outcome(self, node, job_id, *, value=None, error=None):
        key = (node, job_id)
        with self.lock:
            self.abandoned.pop(key, None)
            self.cleanup_attempts.pop(key, None)
            if error is not None:
                self.errors[key] = error
                attempt = getattr(error, 'execution_attempt', None)
                if attempt is not None:
                    self.attempts[key] = attempt
                    self.expectations.pop(key, None)
                    self.retried_expectations.pop(key, None)
            else:
                self.values[key] = value
                self.errors.pop(key, None)
                self.attempts.pop(key, None)
                self.expectations.pop(key, None)
                self.retried_expectations.pop(key, None)

    def record_abandoned(self, node, job_id, generation, execution_id, released):
        key = (node, job_id)
        attempt = (node, job_id, generation, execution_id)
        with self.lock:
            self.abandoned[key] = attempt
            self.attempts[key] = attempt
            if not released:
                error = JobFailedError(f'Job {node}/{job_id} was abandoned before execution')
                error.execution_attempt = attempt
                self.errors.setdefault(key, error)

    def abandoned_attempts(self):
        with self.lock:
            return dict(self.abandoned)

    def record_cleanup_attempt(self, node, job_id, generation, execution_id):
        key = (node, job_id)
        attempt = (node, job_id, generation, execution_id)
        with self.lock:
            self.cleanup_attempts[key] = attempt
            self.attempts[key] = attempt

    def record_pump_error(self, error):
        if not isinstance(error, (JobFailedError, RestartClaimChanged, ComponentExecutionIncomplete)):
            if self.unrelated_error is None:
                self.unrelated_error = error

    def expected_restart(self, node, job_id):
        with self.lock:
            return self.expectations.get((node, job_id))

    def retain_component_restarts(self, completion, replacements):
        """Keep nested attempts that belong to this exact begun component."""
        if self.completed or self.completion != completion:
            return False
        for key, expected in replacements.items():
            if key[0] not in self.component:
                raise RuntimeError('Nested restart lies outside its pending component')
            attempt = (*key, expected['owner']['generation'], expected['owner']['execution_id'])
            self.attempts[key] = attempt
            self.cleanup_attempts[key] = attempt
        return True

    def restart_job_ids(self, node):
        if not self.stop_admission and not self.replacement_epoch:
            return None
        with self.lock:
            return [job_id for (member, job_id) in self.expectations
                    if member == node and (member, job_id) not in self.errors]

    def selected_queued_nodes(self, queued):
        if not self.stop_admission and not self.replacement_epoch:
            return queued
        return [node for node in self.component if self.restart_job_ids(node)]

    def failed_nodes(self):
        return self.component

    def failed_component_outcomes(self):
        if self.completion is None or self.completed:
            return ()
        return (replace(self.completion, lifecycle='failed', stability=None, instability_origin=None),)

    def failed_attempts(self):
        return tuple(attempt for key, attempt in self.attempts.items()
                     if key in self.abandoned or key in self.cleanup_attempts or key in self.expectations
                     or key in self.errors)

    def rejected_restarts(self):
        return {key: self.expectations[key] for key, error in self.errors.items()
                if isinstance(error, RestartClaimChanged) and key in self.expectations}

    def exhausted_restarts(self):
        return {key: expected for key, expected in self.retried_expectations.items()
                if self.expectations.get(key) == expected}

    def primary_error(self, error):
        return self.unrelated_error or next(iter(self.errors.values()), error)

    def resume(self, replacements, *, stop_admission=False):
        for key, expected in replacements.items():
            if key not in self.attempts or (
                key not in self.abandoned and key not in self.cleanup_attempts
                and key not in self.errors and key not in self.expectations
            ):
                raise RuntimeError('Session continuation lies outside this component operation')
            if self.expectations.get(key) == expected:
                self.retried_expectations[key] = expected
            else:
                self.retried_expectations.pop(key, None)
            self.expectations[key] = expected
            self.errors.pop(key, None)
        self.stop_admission = stop_admission or bool(self.errors) or self.unrelated_error is not None

    def run(self):
        if not self.started:
            self.completion = self.workflow.begin_full_component_execution(
                self.component, execution_context=self.execution_context, task_parent=self.task_parent,
            )
            self.started = True
        while True:
            # Validate and execute every accepted successor before reopening the
            # ordinary queues. Cancellation cannot make another queued job the
            # first item of the replacement epoch.
            self.replacement_epoch = bool(self.expectations)
            try:
                self.workflow._run_component(
                    set(self.component), True, self.wait_deadlock_resolver, self.api_pump_allocations,
                    execution_context=self.execution_context, _operation=self,
                )
            except BaseException as error:
                self.record_pump_error(error)
                raise self.primary_error(error)
            for key in self.expectations:
                if key not in self.errors:
                    self.errors[key] = RestartClaimChanged(
                        f'Restarted job {key[0]}/{key[1]} was not claimed before its component stopped'
                    )
            if self.unrelated_error is not None or self.errors:
                raise self.primary_error(None)
            if not self.replacement_epoch:
                break
        self.workflow.refresh_component_status(set(self.component), allow_complete=True)
        if self.stop_admission and self.workflow.component_has_queued_jobs(set(self.component)):
            # A different failed unit can stop this component after its exact
            # replacement finishes. Leave its unfinished lifecycle for the
            # session decision without replacing that original failure.
            return self.ran
        if self.completion is not None:
            self.workflow.storage.finish_successful_component_execution(
                self.execution_context[0], self.completion, task_parent=self.task_parent,
            )
        self.completed = True
        return self.ran
