from __future__ import annotations

from ..errors import JobFailedError
from ..storage.component_states import ComponentExecutionIncomplete
from ..storage.execution_claims import RestartClaimChanged
from .component_execution_operation import ComponentExecutionOperation


class DagExecutionOperation:
    """Keep selected units and their results while a failed unit is resumed."""

    continues_after_nested_restarts = True

    def __init__(self, workflow, units, execution_context, ready_check, options, *, task_parent=None):
        self.workflow = workflow
        self.units = tuple(units)
        self.execution_context = execution_context
        self.ready_check = ready_check
        self.options = options
        self.operations = {}
        self.reported = {}
        self.errors = {}
        self.ran = []
        self.unrelated_error = None
        self.stop_admission = False
        self.admission_stopped = False
        self.futures = {}
        self.task_parent = task_parent

    def component_operation(self, unit, api_pumps):
        if unit not in self.operations:
            self.operations[unit] = ComponentExecutionOperation(
                self.workflow, unit, self.execution_context,
                self.options['wait_deadlock_resolver'], api_pumps,
                task_parent=self.task_parent,
            )
        return self.operations[unit]

    def is_resuming(self, unit):
        operation = self.operations.get(unit)
        return operation is not None and any(
            key not in operation.errors for key in operation.expectations
        )

    def suspended_units(self):
        return {unit for unit, operation in self.operations.items()
                if unit in self.errors or operation.expectations}

    def record_outcome(self, unit, *, error=None):
        operation = self.operations[unit]
        self.ran.extend(operation.ran[self.reported.get(unit, 0):])
        self.reported[unit] = len(operation.ran)
        if error is None:
            self.errors.pop(unit, None)
        else:
            error = operation.primary_error(error)
            self.errors[unit] = error
            if not isinstance(error, (JobFailedError, RestartClaimChanged, ComponentExecutionIncomplete)) and self.unrelated_error is None:
                self.unrelated_error = error

    def retain_component_restarts(self, completion, replacements):
        operation = self.operations.get(completion.component)
        return operation is not None and operation.retain_component_restarts(completion, replacements)

    def failed_attempts(self):
        return tuple(attempt for operation in self.operations.values()
                     for attempt in operation.failed_attempts())

    def rejected_restarts(self):
        return {key: expected for operation in self.operations.values()
                for key, expected in operation.rejected_restarts().items()}

    def exhausted_restarts(self):
        return {key: expected for operation in self.operations.values()
                for key, expected in operation.exhausted_restarts().items()}

    def failed_nodes(self):
        return tuple(node for unit in self.units if unit in self.errors for node in unit)

    def failed_component_outcomes(self):
        return tuple(outcome for operation in self.operations.values()
                     for outcome in operation.failed_component_outcomes())

    def primary_error(self, error):
        return self.unrelated_error or next(iter(self.errors.values()), error)

    def retain_exit_error(self, exception_type, error, traceback):
        if (error is not None and not isinstance(error, (JobFailedError, RestartClaimChanged, ComponentExecutionIncomplete))
                and self.unrelated_error is None):
            self.unrelated_error = error
        return False

    def resume(self, replacements, *, stop_admission=False):
        self.stop_admission = (
            stop_admission or self.admission_stopped or self.unrelated_error is not None
            or any(operation.unrelated_error is not None
                   or any(key not in replacements for key in operation.errors)
                   for operation in self.operations.values())
        )
        remaining = dict(replacements)
        for unit, operation in self.operations.items():
            selected = {key: expected for key, expected in replacements.items() if key[0] in unit}
            if selected:
                operation.resume(selected, stop_admission=self.stop_admission)
                if not operation.errors and operation.unrelated_error is None:
                    self.errors.pop(unit, None)
                for key in selected:
                    remaining.pop(key)
            elif not operation.completed:
                operation.resume({}, stop_admission=self.stop_admission)
        if remaining:
            raise RuntimeError('Session continuation lies outside the selected DAG operations')

    def run(self):
        error = None
        try:
            self.workflow._run_concurrently(
                ready_check=self.ready_check, execution_context=self.execution_context,
                _operation=self, **self.options,
            )
        except BaseException as caught:
            error = caught
            self.retain_exit_error(type(caught), caught, caught.__traceback__)
        # The scheduler's executor has joined even when submission, readiness,
        # or resource cleanup raised outside its Future-result loop.
        for future, unit in tuple(self.futures.items()):
            if not future.cancelled():
                try:
                    future.result()
                except BaseException as pending_error:
                    self.record_outcome(unit, error=pending_error)
                else:
                    self.record_outcome(unit)
            self.futures.pop(future)
        if error is not None or self.unrelated_error is not None or self.errors:
            raise self.primary_error(error)
        return self.ran
