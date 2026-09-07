from __future__ import annotations

from micro_workflow_manager.errors import JobFailedError, safe_exception_repr
from micro_workflow_manager.storage.execution_claims import RestartClaimChanged
from micro_workflow_manager.storage.component_states import ComponentExecutionIncomplete


class ExecutionSessionDriver:
    """Retain an execution operation until its owning session decides to end."""

    def __init__(self, workflow, finish, is_finished):
        self.workflow = workflow
        self.finish = finish
        self._is_finished = is_finished
        self._root_operation = None

    @property
    def finished(self):
        return self._is_finished()

    def __call__(self, status, error=None):
        return self.complete(status, error)

    def finish_component(self, completion, *, task_parent=None):
        from .component_execution_operation import ComponentExecutionOperation

        try:
            self.workflow.storage.finish_successful_component_execution(
                self.workflow.execution_session_context[0], completion, task_parent=task_parent,
            )
        except ComponentExecutionIncomplete as error:
            operation = ComponentExecutionOperation.from_pending_component(
                self.workflow, completion, self.workflow.execution_session_context,
            )
            # The node adapter has joined its jobs and closed its source. Keep
            # its Python result while arbitration handles unfinished work.
            self.drive(operation, initial_failure=error)

    def complete(self, status='done', error=None, *, body_exception=None):
        from .component_execution_operation import ComponentExecutionOperation

        retained_root = self._root_operation
        while True:
            decision = self.finish(status, error, body_exception=body_exception)
            if not decision:
                return
            proposals = decision.get('component_restarts', {})
            selected = {}
            for key, expected in decision['restarts'].items():
                proposal = proposals.get(key)
                component = proposal.component if proposal is not None else expected['owner']['component']
                selected.setdefault(component, {})[key] = expected
            try:
                continuations = []
                for component, replacements in selected.items():
                    proposal = next((proposals[key] for key in replacements if key in proposals), None)
                    if proposal is None:
                        continuation = ComponentExecutionOperation.from_selected_restarts(
                            self.workflow, component, self.workflow.execution_session_context, replacements,
                        )
                    else:
                        continuation = ComponentExecutionOperation.from_pending_component(
                            self.workflow, proposal, self.workflow.execution_session_context, replacements,
                        )
                    continuations.append(continuation)
                # Retain every accepted repair before any ordinary queue can
                # reopen. Later decisions update these same operations.
                self.drive(
                    continuations[0], adopted_operations=continuations[1:],
                    continue_full_components=body_exception is None,
                )
                if body_exception is None and getattr(retained_root, 'continues_after_nested_restarts', False):
                    self.drive(retained_root)
            except BaseException as continuation_error:
                if body_exception is not None:
                    note = f'Nested session continuation failed: {safe_exception_repr(continuation_error)}'
                    body_exception.__notes__ = [*getattr(body_exception, '__notes__', ()), note]
                    raise body_exception
                raise

    def drive(self, operation, runner=None, run_one=None, *, continue_full_components=True,
              adopted_operations=(), initial_failure: ComponentExecutionIncomplete | None = None):
        from .component_execution_operation import ComponentExecutionOperation

        if self._root_operation is None:
            self._root_operation = operation
        unrelated_error = None
        root_failure = initial_failure
        run_root = initial_failure is None
        root_replacements = None
        root_paused_for_adopted = False
        root_incomplete_for_adopted = False
        result = None
        adopted = {item.component: item for item in adopted_operations}
        adopted_by_job = {key: item.component for item in adopted.values() for key in item.attempts}
        adopted_errors = {}
        ready_adopted = list(adopted)
        while True:
            for component in ready_adopted:
                continuation = adopted[component]
                try:
                    continuation.run()
                except BaseException as error:
                    adopted_errors[component] = continuation.primary_error(error)
                else:
                    adopted_errors.pop(component, None)
            ready_adopted = []
            if (continue_full_components and root_paused_for_adopted and not root_incomplete_for_adopted
                    and not adopted_errors and root_failure is None):
                root_replacements = root_replacements or {}
                run_root = True
                root_paused_for_adopted = False
            if run_root:
                if root_replacements is not None:
                    operation.resume(
                        root_replacements,
                        stop_admission=(not continue_full_components
                                        or root_incomplete_for_adopted
                                        or unrelated_error is not None or bool(adopted_errors)),
                    )
                    root_paused_for_adopted = bool(adopted_errors)
                    root_replacements = None
                try:
                    result = operation.run(runner, run_one) if runner is not None else operation.run()
                except BaseException as error:
                    root_failure = unrelated_error or operation.primary_error(error)
                    if not isinstance(root_failure, (JobFailedError, RestartClaimChanged, ComponentExecutionIncomplete)):
                        unrelated_error = root_failure
                else:
                    root_failure = unrelated_error
            failure = root_failure or next(iter(adopted_errors.values()), None)
            if failure is None:
                if root_incomplete_for_adopted:
                    unfinished = next((item for item in adopted.values() if not item.completed), None)
                    if unfinished is not None:
                        unfinished.resume({}, stop_admission=False)
                        ready_adopted = [unfinished.component]
                        run_root = False
                    else:
                        root_incomplete_for_adopted = False
                        root_replacements = {}
                        run_root = True
                    continue
                if continue_full_components:
                    full_operations = (
                        [operation] if isinstance(operation, ComponentExecutionOperation) else []
                    ) + list(adopted.values())
                    unfinished = next((item for item in full_operations if not item.completed), None)
                    if unfinished is not None:
                        # Every accepted repair has succeeded. Continue only
                        # one retained full selection before assessing errors
                        # again, so its failure cannot open a sibling queue.
                        unfinished.resume({}, stop_admission=False)
                        run_root = unfinished is operation
                        if not run_root:
                            ready_adopted = [unfinished.component]
                        continue
                return result
            pending_operations = [operation, *(item for item in adopted.values() if not item.completed)]
            component_outcomes = {outcome.component: outcome for item in pending_operations
                                  for outcome in item.failed_component_outcomes()}
            decision = self.finish(
                'failed', safe_exception_repr(failure),
                restart_attempts=tuple(attempt for item in pending_operations for attempt in item.failed_attempts()),
                rejected_restarts={key: expected for item in pending_operations
                                   for key, expected in item.rejected_restarts().items()},
                exhausted_restarts={key: expected for item in pending_operations
                                    for key, expected in item.exhausted_restarts().items()},
                body_exception=failure,
                failed_nodes=tuple(dict.fromkeys(node for item in pending_operations for node in item.failed_nodes())),
                component_outcomes=tuple(component_outcomes.values()),
            )
            if not decision:
                raise failure
            replacements = decision['restarts']
            proposals = decision.get('component_restarts', {})
            root_attempts = {attempt[:2] for attempt in operation.failed_attempts()}
            selected_adopted = {}
            local = {}
            for key, expected in replacements.items():
                if key in root_attempts and key not in adopted_by_job:
                    local[key] = expected
                    continue
                proposal = proposals.get(key)
                retain = getattr(operation, 'retain_component_restarts', None)
                if proposal is not None and retain is not None and retain(proposal, {key: expected}):
                    local[key] = expected
                    continue
                component = proposal.component if proposal is not None else expected['owner']['component']
                selected_adopted.setdefault(component, {})[key] = expected
                adopted_by_job[key] = component
            for component, selected in selected_adopted.items():
                if component not in adopted:
                    proposal = next((proposals[key] for key in selected if key in proposals), None)
                    if proposal is None:
                        adopted[component] = ComponentExecutionOperation.from_selected_restarts(
                            self.workflow, component, self.workflow.execution_session_context, selected,
                        )
                    else:
                        adopted[component] = ComponentExecutionOperation.from_pending_component(
                            self.workflow, proposal, self.workflow.execution_session_context, selected,
                        )
                else:
                    continuation = adopted[component]
                    for key, expected in selected.items():
                        if key not in continuation.attempts:
                            attempt = (*key, expected['owner']['generation'], expected['owner']['execution_id'])
                            continuation.attempts[key] = attempt
                            continuation.cleanup_attempts[key] = attempt
                    continuation.resume(selected, stop_admission=True)
                ready_adopted.append(component)
            if (selected_adopted and continue_full_components
                    and getattr(operation, 'continues_after_nested_restarts', False)
                    and isinstance(root_failure, ComponentExecutionIncomplete)
                    and unrelated_error is None):
                # A joined causal frontier can be incomplete because a nested
                # operation failed. Finish its accepted continuation, including
                # pending full work, before rechecking the retained root.
                root_failure = None
                root_incomplete_for_adopted = True
            run_root = bool(local)
            if run_root:
                root_replacements = local
