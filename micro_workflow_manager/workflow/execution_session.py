from __future__ import annotations

import os
import socket
import sys
import time
from contextlib import contextmanager
from types import MappingProxyType
from typing import TYPE_CHECKING, Callable
from uuid import uuid4

from micro_workflow_manager import __version__
from micro_workflow_manager.errors import JobFailedError, safe_exception_repr
from micro_workflow_manager.monitor import InlineMonitorReporter, InlineStatsReporter, now_iso
from micro_workflow_manager.processes import process_identity
from micro_workflow_manager.storage.execution_claims import RestartClaimChanged
from micro_workflow_manager.storage.component_states import ComponentExecutionIncomplete
if TYPE_CHECKING:
    from micro_workflow_manager.system import MicroWorkflow


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
            if (continue_full_components and root_paused_for_adopted
                    and not adopted_errors and root_failure is None):
                root_replacements = root_replacements or {}
                run_root = True
                root_paused_for_adopted = False
            if run_root:
                if root_replacements is not None:
                    operation.resume(
                        root_replacements,
                        stop_admission=(not continue_full_components
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
            run_root = bool(local)
            if run_root:
                root_replacements = local


def refuse_competing_run(storage_or_workflow):
    storage = getattr(storage_or_workflow, 'storage', storage_or_workflow)
    active = storage.get_live_main_session()
    if active is None:
        abandoned = next((session for session in storage.list_execution_sessions()
                          if session['session_kind'] == 'main' and session['status'] == 'running'), None)
        if abandoned is not None:
            raise RuntimeError(
                f"Main session {abandoned['session_id']} requires recovery before another main can start"
            )
        return
    command = active.get("command", "workflow")
    session_id = active["session_id"]
    pid = active.get("pid", "?")
    raise RuntimeError(
        f"A {command} sequence is already active (run {session_id}, process {pid}). "
        "Do not start a competing run from a second terminal. To restart the "
        "running and failed work in one active component, use: mwf restart <node>. "
        "The explicit mwf restart <node> job <id> form is also available."
    )


def validate_selected_jobs(workflow, node_name, selected_jobs):
    selected = list(selected_jobs or [])
    for job_id in selected:
        workflow.storage.validate_job_id(job_id)
        if not workflow.storage.job_exists(node_name, job_id):
            raise FileNotFoundError(f'Job does not exist: {node_name}/{job_id}')
    if len(set(selected)) != len(selected):
        raise ValueError('Selected jobs must not contain duplicate IDs')
    return selected


@contextmanager
def execution_session(
    workflow: MicroWorkflow,
    *,
    queue_autostarts: bool = False,
    **kwargs,
):
    if not workflow._execution_entry_lock.acquire(blocking=False):
        raise RuntimeError('An execution call is already active on this workflow')
    try:
        if workflow.execution_session_context is not None:
            raise RuntimeError('Workflow already holds an execution session context')
        previous_allowed_nodes = workflow.allowed_run_nodes
        previous_autostart_mode = workflow.autostart_mode
        try:
            if queue_autostarts:
                workflow.allowed_run_nodes = set(kwargs['nodes'])
                workflow.autostart_mode = 'queue'
            with _execution_session(workflow, **kwargs) as finish:
                yield finish
        finally:
            if queue_autostarts:
                workflow.allowed_run_nodes = previous_allowed_nodes
                workflow.autostart_mode = previous_autostart_mode
    finally:
        workflow._execution_entry_lock.release()


@contextmanager
def _execution_session(
    workflow: MicroWorkflow,
    *,
    command: str,
    start_node: str,
    nodes: list[str],
    selected_jobs: list[int] | None = None,
    selection_builder: Callable[[], dict] | None = None,
    refuse_after_node: str | None = None,
    refuse_before_node: str | None = None,
    stats: bool = False,
    stats_interval: float = 5.0,
    monitor: bool = False,
    monitor_interval: float = 2.0,
):
    with workflow.lock:
        snapshot = workflow.topology.snapshot()
        selected_components = workflow.topology.execution_components(nodes)
    components_by_node = {node: component for component in snapshot.components for node in component}
    if start_node not in components_by_node or any(node not in components_by_node for node in nodes):
        raise ValueError('Execution selection contains a node outside the captured graph')
    ownership = MappingProxyType({node: component for component in selected_components for node in component})
    if start_node not in ownership:
        raise ValueError('The starting node must belong to a selected component')
    run_id = f"{int(time.time())}-{os.getpid()}-{uuid4().hex[:8]}"
    api_startup_strategy = os.environ.get("MWF_API_STARTUP_STRATEGY", "adaptive").strip().lower()
    if api_startup_strategy in {"single", "event", "latency", "serial", "legacy"}:
        api_startup_windows = "1"
    elif api_startup_strategy == "balanced":
        api_startup_windows = "auto:1-2"
    elif api_startup_strategy == "elastic":
        api_startup_windows = "auto:1-4"
    elif api_startup_strategy == "adaptive":
        api_startup_windows = "auto:1-12"
    elif api_startup_strategy.startswith("lanes:"):
        api_startup_windows = api_startup_strategy.split(":", 1)[1]
    else:
        api_startup_windows = "auto"
    api_completion_service_batch = (
        "8" if api_startup_strategy == "latency"
        else "12" if api_startup_strategy in {"event", "balanced"}
        else "16"
    )
    data = {
        "start_node": start_node,
        "refuse_after_node": refuse_after_node,
        "refuse_before_node": refuse_before_node,
        "mwf_version": __version__,
        "api_startup_strategy": api_startup_strategy,
        "api_event_drain_seconds": os.environ.get("MWF_API_EVENT_DRAIN_SECONDS", "0.010"),
        "api_terminal_microbatch": os.environ.get("MWF_API_TERMINAL_MICROBATCH", "1"),
        "api_max_admission_burst": os.environ.get("MWF_API_MAX_ADMISSION_BURST", "512"),
        "api_completion_service_batch": api_completion_service_batch,
        "api_admission_target_rounds": os.environ.get("MWF_API_ADMISSION_TARGET_ROUNDS", "4"),
        "api_startup_windows": api_startup_windows,
        "api_claim_transaction_rows": os.environ.get("MWF_SQLITE_CLAIM_TRANSACTION_ROWS", "192"),
        "api_prefetch": os.environ.get("MWF_API_PREFETCH", "0"),
    }

    admitted_context = None
    created = False
    reserved = False
    finished = False
    exit_failed = False
    terminal_request = None
    body_error = None
    driver = None
    stats_reporter = None
    monitor_reporter = None

    def stop_local_reporting():
        cleanup_error = None
        for reporter in (stats_reporter, monitor_reporter):
            if reporter is not None:
                try:
                    reporter.stop_periodic()
                except BaseException as error:
                    if cleanup_error is None:
                        cleanup_error = error
        workflow.scheduler_supervisor.stop_run_heartbeat(run_id)
        return cleanup_error

    def finish(
        status: str, error: str | None = None, *, restart_attempts=(),
        rejected_restarts=None, exhausted_restarts=None, body_exception=None, failed_nodes=(),
        component_outcomes=(),
    ):
        nonlocal finished, exit_failed, terminal_request, body_error
        if finished or exit_failed:
            return
        if body_exception is not None:
            body_error = body_exception
        if terminal_request is None:
            terminal_request = (status, error)
        status, error = terminal_request

        try:
            with workflow.storage.interprocess_lock("active-run-state"):
                decision = workflow.storage.decide_execution_session_exit(
                    run_id, outcome=status, finished_at=now_iso(),
                    failures=[] if error is None else [{'error': error}],
                    restart_attempts=restart_attempts,
                    rejected_restarts=rejected_restarts,
                    exhausted_restarts=exhausted_restarts,
                    failed_nodes=failed_nodes,
                    component_outcomes=component_outcomes,
                )
                if decision['restarts']:
                    terminal_request = None
                    return decision
                finished = True
        except BaseException as decision_error:
            # A rejected terminal decision leaves durable ownership available
            # for recovery. Retire this caller's reporters without retrying the
            # decision during context-manager unwinding.
            exit_failed = True
            stop_local_reporting()
            if body_error is not None:
                note = f'Execution session exit failed: {safe_exception_repr(decision_error)}'
                body_error.__notes__ = [*getattr(body_error, '__notes__', ()), note]
                raise body_error
            raise

        reporter_cleanup_error = stop_local_reporting()
        override_cleanup_error: Exception | None = None
        with workflow.storage.interprocess_lock("active-run-state"):
            try:
                released = decision['released']
                if reserved and released != len(selected_components):
                    raise RuntimeError(
                        f'Execution session {run_id} reservation cleanup released {released}, '
                        f'expected {len(selected_components)}'
                    )
            finally:
                try:
                    workflow.storage.clear_thread_overrides_for_run(run_id)
                except Exception as cleanup_error:
                    override_cleanup_error = cleanup_error
                finally:
                    workflow.invalidate_thread_override_cache()

        if reporter_cleanup_error is not None:
            if body_error is not None:
                raise body_error from reporter_cleanup_error
            raise reporter_cleanup_error
        for reporter in (stats_reporter, monitor_reporter):
            if reporter is not None:
                reporter.print_final()
        if override_cleanup_error is not None:
            print(
                "Warning: the run completed, but its temporary thread override "
                f"could not be removed: {override_cleanup_error}",
                file=sys.stderr,
            )

    try:
        with workflow.storage.interprocess_lock("active-run-state"):
            refuse_competing_run(workflow)
            selection = selection_builder() if selection_builder is not None else None
            selected_jobs = validate_selected_jobs(workflow, start_node, selected_jobs)
            if selection is not None:
                data['selection'] = selection
            workflow.storage.register_component_topology(snapshot)
            workflow.storage.create_execution_session(
                run_id, session_kind='main', command=command,
                start_component=components_by_node[start_node],
                selected_components=selected_components,
                selected_jobs=[(start_node, job_id) for job_id in (selected_jobs or [])],
                started_at=now_iso(), hostname=socket.gethostname(), pid=os.getpid(),
                process_identity=process_identity(os.getpid()), details=data,
            )
            created = True
            workflow.storage.reserve_execution_components(run_id, expected_shape=snapshot.shape_json)
            reserved = True
            workflow.storage.bind_thread_overrides_to_run(run_id)
            admitted_context = (run_id, ownership, snapshot.shape_json)
            workflow.execution_session_context = admitted_context
            workflow.invalidate_thread_override_cache()

        workflow.scheduler_supervisor.start_run_heartbeat(run_id, interval=2.0)
        stats_reporter = InlineStatsReporter(
            workflow, nodes=nodes, enabled=stats, interval=stats_interval,
        ).start()
        monitor_reporter = InlineMonitorReporter(
            workflow, nodes=nodes, enabled=monitor, interval=monitor_interval,
        ).start()
        driver = ExecutionSessionDriver(workflow, finish, lambda: finished)
        yield driver
    except BaseException as error:
        body_error = error
        if created and not finished:
            if driver is None:
                finish('failed', safe_exception_repr(error))
            else:
                driver.complete('failed', safe_exception_repr(error), body_exception=error)
        raise
    finally:
        try:
            if created and not finished:
                if driver is None:
                    finish('done')
                else:
                    driver.complete()
        finally:
            if workflow.execution_session_context is not admitted_context:
                raise RuntimeError('Workflow execution session context changed during execution')
            workflow.execution_session_context = None
