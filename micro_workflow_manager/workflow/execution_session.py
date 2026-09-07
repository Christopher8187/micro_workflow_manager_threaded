from __future__ import annotations

import os
import socket
import sys
import time
from contextlib import contextmanager
from types import MappingProxyType
from typing import TYPE_CHECKING
from uuid import uuid4

from micro_workflow_manager import __version__
from micro_workflow_manager.errors import safe_exception_repr
from micro_workflow_manager.monitor import InlineMonitorReporter, InlineStatsReporter, now_iso
from micro_workflow_manager.processes import process_identity
from .admission_wait import resolve_admission_future
from .execution_session_driver import ExecutionSessionDriver
from .sample_admission import SampleRequest, admit_sample_execution_session
if TYPE_CHECKING:
    from micro_workflow_manager.system import MicroWorkflow


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
    sample_request: SampleRequest | None = None,
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
    if sample_request is not None:
        if not isinstance(sample_request, SampleRequest):
            raise TypeError('sample_request must be a SampleRequest')
        if selected_jobs is not None:
            raise ValueError('Sample admission cannot also specify selected jobs')
        if selected_components != [components_by_node[start_node]]:
            raise ValueError('Sample admission requires exactly its starting component')
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
    sample_admission = None

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
            if sample_request is None:
                # Preserve exact-job refusal before topology registration.
                selected_jobs = validate_selected_jobs(workflow, start_node, selected_jobs)
            workflow.storage.register_component_topology(snapshot)
            started_at = now_iso()
            hostname = socket.gethostname()
            pid = os.getpid()
            identity = process_identity(pid)
            if sample_request is None:
                pending_creation = workflow.storage.create_execution_session(
                    run_id, session_kind='main', command=command,
                    start_component=components_by_node[start_node],
                    selected_components=selected_components,
                    selected_jobs=[(start_node, job_id) for job_id in (selected_jobs or [])],
                    started_at=started_at, hostname=hostname, pid=pid,
                    process_identity=identity, details=data, _wait=False,
                )
                admission_arguments = dict(
                    session_kind='main', command=command,
                    start_component=components_by_node[start_node],
                    selected_components=selected_components,
                    selected_jobs=[(start_node, job_id) for job_id in (selected_jobs or [])],
                    started_at=started_at, hostname=hostname, pid=pid,
                    process_identity=identity, details=data,
                )
                creation = resolve_admission_future(
                    pending_creation,
                    lambda: workflow.storage._read_execution_session_admission(
                        run_id, **admission_arguments, reserved=False,
                    ),
                )
                created = True
                if creation.interruption is not None:
                    raise creation.interruption
                pending_reservation = workflow.storage.reserve_execution_components(
                    run_id, expected_shape=snapshot.shape_json, _wait=False,
                )
                reservation = resolve_admission_future(
                    pending_reservation,
                    lambda: workflow.storage._read_execution_session_admission(
                        run_id, **admission_arguments, reserved=True,
                    ),
                )
                reserved = True
                if reservation.interruption is not None:
                    raise reservation.interruption
            else:
                sample_admission = admit_sample_execution_session(
                    workflow, session_id=run_id, command=command, start_node=start_node,
                    snapshot=snapshot, selected_components=selected_components,
                    started_at=started_at, hostname=hostname, pid=pid,
                    process_identity=identity, details=data, request=sample_request,
                )
                # Session, reservation, roots, and selection committed together.
                created = reserved = True
                if sample_admission.interruption is not None:
                    raise sample_admission.interruption
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
        driver = ExecutionSessionDriver(
            workflow, finish, lambda: finished, sample_admission=sample_admission,
        )
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
