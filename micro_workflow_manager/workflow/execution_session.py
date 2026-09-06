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
from micro_workflow_manager.errors import safe_exception_repr
from micro_workflow_manager.monitor import InlineMonitorReporter, InlineStatsReporter, now_iso
from micro_workflow_manager.processes import process_identity
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
    terminal_request = None
    body_error = None
    stats_reporter = None
    monitor_reporter = None

    def finish(status: str, error: str | None = None):
        nonlocal finished, terminal_request
        if finished:
            return
        if terminal_request is None:
            terminal_request = (status, error)
        status, error = terminal_request

        reporter_cleanup_error = None
        for reporter in (stats_reporter, monitor_reporter):
            if reporter is not None:
                try:
                    reporter.stop_periodic()
                except BaseException as cleanup_error:
                    if reporter_cleanup_error is None:
                        reporter_cleanup_error = cleanup_error
        workflow.scheduler_supervisor.stop_run_heartbeat(run_id)
        override_cleanup_error: Exception | None = None
        with workflow.storage.interprocess_lock("active-run-state"):
            if not workflow.storage.finish_execution_session(
                run_id, outcome=status, finished_at=now_iso(),
                failures=[] if error is None else [{'error': error}],
            ):
                raise RuntimeError(f'Execution session {run_id} is missing or already terminal')
            finished = True
            try:
                released = workflow.storage.release_execution_components(run_id)
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
        yield finish
    except BaseException as error:
        body_error = error
        if created and not finished:
            finish("failed", safe_exception_repr(error))
        raise
    finally:
        try:
            if created and not finished:
                finish("done")
        finally:
            if workflow.execution_session_context is not admitted_context:
                raise RuntimeError('Workflow execution session context changed during execution')
            workflow.execution_session_context = None
