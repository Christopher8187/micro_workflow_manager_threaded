from __future__ import annotations

import os
import socket
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Callable
from uuid import uuid4

from micro_workflow_manager import __version__
from micro_workflow_manager.models import CANCELLED, DONE, FAILED, QUEUED, RUNNING, SKIPPED
from micro_workflow_manager.monitor import InlineMonitorReporter, InlineStatsReporter, now_iso
from micro_workflow_manager.system import MicroWorkflow

from .active_run import refuse_competing_run
from .cleanup import prepare_fresh_components, reset_job_for_run
from .files import read_config
from .project import resolve_configured_graph_path
from .graph_utils import (
    component_topological_nodes,
    descendants_in_order,
    direct_incomplete_inputs,
    expand_to_components,
    ready_for_run_set,
    topo_subset,
)
from .validation import ask, is_ready, print_not_ready

@contextmanager
def active_workflow_run(
    workflow: MicroWorkflow,
    *,
    command: str,
    start_node: str,
    nodes: list[str],
    selected_jobs: list[int] | None = None,
    stats: bool = False,
    stats_interval: float = 5.0,
    monitor: bool = False,
    monitor_interval: float = 2.0,
):
    run_id = f"{int(time.time())}-{os.getpid()}-{uuid4().hex[:8]}"
    data = {
        "run_id": run_id,
        "status": "running",
        "command": command,
        "start_node": start_node,
        "nodes": list(nodes),
        "selected_jobs": list(selected_jobs or []),
        "started_at": now_iso(),
        "heartbeat_at": now_iso(),
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "mwf_version": __version__,
    }

    # Claim the project run slot atomically. This prevents two terminals from
    # replacing .mwf/run.json at the same time. The restart command does not
    # claim this slot; it only controls a job already owned by this run.
    with workflow.storage.interprocess_lock("active-run-state"):
        refuse_competing_run(workflow)
        workflow.storage.write_run_state(data)
        workflow.storage.bind_thread_overrides_to_run(run_id)
        workflow.invalidate_thread_override_cache()

    # The scheduler supervisor owns both project-run heartbeats and handler
    # checkpoint deadlines. One thread services the whole workflow sequence.
    workflow.scheduler_supervisor.start_run_heartbeat(run_id, interval=2.0)

    stats_reporter = InlineStatsReporter(
        workflow,
        nodes=nodes,
        enabled=stats,
        interval=stats_interval,
    ).start()
    monitor_reporter = InlineMonitorReporter(
        workflow,
        nodes=nodes,
        enabled=monitor,
        interval=monitor_interval,
    ).start()

    finished = False

    def finish(status: str, error: str | None = None):
        nonlocal finished
        if finished:
            return

        # Stop periodic output before changing the run record, then print one
        # final snapshot after the record is terminal. This guarantees that an
        # inline or standalone monitor never labels a completed sequence active.
        stats_reporter.stop_periodic()
        monitor_reporter.stop_periodic()
        workflow.scheduler_supervisor.stop_run_heartbeat(run_id)
        workflow.storage.clear_thread_overrides_for_run(run_id)
        workflow.invalidate_thread_override_cache()
        with workflow.storage.interprocess_lock("active-run-state"):
            current = workflow.storage.get_run_state()
            # Never let a stale process overwrite a newer run record.
            if current.get("run_id") == run_id:
                updates = {
                    "status": status,
                    "finished_at": now_iso(),
                }
                if error is not None:
                    updates["error"] = error
                workflow.storage.update_run_state(**updates)
        finished = True
        stats_reporter.print_final()
        monitor_reporter.print_final()

    try:
        yield finish
    except Exception as error:
        finish("failed", repr(error))
        raise
    finally:
        if not finished:
            finish("done")

def run_selected_jobs(
    root: Path,
    workflow: MicroWorkflow,
    node: str,
    job_ids: list[int],
    *,
    stats: bool = False,
    stats_interval: float = 5.0,
    monitor: bool = False,
    monitor_interval: float = 2.0,
) -> int:
    refuse_competing_run(workflow)

    if not is_ready(workflow, node):
        print_not_ready(workflow, node)
        return 1

    for job_id in job_ids:
        if not workflow.storage.job_exists(node, job_id):
            raise RuntimeError(f"Job does not exist: {node}/{job_id}")

    previous_allowed_run_nodes = workflow.allowed_run_nodes
    previous_autostart_mode = workflow.autostart_mode
    previous_restart_enabled = workflow.active_job_restart_enabled
    workflow.allowed_run_nodes = {node}
    workflow.autostart_mode = "queue"
    workflow.active_job_restart_enabled = True

    try:
        with active_workflow_run(
            workflow,
            command="run jobs",
            start_node=node,
            nodes=[node],
            selected_jobs=job_ids,
            stats=stats,
            stats_interval=stats_interval,
            monitor=monitor,
            monitor_interval=monitor_interval,
        ) as finish_run:
            # The run slot is claimed before any selected-job artifacts are
            # reset, so a second run command cannot race with preparation.
            workflow.storage.set_node_status(node, RUNNING)
            for job_id in job_ids:
                reset_job_for_run(root, workflow, node, job_id, mark_queued=False)

            jobs = [workflow.storage.load_job(node, job_id) for job_id in job_ids]
            workflow.run_node_jobs(node, jobs, ignore_readiness=True)
            finish_run("done")
    finally:
        workflow.allowed_run_nodes = previous_allowed_run_nodes
        workflow.autostart_mode = previous_autostart_mode
        workflow.active_job_restart_enabled = previous_restart_enabled

    print(f"Ran jobs for {node}:")
    for job_id in job_ids:
        print(f"  {job_id}")

    return 0

def _component_notice(workflow: MicroWorkflow, node: str) -> list[str]:
    component = list(workflow.component_key(workflow.component_for(node)))
    if len(component) > 1:
        print(
            f"Node {node} belongs to Hoeflein component "
            f"{{{', '.join(component)}}}; this command runs the whole component."
        )
    return component


def _refuse_start_component_inputs(workflow: MicroWorkflow, node: str, command: str) -> bool:
    component = workflow.component_for(node)
    blockers = {
        previous for previous in workflow.component_predecessors(component)
        if not workflow.node_complete(previous)
    }
    if not blockers:
        return False
    print(f"Cannot {command}: incomplete predecessor components: {', '.join(sorted(blockers))}")
    print("Hoeflein components are scheduled on the quotient DAG; run or resume those predecessors first.")
    for previous in sorted(blockers):
        print(f"  {previous}: {workflow.storage.get_node_status(previous) or 'missing'}")
    return True


def run_node(root: Path, workflow: MicroWorkflow, node: str, *, stats: bool = False, stats_interval: float = 5.0, monitor: bool = False, monitor_interval: float = 2.0) -> int:
    refuse_competing_run(workflow)
    nodes = _component_notice(workflow, node)
    if _refuse_start_component_inputs(workflow, node, f"run {node}"):
        return 1

    component = set(nodes)

    def prepare():
        workflow.storage.set_node_status(node, RUNNING)
        removed = prepare_fresh_components(root, workflow, [component])
        if removed:
            summary = ", ".join(f"{name}={count}" for name, count in sorted(removed.items()))
            print(f"Removed jobs produced by Hoeflein component {{{', '.join(nodes)}}}: {summary}")

    return run_nodes(
        workflow, nodes, node, ignore_external=False, command="run",
        stats=stats, stats_interval=stats_interval, monitor=monitor,
        monitor_interval=monitor_interval, prepare=prepare,
    )


def run_from(root: Path, workflow: MicroWorkflow, node: str, *, stats: bool = False, stats_interval: float = 5.0, monitor: bool = False, monitor_interval: float = 2.0) -> int:
    refuse_competing_run(workflow)
    start_component = workflow.component_for(node)
    start_nodes = _component_notice(workflow, node)
    components = [start_component, *[set(item) for item in workflow.component_descendants(start_component)]]
    nodes = [name for component in components for name in workflow.component_key(component)]
    if _refuse_start_component_inputs(workflow, node, f"runfrom {node}"):
        return 1
    external_descendant_blockers = direct_incomplete_inputs(workflow, set(nodes))
    if external_descendant_blockers:
        print(
            "Partial runfrom: preserving work from external predecessor components "
            f"and running this branch independently: {', '.join(sorted(external_descendant_blockers))}"
        )

    def prepare():
        workflow.storage.set_node_status(node, RUNNING)
        removed = prepare_fresh_components(root, workflow, components)
        if removed:
            summary = ", ".join(f"{name}={count}" for name, count in sorted(removed.items()))
            selected = "; ".join("{" + ", ".join(workflow.component_key(c)) + "}" for c in components)
            print(f"Removed jobs produced by selected Hoeflein components {selected}: {summary}")

    return run_nodes(
        workflow, nodes, node, ignore_external=True, command="runfrom",
        stats=stats, stats_interval=stats_interval, monitor=monitor,
        monitor_interval=monitor_interval, prepare=prepare,
    )

def _prepare_node_for_resume(workflow: MicroWorkflow, node: str) -> int:
    """Requeue only unsuccessful work, preserving done/skipped jobs and output."""
    changed = 0
    for job_id in workflow.storage.list_job_ids(node):
        status = workflow.storage.get_job_status(node, job_id)
        if status in {FAILED, CANCELLED, RUNNING}:
            workflow.storage.request_job_restart(
                node,
                job_id,
                reason="resume unsuccessful job",
            )
            changed += 1
    if workflow.storage.has_queued_jobs(node):
        workflow.storage.set_node_status(node, QUEUED)
    return changed


def resume_node(
    root: Path,
    workflow: MicroWorkflow,
    node: str,
    *,
    stats: bool = False,
    stats_interval: float = 5.0,
    monitor: bool = False,
    monitor_interval: float = 2.0,
) -> int:
    refuse_competing_run(workflow)
    nodes = list(workflow.component_key(workflow.component_for(node)))

    blockers = direct_incomplete_inputs(workflow, set(nodes))
    ignore_external = not not blockers
    if blockers:
        print("Resuming with incomplete external inputs:", ", ".join(sorted(blockers)))

    def prepare():
        for item in nodes:
            _prepare_node_for_resume(workflow, item)

    return run_nodes(
        workflow,
        nodes,
        node,
        ignore_external=ignore_external,
        command="resume",
        stats=stats,
        stats_interval=stats_interval,
        monitor=monitor,
        monitor_interval=monitor_interval,
        prepare=prepare,
        require_start_queued=False,
    )


def resume_from(
    root: Path,
    workflow: MicroWorkflow,
    node: str,
    *,
    stats: bool = False,
    stats_interval: float = 5.0,
    monitor: bool = False,
    monitor_interval: float = 2.0,
) -> int:
    refuse_competing_run(workflow)
    start_component = workflow.component_for(node)
    components = [start_component, *[set(item) for item in workflow.component_descendants(start_component)]]
    nodes = [name for component in components for name in workflow.component_key(component)]

    blockers = direct_incomplete_inputs(workflow, set(nodes))
    ignore_external = not not blockers
    if blockers:
        print("Resuming with incomplete external inputs:", ", ".join(sorted(blockers)))

    def prepare():
        for item in nodes:
            _prepare_node_for_resume(workflow, item)

    return run_nodes(
        workflow,
        nodes,
        node,
        ignore_external=ignore_external,
        command="resumefrom",
        stats=stats,
        stats_interval=stats_interval,
        monitor=monitor,
        monitor_interval=monitor_interval,
        prepare=prepare,
        require_start_queued=False,
    )


def run_nodes(
    workflow: MicroWorkflow,
    nodes: list[str],
    start_node: str,
    ignore_external: bool = False,
    *,
    command: str = "run",
    stats: bool = False,
    stats_interval: float = 5.0,
    monitor: bool = False,
    monitor_interval: float = 2.0,
    prepare: Callable[[], None] | None = None,
    require_start_queued: bool = True,
) -> int:
    run_set = set(nodes)
    previous_allowed_run_nodes = workflow.allowed_run_nodes
    previous_autostart_mode = workflow.autostart_mode
    previous_restart_enabled = workflow.active_job_restart_enabled

    workflow.allowed_run_nodes = run_set
    workflow.autostart_mode = "queue"
    workflow.active_job_restart_enabled = True

    try:
        with active_workflow_run(
            workflow,
            command=command,
            start_node=start_node,
            nodes=nodes,
            stats=stats,
            stats_interval=stats_interval,
            monitor=monitor,
            monitor_interval=monitor_interval,
        ) as finish_run:
            if prepare is not None:
                prepare()

            has_any_queued = any(workflow.storage.has_queued_jobs(item) for item in nodes)
            if not has_any_queued:
                if require_start_queued:
                    workflow.storage.set_node_status(start_node, QUEUED)
                    print(
                        f"No queued jobs for {start_node}. "
                        f"Create default jobs in node_behavior/{start_node}.py with "
                        "router.create_job(number=..., params={...})."
                    )
                else:
                    print("No failed, cancelled, stale-running, or queued jobs remain in the resume set.")
                finish_run("done")
                return 0

            if workflow.runner in {"threaded", "api", "process"}:
                ran = workflow.run_concurrently(
                    nodes=nodes,
                    ready_check=lambda item: ready_for_run_set(
                        workflow,
                        item,
                        run_set,
                        ignore_external,
                    ),
                )
            else:
                ran = []
                units = workflow.execution_components(nodes)

                while True:
                    ready_units = [
                        unit
                        for unit in units
                        if any(workflow.storage.has_queued_jobs(node) for node in unit)
                        and all(
                            ready_for_run_set(workflow, node, run_set, ignore_external)
                            for node in unit
                        )
                    ]

                    if not ready_units:
                        break

                    for unit in ready_units:
                        ran.extend(workflow.run_component(set(unit), ignore_readiness=True))

            workflow.finalize_ready_nodes()
            if ignore_external:
                # A partial runfrom may intentionally process one incoming branch
                # of a later component before its other predecessors run. Mark a
                # selected component complete for this branch when all jobs that
                # currently exist are successful and quiescent. Future producers
                # will queue new jobs and reactivate it.
                for unit in workflow.execution_components(nodes):
                    counts = [workflow.storage.job_status_counts(name) for name in unit]
                    total = sum(sum(item.values()) for item in counts)
                    failed = any(item.get(FAILED, 0) for item in counts)
                    active = any(item.get(RUNNING, 0) or item.get(QUEUED, 0) for item in counts)
                    successful = sum(item.get(DONE, 0) + item.get(SKIPPED, 0) for item in counts)
                    if total > 0 and successful == total and not failed and not active:
                        for name in unit:
                            workflow.storage.set_node_status(name, "done")

            blocked = [node for node in nodes if workflow.storage.has_queued_jobs(node)]

            if blocked:
                finish_run("blocked")
                print("Stopped before these queued nodes became ready:")
                for node in blocked:
                    status = workflow.storage.get_node_status(node) or "missing"
                    print(f"  {node}: {status}")
                return 1

            unfinished = [node for node in nodes if not workflow.node_complete(node)]

            if unfinished:
                finish_run("incomplete")
                print("These nodes did not complete:")
                for node in unfinished:
                    status = workflow.storage.get_node_status(node) or "missing"
                    job_count = len(workflow.storage.list_jobs(node))
                    queued_count = len(workflow.storage.queued_job_ids(node))
                    print(f"  {node}: {status}, jobs={job_count}, queued={queued_count}")
                print("This usually means an upstream task did not create the expected downstream jobs.")
                return 1

            finish_run("done")
            print("Ran:")
            for node in ran:
                print(f"  {node}")

            return 0

    finally:
        workflow.allowed_run_nodes = previous_allowed_run_nodes
        workflow.autostart_mode = previous_autostart_mode
        workflow.active_job_restart_enabled = previous_restart_enabled
