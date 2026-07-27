from __future__ import annotations

from pathlib import Path

from micro_workflow_manager.models import CANCELLED, FAILED, QUEUED, RUNNING
from micro_workflow_manager.system import MicroWorkflow

from .active_run import refuse_competing_run
from .cleanup import prepare_fresh_components
from .graph_utils import direct_incomplete_inputs
from .run_orchestration import run_nodes


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

def run_node(
    root: Path,
    workflow: MicroWorkflow,
    node: str,
    *,
    stats: bool = False,
    stats_interval: float = 5.0,
    monitor: bool = False,
    monitor_interval: float = 2.0,
    keep_trace: bool = False,
) -> int:
    refuse_competing_run(workflow)
    nodes = _component_notice(workflow, node)
    if _refuse_start_component_inputs(workflow, node, f"run {node}"):
        return 1

    component = set(nodes)

    def prepare():
        workflow.storage.set_node_status(node, RUNNING)
        removed = prepare_fresh_components(
            root,
            workflow,
            [component],
            keep_trace=keep_trace,
        )
        if removed:
            summary = ", ".join(f"{name}={count}" for name, count in sorted(removed.items()))
            print(f"Removed jobs produced by Hoeflein component {{{', '.join(nodes)}}}: {summary}")

    return run_nodes(
        workflow, nodes, node, ignore_external=False, command="run",
        stats=stats, stats_interval=stats_interval, monitor=monitor,
        monitor_interval=monitor_interval, prepare=prepare,
    )

def run_from(
    root: Path,
    workflow: MicroWorkflow,
    node: str,
    *,
    stats: bool = False,
    stats_interval: float = 5.0,
    monitor: bool = False,
    monitor_interval: float = 2.0,
    keep_trace: bool = False,
    refuse_after_node: str | None = None,
) -> int:
    refuse_competing_run(workflow)
    start_component = workflow.component_for(node)
    start_nodes = _component_notice(workflow, node)
    components = [start_component, *[set(item) for item in workflow.component_descendants(start_component)]]
    nodes = [name for component in components for name in workflow.component_key(component)]
    if refuse_after_node is not None:
        refuse_component = workflow.component_for(refuse_after_node)
        if workflow.component_id(refuse_component) not in {
            workflow.component_id(component) for component in components
        }:
            raise RuntimeError(
                f"refuseafter node {refuse_after_node!r} is not in the runfrom "
                f"selection starting at {node!r}"
            )
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
        removed = prepare_fresh_components(
            root,
            workflow,
            components,
            keep_trace=keep_trace,
        )
        if removed:
            summary = ", ".join(f"{name}={count}" for name, count in sorted(removed.items()))
            selected = "; ".join("{" + ", ".join(workflow.component_key(c)) + "}" for c in components)
            print(f"Removed jobs produced by selected Hoeflein components {selected}: {summary}")

    return run_nodes(
        workflow, nodes, node, ignore_external=True, command="runfrom",
        stats=stats, stats_interval=stats_interval, monitor=monitor,
        monitor_interval=monitor_interval, prepare=prepare,
        refuse_after_node=refuse_after_node,
    )

def _recover_finished_before_resume(
    workflow: MicroWorkflow,
    nodes: list[str],
) -> int:
    """Publish output-backed terminal jobs before deciding what to restart."""
    workflow.storage.db_mutation_barrier()
    recovered = workflow.storage.reconcile_terminal_outputs(nodes)
    workflow.storage.db_mutation_barrier()
    if recovered:
        print(f"Registered {recovered} finished job(s) before resume.")
    return recovered


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
    else:
        workflow.refresh_node_status(node, allow_complete=True)
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
    keep_trace: bool = False,
) -> int:
    refuse_competing_run(workflow)
    nodes = list(workflow.component_key(workflow.component_for(node)))
    _recover_finished_before_resume(workflow, nodes)

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
    keep_trace: bool = False,
) -> int:
    refuse_competing_run(workflow)
    start_component = workflow.component_for(node)
    components = [start_component, *[set(item) for item in workflow.component_descendants(start_component)]]
    nodes = [name for component in components for name in workflow.component_key(component)]
    _recover_finished_before_resume(workflow, nodes)
    if not keep_trace:
        start_nodes = set(workflow.component_key(start_component))
        workflow.storage.clear_job_events_for_nodes(
            [name for name in nodes if name not in start_nodes]
        )

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
