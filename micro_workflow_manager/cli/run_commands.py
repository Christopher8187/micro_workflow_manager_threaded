from __future__ import annotations

from pathlib import Path

from micro_workflow_manager.component_readiness import calculate_component_readiness
from micro_workflow_manager.system import MicroWorkflow
from micro_workflow_manager.workflow.resume_preparation import observe_resume_selection, prepare_admitted_resume

from .active_run import refuse_competing_run
from .cleanup import prepare_fresh_components
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
    with workflow.lock:
        component = workflow.component_for(node)
        parents = sorted(workflow.component_predecessor_components(component))
        expected_shape = workflow.topology.graph_shape()
    observed = workflow.storage.read_component_states(parents, expected_shape=expected_shape, allow_missing=True)
    blockers = [parent for parent, state in observed.items()
                if state is None or state['lifecycle'] != 'done']
    reason = 'incomplete predecessor components'
    if not blockers:
        readiness = calculate_component_readiness(
            (state['lifecycle'], state['stability'], state['instability_origin'])
            for state in observed.values()
        )
        if readiness is not None:
            return False
        blockers = parents
        reason = 'incompatible predecessor component results'
    labels = {parent: parent[0] if len(parent) == 1 else '{' + ', '.join(parent) + '}'
              for parent in blockers}
    print(f"Cannot {command}: {reason}: {', '.join(labels.values())}")
    print("Hoeflein components are scheduled on the quotient DAG; run or resume those predecessors first.")
    for parent in blockers:
        state = observed[parent]
        status = 'not initialized' if state is None else state['lifecycle']
        print(f"  {labels[parent]}: {status}")
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
        workflow, nodes, node, command="run",
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
    refuse_before_node: str | None = None,
) -> int:
    refuse_competing_run(workflow)
    start_component = workflow.component_for(node)
    start_nodes = _component_notice(workflow, node)
    components = [start_component, *[set(item) for item in workflow.component_descendants(start_component)]]
    nodes = [name for component in components for name in workflow.component_key(component)]
    refusal_node = refuse_before_node or refuse_after_node
    if refusal_node is not None:
        refusal_mode = "refuse" if refuse_before_node is not None else "refuseafter"
        refuse_component = workflow.component_for(refusal_node)
        if workflow.component_id(refuse_component) not in {
            workflow.component_id(component) for component in components
        }:
            raise RuntimeError(
                f"{refusal_mode} node {refusal_node!r} is not in the runfrom "
                f"selection starting at {node!r}"
            )
    if _refuse_start_component_inputs(workflow, node, f"runfrom {node}"):
        return 1

    def prepare():
        removed = prepare_fresh_components(
            root,
            workflow,
            components,
            keep_trace=keep_trace,
            operation='runfrom',
        )
        if removed:
            summary = ", ".join(f"{name}={count}" for name, count in sorted(removed.items()))
            selected = "; ".join("{" + ", ".join(workflow.component_key(c)) + "}" for c in components)
            print(f"Removed jobs produced by selected Hoeflein components {selected}: {summary}")

    return run_nodes(
        workflow, nodes, node, command="runfrom",
        stats=stats, stats_interval=stats_interval, monitor=monitor,
        monitor_interval=monitor_interval, prepare=prepare,
        refuse_after_node=refuse_after_node,
        refuse_before_node=refuse_before_node,
    )

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
    selection = observe_resume_selection(workflow, nodes, command='resume', start_node=node)

    def prepare():
        prepare_admitted_resume(workflow, nodes, selection)

    return run_nodes(
        workflow,
        nodes,
        node,
        command="resume",
        stats=stats,
        stats_interval=stats_interval,
        monitor=monitor,
        monitor_interval=monitor_interval,
        prepare=prepare,
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
    refuse_after_node: str | None = None,
    refuse_before_node: str | None = None,
) -> int:
    refuse_competing_run(workflow)
    start_component = workflow.component_for(node)
    components = [start_component, *[set(item) for item in workflow.component_descendants(start_component)]]
    nodes = [name for component in components for name in workflow.component_key(component)]
    refusal_node = refuse_before_node or refuse_after_node
    if refusal_node is not None:
        refusal_mode = "refuse" if refuse_before_node is not None else "refuseafter"
        refuse_component = workflow.component_for(refusal_node)
        if workflow.component_id(refuse_component) not in {
            workflow.component_id(component) for component in components
        }:
            raise RuntimeError(
                f"{refusal_mode} node {refusal_node!r} is not in the resumefrom "
                f"selection starting at {node!r}"
            )
    selection = observe_resume_selection(workflow, nodes, command='resumefrom', start_node=node)
    start_nodes = set(workflow.component_key(start_component))
    clear_trace_nodes = () if keep_trace else tuple(name for name in nodes if name not in start_nodes)

    def prepare():
        prepare_admitted_resume(workflow, nodes, selection, clear_trace_nodes=clear_trace_nodes)

    return run_nodes(
        workflow,
        nodes,
        node,
        command="resumefrom",
        stats=stats,
        stats_interval=stats_interval,
        monitor=monitor,
        monitor_interval=monitor_interval,
        prepare=prepare,
        refuse_after_node=refuse_after_node,
        refuse_before_node=refuse_before_node,
    )
