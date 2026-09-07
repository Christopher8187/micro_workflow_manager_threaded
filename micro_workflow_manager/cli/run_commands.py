from __future__ import annotations

from pathlib import Path

from micro_workflow_manager.component_readiness import calculate_component_readiness
from micro_workflow_manager.system import MicroWorkflow
from micro_workflow_manager.workflow.resume_preparation import observe_resume_selection, prepare_admitted_resume

from micro_workflow_manager.workflow.execution_session import refuse_competing_run
from micro_workflow_manager.workflow.graph_command_selection import select_graph_command
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

def _read_start_component_inputs(workflow: MicroWorkflow, node: str):
    with workflow.lock:
        component = workflow.component_for(node)
        parents = sorted(workflow.component_predecessor_components(component))
        shape = workflow.topology.graph_shape()
    return workflow.storage.read_component_states(parents, expected_shape=shape, allow_missing=True)


def _parent_result_observations(states):
    fields = ('lifecycle', 'stability', 'instability_origin', 'alignment_generation')
    return {component: None if state is None else tuple(state[field] for field in fields)
            for component, state in states.items()}


def _refuse_start_component_inputs(workflow: MicroWorkflow, node: str, command: str, *, observed=None) -> bool:
    observed = _read_start_component_inputs(workflow, node) if observed is None else observed
    parents = sorted(observed)
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

def execute_graph_command(
    root: Path, workflow: MicroWorkflow, node: str, *, command: str,
    end_node: str | None = None, stats: bool = False, stats_interval: float = 5.0,
    monitor: bool = False, monitor_interval: float = 2.0, keep_trace: bool = False,
    refuse_after_node: str | None = None, refuse_before_node: str | None = None,
) -> int:
    refuse_competing_run(workflow)
    with workflow.lock:
        selection = select_graph_command(workflow.topology, command, node, end_node)
        shape = workflow.topology.graph_shape()
    if selection.operation not in ('run', 'resume'):
        raise ValueError('Execution requires a run or resume selection')
    nodes = list(selection.nodes)
    if refuse_before_node is not None and refuse_after_node is not None:
        raise ValueError('refuse and refuseafter are mutually exclusive')
    refusal_node = refuse_before_node or refuse_after_node
    if refusal_node is not None:
        mode = 'refuse' if refuse_before_node is not None else 'refuseafter'
        if workflow.component_id(refusal_node) not in selection.components:
            raise RuntimeError(
                f'{mode} node {refusal_node!r} is not in the {command} selection starting at {node!r}'
            )
    resumed = None
    parent_results = None
    if selection.operation == 'run':
        _component_notice(workflow, node)
        parents = _read_start_component_inputs(workflow, node)
        if _refuse_start_component_inputs(workflow, node, f'{command} {node}', observed=parents):
            return 1
        parent_results = _parent_result_observations(parents)
    else:
        options = {} if end_node is None else {'end_node': end_node}
        resumed = observe_resume_selection(workflow, nodes, command=command, start_node=node, **options)
    clear_trace = () if keep_trace or selection.scope == 'one' else tuple(
        name for name in nodes if name not in selection.start_component
    )

    def prepare():
        context = workflow.execution_session_context
        with workflow.lock:
            current = select_graph_command(workflow.topology, command, node, end_node)
        if (current != selection or context is None or context[2] != shape
                or tuple(dict.fromkeys(context[1].values())) != selection.components):
            raise RuntimeError('Graph command selection changed during admission')
        if resumed is not None:
            prepare_admitted_resume(workflow, nodes, resumed, clear_trace_nodes=clear_trace)
            return
        current_parents = _read_start_component_inputs(workflow, node)
        if _parent_result_observations(current_parents) != parent_results:
            raise RuntimeError('Start-component parent results changed during admission')
        removed = prepare_fresh_components(
            root, workflow, [set(component) for component in selection.components],
            keep_trace=keep_trace, operation=command,
        )
        if removed:
            summary = ', '.join(f'{name}={count}' for name, count in sorted(removed.items()))
            selected = '; '.join('{' + ', '.join(component) + '}' for component in selection.components)
            print(f'Removed jobs produced by selected Hoeflein components {selected}: {summary}')

    return run_nodes(
        workflow, nodes, node, command=command, components=selection.components,
        stats=stats, stats_interval=stats_interval,
        monitor=monitor, monitor_interval=monitor_interval, prepare=prepare,
        refuse_after_node=refuse_after_node, refuse_before_node=refuse_before_node,
    )


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
    return execute_graph_command(
        root, workflow, node, command='run',
        stats=stats,
        stats_interval=stats_interval,
        monitor=monitor,
        monitor_interval=monitor_interval,
        keep_trace=keep_trace,
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
    return execute_graph_command(
        root, workflow, node, command='runfrom',
        stats=stats,
        stats_interval=stats_interval,
        monitor=monitor,
        monitor_interval=monitor_interval,
        keep_trace=keep_trace,
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
    return execute_graph_command(
        root, workflow, node, command='resume',
        stats=stats,
        stats_interval=stats_interval,
        monitor=monitor,
        monitor_interval=monitor_interval,
        keep_trace=keep_trace,
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
    return execute_graph_command(
        root, workflow, node, command='resumefrom',
        stats=stats,
        stats_interval=stats_interval,
        monitor=monitor,
        monitor_interval=monitor_interval,
        keep_trace=keep_trace,
        refuse_after_node=refuse_after_node,
        refuse_before_node=refuse_before_node,
    )


def run_between(root: Path, workflow: MicroWorkflow, node: str, end_node: str, **options) -> int:
    return execute_graph_command(root, workflow, node, command='runbetween', end_node=end_node, **options)


def resume_between(root: Path, workflow: MicroWorkflow, node: str, end_node: str, **options) -> int:
    return execute_graph_command(root, workflow, node, command='resumebetween', end_node=end_node, **options)
