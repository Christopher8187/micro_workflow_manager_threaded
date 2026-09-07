"""Observe an entire resume selection before it changes preserved work."""

from dataclasses import dataclass
from shlex import join

from ..component_readiness import calculate_component_readiness
from ..errors import InvalidGraphError
from ..storage.resume_preparation import prepare_resume_components, validate_resume_job_owners


@dataclass(frozen=True)
class ResumeSelection:
    shape: str
    components: tuple[tuple[str, ...], ...]
    states: dict
    parents: dict
    command: str
    start_node: str


def observe_resume_selection(workflow, nodes, *, command='resume', start_node=None):
    start_node = nodes[0] if start_node is None else start_node
    with workflow.lock:
        shape = workflow.topology.graph_shape()
        components = tuple(workflow.execution_components(nodes))
        selected = set(components)
        parents = {component: workflow.component_predecessor_components(set(component)) - selected
                   for component in components}
        descendants = {component: set(workflow.component_descendants(set(component))) for component in components}
    external = sorted({parent for group in parents.values() for parent in group})
    observations = workflow.storage.read_component_states(
        [*components, *external], expected_shape=shape, allow_missing=True,
    )
    for component in components:
        state = observations[component]
        if state is None:
            raise RuntimeError(f'Resume requires initialized component {component}')
    misaligned = [component for component in components if observations[component]['misaligned']]
    if misaligned:
        if command == 'resume':
            guidance = 'Run ' + join(['mwf', 'run', start_node])
        else:
            roots = [component for component in misaligned
                     if not any(component in descendants[parent] for parent in misaligned)]
            guidance = '\n'.join('Run ' + join(['mwf', 'resetfrom', component[0]]) for component in roots)
            guidance += '\nThen retry ' + join(['mwf', command, start_node])
        raise RuntimeError(f'Cannot {command}: misaligned components {misaligned}.\n{guidance}')
    for component in components:
        state = observations[component]
        if state['lifecycle'] == 'running':
            raise RuntimeError(f'Resume requires recovery of running component {component}')
        inputs = [observations[parent] for parent in parents[component]]
        if any(state is None for state in inputs) or calculate_component_readiness(
            (state['lifecycle'], state['stability'], state['instability_origin']) for state in inputs
        ) is None:
            raise InvalidGraphError(
                f'Cannot resume component {list(component)}: incomplete or incompatible external '
                f'parent components {sorted(parents[component])}'
            )
    states = {key: observations[key] for key in components}
    validate_resume_job_owners(workflow.storage, states)
    return ResumeSelection(shape, components, states,
                           {key: observations[key] for key in external}, command, start_node)


def prepare_admitted_resume(workflow, nodes, selection, *, clear_trace_nodes=()):
    context = workflow.execution_session_context
    if (context is None or context[2] != selection.shape
            or set(context[1].values()) != set(selection.components)
            or observe_resume_selection(workflow, nodes, command=selection.command,
                                        start_node=selection.start_node) != selection):
        raise RuntimeError('Resume selection changed during admission')
    return prepare_resume_components(
        workflow.storage, context[0], selection.states,
        expected_parents=selection.parents, clear_trace_nodes=clear_trace_nodes,
    )
