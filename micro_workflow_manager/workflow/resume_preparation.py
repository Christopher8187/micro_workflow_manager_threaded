"""Observe an entire resume selection before it changes preserved work."""

from dataclasses import dataclass
from shlex import join

from ..component_readiness import calculate_component_readiness, calculate_sampled_resume_lineage
from ..errors import InvalidGraphError
from ..storage.resume_preparation import prepare_resume_components, validate_resume_job_owners


@dataclass(frozen=True)
class ResumeSelection:
    shape: str
    components: tuple[tuple[str, ...], ...]
    states: dict
    parents: dict
    successful_results: dict
    command: str
    start_node: str


def _predict_resume_lineages(components, all_parents, observations, successful_results):
    predicted = {}
    for component in components:
        state = observations[component]
        inputs = []
        for parent in sorted(all_parents[component]):
            if parent in predicted:
                inputs.append(('done', *predicted[parent]))
            elif parent in observations and parent not in components:
                parent_state = observations[parent]
                if parent_state is None:
                    inputs.append(None)
                else:
                    inputs.append(tuple(
                        parent_state[name] for name in ('lifecycle', 'stability', 'instability_origin')
                    ))
            else:
                raise RuntimeError('Resume components are not in quotient-DAG order')
        readiness = None if any(item is None for item in inputs) else calculate_component_readiness(inputs)
        if readiness is None:
            raise InvalidGraphError(
                f'Cannot resume component {list(component)}: incomplete or incompatible parent '
                f'components {sorted(all_parents[component])}'
            )
        retained = None
        if state['lifecycle'] == 'sampled':
            retained = state['stability'], state['instability_origin']
        elif (state['lifecycle'] == 'failed'
              and successful_results[component] is not None
              and successful_results[component][0] == 'sampled'):
            retained = successful_results[component][1:]
        if retained is not None:
            lineage = calculate_sampled_resume_lineage(*retained, readiness)
            if lineage is None:
                raise InvalidGraphError(
                    f'Cannot resume component {list(component)}: retained sampled result is '
                    'incompatible with its predicted parent result'
                )
        elif state['lifecycle'] == 'done':
            lineage = state['stability'], state['instability_origin']
        else:
            lineage = readiness[:2]
        predicted[component] = lineage
    return predicted


def observe_resume_selection(workflow, nodes, *, command='resume', start_node=None):
    start_node = nodes[0] if start_node is None else start_node
    with workflow.lock:
        shape = workflow.topology.graph_shape()
        components = tuple(workflow.execution_components(nodes))
        selected = set(components)
        all_parents = {
            component: workflow.component_predecessor_components(set(component))
            for component in components
        }
        parents = {component: all_parents[component] - selected for component in components}
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
    states = {key: observations[key] for key in components}
    successful_results = validate_resume_job_owners(workflow.storage, states)
    _predict_resume_lineages(components, all_parents, observations, successful_results)
    return ResumeSelection(shape, components, states,
                           {key: observations[key] for key in external}, successful_results,
                           command, start_node)


def prepare_admitted_resume(workflow, nodes, selection, *, clear_trace_nodes=()):
    context = workflow.execution_session_context
    if (context is None or context[2] != selection.shape
            or set(context[1].values()) != set(selection.components)
            or observe_resume_selection(workflow, nodes, command=selection.command,
                                        start_node=selection.start_node) != selection):
        raise RuntimeError('Resume selection changed during admission')
    return prepare_resume_components(
        workflow.storage, context[0], selection.states,
        expected_parents=selection.parents,
        expected_successful_results=selection.successful_results,
        clear_trace_nodes=clear_trace_nodes,
    )
