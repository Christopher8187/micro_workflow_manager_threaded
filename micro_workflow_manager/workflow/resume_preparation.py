"""Observe an entire resume selection before it changes preserved work."""

from dataclasses import dataclass, replace
from shlex import join

from ..component_readiness import calculate_component_readiness, calculate_sampled_resume_lineage
from ..component_readiness import calculate_interrupt_sampled_resume_readiness
from ..errors import InvalidGraphError
from ..storage.resume_preparation import prepare_resume_components, validate_resume_job_owners
from ..storage.unproduced_membership import read_execution_component_states
from .graph_command_selection import select_graph_command


@dataclass(frozen=True)
class ResumeSelection:
    shape: str
    components: tuple[tuple[str, ...], ...]
    states: dict
    parents: dict
    successful_results: dict
    command: str
    start_node: str
    end_node: str | None = None
    interrupt_start_origin: str | None = None


def predict_resume_lineages(
    components, all_parents, observations, successful_results, *,
    interrupt_component=None, interrupt_start_origin=None,
):
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
        if component == interrupt_component:
            readiness = calculate_component_readiness(
                (('queued', None, None) if item is None else item for item in inputs),
                interrupt_start_origin=interrupt_start_origin,
            )
        else:
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
              and successful_results[component].lifecycle == 'sampled'):
            retained = successful_results[component].lineage
        if retained is not None:
            if component == interrupt_component:
                retained_readiness = calculate_interrupt_sampled_resume_readiness(
                    retained, (('queued', None, None) if item is None else item for item in inputs),
                    interrupt_start_origin=interrupt_start_origin,
                )
                lineage = None if retained_readiness is None else retained_readiness[:2]
            else:
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


def observe_resume_selection(
    workflow, nodes, *, command='resume', start_node=None, end_node=None,
    interrupt_start_origin=None,
):
    start_node = nodes[0] if start_node is None else start_node
    with workflow.lock:
        shape = workflow.topology.graph_shape()
        selection = select_graph_command(workflow.topology, command, start_node, end_node)
        if tuple(nodes) != selection.nodes:
            raise RuntimeError('Resume nodes differ from the graph-command selection')
        components = selection.components
        selected = set(components)
        all_parents = {
            component: workflow.component_predecessor_components(set(component))
            for component in components
        }
        parents = {component: all_parents[component] - selected for component in components}
        descendants = {component: set(workflow.component_descendants(set(component))) for component in components}
    external = sorted({parent for group in parents.values() for parent in group})
    observations = read_execution_component_states(
        workflow.storage.db_connection(), [*components, *external],
        expected_shape=shape, allow_missing=True,
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
            repairs = (['mwf', 'resetbetween', component[0], end_node]
                       if command == 'resumebetween' else ['mwf', 'resetfrom', component[0]]
                       for component in roots)
            guidance = '\n'.join('Run ' + join(repair) for repair in repairs)
            retry = ['mwf', command, start_node]
            if end_node is not None:
                retry.append(end_node)
            guidance += '\nThen retry ' + join(retry)
        raise RuntimeError(f'Cannot {command}: misaligned components {misaligned}.\n{guidance}')
    for component in components:
        state = observations[component]
        if state['lifecycle'] == 'running':
            raise RuntimeError(f'Resume requires recovery of running component {component}')
    states = {key: observations[key] for key in components}
    successful_results = validate_resume_job_owners(workflow.storage, states, expected_shape=shape)
    predict_resume_lineages(
        components, all_parents, observations, successful_results,
        interrupt_component=selection.start_component if interrupt_start_origin is not None else None,
        interrupt_start_origin=interrupt_start_origin,
    )
    return ResumeSelection(shape, components, states,
                           {key: observations[key] for key in external}, successful_results,
                           command, start_node, end_node, interrupt_start_origin)


def prepare_admitted_resume(workflow, nodes, selection, *, clear_trace_nodes=(), blocked_components=()):
    context = workflow.execution_session_context
    if (context is None or context[2] != selection.shape
            or tuple(dict.fromkeys(context[1].values())) != selection.components):
        raise RuntimeError('Resume selection changed during admission')
    current = observe_resume_selection(
        workflow, nodes, command=selection.command, start_node=selection.start_node,
        end_node=selection.end_node, interrupt_start_origin=selection.interrupt_start_origin,
    )
    if selection.interrupt_start_origin is not None:
        admission = workflow.storage.get_interrupt_execution_admission(context[0])
        if (context[0] != selection.interrupt_start_origin or admission is None
                or admission['state'] != 'frozen'
                or selection.start_node not in admission['target_component']):
            raise RuntimeError('Interrupt resume lost its exact frozen admission')
        parents = dict(selection.parents)
        for parent, frozen in admission['frozen_parent_states'].items():
            if parent not in parents or current.parents[parent] != frozen:
                raise RuntimeError('Interrupt resume parent changed after its frozen observation')
            parents[parent] = frozen
        selection = replace(selection, parents=parents)
    if current != selection:
        raise RuntimeError('Resume selection changed during admission')
    blocked = frozenset(blocked_components)
    if not blocked <= set(selection.components):
        raise ValueError('Stopped resume components lie outside the admitted selection')
    states = {component: state for component, state in selection.states.items() if component not in blocked}
    if not states:
        return 0
    prepared_nodes = {node for component in states for node in component}
    return prepare_resume_components(
        workflow.storage, context[0], states,
        expected_admitted_shape=selection.shape,
        expected_parents={**selection.parents, **{
            component: selection.states[component] for component in blocked
        }},
        expected_successful_results={component: selection.successful_results[component] for component in states},
        clear_trace_nodes=tuple(node for node in clear_trace_nodes if node in prepared_nodes),
    )
