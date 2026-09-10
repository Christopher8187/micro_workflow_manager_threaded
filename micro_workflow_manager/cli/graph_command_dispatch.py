"""Select and observe full graph commands before runtime initialization."""

from shlex import join

from micro_workflow_manager.storage.native_planning_reader import NativePlanningReader
from micro_workflow_manager.storage.planning_observation import observe_graph_command
from micro_workflow_manager.workflow.graph_command_selection import graph_command_spec, select_graph_command

from .files import safe_node_name
from .validation import require_node
from .graph_command_plan import render_graph_command_plan


def graph_preview_requested(args):
    spec = graph_command_spec(args.command)
    return bool(
        spec is not None and getattr(args, spec.preview_attribute, False)
        and getattr(args, 'job_mode', None) is None and not getattr(args, 'job_specs', None)
    )


def print_graph_preview(root, workflow, args, *, interrupt_preflight=None):
    node = args.node if args.node == "*" else safe_node_name(args.node)
    end_node = getattr(args, 'end_node', None)
    if end_node is not None:
        end_node = safe_node_name(end_node)
    selection = select_graph_command(workflow.topology, args.command, node, end_node)
    refuse_mode, refuse_node = getattr(args, 'refuse_mode', None), getattr(args, 'refuse_node', None)
    if (refuse_mode is None) != (refuse_node is None):
        raise ValueError('A refusal modifier requires its boundary node')
    if refuse_node is not None:
        refuse_node = safe_node_name(refuse_node)
        require_node(workflow, refuse_node)
        boundary = workflow.component_id(refuse_node)
        if boundary not in selection.components:
            raise RuntimeError(
                f'{refuse_mode} node {refuse_node!r} is not in the {args.command} selection starting at {node!r}'
            )
    blocked_components = () if interrupt_preflight is None else interrupt_preflight.blocked_components
    reader = NativePlanningReader(workflow.storage.connection, root, workflow.topology.graph_shape())
    observation = observe_graph_command(
        reader, workflow.topology, selection, keep_trace=bool(getattr(args, 'keeptrace', False)),
        static_receivers_by_node=workflow.static_node_targets, blocked_components=blocked_components,
        interrupt_start_component=(None if interrupt_preflight is None
                                   else interrupt_preflight.explicit_start_component),
    )
    invocation = [args.command, node]
    if end_node is not None:
        invocation.append(end_node)
    print(render_graph_command_plan(
        observation, command_line=join(invocation), blocked_components=blocked_components,
    ))
    if refuse_node is not None:
        label = '{' + ', '.join(boundary) + '}'
        if selection.operation == 'reset':
            print('  reset scope: unchanged; the full selected region is freshened without execution')
        elif refuse_mode == 'refuse':
            print(f'  refusal boundary: stop before admitting {label}; the boundary component does not run')
        else:
            print(f'  refusal boundary: stop admitting new components after {label} terminates')
        if selection.operation == 'run':
            print('  reset scope: unchanged; every selected component is still freshened')
        elif selection.operation == 'resume':
            print('  resume scope: unchanged; later selected work remains queued for a future resume')
    if selection.operation == 'resume':
        trace = ('preserve all affected trace journals'
                 if getattr(args, 'keeptrace', False) else
                 'preserve the current component trace journal' if selection.scope == 'one' else
                 'preserve the start component trace; clear descendant traces')
        print('  trace mode: ' + trace)
    else:
        trace = ('preserve all affected trace journals' if getattr(args, 'keeptrace', False)
                 else 'clear affected trace journals during fresh preparation')
        print('  trace mode: ' + trace)
    return 0


def refuse_reset_running_sessions(root):
    from micro_workflow_manager.session_liveness import execution_session_liveness
    from micro_workflow_manager.storage.execution_sessions import read_execution_sessions_snapshot
    from .preview import load_preview

    workflow = load_preview(root)
    try:
        sessions = read_execution_sessions_snapshot(workflow.storage.connection, running_only=True)
        if sessions:
            details = []
            for session in sessions:
                condition = 'live' if execution_session_liveness(session)['live'] else 'requires recovery'
                details.append(f"{session['session_kind']} {session['session_id']} ({condition})")
            raise RuntimeError('Reset refused while execution sessions remain running: ' + ', '.join(details))
    finally:
        workflow.storage.close()
