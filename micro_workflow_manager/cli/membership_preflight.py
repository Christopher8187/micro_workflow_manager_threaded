"""Check membership using a native snapshot before project initialization."""

from micro_workflow_manager.storage.membership_observation import (
    read_membership_change, require_reusable_membership_match,
)
from micro_workflow_manager.workflow.graph_command_selection import graph_command_spec, select_graph_command

from micro_workflow_manager.storage.membership_activity import require_idle_membership_region

from .files import safe_node_name
from .preview import load_preview


def validate_command_membership(root, args):
    spec = graph_command_spec(args.command)
    if spec is None:
        return
    workflow = load_preview(root)
    try:
        node = args.node if args.node == '*' else safe_node_name(args.node)
        end = getattr(args, 'end_node', None)
        if end is not None:
            end = safe_node_name(end)
        selection = select_graph_command(workflow.topology, args.command, node, end)
        change = read_membership_change(
            workflow.storage.connection, workflow.topology.snapshot(), selection.components,
        )
        fresh = selection.operation in ('run', 'reset') and getattr(args, 'job_mode', None) is None
        if not fresh:
            require_reusable_membership_match(change)
        if change is not None and change.changes_active_membership:
            require_idle_membership_region(
                workflow.storage.connection, (*change.source_components, *change.target_components),
            )
    finally:
        workflow.storage.close()
