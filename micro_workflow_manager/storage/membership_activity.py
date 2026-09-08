"""Refuse membership changes that overlap admitted or active work."""

from micro_workflow_manager.component_identity import decode_component_key


def require_idle_membership_region(connection, components):
    from .execution_sessions import read_execution_sessions_snapshot

    nodes = {node for component in components for node in component}
    for session in read_execution_sessions_snapshot(connection, running_only=True):
        if any(nodes.intersection(component) for component in session['selected_components']):
            raise RuntimeError(
                'Membership changes overlap admitted session ' + session['session_id']
                + ': ' + ', '.join(sorted(nodes))
            )
    for row in connection.execute(
        'SELECT component_key FROM component_reservations UNION '
        'SELECT component_key FROM component_holds UNION '
        'SELECT component_key FROM pending_component_executions',
    ):
        if nodes.intersection(decode_component_key(row['component_key'])):
            raise RuntimeError('Membership changes overlap reserved, held, or pending work')
    for node in sorted(nodes):
        if connection.execute(
            'SELECT 1 FROM receiver_mutation_guards WHERE receiver_node=? UNION ALL '
            "SELECT 1 FROM jobs WHERE node_name=? AND (status='running' OR active_execution_id IS NOT NULL "
            'OR active_pid IS NOT NULL OR active_thread_id IS NOT NULL OR active_started_at IS NOT NULL)',
            (node, node),
        ).fetchone() is not None:
            raise RuntimeError('Membership changes conflict with active receiver work: ' + node)
