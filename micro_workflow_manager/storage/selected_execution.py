from __future__ import annotations

from micro_workflow_manager.component_identity import encode_component_key


def read_selected_execution_jobs(storage, context, roots, expected_identity, *, connection=None):
    """Read the current invocation's exact roots and causal jobs in one snapshot."""
    session_id, ownership, shape = context
    component = ownership[roots[0][0]]
    connection = storage.db_connection() if connection is None else connection
    connection.execute('SAVEPOINT mwf_selected_execution_jobs')
    try:
        storage._require_selected_preparation_roots(connection, session_id, component, roots)
        session = connection.execute(
            'SELECT status FROM execution_sessions WHERE session_id=?', (session_id,),
        ).fetchone()
        reservation = connection.execute(
            'SELECT session_id FROM component_reservations WHERE component_key=?',
            (encode_component_key(component),),
        ).fetchone()
        state = storage._read_component_state(connection, component)
        if (session is None or session['status'] != 'running' or reservation is None
                or reservation['session_id'] != session_id or state is None or state['shape_json'] != shape):
            raise RuntimeError('Selected execution no longer owns its captured component')
        identity = storage._read_component_producing_identity(connection, component)
        if identity != expected_identity:
            raise RuntimeError('Selected execution lost its captured component alignment')
        root_set = set(roots)
        result = []
        for node in component:
            for row in connection.execute(
                'SELECT job_id, status, instance_id, created_by_execution_id '
                'FROM jobs JOIN job_instances USING(node_name, job_id) WHERE node_name=? ORDER BY job_id',
                (node,),
            ):
                if ((node, row['job_id'], row['instance_id']) in root_set
                        or storage._job_descends_from_selected_root(
                            connection, row['created_by_execution_id'], root_set, session_id, component, identity,
                        )):
                    result.append((node, row['job_id'], row['status']))
        return tuple(result)
    finally:
        connection.execute('RELEASE SAVEPOINT mwf_selected_execution_jobs')
