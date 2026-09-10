"""Publish already-decided component outcomes and end their owning session."""

import json

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key

from .session_scope import require_admitted_reservation_scope


def settle_execution_session(storage, connection, session_id, *, outcome, finished_at,
                             failure_data, component_outcomes, failed_nodes=()):
    from .execution_sessions import ExecutionSessionHasActiveJobs

    require_admitted_reservation_scope(connection, session_id)
    outcomes = storage._normalize_component_terminal_outcomes(component_outcomes)
    boundary_details = None
    if outcome == 'stopped':
        boundaries = storage._read_interrupt_session_boundaries(connection, session_id)
        if boundaries:
            current = connection.execute(
                'SELECT details_json FROM execution_sessions WHERE session_id=? AND status=\'running\'',
                (session_id,),
            ).fetchone()
            if current is None:
                raise RuntimeError('Stopped session lost its running details')
            try:
                boundary_details = json.loads(current['details_json'])
            except (TypeError, json.JSONDecodeError) as error:
                raise RuntimeError('Stopped session has damaged details') from error
            if not isinstance(boundary_details, dict):
                raise RuntimeError('Stopped session has damaged details')
            boundary_details['post_interrupt_boundaries'] = [{
                **item, 'component': list(item['component']),
            } for item in boundaries]
    storage._validate_component_terminal_outcomes(connection, session_id, outcomes)
    supplied_keys = {encode_component_key(result.component) for result in outcomes}
    running_keys = {row[0] for row in connection.execute(
        'SELECT state.component_key FROM component_states AS state '
        'JOIN component_reservations AS reservation USING(component_key) '
        "WHERE reservation.session_id=? AND state.lifecycle='running'", (session_id,),
    )}
    if running_keys - supplied_keys:
        raise RuntimeError('Terminal settlement omitted an owned running component')
    reserved = tuple(row[0] for row in connection.execute(
        'SELECT component_key FROM component_reservations WHERE session_id=?', (session_id,),
    ))
    owned_nodes = {node for key in reserved for node in decode_component_key(key)}
    for node in sorted(owned_nodes):
        active = connection.execute(
            "SELECT job_id FROM jobs WHERE node_name=? AND (status='running' OR active_execution_id IS NOT NULL) LIMIT 1",
            (node,),
        ).fetchone()
        if active is not None:
            raise ExecutionSessionHasActiveJobs(f'Session settlement still has active job {node}/{active[0]}')
    # Normal execution releases each permit before terminal job publication.
    # Abandoned-session recovery may finalize the jobs in this same writer and
    # then clear their retained capacity here. Validate immutable owners before
    # any component/session settlement and never touch another session's rows.
    storage._clear_api_execution_permits_for_session(connection, session_id)
    storage._publish_component_terminal_outcomes(connection, session_id, outcomes)
    transfer_sources = tuple(row[0] for row in connection.execute(
        "SELECT DISTINCT source_session_id FROM interrupt_scope_transfers "
        "WHERE child_session_id=? AND state='active' ORDER BY source_session_id",
        (session_id,),
    ))
    interrupt_scope = storage._finish_interrupt_session_scope(
        connection, session_id, finished_at=finished_at,
    )
    for source_session_id in transfer_sources:
        source = connection.execute(
            "SELECT status FROM execution_sessions WHERE session_id=?", (source_session_id,),
        ).fetchone()
        if source is not None and source['status'] == 'running':
            require_admitted_reservation_scope(connection, source_session_id)
    if failed_nodes:
        if not set(failed_nodes) <= owned_nodes:
            raise RuntimeError('Failed nodes are outside the session reserved scope')
        connection.executemany(
            "UPDATE nodes SET status='failed', updated_at=CURRENT_TIMESTAMP WHERE node_name=?",
            [(node,) for node in failed_nodes],
        )
    if boundary_details is None:
        finished = connection.execute(
            "UPDATE execution_sessions SET status='terminal', outcome=?, finished_at=?, failures_json=? "
            "WHERE session_id=? AND status='running'", (outcome, finished_at, failure_data, session_id),
        ).rowcount
    else:
        finished = connection.execute(
            "UPDATE execution_sessions SET status='terminal', outcome=?, finished_at=?, "
            "failures_json=?, details_json=? WHERE session_id=? AND status='running'",
            (outcome, finished_at, failure_data, json.dumps(boundary_details), session_id),
        ).rowcount
    if finished != 1:
        raise RuntimeError('Execution session changed before terminal settlement')
    released = connection.execute(
        'DELETE FROM component_reservations WHERE session_id=?', (session_id,),
    ).rowcount
    interrupt_released = interrupt_scope['released']
    transferred = 0 if interrupt_released is None else (
        interrupt_scope['returned'] + interrupt_released
    )
    if released + transferred != len(reserved):
        raise RuntimeError('Execution session reservations changed before terminal settlement')
    storage._clear_thread_overrides_for_session(connection, session_id)
    if interrupt_released is not None:
        storage._read_interrupt_admission(connection, session_id)
        storage._require_terminal_interrupt_scope(connection, session_id)
    result = {
        'restarts': {},
        'released': released + (0 if interrupt_released is None else interrupt_released),
    }
    if interrupt_released is not None:
        result['returned'] = interrupt_scope['returned']
    return result
