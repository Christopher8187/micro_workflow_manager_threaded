"""Observe abandoned native sessions from a fixed database view."""

from __future__ import annotations

from pathlib import Path
import json

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key
from micro_workflow_manager.session_liveness import execution_session_liveness

from .component_states import ComponentStateStorageMixin, ComponentTerminalOutcome
from .execution_ownership import JobExecutionOwnerStorageMixin
from .execution_sessions import ExecutionSessionStorageMixin, validate_execution_session_snapshot
from .component_result_identity import read_component_state_record
from .recovery_output import observe_recovery_file, recovery_terminal_output
from .session_scope import require_admitted_reservation_scope


def _terminal_output(root, node, job_id, observed):
    captured = observe_recovery_file(root, f'node/{node}/jobs/{job_id}/output.json')
    output = recovery_terminal_output(captured, observed['generation'], observed['active_execution_id'])
    return None if output is None else output['status']


def _validate_recovery_component(connection, session_id, component):
    key = encode_component_key(component)
    pending = connection.execute(
        'SELECT pending.*, shape.shape_json FROM pending_component_executions AS pending '
        'LEFT JOIN graph_shapes AS shape USING(shape_id) WHERE component_key=?', (key,),
    ).fetchall()
    if len(pending) != 1 or pending[0]['session_id'] != session_id:
        raise RuntimeError('Recovery requires one matching pending component execution: ' + key)
    pending = pending[0]
    roots = ExecutionSessionStorageMixin._read_session_job_roots(connection, session_id)
    session = connection.execute(
        'SELECT command FROM execution_sessions WHERE session_id=?', (session_id,),
    ).fetchone()
    expected_kind = 'jobs' if roots else 'full'
    if (not roots and session is not None and session['command'] in ('resume', 'resumefrom', 'resumebetween')
            and pending['starting_lifecycle'] == 'sampled'):
        expected_kind = 'resume'
    if pending['execution_kind'] != expected_kind:
        raise RuntimeError('Recovery pending execution kind disagrees with session selection: ' + key)
    proposal = ComponentTerminalOutcome(
        component, pending['shape_json'], pending['alignment_generation'], 'failed', None, None,
    )
    # These settlement validators only read the supplied connection.
    ComponentStateStorageMixin()._validate_component_terminal_outcomes(connection, session_id, (proposal,))


def _active_job_owner(connection, node, job_id):
    observed = JobExecutionOwnerStorageMixin._read_job_owner_observation(connection, node, job_id)
    owner = observed['owner']
    if (owner is None or observed['active_execution_id'] != owner['execution_id']
            or observed['status'] != 'running'):
        raise RuntimeError('Recovery requires an exact active owner')
    control = connection.execute(
        'SELECT active_pid, active_thread_id, active_started_at FROM jobs WHERE node_name=? AND job_id=?',
        (node, job_id),
    ).fetchone()
    if any(type(control[field]) is not int or control[field] < 1
           for field in ('active_pid', 'active_thread_id')):
        raise RuntimeError('Recovery requires valid active process and thread IDs')
    ExecutionSessionStorageMixin._session_time(control['active_started_at'], 'active_started_at')
    component_key = encode_component_key(owner['component'])
    reservation = connection.execute(
        'SELECT reservation.session_id, definition.shape_id, state.alignment_generation '
        'FROM component_reservations AS reservation '
        'JOIN component_states AS state USING(component_key) '
        'JOIN component_definitions AS definition '
        'ON definition.component_key=state.component_key AND definition.shape_id=state.shape_id '
        'WHERE reservation.component_key=?', (component_key,),
    ).fetchone()
    if (reservation is None or reservation['session_id'] != owner['session_id']
            or reservation['shape_id'] != owner['shape_id']
            or reservation['alignment_generation'] != owner['alignment_generation']):
        raise RuntimeError('Recovery requires the recorded producing component')
    _validate_recovery_component(connection, owner['session_id'], owner['component'])
    return observed


def _job_recovery(connection, root, node, job_id, observed):
    terminal = _terminal_output(root, node, job_id, observed)
    return {
        'node': node, 'job_id': job_id, 'owner': observed['owner'],
        'previous_generation': observed['generation'],
        'generation': observed['generation'] if terminal else observed['generation'] + 1,
        'action': 'reconcile terminal output' if terminal else 'requeue abandoned execution',
        'terminal_status': terminal,
    }


def _validate_session_identity(row):
    reader = ExecutionSessionStorageMixin
    reader._session_text(row['session_id'], 'session_id')
    reader._session_text(row['hostname'], 'hostname')
    reader._session_time(row['started_at'], 'started_at')
    reader._session_time(row['heartbeat_at'], 'heartbeat_at')
    if type(row['pid']) is not int or row['pid'] < 1:
        raise ValueError('Session PID must be a positive integer')
    if row['process_identity'] is not None:
        reader._session_text(row['process_identity'], 'process_identity')


def _refuse_unfinished_file_operations(connection, session_id, members, root):
    from .native_recovery_records import require_resolved_recovery_receipts
    from .file_operation_receipts import validate_file_receipt
    require_resolved_recovery_receipts(connection, session_id, root)
    for row in connection.execute('SELECT * FROM preparation_receipts'):
        affected = set(decode_component_key(row['component_key']))
        affected.update(item[0] for item in connection.execute(
            'SELECT receiver_node FROM receiver_mutation_guards WHERE operation_id=?', (row['guard_id'],),
        ))
        try:
            manifest = json.loads(row['manifest_json'])
            receivers = manifest['receivers']
            if not isinstance(receivers, list) or any(not isinstance(node, str) for node in receivers):
                raise ValueError('Invalid preparation receivers')
            affected.update(receivers)
        except (ValueError, TypeError, KeyError):
            if row['session_id'] == session_id or members.intersection(affected):
                raise RuntimeError('Damaged unfinished preparation requires recovery: ' + row['operation_id'])
        if row['session_id'] == session_id or members.intersection(affected):
            validate_file_receipt('preparation_receipts', row)
            if row['state'] == 'prepared':
                raise RuntimeError('Unfinished preparation requires recovery: ' + row['operation_id'])
    for row in connection.execute(
        "SELECT publication.*, owner.session_id FROM input_publications AS publication "
        "LEFT JOIN job_execution_owners AS owner USING(execution_id)",
    ):
        if row['session_id'] == session_id or row['receiver_node'] in members:
            validate_file_receipt('input_publications', row)
            if row['state'] == 'prepared':
                raise RuntimeError('Unfinished managed input publication requires recovery: ' + row['operation_id'])


def observe_abandoned_sessions(connection, root: Path) -> dict:
    """Report all stale sessions and damaged active claims from a fixed read view."""
    sessions, errors, live = [], [], []
    by_session = {}
    reserved_sessions = {}
    rows = connection.execute(
        "SELECT * FROM execution_sessions WHERE status='running' ORDER BY session_id",
    ).fetchall()
    for row in rows:
        session_id = row['session_id']
        identity_errors = []
        try:
            _validate_session_identity(row)
            liveness = execution_session_liveness(dict(row))
        except (ValueError, TypeError) as error:
            identity_errors.append(str(error))
            liveness = {'live': False, 'reason': 'session liveness metadata is damaged'}
        if liveness['live']:
            live.append(session_id)
            continue
        observation = {
            'session_id': session_id, 'liveness': liveness, 'session': None,
            'classification': 'damaged running' if identity_errors else 'abandoned',
            'reservations': [], 'holds': [], 'jobs': [], 'errors': identity_errors,
        }
        sessions.append(observation)
        by_session[session_id] = observation
        try:
            observation['session'] = ExecutionSessionStorageMixin._execution_session_from_row(connection, row)
            validate_execution_session_snapshot(observation['session'])
            require_admitted_reservation_scope(connection, session_id)
            observation['reservations'] = [
                decode_component_key(item[0]) for item in connection.execute(
                    'SELECT component_key FROM component_reservations WHERE session_id=? ORDER BY component_key',
                    (session_id,),
                )
            ]
            for component in observation['reservations']:
                for node in component:
                    reserved_sessions.setdefault(node, set()).add(session_id)
                if component not in observation['session']['selected_components']:
                    raise RuntimeError('Recovery reservation is outside its session selection')
                state = read_component_state_record(connection, component)
                if state is None:
                    raise RuntimeError('Recovery reservation has no component state')
                if state.lifecycle == 'running':
                    _validate_recovery_component(connection, session_id, component)
            _refuse_unfinished_file_operations(connection, session_id,
                                               {node for component in observation['reservations'] for node in component}, root)
            observation['holds'] = [
                {'component': decode_component_key(item[0]), 'count': item[1]}
                for item in connection.execute(
                    'SELECT component_key, hold_count FROM component_holds WHERE session_id=? ORDER BY component_key',
                    (session_id,),
                )
            ]
            for pending in connection.execute(
                'SELECT component_key FROM pending_component_executions WHERE session_id=? ORDER BY component_key',
                (session_id,),
            ).fetchall():
                _validate_recovery_component(connection, session_id, decode_component_key(pending[0]))
        except (RuntimeError, ValueError, TypeError) as error:
            observation['errors'].append(str(error))
    jobs = connection.execute(
        'SELECT job.node_name, job.job_id, owner.session_id AS reported_session_id '
        'FROM jobs AS job LEFT JOIN job_instances AS instance USING(node_name, job_id) '
        'LEFT JOIN job_execution_owners AS owner '
        'ON owner.execution_id=COALESCE(job.active_execution_id, instance.last_execution_id) '
        "WHERE job.status='running' OR job.active_execution_id IS NOT NULL OR job.active_pid IS NOT NULL "
        'OR job.active_thread_id IS NOT NULL OR job.active_started_at IS NOT NULL ORDER BY job.node_name, job.job_id',
    ).fetchall()
    for row in jobs:
        node, job_id = row['node_name'], row['job_id']
        try:
            observed = _active_job_owner(connection, node, job_id)
            session_id = observed['owner']['session_id']
            if session_id in by_session:
                by_session[session_id]['jobs'].append(_job_recovery(connection, root, node, job_id, observed))
            elif session_id not in live:
                raise RuntimeError('Active execution has no running native session')
        except (RuntimeError, ValueError, TypeError, OSError) as error:
            message = f'{node}/{job_id}: {error}'
            affected = ({row['reported_session_id']} | reserved_sessions.get(node, set())) & by_session.keys()
            if not affected:
                errors.append(message)
            for session_id in affected:
                by_session[session_id]['errors'].append(message)
    return {'sessions': sessions, 'live_sessions': live, 'errors': errors}
