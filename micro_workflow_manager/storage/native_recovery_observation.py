"""Capture immutable, session-scoped native recovery decisions."""

from __future__ import annotations

from dataclasses import dataclass
import json

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key
from micro_workflow_manager.session_liveness import execution_session_liveness

from .component_states import ComponentTerminalOutcome
from .execution_sessions import validate_execution_session_snapshot
from .preview_recovery import _active_job_owner, observe_abandoned_sessions
from .recovery_output import RecoveryFileObservation, observe_recovery_file, recovery_terminal_output


@dataclass(frozen=True, slots=True)
class JobRecoveryPlan:
    node: str
    job_id: int
    generation: int
    execution_id: str
    job_instance_id: str
    component: tuple[str, ...]
    output: RecoveryFileObservation
    terminal_status: str | None


@dataclass(frozen=True, slots=True)
class SessionRecoveryPlan:
    session_id: str
    reason: str
    session_json: str
    scope: tuple
    jobs: tuple[JobRecoveryPlan, ...]
    outcomes: tuple[ComponentTerminalOutcome, ...]

    def manifest(self):
        return {
            'session': json.loads(self.session_json), 'scope': self.scope,
            'jobs': [dict(node=job.node, job_id=job.job_id, generation=job.generation,
                          execution_id=job.execution_id, job_instance_id=job.job_instance_id,
                          component=job.component, terminal_status=job.terminal_status,
                          output=job.output.manifest()) for job in self.jobs],
        }


@dataclass(frozen=True, slots=True)
class RecoveryObservation:
    sessions: tuple[SessionRecoveryPlan, ...]
    refusals: tuple[tuple[str, tuple[str, ...]], ...]
    live_sessions: tuple[str, ...]
    errors: tuple[str, ...]


def _rows(connection, table, where, values):
    return tuple(sorted((tuple(row) for row in connection.execute(
        f'SELECT * FROM "{table}" WHERE {where}', values,
    )), key=repr))


def _selected_rows(connection, table, column, values):
    values = tuple(sorted(set(values)))
    where = f'"{column}" IN ({",".join("?" for _ in values)})' if values else '0'
    return _rows(connection, table, where, values)


def capture_recovery_scope(connection, session_id):
    session = connection.execute(
        'SELECT * FROM execution_sessions WHERE session_id=?', (session_id,),
    ).fetchone()
    if session is None:
        raise RuntimeError('Recovery session disappeared: ' + session_id)
    keys = [row[0] for row in connection.execute(
        'SELECT component_key FROM component_reservations WHERE session_id=?', (session_id,),
    )]
    members = {node for key in keys for node in decode_component_key(key)}
    overlapping = []
    for row in connection.execute('SELECT * FROM component_reservations'):
        if members.intersection(decode_component_key(row['component_key'])):
            if row['session_id'] != session_id:
                raise RuntimeError('Recovery overlaps another component reservation: ' + row['session_id'])
            overlapping.append(tuple(row))
    captured = []
    for table in ('execution_sessions', 'session_components', 'session_jobs',
                  'component_holds', 'pending_component_executions'):
        captured.append((table, _rows(connection, table, 'session_id=?', (session_id,))))
    captured.append(('component_reservations', tuple(sorted(overlapping, key=repr))))
    for table, columns in (
        ('execution_session_parents', ('child_session_id', 'parent_session_id')),
        ('interrupt_admissions', ('session_id',)),
        ('interrupt_scope_transfers', ('child_session_id', 'source_session_id')),
        ('interrupt_pause_requests', ('child_session_id', 'owner_session_id')),
        ('interrupt_paused_executions', ('child_session_id', 'owner_session_id')),
        ('interrupt_frozen_parent_states', ('child_session_id',)),
        ('post_interrupt_fences', ('source_session_id',)),
        ('session_fence_authorizations', ('session_id', 'source_session_id')),
    ):
        where = ' OR '.join(column + '=?' for column in columns)
        captured.append((table, _rows(connection, table, where, (session_id,) * len(columns))))
    for row in connection.execute('SELECT * FROM component_holds WHERE session_id=?', (session_id,)):
        component = decode_component_key(row['component_key'])
        if (encode_component_key(component) != row['component_key']
                or type(row['hold_count']) is not int or row['hold_count'] < 1):
            raise RuntimeError('Recovery requires exact positive component hold counts')
    for table in ('component_states', 'component_successful_results', 'active_components'):
        captured.append((table, _selected_rows(connection, table, 'component_key', keys)))
    for table in ('active_component_members', 'nodes', 'jobs', 'job_instances', 'job_events'):
        captured.append((table, _selected_rows(connection, table, 'node_name', members)))
    owners = connection.execute(
        'SELECT * FROM job_execution_owners WHERE session_id=?', (session_id,),
    ).fetchall()
    captured.append(('job_execution_owners', tuple(sorted((tuple(row) for row in owners), key=repr))))
    identities = {(row['component_key'], row['shape_id']) for row in owners}
    selected = connection.execute(
        'SELECT component_key FROM session_components WHERE session_id=?', (session_id,),
    ).fetchall()
    identities.update((row[0], session['admitted_shape_id']) for row in selected)
    for key in keys:
        state = connection.execute('SELECT * FROM component_states WHERE component_key=?', (key,)).fetchone()
        if state is None:
            raise RuntimeError('Recovery component state disappeared: ' + key)
        identities.add((key, state['shape_id']))
        if state['retained_result_shape_id'] is not None:
            identities.add((key, state['retained_result_shape_id']))
    definitions = []
    for key, shape_id in sorted(identities):
        definitions.extend(_rows(connection, 'component_definitions',
                                 'component_key=? AND shape_id=?', (key, shape_id)))
    captured.append(('component_definitions', tuple(definitions)))
    captured.append(('graph_shapes', _selected_rows(connection, 'graph_shapes', 'shape_id',
                                                   [shape for _, shape in identities])))
    return tuple(captured)


def require_session_recovery_unchanged(connection, plan):
    row = connection.execute(
        'SELECT * FROM execution_sessions WHERE session_id=?', (plan.session_id,),
    ).fetchone()
    if row is None or dict(row) != json.loads(plan.session_json):
        raise RuntimeError('Recovery session changed after observation: ' + plan.session_id)
    if execution_session_liveness(dict(row))['live']:
        raise RuntimeError('Recovery session is active again: ' + plan.session_id)
    if capture_recovery_scope(connection, plan.session_id) != plan.scope:
        raise RuntimeError('Recovery ownership or state changed after observation: ' + plan.session_id)
    for job in plan.jobs:
        observed = _active_job_owner(connection, job.node, job.job_id)
        owner = observed['owner']
        if (owner['execution_id'] != job.execution_id or owner['session_id'] != plan.session_id
                or owner['component'] != job.component or observed['generation'] != job.generation
                or observed['job_instance_id'] != job.job_instance_id):
            raise RuntimeError(f'Recovery job ownership changed: {job.node}/{job.job_id}')


def _session_plan(connection, root, observation):
    session_id = observation['session_id']
    session = observation['session']
    validate_execution_session_snapshot(session)
    row = connection.execute('SELECT * FROM execution_sessions WHERE session_id=?', (session_id,)).fetchone()
    jobs = []
    for proposed in observation['jobs']:
        node, job_id = proposed['node'], proposed['job_id']
        observed = _active_job_owner(connection, node, job_id)
        output = observe_recovery_file(root, f'node/{node}/jobs/{job_id}/output.json')
        terminal = recovery_terminal_output(output, observed['generation'], observed['active_execution_id'])
        status = None if terminal is None else terminal['status']
        if status != proposed['terminal_status']:
            raise RuntimeError(f'Recovery output changed between observations: {node}/{job_id}')
        owner = observed['owner']
        if status is None:
            raw = connection.execute('SELECT runtime_json FROM jobs WHERE node_name=? AND job_id=?',
                                     (node, job_id)).fetchone()[0]
            if raw is not None and not isinstance(json.loads(raw), dict):
                raise RuntimeError(f'Damaged recovery runtime: {node}/{job_id}')
        jobs.append(JobRecoveryPlan(node, job_id, observed['generation'], owner['execution_id'],
                                    observed['job_instance_id'], owner['component'], output, status))
    outcomes = tuple(ComponentTerminalOutcome(
        decode_component_key(pending['component_key']), pending['shape_json'],
        pending['alignment_generation'], 'failed', None, None,
    ) for pending in connection.execute(
        'SELECT pending.*, shape.shape_json FROM pending_component_executions AS pending '
        'JOIN graph_shapes AS shape USING(shape_id) WHERE session_id=? ORDER BY component_key', (session_id,),
    ))
    return SessionRecoveryPlan(session_id, observation['liveness']['reason'],
                               json.dumps(dict(row), sort_keys=True),
                               capture_recovery_scope(connection, session_id), tuple(jobs), outcomes)


def observe_native_recovery(connection, root):
    observed = observe_abandoned_sessions(connection, root)
    sessions, refusals = [], []
    for item in observed['sessions']:
        if item['errors']:
            refusals.append((item['session_id'], tuple(item['errors'])))
            continue
        try:
            sessions.append(_session_plan(connection, root, item))
        except (RuntimeError, ValueError, TypeError, OSError) as error:
            refusals.append((item['session_id'], (str(error),)))
    return RecoveryObservation(tuple(sessions), tuple(refusals),
                               tuple(observed['live_sessions']), tuple(observed['errors']))
