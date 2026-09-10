"""Validate immutable recovery intent and its durable terminal decision."""

from dataclasses import dataclass
import hashlib
import json
import re
import stat

from .execution_sessions import ExecutionSessionStorageMixin
from .native_recovery_observation import JobRecoveryPlan, SessionRecoveryPlan
from .interrupt_settlement import (
    read_interrupt_terminal_rows,
    require_terminal_interrupt_scope,
)


INTENT_FIELDS = ('operation_id', 'session_id', 'owner_hostname', 'owner_pid',
                 'owner_identity', 'prepared_at', 'observation_json', 'manifest_json')


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def digest(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


def intent_digest(row):
    return digest({name: row[name] for name in INTENT_FIELDS})


def read_receipt(connection, operation_id):
    row = connection.execute('SELECT * FROM recovery_receipts WHERE operation_id=?', (operation_id,)).fetchone()
    if row is None:
        return None
    row = dict(row)
    validate_receipt(row)
    if row['state'] == 'committed':
        require_committed_decision(connection, row)
    return row


def validate_receipt(row):
    if not re.fullmatch(r'[0-9a-f]{32}', row['operation_id']):
        raise RuntimeError('Invalid recovery operation identity')
    if row['intent_digest'] != intent_digest(row):
        raise RuntimeError('Recovery receipt immutable fields changed: ' + row['operation_id'])
    validator = ExecutionSessionStorageMixin
    for field in ('session_id', 'owner_hostname', 'owner_identity'):
        validator._session_text(row[field], field)
    validator._session_time(row['prepared_at'], 'prepared_at')
    if type(row['owner_pid']) is not int or row['owner_pid'] < 1:
        raise RuntimeError('Invalid recovery process identity')
    if row['state'] == 'prepared':
        if row['decision_json'] is not None or row['decision_digest'] is not None:
            raise RuntimeError('Prepared recovery has an unexpected terminal decision')
        return
    decision = json.loads(row['decision_json'])
    if (row['state'] not in ('committed', 'aborted') or not isinstance(decision, dict)
            or decision.get('state') != row['state'] or decision.get('operation_id') != row['operation_id']
            or decision.get('intent_digest') != row['intent_digest']
            or row['decision_digest'] != digest(decision)):
        raise RuntimeError('Recovery receipt has no matching terminal decision: ' + row['operation_id'])


def require_committed_decision(connection, row):
    decision = json.loads(row['decision_json'])
    session = connection.execute('SELECT * FROM execution_sessions WHERE session_id=?', (row['session_id'],)).fetchone()
    if (session is None or decision.get('session') != dict(session)
            or session['status'] != 'terminal' or session['outcome'] != 'failed'
            or not session['finished_at'] or not json.loads(session['failures_json'])):
        raise RuntimeError('Recovery committed decision disagrees with its terminal session')
    for table in ('component_reservations', 'component_holds', 'pending_component_executions'):
        if connection.execute('SELECT 1 FROM ' + table + ' WHERE session_id=? LIMIT 1', (row['session_id'],)).fetchone():
            raise RuntimeError('Recovery committed decision still owns active work')
    require_terminal_interrupt_scope(connection, row['session_id'])
    if decision.get('interrupt') != read_interrupt_terminal_rows(connection, row['session_id']):
        raise RuntimeError('Recovery committed interrupt scope changed')
    owners = [dict(item) for item in connection.execute(
        'SELECT * FROM job_execution_owners WHERE session_id=? ORDER BY execution_id', (row['session_id'],))]
    if decision.get('owners') != owners:
        raise RuntimeError('Recovery committed producing owners changed')


def terminal_decision(connection, row, plan, state):
    decision = {'state': state, 'operation_id': row['operation_id'], 'intent_digest': row['intent_digest']}
    if state == 'committed':
        decision['session'] = dict(connection.execute(
            'SELECT * FROM execution_sessions WHERE session_id=?', (plan.session_id,)).fetchone())
        decision['owners'] = [dict(item) for item in connection.execute(
            'SELECT * FROM job_execution_owners WHERE session_id=? ORDER BY execution_id', (plan.session_id,))]
        decision['jobs'] = [dict(connection.execute('SELECT * FROM jobs WHERE node_name=? AND job_id=?',
                             (job.node, job.job_id)).fetchone()) for job in plan.jobs]
        decision['events'] = [dict(item) for job in plan.jobs for item in connection.execute(
            'SELECT * FROM job_events WHERE node_name=? AND job_id=? ORDER BY rowid', (job.node, job.job_id))]
        decision['components'] = [dict(item) for item in connection.execute(
            'SELECT state.* FROM component_states AS state JOIN session_components AS selected USING(component_key) '
            'WHERE selected.session_id=? ORDER BY component_key', (plan.session_id,))]
        decision['interrupt'] = read_interrupt_terminal_rows(connection, plan.session_id)
    return decision


def finish_receipt(connection, row, plan, state):
    actual = read_receipt(connection, row['operation_id'])
    if actual != row or actual['state'] != 'prepared':
        raise RuntimeError('Recovery lost its exact prepared receipt: ' + row['operation_id'])
    decision = terminal_decision(connection, row, plan, state)
    if connection.execute(
        'UPDATE recovery_receipts SET state=?, decision_json=?, decision_digest=? '
        "WHERE operation_id=? AND state='prepared' AND intent_digest=?",
        (state, canonical(decision), digest(decision), row['operation_id'], row['intent_digest']),
    ).rowcount != 1:
        raise RuntimeError('Recovery lost its prepared receipt before settlement')
    terminal = dict(row, state=state, decision_json=canonical(decision), decision_digest=digest(decision))
    if read_receipt(connection, row['operation_id']) != terminal:
        raise RuntimeError('Recovery did not retain its exact terminal receipt: ' + row['operation_id'])


@dataclass(frozen=True, slots=True)
class RecordedRecoveryFile:
    relative_path: str
    marker: tuple | None
    sha256: str | None

    def manifest(self):
        return {'path': self.relative_path, 'marker': None if self.marker is None else list(self.marker),
                'sha256': self.sha256}


def recorded_file(value):
    if not isinstance(value, dict) or set(value) != {'path', 'marker', 'sha256'}:
        raise RuntimeError('Invalid recorded recovery output')
    marker, sha = value['marker'], value['sha256']
    if marker is None:
        if sha is not None:
            raise RuntimeError('Missing recovery output has a content digest')
    elif (not isinstance(marker, list) or len(marker) != 5 or any(type(item) is not int for item in marker)
          or not stat.S_ISREG(marker[2]) or not isinstance(sha, str) or not re.fullmatch(r'[0-9a-f]{64}', sha)):
        raise RuntimeError('Invalid recorded recovery file identity')
    return RecordedRecoveryFile(value['path'], None if marker is None else tuple(marker), sha)


def _tuples(value):
    return tuple(_tuples(item) for item in value) if isinstance(value, list) else value


def recorded_plan(row):
    validate_receipt(row)
    observed = json.loads(row['observation_json'])
    if (not isinstance(observed, dict) or set(observed) != {'session', 'scope', 'jobs'}
            or observed['session']['session_id'] != row['session_id']):
        raise RuntimeError('Invalid recorded recovery observation')
    jobs = []
    for job in observed['jobs']:
        output = recorded_file(job['output'])
        if output.relative_path != f"node/{job['node']}/jobs/{job['job_id']}/output.json":
            raise RuntimeError('Recorded recovery output belongs to another job')
        jobs.append(JobRecoveryPlan(job['node'], job['job_id'], job['generation'], job['execution_id'],
                                   job['job_instance_id'], tuple(job['component']), output, job['terminal_status']))
    return SessionRecoveryPlan(row['session_id'], 'interrupted recovery', json.dumps(observed['session'], sort_keys=True),
                               _tuples(observed['scope']), tuple(jobs), ())


def require_resolved_recovery_receipts(connection, session_id, root):
    import os
    from .recovery_output import recovery_path
    for row in connection.execute('SELECT * FROM recovery_receipts WHERE session_id=?', (session_id,)):
        row = dict(row)
        validate_receipt(row)
        if row['state'] == 'prepared' or os.path.lexists(recovery_path(root, '.mwf/recovery-trash/' + row['operation_id'])):
            raise RuntimeError('Unfinished session recovery requires restoration: ' + row['operation_id'])
        if row['state'] == 'committed':
            require_committed_decision(connection, row)
