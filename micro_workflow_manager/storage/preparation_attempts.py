"""Keep preparation ownership and its complete intended result recoverable."""

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
import socket
from uuid import uuid4

from micro_workflow_manager.processes import process_identity
from .operation_lock import operation_lock
from .preparation_receipts import submit_preparation_decision
from .file_operation_receipts import canonical, require_attempt_receipts
from .recovery_errors import add_recovery_note


def checked_attempt_receipts(connection, attempt):
    revision, completed_revision = attempt['membership_revision'], attempt['completed_membership_revision']
    if ((revision is not None and (type(revision) is not int or revision < 0))
            or (completed_revision is not None and (type(completed_revision) is not int
                or revision is None or completed_revision != revision))):
        raise RuntimeError('Invalid preparation membership completion')
    expected = json.loads(attempt['expected_receipts_json'])
    if (not isinstance(expected, list) or not expected or any(
            not isinstance(item, dict) or set(item) != {'operation', 'component_key'}
            or any(not isinstance(value, str) or not value for value in item.values()) for item in expected)):
        raise RuntimeError('Invalid preparation attempt receipt scope')
    pairs = {(item['operation'], item['component_key']) for item in expected}
    if len(pairs) != len(expected):
        raise RuntimeError('Duplicate intended preparation unit')
    receipts = require_attempt_receipts(connection, attempt['operation_id'])
    actual = [(row['operation'], row['component_key']) for row in receipts]
    if (len(set(actual)) != len(actual) or not set(actual) <= pairs
            or any(row['session_id'] != attempt['session_id'] for row in receipts)):
        raise RuntimeError('Preparation receipt disagrees with its intended unit')
    for row in receipts:
        manifest = json.loads(row['manifest_json'])
        if ('membership' in manifest) != (revision is not None):
            raise RuntimeError('Preparation receipt has another completion requirement')
        if not set(manifest['receivers']) <= set(json.loads(attempt['affected_nodes_json'])):
            raise RuntimeError('Preparation receipt exceeds its intended node scope')
    complete = set(actual) == pairs
    if completed_revision is not None and (not complete or any(row['state'] != 'committed' for row in receipts)):
        raise RuntimeError('Membership completion lacks its complete committed preparation')
    return receipts, complete


def attempt_outcome(connection, attempt):
    receipts, complete = checked_attempt_receipts(connection, attempt)
    if any(row['state'] == 'prepared' for row in receipts):
        return 'interrupted'
    finalized = attempt['membership_revision'] is None or attempt['completed_membership_revision'] is not None
    return 'committed' if finalized and complete and all(row['state'] == 'committed' for row in receipts) else 'aborted'


def _require_exact_attempt(connection, expected, guards):
    row = connection.execute('SELECT * FROM preparation_attempts WHERE operation_id=?',
                             (expected['operation_id'],)).fetchone()
    actual_guards = tuple(tuple(row) for row in connection.execute(
        'SELECT * FROM receiver_mutation_guards WHERE operation_id=? ORDER BY receiver_node',
        (expected['operation_id'],)))
    if row is None or dict(row) != expected or actual_guards != guards:
        raise RuntimeError('Preparation attempt or receiver guards changed')


def _require_attempt_completion(connection, expected, guards):
    row = connection.execute('SELECT completed_membership_revision FROM preparation_attempts WHERE operation_id=?',
                             (expected['operation_id'],)).fetchone()
    if row is None:
        raise RuntimeError('Preparation attempt is missing')
    current = dict(expected, completed_membership_revision=row['completed_membership_revision'])
    _require_exact_attempt(connection, current, guards)
    checked_attempt_receipts(connection, current)
    return current


def _interrupt(storage, expected, guards):
    if expected is None:
        return
    connection = storage._new_db_connection()
    try:
        row = connection.execute('SELECT * FROM preparation_attempts WHERE operation_id=?',
                                 (expected['operation_id'],)).fetchone()
    finally:
        connection.close()
    if row is None or row['state'] != 'preparing':
        return

    def interrupt(connection):
        current = _require_attempt_completion(connection, expected, guards)
        if connection.execute("UPDATE preparation_attempts SET state='interrupted' "
                              "WHERE operation_id=? AND state='preparing'", (expected['operation_id'],)).rowcount != 1:
            raise RuntimeError('Preparation attempt changed before interruption')
        _require_exact_attempt(connection, dict(current, state='interrupted'), guards)
    submit_preparation_decision(storage, interrupt)


def _finish(storage, expected, guards, requested_outcome):
    def finish(connection):
        current = _require_attempt_completion(connection, expected, guards)
        outcome = attempt_outcome(connection, current)
        if outcome != 'interrupted':
            if requested_outcome == 'committed' and outcome != 'committed':
                raise RuntimeError('Preparation finished without its complete committed result')
            if connection.execute('DELETE FROM receiver_mutation_guards WHERE operation_id=?',
                                  (expected['operation_id'],)).rowcount != len(guards):
                raise RuntimeError('Preparation receiver guard count changed before release')
        timestamp = None if outcome == 'interrupted' else datetime.now(timezone.utc).isoformat(timespec='milliseconds')
        if connection.execute("UPDATE preparation_attempts SET state=?, finished_at=? "
                              "WHERE operation_id=? AND state='preparing'",
                              (outcome, timestamp, expected['operation_id'])).rowcount != 1:
            raise RuntimeError('Preparation attempt changed before release')
        _require_exact_attempt(connection, dict(current, state=outcome, finished_at=timestamp),
                               guards if outcome == 'interrupted' else ())
        return outcome
    if submit_preparation_decision(storage, finish) == 'interrupted':
        raise RuntimeError('Preparation guards retained for recovery: ' + expected['operation_id'])


@contextmanager
def hold_preparation_attempt(storage, session_id, receivers, affected_nodes, expected_receipts, validate,
                             *, after_acquire=None, membership_revision=None):
    operation_id = uuid4().hex
    receivers = tuple(sorted(set(receivers)))
    affected_nodes = tuple(sorted(set(affected_nodes)))
    identity = process_identity(os.getpid())
    if not identity:
        raise RuntimeError('Preparation requires the current process identity')
    guards = tuple((node, operation_id, session_id, os.getpid(), identity, socket.gethostname()) for node in receivers)
    expected = None

    def acquire(connection):
        nonlocal expected
        validate(connection)
        timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
        expected = dict(operation_id=operation_id, state='preparing', session_id=session_id,
                        owner_pid=os.getpid(), process_identity=identity, hostname=socket.gethostname(),
                        started_at=timestamp, heartbeat_at=timestamp, finished_at=None,
                        receivers_json=canonical(receivers), affected_nodes_json=canonical(affected_nodes),
                        expected_receipts_json=canonical(expected_receipts), membership_revision=membership_revision,
                        completed_membership_revision=None)
        names = tuple(expected)
        connection.execute('INSERT INTO preparation_attempts(' + ','.join(names) + ') VALUES(' +
                           ','.join('?' for _ in names) + ')', tuple(expected.values()))
        connection.executemany('INSERT INTO receiver_mutation_guards VALUES(?,?,?,?,?,?)', guards)
        checked_attempt_receipts(connection, expected)
        if after_acquire is not None:
            after_acquire(connection, operation_id)
        _require_exact_attempt(connection, expected, guards)

    try:
        submit_preparation_decision(storage, acquire)
        with operation_lock(storage, 'preparation-operations', operation_id):
            try:
                yield operation_id
            except BaseException as error:
                try:
                    _finish(storage, expected, guards, 'aborted')
                except BaseException as release_error:
                    add_recovery_note(error, 'Receiver guard release failed: ' + str(release_error))
                raise
            else:
                _finish(storage, expected, guards, 'committed')
    except BaseException as error:
        try:
            _interrupt(storage, expected, guards)
        except BaseException as observation_error:
            add_recovery_note(error, 'Preparation attempt requires recovery: ' + str(observation_error))
        raise
