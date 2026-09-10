"""Bind each reversible file operation to its immutable intent and decision."""

import hashlib
import json
import re


INTENT_FIELDS = {
    'preparation_receipts': ('operation_id', 'guard_id', 'operation', 'component_key', 'session_id', 'manifest_json'),
    'input_publications': ('operation_id', 'execution_id', 'receiver_node', 'changes_json'),
    'restart_receipts': ('operation_id', 'session_id', 'manifest_json'),
    'clipboard_receipts': ('operation_id', 'operation', 'node_name', 'manifest_json'),
}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def digest(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


def _intent(table, row):
    return {'table': table, 'fields': {name: row[name] for name in INTENT_FIELDS[table]}}


def prepared_receipt(table, **fields):
    if set(fields) != set(INTENT_FIELDS[table]):
        raise ValueError('Incomplete file operation intent')
    return dict(fields, state='prepared', intent_digest=digest(_intent(table, fields)),
                decision_json=None, decision_digest=None)


def validate_file_receipt(table, row):
    operation_id = row['operation_id']
    if not isinstance(operation_id, str) or not re.fullmatch(r'[0-9a-f]{32}', operation_id):
        raise RuntimeError('Invalid file operation identity')
    if row['intent_digest'] != digest(_intent(table, row)):
        raise RuntimeError('File operation immutable intent changed: ' + operation_id)
    if row['state'] == 'prepared':
        if row['decision_json'] is not None or row['decision_digest'] is not None:
            raise RuntimeError('Prepared file operation has a terminal decision: ' + operation_id)
        return
    try:
        decision = json.loads(row['decision_json'])
    except (TypeError, ValueError) as error:
        raise RuntimeError('File operation has no terminal decision: ' + operation_id) from error
    if (row['state'] not in ('committed', 'aborted') or not isinstance(decision, dict)
            or set(decision) != {'table', 'operation_id', 'state', 'intent_digest', 'details'}
            or decision['table'] != table or decision['operation_id'] != operation_id
            or decision['state'] != row['state'] or decision['intent_digest'] != row['intent_digest']
            or not isinstance(decision['details'], dict)
            or row['decision_digest'] != digest(decision)):
        raise RuntimeError('File operation terminal decision changed: ' + operation_id)


def read_file_receipt(connection, table, operation_id):
    if table not in INTENT_FIELDS:
        raise ValueError('Unknown file operation table')
    row = connection.execute('SELECT * FROM ' + table + ' WHERE operation_id=?', (operation_id,)).fetchone()
    if row is None:
        return None
    row = dict(row)
    validate_file_receipt(table, row)
    return row


def insert_file_receipt(connection, table, row):
    validate_file_receipt(table, row)
    names = (*INTENT_FIELDS[table], 'state', 'intent_digest', 'decision_json', 'decision_digest')
    changed = connection.execute('INSERT INTO ' + table + '(' + ','.join(names) + ') VALUES(' +
                                 ','.join('?' for _ in names) + ')', tuple(row[name] for name in names)).rowcount
    if changed != 1 or read_file_receipt(connection, table, row['operation_id']) != row:
        raise RuntimeError('File operation did not record its exact prepared receipt: ' + row['operation_id'])


def require_prepared_file_receipt(connection, table, expected):
    if expected is None:
        raise RuntimeError('File operation has no prepared intent')
    actual = read_file_receipt(connection, table, expected['operation_id'])
    if actual != expected or actual['state'] != 'prepared':
        raise RuntimeError('File operation lost its exact prepared receipt: ' + expected['operation_id'])
    return actual


def finish_file_receipt(connection, table, expected, state, details):
    if state not in ('committed', 'aborted'):
        raise ValueError('File operation requires a terminal decision')
    require_prepared_file_receipt(connection, table, expected)
    decision = dict(table=table, operation_id=expected['operation_id'], state=state,
                    intent_digest=expected['intent_digest'], details=details)
    if connection.execute(
        'UPDATE ' + table + ' SET state=?, decision_json=?, decision_digest=? '
        "WHERE operation_id=? AND state='prepared' AND intent_digest=?",
        (state, canonical(decision), digest(decision), expected['operation_id'], expected['intent_digest']),
    ).rowcount != 1:
        raise RuntimeError('File operation lost its prepared receipt before settlement')
    terminal = dict(expected, state=state, decision_json=canonical(decision), decision_digest=digest(decision))
    if read_file_receipt(connection, table, expected['operation_id']) != terminal:
        raise RuntimeError('File operation did not retain its exact terminal receipt: ' + expected['operation_id'])


def require_attempt_receipts(connection, operation_id):
    rows = [dict(row) for row in connection.execute(
        'SELECT * FROM preparation_receipts WHERE guard_id=? ORDER BY operation_id', (operation_id,))]
    for row in rows:
        validate_file_receipt('preparation_receipts', row)
    return rows


def preparation_decision(connection, manifest):
    """Retain the scoped business rows accepted by this writer decision."""
    nodes = manifest['receivers']
    result = {'effects': manifest['effects']}
    for table in ('nodes', 'jobs', 'job_events'):
        result[table] = [dict(row) for node in nodes for row in connection.execute(
            'SELECT * FROM ' + table + ' WHERE node_name=? ORDER BY rowid', (node,))]
    for table in ('managed_input_files', 'managed_input_producers'):
        result[table] = [dict(row) for node in nodes for row in connection.execute(
            'SELECT * FROM ' + table + ' WHERE receiver_node=? ORDER BY rowid', (node,))]
    result['components'] = [dict(row) for row in connection.execute('SELECT * FROM component_states ORDER BY component_key')
                            if set(json.loads(row['component_key'])).intersection(nodes)]
    return result


def input_decision(connection, expected, relatives):
    result = {}
    for table in ('managed_input_files', 'managed_input_producers'):
        result[table] = [dict(row) for relative in relatives for row in connection.execute(
            'SELECT * FROM ' + table + ' WHERE receiver_node=? AND relative_path=? ORDER BY rowid',
            (expected['receiver_node'], relative))]
    result['owner'] = dict(connection.execute('SELECT * FROM job_execution_owners WHERE execution_id=?',
                                              (expected['execution_id'],)).fetchone())
    return result
