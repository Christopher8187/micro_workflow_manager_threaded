"""Persist exact manual-restart intent and terminal decisions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
import socket

from micro_workflow_manager.models import QUEUED, RUNNING
from micro_workflow_manager.processes import process_identity

from .execution_sessions import ExecutionSessionStorageMixin
from .file_operation_receipts import (
    finish_file_receipt,
    insert_file_receipt,
    prepared_receipt,
    read_file_receipt,
    require_prepared_file_receipt,
    validate_file_receipt,
)
from .recovery_errors import add_recovery_note
from .restart_files import RestartFiles


def _json_value(value):
    return json.loads(json.dumps(value, sort_keys=True, separators=(',', ':')))


def _row(connection, table, where, values):
    found = connection.execute('SELECT * FROM ' + table + ' WHERE ' + where, values).fetchone()
    return None if found is None else dict(found)


def _job_events(connection, node, job_id):
    return [dict(row) for row in connection.execute(
        'SELECT * FROM job_events WHERE node_name=? AND job_id=? ORDER BY event_id',
        (node, job_id),
    )]


def _database_targets(connection, targets):
    rows = []
    for target in targets:
        node, job_id = target['node'], target['job_id']
        owner = target.get('owner')
        rows.append({
            'node': node,
            'job_id': job_id,
            'job': _row(connection, 'jobs', 'node_name=? AND job_id=?', (node, job_id)),
            'instance': _row(connection, 'job_instances', 'node_name=? AND job_id=?', (node, job_id)),
            'owner': None if owner is None else _row(
                connection, 'job_execution_owners', 'execution_id=?', (owner['execution_id'],),
            ),
            'events': _job_events(connection, node, job_id),
        })
    if any(
        item['job'] is None or item['instance'] is None
        or (targets[position].get('owner') is not None and item['owner'] is None)
        for position, item in enumerate(rows)
    ):
        raise RuntimeError('Restart target identity disappeared before receipt preparation')
    return rows


def _validate_operation_owner(owner):
    validator = ExecutionSessionStorageMixin
    if not isinstance(owner, dict) or set(owner) != {
        'owner_pid', 'process_identity', 'hostname', 'started_at',
    }:
        raise RuntimeError('Invalid restart operation owner')
    validator._session_text(owner['process_identity'], 'process_identity')
    validator._session_text(owner['hostname'], 'hostname')
    validator._session_time(owner['started_at'], 'started_at')
    if type(owner['owner_pid']) is not int or owner['owner_pid'] < 1:
        raise RuntimeError('Invalid restart operation process')


def _validate_target(target):
    if not isinstance(target, dict) or set(target) != {
        'node', 'job_id', 'mode', 'owner', 'job_instance_id', 'generation',
        'status', 'active_execution_id',
    }:
        raise RuntimeError('Invalid restart target identity')
    if (
        not isinstance(target['node'], str) or not target['node']
        or type(target['job_id']) is not int or target['job_id'] < 1
        or target['mode'] not in ('running', 'failed', 'independent')
        or not isinstance(target['job_instance_id'], str)
        or type(target['generation']) is not int or target['generation'] < 0
        or not isinstance(target['status'], str)
        or target['active_execution_id'] is not None
        and not isinstance(target['active_execution_id'], str)
    ):
        raise RuntimeError('Invalid restart target identity')
    owner = target['owner']
    if owner is not None and (
        not isinstance(owner, dict)
        or set(owner) != {
            'execution_id', 'node_name', 'job_id', 'generation', 'session_id',
            'component', 'job_instance_id', 'shape_id', 'alignment_generation',
            'created_by_execution_id',
        }
        or owner['node_name'] != target['node']
        or owner['job_id'] != target['job_id']
        or owner['job_instance_id'] != target['job_instance_id']
        or type(owner['generation']) is not int
        or not 0 <= owner['generation'] <= target['generation']
    ):
        raise RuntimeError('Invalid restart producing owner')


def validate_restart_manifest(root, receipt):
    validate_file_receipt('restart_receipts', receipt)
    try:
        manifest = json.loads(receipt['manifest_json'])
    except (TypeError, ValueError) as error:
        raise RuntimeError('Invalid restart recovery manifest') from error
    if (
        not isinstance(manifest, dict)
        or set(manifest) != {
            'version', 'operation_id', 'session_id', 'owner', 'requested_at',
            'requested_by_pid', 'reason', 'component_plan', 'owned', 'targets', 'database',
        }
        or manifest['version'] != 1
        or manifest['operation_id'] != receipt['operation_id']
        or manifest['session_id'] != receipt['session_id']
        or type(manifest['owned']) is not bool
    ):
        raise RuntimeError('Invalid restart recovery manifest')
    _validate_operation_owner(manifest['owner'])
    validator = ExecutionSessionStorageMixin
    validator._session_time(manifest['requested_at'], 'requested_at')
    if not isinstance(manifest['reason'], str):
        raise RuntimeError('Invalid restart reason')
    if type(manifest['requested_by_pid']) is not int or manifest['requested_by_pid'] < 1:
        raise RuntimeError('Invalid restart requester process')
    files = RestartFiles(root, receipt['operation_id'], manifest['targets'])
    database = manifest['database']
    if (
        not isinstance(database, list)
        or len(database) != len(files.targets)
        or any(
            not isinstance(item, dict)
            or set(item) != {'node', 'job_id', 'job', 'instance', 'owner', 'events'}
            or not isinstance(item['events'], list)
            for item in database
        )
    ):
        raise RuntimeError('Invalid restart database observation')
    addresses = [(item['node'], item['job_id']) for item in database]
    file_addresses = [(item['node'], item['job_id']) for item in files.targets]
    if addresses != sorted(addresses) or addresses != file_addresses:
        raise RuntimeError('Invalid restart target ordering')
    for item, file_item in zip(database, files.targets):
        _validate_target(file_item['target'])
        target = file_item['target']
        if (
            item['node'] != target['node'] or item['job_id'] != target['job_id']
            or manifest['owned'] != (target['mode'] != 'independent')
            or manifest['owned'] and target['owner'] is None
            or target['owner'] is not None
            and target['owner']['session_id'] != manifest['session_id']
        ):
            raise RuntimeError('Restart database observation names another target')
        if not isinstance(item['job'], dict) or not isinstance(item['instance'], dict):
            raise RuntimeError('Invalid restart database observation')
    return manifest, files


def _current_database_targets(connection, manifest):
    return _database_targets(connection, [item['target'] for item in manifest['targets']])


def require_restart_pre_state(connection, manifest):
    if _current_database_targets(connection, manifest) != manifest['database']:
        raise RuntimeError('Restart target state changed after receipt preparation')


def require_restart_owners(connection, manifest):
    for item in manifest['database']:
        expected = item['owner']
        if expected is not None and _row(
            connection, 'job_execution_owners', 'execution_id=?',
            (expected['execution_id'],),
        ) != expected:
            raise RuntimeError(f"Restart owner changed: {item['node']}/{item['job_id']}")


def restart_revision(connection):
    row = connection.execute(
        "SELECT value FROM metadata WHERE key='job_restart_revision'",
    ).fetchone()
    try:
        return 0 if row is None else max(0, int(row[0]))
    except (TypeError, ValueError) as error:
        raise RuntimeError('Invalid job restart revision') from error


def restart_decision(connection, targets, *, previous_revision):
    nodes = sorted({target['node'] for target in targets})
    return {
        'database': _database_targets(connection, targets),
        'nodes': [_row(connection, 'nodes', 'node_name=?', (node,)) for node in nodes],
        'previous_restart_revision': previous_revision,
        'restart_revision': restart_revision(connection),
    }


def _expected_job(previous, manifest):
    target = previous['target']
    job = dict(previous['job'])
    job.update(
        generation=target['generation'] + 1,
        active_execution_id=None,
        active_pid=None,
        active_thread_id=None,
        active_started_at=None,
        restart_requested_at=manifest['requested_at'],
        restart_requested_by_pid=manifest['requested_by_pid'],
        restart_reason=manifest['reason'],
        status=QUEUED,
        status_json='{}',
    )
    raw = previous['job']['runtime_json']
    runtime = json.loads(raw) if raw else None
    if runtime:
        runtime.update(
            state='restarted',
            updated_at=manifest['requested_at'],
            restart_reason=manifest['reason'],
            previous_generation=target['generation'],
            generation=target['generation'] + 1,
        )
    job['runtime_json'] = None if runtime is None else json.dumps(
        runtime, ensure_ascii=False, separators=(',', ':'),
    )
    return job


def _expected_events(previous, manifest):
    target = previous['target']
    owner = target['owner']
    owner_data = {} if not manifest['owned'] else {
        'session_id': owner['session_id'],
        'execution_id': owner['execution_id'],
        'job_instance_id': target['job_instance_id'],
        'component': owner['component'],
    }
    return (
        ('queued', {**owner_data, 'previous_status': target['status'], 'status': QUEUED}),
        ('restart_requested', {
            **owner_data,
            'previous_generation': target['generation'],
            'generation': target['generation'] + 1,
            'reason': manifest['reason'],
            'requested_by_pid': manifest['requested_by_pid'],
        }),
    )


def validate_restart_terminal_decision(manifest, receipt):
    decision = json.loads(receipt['decision_json'])
    details = decision['details']
    if receipt['state'] == 'aborted':
        if details != {'restored': True}:
            raise RuntimeError('Invalid aborted restart decision')
        return
    if (
        receipt['state'] != 'committed'
        or not isinstance(details, dict)
        or set(details) != {
            'database', 'nodes', 'previous_restart_revision', 'restart_revision',
        }
        or type(details['previous_restart_revision']) is not int
        or details['previous_restart_revision'] < 0
        or type(details['restart_revision']) is not int
        or details['restart_revision'] != details['previous_restart_revision'] + 1
        or not isinstance(details['nodes'], list)
        or not isinstance(details['database'], list)
        or len(details['database']) != len(manifest['database'])
    ):
        raise RuntimeError('Invalid committed restart decision')
    targets = {
        (item['node'], item['job_id']): item['target'] for item in manifest['targets']
    }
    for previous, current in zip(manifest['database'], details['database']):
        target = targets[(previous['node'], previous['job_id'])]
        expected_previous = dict(previous, target=target)
        if (
            current['node'] != previous['node']
            or current['job_id'] != previous['job_id']
            or current['job'] != _expected_job(expected_previous, manifest)
            or current['instance'] != previous['instance']
            or current['owner'] != previous['owner']
            or len(current['events']) != len(previous['events']) + 2
            or current['events'][:-2] != previous['events']
        ):
            raise RuntimeError('Committed restart decision changed its target state')
        for event, (name, data) in zip(
            current['events'][-2:], _expected_events(expected_previous, manifest),
        ):
            if (
                event['node_name'] != previous['node']
                or event['job_id'] != previous['job_id']
                or event['time'] != manifest['requested_at']
                or event['event'] != name
                or json.loads(event['data_json']) != data
            ):
                raise RuntimeError('Committed restart decision has invalid events')
    nodes = {row['node_name']: row for row in details['nodes'] if isinstance(row, dict)}
    expected_nodes = sorted({item['node'] for item in manifest['database']})
    if len(details['nodes']) != len(expected_nodes) or sorted(nodes) != expected_nodes or (
        manifest['owned'] and any(nodes[node]['status'] != RUNNING for node in expected_nodes)
    ):
        raise RuntimeError('Committed restart decision has invalid node states')


def require_restart_terminal(connection, root, receipt):
    manifest, files = validate_restart_manifest(root, receipt)
    validate_restart_terminal_decision(manifest, receipt)
    require_restart_owners(connection, manifest)
    return manifest, files


def require_no_prepared_overlap(connection, targets):
    addresses = {(target['node'], target['job_id']) for target in targets}
    for row in connection.execute("SELECT * FROM restart_receipts WHERE state='prepared'"):
        row = dict(row)
        validate_file_receipt('restart_receipts', row)
        manifest = json.loads(row['manifest_json'])
        pending = {(item['node'], item['job_id']) for item in manifest.get('targets', ())}
        if addresses.intersection(pending):
            raise RuntimeError('Restart target already has an interrupted restart operation')


@dataclass(slots=True)
class RestartReceipt:
    storage: object
    operation_id: str
    session_id: str | None
    requested_at: str
    requested_by_pid: int
    reason: str
    component_plan: dict | None
    owned: bool
    expected: dict | None = None
    terminal: dict | None = None
    files: RestartFiles | None = None

    @classmethod
    def create(
        cls, storage, operation_id, targets, *, requested_at, requested_by_pid,
        reason, component_plan, owned,
    ):
        sessions = {
            target['owner']['session_id'] for target in targets
            if target.get('owner') is not None
        }
        if len(sessions) > 1:
            raise RuntimeError('Restart receipt has several owning sessions')
        return cls(
            storage, operation_id, next(iter(sessions), None), requested_at,
            requested_by_pid, reason, component_plan, owned,
        )

    def read(self):
        row = self.storage._restart_receipt_state(self.operation_id)
        expected = self.terminal if self.terminal is not None else self.expected
        if row is not None and (
            expected is None or row['intent_digest'] != expected['intent_digest']
        ):
            raise RuntimeError('Restart receipt no longer matches its intent: ' + self.operation_id)
        if (
            row is not None and expected is not None
            and row['state'] == expected['state'] and row != expected
        ):
            raise RuntimeError('Restart receipt no longer matches its decision: ' + self.operation_id)
        return row

    def state(self):
        row = self.read()
        return None if row is None else row['state']

    def _submit(self, expected_state, operation, warnings=None):
        future = self.storage.submit_db_mutation(operation, wait=False, priority=0)
        error = None
        try:
            result = future.result()
        except BaseException as caught:
            error = caught
            while not future.done():
                try:
                    future.result()
                except BaseException:
                    pass
        try:
            row = self.read()
        except BaseException as observation_error:
            if error is not None:
                add_recovery_note(error, 'Restart decision could not be read: ' + str(observation_error))
                raise error
            raise
        expected = self.terminal if expected_state != 'prepared' else self.expected
        if row != expected or row is None or row['state'] != expected_state:
            if error is not None:
                raise error
            raise RuntimeError('Restart writer did not retain its exact decision: ' + self.operation_id)
        if error is None:
            return result
        if not isinstance(error, Exception):
            raise error
        if warnings is not None:
            warnings.append('Restart committed; state notification requires attention: ' + str(error))
        return None

    def prepare(self, files, validate):
        identity = process_identity(os.getpid())
        if identity is None:
            raise RuntimeError('Restart cannot identify its operation process')
        self.files = files

        def prepare(connection):
            targets = validate(connection)
            require_no_prepared_overlap(connection, targets)
            database = _database_targets(connection, targets)
            file_manifest = files.manifest()
            manifest = {
                **file_manifest,
                'session_id': self.session_id,
                'owner': {
                    'owner_pid': os.getpid(),
                    'process_identity': identity,
                    'hostname': socket.gethostname(),
                    'started_at': datetime.now(timezone.utc).isoformat(timespec='milliseconds'),
                },
                'requested_at': self.requested_at,
                'requested_by_pid': self.requested_by_pid,
                'reason': self.reason,
                'component_plan': _json_value(self.component_plan),
                'owned': self.owned,
                'database': database,
            }
            self.expected = prepared_receipt(
                'restart_receipts', operation_id=self.operation_id, session_id=self.session_id,
                manifest_json=json.dumps(manifest, separators=(',', ':')),
            )
            insert_file_receipt(connection, 'restart_receipts', self.expected)

        self._submit('prepared', prepare)

    def require_prepared(self, connection):
        return require_prepared_file_receipt(
            connection, 'restart_receipts', self.expected,
        )

    def commit(self, connection, targets, *, previous_revision):
        details = restart_decision(connection, targets, previous_revision=previous_revision)
        finish_file_receipt(
            connection, 'restart_receipts', self.expected, 'committed', details,
        )
        self.terminal = read_file_receipt(connection, 'restart_receipts', self.operation_id)
        manifest, _ = validate_restart_manifest(self.storage.project_dir, self.terminal)
        validate_restart_terminal_decision(manifest, self.terminal)

    def submit_commit(self, operation, warnings):
        return self._submit('committed', operation, warnings)

    def abort(self):
        def abort(connection):
            self.require_prepared(connection)
            manifest, _ = validate_restart_manifest(self.storage.project_dir, self.expected)
            require_restart_pre_state(connection, manifest)
            self.files.require_restored()
            finish_file_receipt(
                connection, 'restart_receipts', self.expected, 'aborted', {'restored': True},
            )
            self.terminal = read_file_receipt(connection, 'restart_receipts', self.operation_id)
            validate_restart_terminal_decision(manifest, self.terminal)

        self._submit('aborted', abort)
