"""Observe interrupted preparation and managed-input receipts without mutations."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
import re

from micro_workflow_manager.session_liveness import execution_session_liveness
from .execution_ownership import JobExecutionOwnerStorageMixin
from .execution_sessions import execution_session_from_row_snapshot, validate_execution_session_snapshot
from .recovery_output import recovery_path
from .preparation_staging import PreparationStaging
from .file_operation_receipts import validate_file_receipt
from .preparation_attempts import checked_attempt_receipts
from .session_scope import require_admitted_reservation_scope
from .restart_receipts import (
    validate_restart_manifest,
    validate_restart_terminal_decision,
)


@dataclass(frozen=True, slots=True)
class CleanupPlan:
    kind: str
    operation_id: str
    receipt_json: str
    manifest_json: str
    session_json: str | None
    attempt_json: str | None
    guards: tuple
    execution_owner_json: str | None
    receivers: tuple[str, ...]
    job_fences: tuple[tuple[str, int], ...] = ()

    @property
    def manifest(self):
        return json.loads(self.manifest_json)

    @property
    def receipt(self):
        return json.loads(self.receipt_json)

    @property
    def lock_identity(self):
        if self.kind in ('preparation', 'attempt'):
            return 'preparation-operations', json.loads(self.attempt_json)['operation_id']
        if self.kind == 'restart':
            return 'restart-operations', self.operation_id
        return 'input-publication-operations', self.operation_id


@dataclass(frozen=True, slots=True)
class CleanupObservation:
    plans: tuple[CleanupPlan, ...]
    errors: tuple[str, ...]
    live_operations: tuple[str, ...]
    retained_material: tuple[str, ...]


def _json(row):
    return None if row is None else json.dumps(dict(row), sort_keys=True, separators=(',', ':'))


def _session(connection, session_id):
    if session_id is None:
        return None
    row = connection.execute('SELECT * FROM execution_sessions WHERE session_id=?', (session_id,)).fetchone()
    if row is None:
        raise RuntimeError('Cleanup owning session is missing')
    session = execution_session_from_row_snapshot(connection, row)
    validate_execution_session_snapshot(session)
    if row['status'] == 'running':
        require_admitted_reservation_scope(connection, session_id)
    return _json(row)


def _owner_live(owner):
    from .execution_sessions import ExecutionSessionStorageMixin as Validator
    for name in ('process_identity', 'hostname'):
        Validator._session_text(owner[name], name)
    Validator._session_time(owner['heartbeat_at'], 'heartbeat_at')
    if type(owner['owner_pid']) is not int or owner['owner_pid'] < 1:
        raise RuntimeError('Invalid cleanup owner process')
    return execution_session_liveness(dict(owner, status='running', pid=owner['owner_pid'],
                                           started_at=owner['heartbeat_at']))['live']


def _guards(connection, operation_id):
    return tuple(tuple(row) for row in connection.execute(
        'SELECT * FROM receiver_mutation_guards WHERE operation_id=? ORDER BY receiver_node', (operation_id,),
    ))


def _preparation_plan(connection, root, row, *, attempt_only=False):
    operation_id = row['operation_id']
    attempt = row if attempt_only else connection.execute(
        'SELECT * FROM preparation_attempts WHERE operation_id=?', (row['guard_id'],),
    ).fetchone()
    if attempt is None:
        raise RuntimeError('Cleanup preparation attempt is missing')
    checked_attempt_receipts(connection, attempt)
    if attempt['session_id'] != row['session_id']:
        raise RuntimeError('Preparation receipt has another session owner')
    session = _session(connection, attempt['session_id'])
    owner_live = _owner_live(dict(attempt))
    active = (attempt['state'] == 'preparing' and owner_live)
    if session is not None:
        active = active or execution_session_liveness(json.loads(session))['live']
    guards = _guards(connection, attempt['operation_id'])
    expected_receivers = json.loads(attempt['receivers_json'])
    affected_nodes = json.loads(attempt['affected_nodes_json'])
    for names in (expected_receivers, affected_nodes):
        if (not isinstance(names, list) or any(not isinstance(node, str) or not node for node in names)
                or names != sorted(set(names))):
            raise RuntimeError('Invalid preparation attempt receiver scope')
    preparing = attempt_only or row['state'] == 'prepared'
    if preparing:
        if attempt['state'] not in ('preparing', 'interrupted'):
            raise RuntimeError('Prepared receipt has a terminal preparation attempt')
        if [guard[0] for guard in guards] != expected_receivers:
            raise RuntimeError('Preparation attempt has missing or additional receiver guards')
        for guard in guards:
            if guard[2:] != (attempt['session_id'], attempt['owner_pid'], attempt['process_identity'], attempt['hostname']):
                raise RuntimeError('Preparation receiver guard has another owner')
    manifest = {} if attempt_only else json.loads(row['manifest_json'])
    receivers = tuple(affected_nodes) if attempt_only else tuple(manifest['receivers'])
    if not attempt_only and not set(receivers) <= set(affected_nodes):
        raise RuntimeError('Preparation receipt exceeds its attempt receiver scope')
    if not attempt_only:
        files = PreparationStaging(root, manifest)
        if files.relative != '.mwf/preparation-trash/' + operation_id:
            raise RuntimeError('Preparation receipt names another staging operation')
        if row['state'] == 'prepared':
            if not active:
                files.restoration_steps()
        else:
            files.require_private(cleanup=True)
    plan = CleanupPlan('attempt' if attempt_only else 'preparation', operation_id, _json(row),
                       json.dumps(manifest, sort_keys=True), session, _json(attempt), guards, None, receivers)
    return plan, active and (attempt_only or row['state'] == 'prepared')


def _input_plan(connection, root, row):
    from .input_cleanup_files import load_input_cleanup_files
    from types import SimpleNamespace

    manifest = json.loads(row['changes_json'])
    if not isinstance(manifest, dict):
        raise RuntimeError('Invalid native input cleanup manifest')
    owner = JobExecutionOwnerStorageMixin._read_execution_owner(connection, row['execution_id'])
    if owner is None:
        raise RuntimeError('Input cleanup producing execution is missing')
    from .input_publications import InputPublicationStorageMixin
    if manifest['producer'] != owner['node_name']:
        raise RuntimeError('Input cleanup manifest has another producing node')
    InputPublicationStorageMixin._validate_input_edge(connection, owner, row['receiver_node'])
    session = _session(connection, owner['session_id'])
    active = _owner_live(manifest['owner']) and execution_session_liveness(json.loads(session))['live']
    storage = SimpleNamespace(project_dir=root)
    files = load_input_cleanup_files(storage, row['receiver_node'], row['operation_id'], manifest)
    if row['state'] == 'prepared':
        if not active:
            files.preflight_restore()
    else:
        files.preflight_discard()
    return CleanupPlan('input', row['operation_id'], _json(row), json.dumps(manifest, sort_keys=True),
                       session, None, (), json.dumps(owner, sort_keys=True), (row['receiver_node'],)), active and row['state'] == 'prepared'


def _restart_plan(connection, root, row):
    manifest, files = validate_restart_manifest(root, dict(row))
    if row['state'] != 'prepared':
        validate_restart_terminal_decision(manifest, dict(row))
    addresses = tuple((item['node'], item['job_id']) for item in manifest['targets'])
    plan = CleanupPlan(
        'restart', row['operation_id'], _json(row),
        json.dumps(manifest, sort_keys=True, separators=(',', ':')),
        None, None, (), None, (), addresses,
    )
    return plan, files


def observe_native_cleanup(connection, root):
    plans, errors, live, retained = [], [], [], []
    known = {'preparation-trash': set(), 'input-publications': set()}
    for table, directory, reader in (
        ('preparation_receipts', 'preparation-trash', _preparation_plan),
        ('input_publications', 'input-publications', _input_plan),
    ):
        for row in connection.execute('SELECT * FROM ' + table + ' ORDER BY operation_id').fetchall():
            operation_id = row['operation_id']
            known[directory].add(operation_id)
            try:
                if not isinstance(operation_id, str) or not re.fullmatch(r'[0-9a-f]{32}', operation_id):
                    raise RuntimeError('Invalid cleanup operation identity')
                validate_file_receipt(table, row)
                path = recovery_path(root, '.mwf/' + directory + '/' + operation_id)
                if row['state'] != 'prepared' and not os.path.lexists(path):
                    continue
                plan, active = reader(connection, root, row)
                if active:
                    live.append(operation_id)
                else:
                    plans.append(plan)
            except (RuntimeError, ValueError, TypeError, KeyError, OSError) as error:
                errors.append(str(operation_id) + ': ' + str(error))
    for row in connection.execute('SELECT * FROM restart_receipts ORDER BY operation_id').fetchall():
        operation_id = row['operation_id']
        try:
            if not isinstance(operation_id, str) or not re.fullmatch(r'[0-9a-f]{32}', operation_id):
                raise RuntimeError('Invalid cleanup operation identity')
            plan, files = _restart_plan(connection, root, row)
            # The operation lock, acquired during recovery, owns content and
            # identity validation. Observation only decides whether terminal
            # private material may remain, so it cannot race an active discard.
            if row['state'] == 'prepared' or files.has_private_path():
                plans.append(plan)
        except (RuntimeError, ValueError, TypeError, KeyError, OSError) as error:
            errors.append(str(operation_id) + ': ' + str(error))
    for row in connection.execute(
        "SELECT * FROM preparation_attempts AS attempt WHERE state IN ('preparing','interrupted') "
        "AND NOT EXISTS (SELECT 1 FROM preparation_receipts WHERE guard_id=attempt.operation_id AND state='prepared') "
        'ORDER BY operation_id',
    ).fetchall():
        try:
            plan, active = _preparation_plan(connection, root, row, attempt_only=True)
            (live if active else plans).append(row['operation_id'] if active else plan)
        except (RuntimeError, ValueError, TypeError, KeyError, OSError) as error:
            errors.append(row['operation_id'] + ': ' + str(error))
    for directory, recorded in known.items():
        parent = recovery_path(root, '.mwf/' + directory)
        if parent.exists():
            for path in parent.iterdir():
                if path.name not in recorded:
                    retained.append(str(path))
    return CleanupObservation(tuple(plans), tuple(errors), tuple(live), tuple(retained))
