"""Restore abandoned file operations before ending their durable receipts."""

from __future__ import annotations

from contextlib import ExitStack
from datetime import datetime, timezone
import json
import sqlite3

from .execution_ownership import JobExecutionOwnerStorageMixin
from .native_cleanup_observation import _json, _guards, _owner_live
from .operation_lock import (
    OperationStillActive,
    operation_lock,
    recovery_advisory_lock,
    recovery_job_fence,
)
from .preparation_receipts import submit_preparation_decision
from .preparation_staging import PreparationStaging
from .file_operation_receipts import validate_file_receipt, finish_file_receipt
from .preparation_attempts import checked_attempt_receipts, attempt_outcome, _require_exact_attempt
from .session_scope import require_admitted_reservation_scope
from .restart_receipts import (
    require_restart_pre_state,
    require_restart_terminal,
    validate_restart_manifest,
)
from micro_workflow_manager.session_liveness import execution_session_liveness


_TABLES = {
    'preparation': 'preparation_receipts',
    'input': 'input_publications',
    'attempt': 'preparation_attempts',
    'restart': 'restart_receipts',
}


def require_cleanup_unchanged(storage, connection, plan):
    table = _TABLES[plan.kind]
    row = connection.execute('SELECT * FROM ' + table + ' WHERE operation_id=?', (plan.operation_id,)).fetchone()
    if _json(row) != plan.receipt_json:
        raise RuntimeError('Cleanup receipt changed: ' + plan.operation_id)
    if plan.kind != 'attempt':
        validate_file_receipt(table, row)
    if plan.kind == 'restart':
        manifest, files = validate_restart_manifest(storage.project_dir, dict(row))
        if row['state'] == 'prepared':
            require_restart_pre_state(connection, manifest)
            files.restoration_steps()
        else:
            require_restart_terminal(connection, storage.project_dir, dict(row))
            files.require_private(cleanup=True)
        return
    preparing = plan.kind == 'attempt' or row['state'] == 'prepared'
    if plan.attempt_json is not None:
        expected = json.loads(plan.attempt_json)
        checked_attempt_receipts(connection, expected)
        attempt = connection.execute('SELECT * FROM preparation_attempts WHERE operation_id=?',
                                     (expected['operation_id'],)).fetchone()
        if preparing and (_json(attempt) != plan.attempt_json or _guards(connection, expected['operation_id']) != plan.guards):
            raise RuntimeError('Cleanup preparation attempt or guards changed')
        if preparing and attempt['state'] == 'preparing' and _owner_live(dict(attempt)):
            raise RuntimeError('Cleanup preparation owner is active again')
    if plan.execution_owner_json is not None:
        owner = JobExecutionOwnerStorageMixin._read_execution_owner(connection, row['execution_id'])
        if json.dumps(owner, sort_keys=True) != plan.execution_owner_json:
            raise RuntimeError('Cleanup input producing owner changed')
        from .input_publications import InputPublicationStorageMixin
        InputPublicationStorageMixin._validate_input_edge(connection, owner, row['receiver_node'])
    if plan.session_json is not None:
        expected = json.loads(plan.session_json)
        session = connection.execute('SELECT * FROM execution_sessions WHERE session_id=?',
                                     (expected['session_id'],)).fetchone()
        if preparing and _json(session) != plan.session_json:
            raise RuntimeError('Cleanup session changed after observation')
        if preparing and session['status'] == 'running':
            require_admitted_reservation_scope(connection, expected['session_id'])
        if preparing and execution_session_liveness(dict(session))['live']:
            raise RuntimeError('Cleanup owning session is still active')


def load_cleanup_files(storage, plan):
    if plan.kind == 'preparation':
        return PreparationStaging(storage.project_dir, plan.manifest)
    if plan.kind == 'input':
        from .input_cleanup_files import load_input_cleanup_files
        return load_input_cleanup_files(storage, plan.receipt['receiver_node'], plan.operation_id, plan.manifest)
    if plan.kind == 'restart':
        return validate_restart_manifest(storage.project_dir, plan.receipt)[1]
    return None


def finish_cleanup_receipt(storage, plan, state):
    if state != 'aborted':
        raise ValueError('Prepared cleanup must finish as aborted')

    def finish(connection):
        require_cleanup_unchanged(storage, connection, plan)
        files = load_cleanup_files(storage, plan)
        if files is not None:
            files.require_restored()
        if plan.kind != 'attempt':
            finish_file_receipt(connection, _TABLES[plan.kind], plan.receipt, 'aborted', {'restored': True})
            if plan.kind == 'restart':
                row = connection.execute(
                    'SELECT * FROM restart_receipts WHERE operation_id=?',
                    (plan.operation_id,),
                ).fetchone()
                require_restart_terminal(connection, storage.project_dir, dict(row))
        if plan.attempt_json is not None:
            attempt_id = json.loads(plan.attempt_json)['operation_id']
            pending = connection.execute(
                "SELECT 1 FROM preparation_receipts WHERE guard_id=? AND state='prepared' LIMIT 1", (attempt_id,),
            ).fetchone()
            if pending is None:
                expected = json.loads(plan.attempt_json)
                outcome = attempt_outcome(connection, expected)
                receipts, _ = checked_attempt_receipts(connection, expected)
                if outcome == 'interrupted':
                    raise RuntimeError('Cleanup attempt still requires prepared file recovery')
                count = connection.execute('DELETE FROM receiver_mutation_guards WHERE operation_id=?',
                                           (attempt_id,)).rowcount
                if count != len(plan.guards):
                    raise RuntimeError('Cleanup receiver guard count changed before release')
                timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
                changed = connection.execute(
                    "UPDATE preparation_attempts SET state=?, finished_at=? WHERE operation_id=? "
                    "AND state IN ('preparing','interrupted')",
                    (outcome, timestamp, attempt_id),
                ).rowcount
                if changed != 1:
                    raise RuntimeError('Cleanup attempt changed before retirement')
                expected = dict(expected, state=outcome, finished_at=timestamp)
                _require_exact_attempt(connection, expected, ())
                observed, _ = checked_attempt_receipts(connection, expected)
                if observed != receipts or attempt_outcome(connection, expected) != outcome:
                    raise RuntimeError('Cleanup receipts changed during attempt retirement')
                return outcome if plan.kind == 'attempt' else 'aborted'
        return 'aborted'
    return submit_preparation_decision(storage, finish)


def recover_cleanup(storage, observation, finish_receipt):
    recovered, errors = [], list(observation.errors)
    live = list(observation.live_operations)
    for plan in observation.plans:
        try:
            with ExitStack() as locks:
                try:
                    locks.enter_context(operation_lock(storage, *plan.lock_identity))
                except OperationStillActive:
                    live.append(plan.operation_id)
                    continue
                for node, job_id in plan.job_fences:
                    locks.enter_context(recovery_job_fence(storage, node, job_id))
                if plan.kind != 'restart':
                    if plan.execution_owner_json is not None:
                        owner = json.loads(plan.execution_owner_json)
                        locks.enter_context(recovery_job_fence(
                            storage, owner['node_name'], owner['job_id'],
                        ))
                    locks.enter_context(recovery_advisory_lock(storage, 'active-run-state'))
                    for receiver in sorted(plan.receivers):
                        locks.enter_context(recovery_advisory_lock(storage, f'node-{receiver}-input'))
                        locks.enter_context(recovery_advisory_lock(storage, f'node-{receiver}-jobs'))
                require_cleanup_unchanged(storage, storage.db_connection(), plan)
                files = load_cleanup_files(storage, plan)
                preparing = plan.kind == 'attempt' or plan.receipt['state'] == 'prepared'
                outcome = plan.receipt['state']
                if preparing:
                    if files is not None:
                        files.restore()
                    outcome = finish_receipt(storage, plan, 'aborted')
                if files is not None:
                    files.discard()
                recovered.append({'operation_id': plan.operation_id,
                                  'state': outcome})
        except (RuntimeError, ValueError, TypeError, KeyError, OSError, sqlite3.Error) as error:
            errors.append(plan.operation_id + ': ' + str(error))
    return {
        'cleanup': recovered,
        'errors': errors,
        'live_operations': tuple(sorted(set(live))),
    }
