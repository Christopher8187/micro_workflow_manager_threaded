"""Finish interrupted recovery receipts before observing fresh session work."""

from contextlib import ExitStack
import json
import os
import sqlite3

from .native_recovery_files import StagedRecoveryFiles
from .native_recovery_observation import require_session_recovery_unchanged
from .native_recovery_records import read_receipt, recorded_plan, finish_receipt
from .operation_lock import operation_lock, session_recovery_lock, recovery_job_fence, recovery_advisory_lock
from .preparation_receipts import submit_preparation_decision
from .recovery_output import recovery_path


def observe_recovery_receipts(connection, root):
    rows, errors = [], []
    for row in connection.execute('SELECT * FROM recovery_receipts ORDER BY operation_id'):
        row = dict(row)
        try:
            path = recovery_path(root, '.mwf/recovery-trash/' + row['operation_id'])
            if row['state'] == 'prepared' or os.path.lexists(path):
                rows.append(row)
        except (RuntimeError, ValueError, TypeError, OSError) as error:
            errors.append(str(row['operation_id']) + ': ' + str(error))
    return {'rows': rows, 'errors': errors}


def recover_receipts(storage, observation):
    recovered, errors = [], list(observation['errors'])
    for row in observation['rows']:
        operation_id = row['operation_id']
        try:
            with ExitStack() as locks:
                locks.enter_context(session_recovery_lock(storage, row['session_id']))
                locks.enter_context(operation_lock(storage, 'recovery-operations', operation_id))
                current = read_receipt(storage.db_connection(), operation_id)
                if current != row:
                    raise RuntimeError('Recovery receipt changed after observation')
                plan = recorded_plan(row)
                files = StagedRecoveryFiles.reopen(storage.project_dir, plan, operation_id, json.loads(row['manifest_json']))
                if row['state'] == 'prepared':
                    for job in sorted(plan.jobs, key=lambda item: (item.node, item.job_id)):
                        locks.enter_context(recovery_job_fence(storage, job.node, job.job_id))
                    locks.enter_context(recovery_advisory_lock(storage, 'active-run-state'))
                    require_session_recovery_unchanged(storage.db_connection(), plan)
                    files.restore()

                    def abort(connection):
                        require_session_recovery_unchanged(connection, plan)
                        files.require_restored()
                        finish_receipt(connection, row, plan, 'aborted')
                    submit_preparation_decision(storage, abort)
                # Revalidate the exact terminal decision immediately before deleting
                # any saved material; a state value alone never authorizes cleanup.
                read_receipt(storage.db_connection(), operation_id)
                files.discard()
                recovered.append({'operation_id': operation_id,
                                  'state': 'aborted' if row['state'] == 'prepared' else row['state']})
        except (RuntimeError, ValueError, TypeError, KeyError, OSError, sqlite3.Error) as error:
            errors.append(operation_id + ': ' + str(error))
    return {'cleanup': recovered, 'errors': errors}
