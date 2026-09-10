"""Apply independently revalidated native session recovery decisions."""

from __future__ import annotations

from contextlib import ExitStack
import json
import sqlite3

from .execution_terminal import TerminalOwnerExpectation, TerminalUpdate
from .native_recovery_files import require_recovery_outputs_unchanged, stage_session_recovery_files
from .native_recovery_observation import require_session_recovery_unchanged
from .native_recovery_receipts import RecoveryReceipt
from .operation_lock import operation_lock, session_recovery_lock, recovery_job_fence, recovery_advisory_lock
from .native_recovery_records import require_resolved_recovery_receipts
from .preparation_receipts import submit_preparation_decision
from .session_settlement import settle_execution_session


def _recovery_events(connection, session_id, job, now, reason):
    data = {'session_id': session_id, 'execution_id': job.execution_id,
            'job_instance_id': job.job_instance_id, 'component': job.component,
            'previous_generation': job.generation, 'generation': job.generation + 1,
            'reason': reason, 'previous_status': 'running', 'status': 'queued'}
    connection.executemany(
        'INSERT INTO job_events(node_name, job_id, time, event, data_json) VALUES(?,?,?,?,?)',
        [(job.node, job.job_id, now, event, json.dumps(data, separators=(',', ':')))
         for event in ('recovered', 'queued')],
    )


def _commit_session_recovery(storage, plan, receipt):
    def commit(connection):
        receipt.require_prepared(connection)
        require_session_recovery_unchanged(connection, plan)
        receipt.files.require_staged()
        terminal = [TerminalUpdate(
            job.node, job.job_id, job.generation, job.execution_id, job.terminal_status,
            {'execution_id': job.execution_id, 'generation': job.generation, 'recovered_from_output': True},
            TerminalOwnerExpectation(plan.session_id, job.component, job.job_instance_id),
        ) for job in plan.jobs if job.terminal_status is not None]
        for succeeded, error in storage._apply_terminal_updates(connection, terminal):
            if not succeeded:
                raise error
        requeued = 0
        for job in plan.jobs:
            if job.terminal_status is not None:
                connection.execute(
                    'UPDATE jobs SET restart_requested_at=NULL, restart_requested_by_pid=NULL, restart_reason=NULL '
                    'WHERE node_name=? AND job_id=?', (job.node, job.job_id),
                )
                continue
            raw = connection.execute('SELECT runtime_json FROM jobs WHERE node_name=? AND job_id=?',
                                     (job.node, job.job_id)).fetchone()[0]
            runtime = None if raw is None else json.loads(raw)
            if runtime is not None:
                if not isinstance(runtime, dict):
                    raise RuntimeError(f'Damaged recovery runtime: {job.node}/{job.job_id}')
                runtime.update(state='recovered', recovered_at=receipt.prepared_at,
                               recovery_reason=plan.reason)
            changed = connection.execute(
                "UPDATE jobs SET status='queued', status_json='{}', generation=generation+1, runtime_json=?, "
                'active_execution_id=NULL, active_pid=NULL, active_thread_id=NULL, active_started_at=NULL, '
                'restart_requested_at=NULL, restart_requested_by_pid=NULL, restart_reason=NULL '
                "WHERE node_name=? AND job_id=? AND status='running' AND generation=? AND active_execution_id=?",
                (None if runtime is None else json.dumps(runtime, separators=(',', ':')),
                 job.node, job.job_id, job.generation, job.execution_id),
            ).rowcount
            if changed != 1:
                raise RuntimeError(f'Recovery job changed: {job.node}/{job.job_id}')
            _recovery_events(connection, plan.session_id, job, receipt.prepared_at, plan.reason)
            requeued += 1
        if requeued:
            storage._increment_job_restart_revision(connection)
        interrupt = connection.execute(
            'SELECT 1 FROM interrupt_admissions WHERE session_id=?', (plan.session_id,),
        ).fetchone()
        if interrupt is None:
            held = connection.execute(
                'SELECT COUNT(*) FROM component_holds WHERE session_id=?',
                (plan.session_id,),
            ).fetchone()[0]
            if connection.execute(
                'DELETE FROM component_holds WHERE session_id=?', (plan.session_id,),
            ).rowcount != held:
                raise RuntimeError('Recovery component holds changed: ' + plan.session_id)
        settle_execution_session(
            storage, connection, plan.session_id, outcome='failed', finished_at=receipt.prepared_at,
            failure_data=json.dumps([{'reason': 'abandoned execution session', 'detail': plan.reason}]),
            component_outcomes=plan.outcomes,
        )
        receipt.commit(connection)
        return {'session_id': plan.session_id, 'requeued': requeued, 'terminal': len(terminal)}
    return submit_preparation_decision(storage, commit)


def recover_observed_sessions(storage, observation):
    recovered, errors = [], list(observation.errors)
    errors.extend(session_id + ': ' + '; '.join(messages) for session_id, messages in observation.refusals)
    for plan in observation.sessions:
        try:
            with ExitStack() as locks:
                locks.enter_context(session_recovery_lock(storage, plan.session_id))
                require_resolved_recovery_receipts(storage.db_connection(), plan.session_id, storage.project_dir)
                for job in sorted(plan.jobs, key=lambda job: (job.node, job.job_id)):
                    locks.enter_context(recovery_job_fence(storage, job.node, job.job_id))
                locks.enter_context(recovery_advisory_lock(storage, 'active-run-state'))
                require_session_recovery_unchanged(storage.db_connection(), plan)
                require_recovery_outputs_unchanged(storage.project_dir, plan)
                receipt = RecoveryReceipt(storage, plan)
                locks.enter_context(operation_lock(storage, 'recovery-operations', receipt.operation_id))
                with stage_session_recovery_files(storage, plan, receipt) as files:
                    files.require_staged()
                    recovered.append(_commit_session_recovery(storage, plan, receipt))
        except (RuntimeError, ValueError, TypeError, OSError, sqlite3.Error) as error:
            errors.append(plan.session_id + ': ' + str(error))
    return {'recovered': recovered, 'errors': errors, 'live_sessions': observation.live_sessions}


def _finish_cleanup_receipt(storage, plan, state):
    from .native_cleanup import finish_cleanup_receipt
    return finish_cleanup_receipt(storage, plan, state)


def recover_observed_cleanup(storage, observation):
    from .native_cleanup import recover_cleanup
    return recover_cleanup(storage, observation, _finish_cleanup_receipt)
