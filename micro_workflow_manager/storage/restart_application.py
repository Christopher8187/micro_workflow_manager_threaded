"""Apply one fenced manual restart with durable output recovery intent."""

from __future__ import annotations

from contextlib import ExitStack
from datetime import datetime
import json
import os
import warnings
from uuid import uuid4

from micro_workflow_manager.models import CANCELLED, FAILED, QUEUED, RUNNING
from micro_workflow_manager.session_liveness import execution_session_liveness

from .operation_lock import operation_lock
from .recovery_errors import add_recovery_note
from .restart_files import RestartFiles
from .restart_receipts import RestartReceipt, restart_revision


def _independent_target(storage, connection, node_name, job_id):
    observed = storage._read_job_owner_observation(connection, node_name, job_id)
    if observed is None:
        raise FileNotFoundError(f'Job does not exist: {node_name}/{job_id}')
    previous_status = observed['status']
    owner_live = (
        observed['session'] is not None
        and execution_session_liveness(observed['session'])['live']
    )
    if owner_live and previous_status in (RUNNING, FAILED, CANCELLED):
        raise RuntimeError(f'Job {node_name}/{job_id} owning-session restart state changed')
    if previous_status == RUNNING:
        owner = observed['owner']
        if owner is None or observed['active_execution_id'] != owner['execution_id']:
            raise RuntimeError(f'Job {node_name}/{job_id} has damaged abandoned execution ownership')
    elif observed['active_execution_id'] is not None:
        raise RuntimeError(f'Job {node_name}/{job_id} has inconsistent active execution ownership')
    return {
        'node': node_name,
        'job_id': job_id,
        'mode': 'independent',
        'owner': observed['owner'],
        'job_instance_id': observed['job_instance_id'],
        'generation': observed['generation'],
        'status': previous_status,
        'active_execution_id': observed['active_execution_id'],
    }


def _runtime_after_restart(raw, *, requested_at, reason, previous_generation, generation):
    runtime = json.loads(raw) if raw else None
    if runtime:
        runtime = {
            **runtime,
            'state': 'restarted',
            'updated_at': requested_at,
            'restart_reason': reason,
            'previous_generation': previous_generation,
            'generation': generation,
        }
    return None if runtime is None else json.dumps(
        runtime, ensure_ascii=False, separators=(',', ':'),
    )


def _apply_targets(storage, connection, targets, *, requested_at, requester, reason, owned):
    previous_revision = restart_revision(connection)
    restarted = []
    for target in targets:
        node, job_id = target['node'], target['job_id']
        generation = target['generation'] + 1
        row = connection.execute(
            'SELECT runtime_json FROM jobs WHERE node_name=? AND job_id=?', (node, job_id),
        ).fetchone()
        runtime = _runtime_after_restart(
            row['runtime_json'], requested_at=requested_at, reason=reason,
            previous_generation=target['generation'], generation=generation,
        )
        changed = connection.execute(
            "UPDATE jobs SET generation=?, active_execution_id=NULL, active_pid=NULL, "
            "active_thread_id=NULL, active_started_at=NULL, restart_requested_at=?, "
            "restart_requested_by_pid=?, restart_reason=?, status='queued', status_json='{}', "
            "runtime_json=? WHERE node_name=? AND job_id=? AND generation=? AND status=? "
            "AND active_execution_id IS ? AND EXISTS (SELECT 1 FROM job_instances AS i "
            "WHERE i.node_name=jobs.node_name AND i.job_id=jobs.job_id "
            "AND i.instance_id=? AND i.last_execution_id IS ?)",
            (
                generation, requested_at, requester, reason, runtime, node, job_id,
                target['generation'], target['status'], target['active_execution_id'],
                target['job_instance_id'],
                None if target['owner'] is None else target['owner']['execution_id'],
            ),
        ).rowcount
        if changed != 1:
            raise RuntimeError(f'Restart state changed for {node}/{job_id}')
        if owned:
            owner_data = {
                'session_id': target['owner']['session_id'],
                'execution_id': target['owner']['execution_id'],
                'job_instance_id': target['job_instance_id'],
                'component': list(target['owner']['component']),
            }
        else:
            owner_data = {}
        events = [
            ('queued', {**owner_data, 'previous_status': target['status'], 'status': QUEUED}),
            ('restart_requested', {
                **owner_data,
                'previous_generation': target['generation'],
                'generation': generation,
                'reason': reason,
                'requested_by_pid': requester,
            }),
        ]
        connection.executemany(
            'INSERT INTO job_events(node_name, job_id, time, event, data_json) VALUES(?,?,?,?,?)',
            [
                (node, job_id, requested_at, event, json.dumps(data, separators=(',', ':')))
                for event, data in events
            ],
        )
        if owned:
            restarted.append({
                'node': node,
                'job_id': job_id,
                'mode': target['mode'],
                'session_id': target['owner']['session_id'],
                'previous_generation': target['generation'],
                'generation': generation,
                'requested_at': requested_at,
                'warnings': [],
            })
        else:
            restarted.append({
                'node': node,
                'job_id': job_id,
                'previous_generation': target['generation'],
                'generation': generation,
                'requested_at': requested_at,
            })
    if owned:
        connection.executemany(
            'INSERT INTO nodes(node_name, status) VALUES(?, ?) '
            'ON CONFLICT(node_name) DO UPDATE SET status=excluded.status, '
            'updated_at=CURRENT_TIMESTAMP WHERE nodes.status IS NOT excluded.status',
            [(node, RUNNING) for node in sorted({target['node'] for target in targets})],
        )
    storage._increment_job_restart_revision(connection)
    return restarted, previous_revision


def _apply_operation(
    storage, targets, validate, *, requested_by_pid, reason, component_plan, owned,
):
    targets = sorted(targets, key=lambda target: (target['node'], target['job_id']))
    addresses = [(target['node'], target['job_id']) for target in targets]
    operation_id = uuid4().hex
    requested_at = datetime.now().isoformat(timespec='milliseconds' if owned else 'seconds')
    requester = os.getpid() if requested_by_pid is None else requested_by_pid
    if type(requester) is not int or requester < 1:
        raise ValueError('requested_by_pid must be an integer >= 1')
    receipt = RestartReceipt.create(
        storage, operation_id, targets, requested_at=requested_at,
        requested_by_pid=requester, reason=reason, component_plan=component_plan,
        owned=owned,
    )
    restarted = []
    with operation_lock(storage, 'restart-operations', operation_id):
        with ExitStack() as fences:
            for node, job_id in addresses:
                fences.enter_context(storage.filesystem_interprocess_lock(
                    'execution-fences', storage.job_execution_lock_name(node, job_id),
                ))
            current = validate(storage.db_connection())
            if current != targets:
                raise RuntimeError('Restart ownership or state changed after preflight')
            files = RestartFiles.capture(storage, operation_id, targets)
            try:
                receipt.prepare(files, validate)
                files.stage()

                def commit(connection):
                    receipt.require_prepared(connection)
                    files.require_staged()
                    current_targets = validate(connection)
                    if current_targets != targets:
                        raise RuntimeError('Restart ownership or state changed before commit')
                    results, previous_revision = _apply_targets(
                        storage, connection, targets, requested_at=requested_at,
                        requester=requester, reason=reason, owned=owned,
                    )
                    restarted.extend(results)
                    receipt.commit(
                        connection, targets, previous_revision=previous_revision,
                    )

                commit_warnings = []
                receipt.submit_commit(commit, commit_warnings)
                if commit_warnings:
                    if owned:
                        restarted[0]['warnings'].extend(commit_warnings)
                    else:
                        for message in commit_warnings:
                            warnings.warn(message, RuntimeWarning)
            except BaseException as error:
                if receipt.expected is None:
                    raise
                try:
                    state = receipt.state()
                except BaseException as state_error:
                    add_recovery_note(
                        error, 'Restart receipt state unavailable; outputs retained: ' + str(state_error),
                    )
                    raise error
                try:
                    if state != 'committed':
                        files.restore()
                        if state == 'prepared':
                            receipt.abort()
                    files.discard()
                except BaseException as cleanup_error:
                    add_recovery_note(error, 'Restart outputs require recovery: ' + str(cleanup_error))
                    for note in getattr(cleanup_error, '__notes__', ()):
                        add_recovery_note(error, note)
                raise
            else:
                try:
                    files.discard()
                except (OSError, RuntimeError) as error:
                    message = 'Restart committed; staged output cleanup requires attention: ' + str(error)
                    if owned:
                        restarted[0]['warnings'].append(message)
                    else:
                        warnings.warn(message, RuntimeWarning)
        if owned:
            for node in sorted({node for node, _ in addresses}):
                try:
                    storage.notify_queue_change(node)
                except Exception as error:
                    restarted[0]['warnings'].append(
                        'Restart committed; scheduler notification requires attention: ' + str(error),
                    )
        return restarted


def apply_owned_restarts(storage, targets, *, requested_by_pid, reason, component_plan):
    targets = list(targets)
    addresses = [
        (storage.validate_node_name(target['node']), storage.validate_job_id(target['job_id']))
        for target in targets
    ]
    if len(set(addresses)) != len(addresses):
        raise ValueError('Restart selection contains duplicate jobs')
    if not targets:
        if component_plan is None:
            return []
        current = storage._read_component_restart_plan(
            storage.db_connection(), component_plan['node'],
            failed_only=component_plan['failed_only'],
        )
        if current != component_plan:
            raise RuntimeError('Restart component selection or ownership changed after preflight')
        return []
    if len({target['owner']['session_id'] for target in targets}) != 1:
        raise RuntimeError('Restart selection must have one owning session')
    expected = sorted(targets, key=lambda target: (target['node'], target['job_id']))

    def validate(connection):
        if component_plan is not None:
            current_plan = storage._read_component_restart_plan(
                connection, component_plan['node'], failed_only=component_plan['failed_only'],
            )
            if (
                current_plan != component_plan
                or sorted(
                    current_plan['targets'], key=lambda target: (target['node'], target['job_id']),
                ) != expected
            ):
                raise RuntimeError('Restart component selection or ownership changed after preflight')
        current = [
            storage._read_owned_restart_target(connection, target['node'], target['job_id'])
            for target in expected
        ]
        if current != expected:
            raise RuntimeError('Restart ownership or state changed after preflight')
        return current

    results = _apply_operation(
        storage, expected, validate, requested_by_pid=requested_by_pid,
        reason=reason, component_plan=component_plan, owned=True,
    )
    by_address = {(result['node'], result['job_id']): result for result in results}
    ordered = [by_address[address] for address in addresses]
    messages = [message for result in results for message in result['warnings']]
    for result in ordered:
        result['warnings'] = []
    ordered[0]['warnings'].extend(messages)
    return ordered


def apply_independent_restart(storage, node_name, job_id, *, requested_by_pid, reason):
    expected = _independent_target(storage, storage.db_connection(), node_name, job_id)

    def validate(connection):
        current = _independent_target(storage, connection, node_name, job_id)
        if current != expected:
            raise RuntimeError(f'Restart state changed for {node_name}/{job_id}')
        return [current]

    return _apply_operation(
        storage, [expected], validate, requested_by_pid=requested_by_pid,
        reason=reason, component_plan=None, owned=False,
    )[0]
