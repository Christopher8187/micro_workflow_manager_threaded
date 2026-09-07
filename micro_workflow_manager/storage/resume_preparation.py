"""Requeue unsuccessful work and its native lifecycle in one guarded decision."""

import json
import os
from contextlib import ExitStack
from datetime import datetime
from uuid import uuid4

from ..component_identity import encode_component_key
from .job_preparation import NodeJobPreparation, _read_jobs
from .execution_terminal import TerminalUpdate
from .preparation_files import stage_preparation_files
from .preparation_guards import refuse_receiver_mutation
from .preparation_receipts import PreparationReceipt, submit_preparation_decision


def _read_resume_successful_result(storage, connection, component, state):
    identity = storage._read_component_producing_identity(connection, component)
    if state['lifecycle'] in ('sampled', 'done'):
        current = tuple(state[name] for name in ('lifecycle', 'stability', 'instability_origin'))
        return storage._match_component_successful_result(connection, component, identity, current)
    retained = storage._read_component_successful_result(connection, component, identity)
    if state['lifecycle'] == 'queued' and retained is not None:
        raise RuntimeError('Queued component unexpectedly retains success at its current alignment')
    return retained


def _validate_resume_job(storage, connection, node, job, component):
    observation = storage._read_job_owner_observation(connection, node, job.job_id)
    if job.status == 'failed' and observation['owner'] is None:
        raise RuntimeError(f'Resume requires an exact owner for failed job {node}/{job.job_id}')
    if (job.status in ('failed', 'cancelled') and observation['owner'] is not None
            and observation['owner']['component'] != component):
        raise RuntimeError(f'Resume owner has a different producing component: {node}/{job.job_id}')
    if job.status == 'running':
        if any(value is None for value in (job.active_execution_id, job.active_pid,
                                           job.active_thread_id, job.active_started_at)):
            raise RuntimeError(f'Resume requires complete active metadata for {node}/{job.job_id}')
        owner = observation['owner']
        if (owner is None or observation['session']['status'] != 'terminal'
                or job.active_execution_id != owner['execution_id']):
            raise RuntimeError(f'Resume requires an abandoned exact owner: {node}/{job.job_id}')
        producing_identity = storage._read_component_producing_identity(connection, component)
        if (owner['component'] != component
                or (owner['shape_id'], owner['alignment_generation']) != producing_identity):
            raise RuntimeError(f'Resume owner has a different producing component: {node}/{job.job_id}')
    elif any(value is not None for value in (job.active_execution_id, job.active_pid,
                                              job.active_thread_id, job.active_started_at)):
        raise RuntimeError(f'Resume cannot change active job {node}/{job.job_id}')


def validate_resume_job_owners(storage, states):
    """Reject damaged or incompatible abandoned work before admitting a session."""
    connection = storage.db_connection()
    connection.execute('SAVEPOINT mwf_resume_owner_observation')
    try:
        successful_results = {}
        for component, state in states.items():
            if storage._read_component_state(connection, component) != state:
                raise RuntimeError('Component changed during resume preflight: ' + repr(component))
            successful_results[component] = _read_resume_successful_result(
                storage, connection, component, state,
            )
            for node in component:
                for job in _read_jobs(connection, node):
                    _validate_resume_job(storage, connection, node, job, component)
        return successful_results
    finally:
        connection.execute('RELEASE SAVEPOINT mwf_resume_owner_observation')


def _read_finished_outputs(storage, captured):
    recovered = {}
    for node, jobs in captured.items():
        for job in jobs:
            if job.status != 'running':
                continue
            path = storage.output_file(node, job.job_id)
            output = storage.read_json(path, default=None)
            if (not isinstance(output, dict) or output.get('status') not in ('done', 'skipped', 'failed', 'cancelled')
                    or output.get('generation') != job.generation
                    or output.get('execution_id') != job.active_execution_id):
                continue
            try:
                finished_at = datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec='milliseconds')
            except OSError:
                finished_at = datetime.now().isoformat(timespec='milliseconds')
            recovered[node, job.job_id] = TerminalUpdate(
                node, job.job_id, job.generation, job.active_execution_id, output['status'],
                {'started_at': job.active_started_at, 'finished_at': finished_at,
                 'generation': job.generation, 'execution_id': job.active_execution_id, 'recovered_from_output': True},
            )
    return recovered


def prepare_resume_components(
    storage, session_id, expected_states, *, expected_parents=None,
    expected_successful_results=None, clear_trace_nodes=(),
):
    storage._require_execution_session_storage()
    storage._session_text(session_id, 'session_id')
    expected = {storage._session_component(component): dict(state) for component, state in expected_states.items()}
    if not expected:
        raise ValueError('Resume requires a nonempty component selection')
    nodes = tuple(node for component in expected for node in component)
    components_by_node = {node: component for component in expected for node in component}
    trace_nodes = tuple(sorted(set(clear_trace_nodes)))
    if set(trace_nodes) - set(nodes):
        raise ValueError('Resume trace selection must belong to the admitted components')
    parents = {storage._session_component(component): dict(state)
               for component, state in (expected_parents or {}).items()}
    captured = {}
    retained_results = None
    if expected_successful_results is not None:
        retained_results = {
            storage._session_component(component): None if result is None else tuple(result)
            for component, result in expected_successful_results.items()
        }
        if set(retained_results) != set(expected):
            raise ValueError('Resume successful-result observations must match selected components')

    def validate(connection, *, initial=False):
        for component, state in expected.items():
            key = encode_component_key(component)
            owner = connection.execute(
                'SELECT session.status, selected.component_key, reservation.session_id AS owner '
                'FROM execution_sessions AS session LEFT JOIN session_components AS selected '
                'ON selected.session_id=session.session_id AND selected.component_key=? '
                'LEFT JOIN component_reservations AS reservation ON reservation.component_key=? '
                'WHERE session.session_id=?', (key, key, session_id),
            ).fetchone()
            if (owner is None or owner['status'] != 'running' or owner['component_key'] is None
                    or owner['owner'] != session_id or storage._read_session_job_roots(connection, session_id)):
                raise RuntimeError('Resume requires its exact full component owner and reservation: ' + key)
            current = storage._read_component_state(connection, component)
            if current != state or current['misaligned'] or current['lifecycle'] == 'running':
                raise RuntimeError('Component changed before resume preparation: ' + repr(component))
            retained = _read_resume_successful_result(storage, connection, component, current)
            if retained_results is not None and retained_results[component] != retained:
                raise RuntimeError('Retained component result changed before resume preparation')
            if retained_results is None:
                if component in observed_results and observed_results[component] != retained:
                    raise RuntimeError('Retained component result changed before resume preparation')
                observed_results.setdefault(component, retained)
            if connection.execute(
                'SELECT 1 FROM pending_component_executions WHERE component_key=? UNION ALL '
                'SELECT 1 FROM component_holds WHERE component_key=? LIMIT 1', (key, key),
            ).fetchone():
                raise RuntimeError('Resume requires recovery of a held or pending component: ' + key)
        for component, state in parents.items():
            if storage._read_component_state(connection, component) != state:
                raise RuntimeError('Component parent changed before resume preparation: ' + repr(component))
        for node in nodes:
            if initial:
                refuse_receiver_mutation(connection, node)
                storage._require_settled_input_publications(connection, node)
            jobs = _read_jobs(connection, node)
            if node in captured and jobs != captured[node]:
                raise RuntimeError('Jobs changed before resume preparation: ' + node)
            for job in jobs:
                _validate_resume_job(storage, connection, node, job, components_by_node[node])
            captured[node] = jobs
        for component, state in expected.items():
            if state['lifecycle'] == 'done' and any(
                job.status not in ('done', 'skipped') for node in component for job in captured[node]
            ):
                raise RuntimeError('Successful component contains unfinished work: ' + repr(component))

    connection = storage.db_connection()
    observed_results = {}
    connection.execute('SAVEPOINT mwf_resume_preparation')
    try:
        validate(connection, initial=True)
    finally:
        connection.execute('RELEASE SAVEPOINT mwf_resume_preparation')
    def needs_restart(node, job):
        update = recovered.get((node, job.job_id))
        status = job.status if update is None else update.status
        return status in ('failed', 'cancelled', 'running')

    def commit(connection):
        validate(connection)
        connection.executemany('DELETE FROM job_events WHERE node_name=?', [(node,) for node in trace_nodes])
        for succeeded, error in storage._apply_terminal_updates(connection, list(recovered.values())):
            if not succeeded:
                raise error
        now = datetime.now().isoformat(timespec='milliseconds')
        changed_jobs = 0
        for plan in plans:
            for job in plan.jobs:
                if job.job_id not in plan.reset_ids:
                    continue
                changed = connection.execute(
                    "UPDATE jobs SET status='queued', status_json='{}', runtime_json=NULL, generation=generation+1, "
                    'active_execution_id=NULL, active_pid=NULL, active_thread_id=NULL, active_started_at=NULL, '
                    'restart_requested_at=?, restart_requested_by_pid=?, restart_reason=? '
                    'WHERE node_name=? AND job_id=? AND generation=?',
                    (now, os.getpid(), 'resume unsuccessful job', plan.node, job.job_id, job.generation),
                ).rowcount
                if changed != 1:
                    raise RuntimeError(f'Job changed before resume: {plan.node}/{job.job_id}')
                update = recovered.get((plan.node, job.job_id))
                previous_status = job.status if update is None else update.status
                events = (
                    ('queued', {'previous_status': previous_status, 'status': 'queued'}),
                    ('restart_requested', {'previous_generation': job.generation, 'generation': job.generation + 1,
                                           'reason': 'resume unsuccessful job', 'requested_by_pid': os.getpid()}),
                )
                connection.executemany(
                    'INSERT INTO job_events(node_name, job_id, time, event, data_json) VALUES(?,?,?,?,?)',
                    [(plan.node, job.job_id, now, event, json.dumps(data, separators=(',', ':')))
                     for event, data in events],
                )
                changed_jobs += 1
        for component, state in expected.items():
            if state['lifecycle'] == 'failed':
                retained = (observed_results if retained_results is None else retained_results)[component]
                restored = retained if retained is not None and retained[0] == 'sampled' else None
                changed = connection.execute(
                    'UPDATE component_states SET lifecycle=?, stability=?, instability_origin=? '
                    "WHERE component_key=? AND lifecycle='failed' "
                    'AND misaligned=0 AND alignment_generation=? AND stability IS NULL AND instability_origin IS NULL',
                    (restored[0] if restored is not None else 'queued',
                     restored[1] if restored is not None else None,
                     restored[2] if restored is not None else None,
                     encode_component_key(component), state['alignment_generation']),
                ).rowcount
                if changed != 1:
                    raise RuntimeError('Component changed before resume: ' + repr(component))
            for node in component:
                if state['lifecycle'] == 'failed' or any(job.status == 'queued' for job in captured[node]):
                    connection.execute("UPDATE nodes SET status='queued' WHERE node_name=?", (node,))
        if changed_jobs:
            storage._increment_job_restart_revision(connection)
        receipt.commit(connection)
        return changed_jobs

    with ExitStack() as locks:
        for node in sorted(captured):
            for job in captured[node]:
                if job.status in ('failed', 'cancelled', 'running'):
                    locks.enter_context(storage.filesystem_interprocess_lock(
                        'execution-fences', storage.job_execution_lock_name(node, job.job_id),
                    ))
        for node in sorted(nodes):
            locks.enter_context(storage.interprocess_lock(f'node-{node}-input'))
            locks.enter_context(storage.interprocess_lock(f'node-{node}-jobs'))
        validate(connection, initial=True)
        recovered = _read_finished_outputs(storage, captured)
        plans = tuple(NodeJobPreparation(
            node, captured[node], (), (), tuple(job.job_id for job in captured[node]
                                               if needs_restart(node, job)), (), False, False,
        ) for node in nodes)
        receipt = PreparationReceipt(storage, uuid4().hex, 'resume', next(iter(expected)), session_id,
                                     lambda connection: validate(connection, initial=True), [])
        with stage_preparation_files(storage.project_dir, plans, receipt=receipt):
            changed = submit_preparation_decision(storage, commit)
    storage.notify_queue_changes(nodes)
    return changed
