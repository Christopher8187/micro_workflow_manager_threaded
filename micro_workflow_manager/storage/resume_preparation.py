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
from .preparation_footprint import _SnapshotPreparationStorage
from .unproduced_membership import read_execution_component_states, read_unproduced_component_states
from .component_result_identity import (
    SuccessfulResultObservation,
    observe_current_success,
    read_admitted_component_shape,
    read_component_state_record,
)


def _read_resume_successful_result(connection, component, state):
    recorded = read_component_state_record(connection, component)
    if recorded is None or recorded.snapshot != state:
        raise RuntimeError('Component changed during retained-result observation')
    return observe_current_success(connection, recorded)


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


def validate_resume_job_owners(storage, states, *, expected_shape):
    """Reject damaged or incompatible abandoned work before admitting a session."""
    from .component_definitions import component_snapshot_from_shape

    connection = storage.db_connection()
    connection.execute('SAVEPOINT mwf_resume_owner_observation')
    try:
        observed = read_execution_component_states(
            connection, states, expected_shape=expected_shape,
        )
        if observed != states:
            raise RuntimeError('Component changed during resume preflight')
        projected = read_unproduced_component_states(
            connection, component_snapshot_from_shape(expected_shape),
        )
        successful_results = {}
        for component, state in states.items():
            successful_results[component] = (
                None if component in projected else _read_resume_successful_result(
                    connection, component, state,
                )
            )
            for node in component:
                for job in _read_jobs(connection, node):
                    _validate_resume_job(storage, connection, node, job, component)
        return successful_results
    finally:
        connection.execute('RELEASE SAVEPOINT mwf_resume_owner_observation')


class _SnapshotResumeStorage(_SnapshotPreparationStorage):
    """Extend preparation observation with component-history readers."""

    def _read_component_state(self, connection, component):
        from .component_states import read_component_state_snapshot

        return read_component_state_snapshot(connection, component)

    def _read_component_producing_identity(self, connection, component):
        from .component_definitions import ComponentDefinitionStorageMixin

        return ComponentDefinitionStorageMixin._read_component_producing_identity(
            self, connection, component,
        )

    def _read_component_successful_result(self, connection, component, identity):
        from .selected_component_lifecycle import SelectedComponentLifecycleStorageMixin

        return SelectedComponentLifecycleStorageMixin._read_component_successful_result(
            self, connection, component, identity,
        )

    def _match_component_successful_result(
        self, connection, component, identity, current, **options,
    ):
        from .selected_component_lifecycle import SelectedComponentLifecycleStorageMixin

        return SelectedComponentLifecycleStorageMixin._match_component_successful_result(
            self, connection, component, identity, current, **options,
        )


def read_resume_plan_snapshot(
    connection, project_root, components, states, *, expected_shape,
):
    """Validate and summarize resume work without recovering or requeueing it."""
    from .planning_observation import ResumeJobEffect, ResumePlanEffects
    from .preparation_guards import refuse_receiver_mutation

    components = tuple(tuple(component) for component in components)
    expected = {tuple(component): dict(state) for component, state in states.items()}
    if set(expected) != set(components):
        raise ValueError('Resume plan states must match its selected components')
    observed = read_execution_component_states(
        connection, components, expected_shape=expected_shape,
    )
    if observed != expected:
        raise RuntimeError('Component changed during resume observation')
    storage = _SnapshotResumeStorage(connection, project_root)
    connection.execute('SAVEPOINT mwf_resume_plan')
    try:
        successful_results = validate_resume_job_owners(
            storage, expected, expected_shape=expected_shape,
        )
        jobs = []
        for component in components:
            key = encode_component_key(component)
            if connection.execute(
                'SELECT 1 FROM pending_component_executions WHERE component_key=? UNION ALL '
                'SELECT 1 FROM component_holds WHERE component_key=? LIMIT 1',
                (key, key),
            ).fetchone():
                raise RuntimeError('Resume requires recovery of a held or pending component: ' + key)
            for node in component:
                refuse_receiver_mutation(connection, node)
                storage._require_settled_input_publications(connection, node)
                for job in _read_jobs(connection, node):
                    if job.status in ('queued', 'failed', 'cancelled', 'running'):
                        jobs.append(ResumeJobEffect(
                            node, job.job_id, job.status, job.generation,
                        ))
        return ResumePlanEffects(
            tuple(jobs),
            tuple((component, successful_results[component]) for component in components),
        )
    finally:
        connection.execute('RELEASE SAVEPOINT mwf_resume_plan')


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
    expected_successful_results=None, expected_admitted_shape, clear_trace_nodes=(),
):
    storage._require_execution_session_storage()
    storage._session_text(session_id, 'session_id')
    storage._session_text(expected_admitted_shape, 'expected_admitted_shape')
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
            storage._session_component(component): result
            for component, result in expected_successful_results.items()
        }
        if set(retained_results) != set(expected):
            raise ValueError('Resume successful-result observations must match selected components')
        if any(result is not None and not isinstance(result, SuccessfulResultObservation)
               for result in retained_results.values()):
            raise ValueError('Resume requires exact successful-result observations')

    def validate(connection, *, initial=False):
        for component, state in expected.items():
            key = encode_component_key(component)
            read_admitted_component_shape(
                connection, session_id, component, expected_admitted_shape,
            )
            if storage._read_session_job_roots(connection, session_id):
                raise RuntimeError('Resume requires its exact full component owner and reservation: ' + key)
            current = storage._read_component_state(connection, component)
            if current != state or current['misaligned'] or current['lifecycle'] == 'running':
                raise RuntimeError('Component changed before resume preparation: ' + repr(component))
            retained = _read_resume_successful_result(connection, component, current)
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
                restored = retained if retained is not None and retained.lifecycle == 'sampled' else None
                retained_identity = None if retained is None else retained.identity
                current_record = read_component_state_record(connection, component)
                if current_record is None or current_record.snapshot != state:
                    raise RuntimeError('Component changed before resume: ' + repr(component))
                next_pointer = retained_identity if restored is not None else None
                changed = connection.execute(
                    'UPDATE component_states SET lifecycle=?, stability=?, instability_origin=?, '
                    'retained_result_shape_id=?, retained_result_alignment_generation=? '
                    "WHERE component_key=? AND lifecycle='failed' "
                    'AND shape_id=? AND misaligned=0 AND alignment_generation=? '
                    'AND stability IS NULL AND instability_origin IS NULL '
                    'AND retained_result_shape_id IS ? AND retained_result_alignment_generation IS ?',
                    (restored.lifecycle if restored is not None else 'queued',
                     restored.stability if restored is not None else None,
                     restored.instability_origin if restored is not None else None,
                     None if next_pointer is None else next_pointer.shape_id,
                     None if next_pointer is None else next_pointer.alignment_generation,
                     encode_component_key(component),
                     current_record.identity.shape_id,
                     state['alignment_generation'],
                     None if retained_identity is None else retained_identity.shape_id,
                     None if retained_identity is None else retained_identity.alignment_generation),
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
