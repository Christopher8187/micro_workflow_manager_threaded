"""Strictly decode the sample admission record used by lifecycle settlement."""

from __future__ import annotations

import json
from dataclasses import dataclass

from micro_workflow_manager.storage.component_definitions import component_snapshot_from_shape
from .component_result_identity import read_admitted_component_shape
from .sample_planning import SAMPLE_ALGORITHM, SampleMember, SamplePlan


@dataclass(frozen=True)
class SampleAdmissionHistory:
    sample_id: str
    plan: SamplePlan
    roots: tuple[tuple[str, int, str], ...]

    @property
    def full_starting_coverage(self):
        return self.plan.full_starting_coverage

    @property
    def starting_instances(self):
        return {
            (member.node, job['job_id']): job['job_instance_id']
            for member in self.plan.members for job in member.starting_jobs
        }


def _object(value, fields, label):
    if not isinstance(value, dict) or set(value) != set(fields):
        raise RuntimeError(f'Damaged persisted sample {label}')
    return value


def _list(value, label):
    if not isinstance(value, list):
        raise RuntimeError(f'Damaged persisted sample {label}')
    return value


def _candidate(value, *, with_input):
    fields = {'job_id', 'job_instance_id', 'status', 'generation'}
    if with_input:
        fields.add('input_digest')
    return dict(_object(value, fields, 'job observation'))


def _decode_plan(selection):
    selection = _object(selection, {
        'kind', 'algorithm', 'sample_id', 'seed', 'node', 'selectors',
        'status_filter', 'shape', 'members', 'combined_digest',
        'full_starting_coverage',
    }, 'record')
    if selection['kind'] != 'sample' or selection['algorithm'] != SAMPLE_ALGORITHM:
        raise RuntimeError('Damaged persisted sample algorithm')
    if type(selection['full_starting_coverage']) is not bool:
        raise RuntimeError('Damaged persisted sample coverage flag')
    raw_members = selection['members']
    if not isinstance(raw_members, dict):
        raise RuntimeError('Damaged persisted sample members')
    members = []
    for node, raw in raw_members.items():
        if not isinstance(node, str):
            raise RuntimeError('Damaged persisted sample member node')
        raw = _object(raw, {
            'selector', 'starting_jobs', 'starting_jobs_digest',
            'population_count', 'population', 'selected_job_ids',
            'population_digest', 'input_digest',
        }, 'member')
        selector = _object(raw['selector'], {'kind', 'value'}, 'selector')
        starting = tuple(_candidate(item, with_input=False)
                         for item in _list(raw['starting_jobs'], 'starting jobs'))
        population = tuple(_candidate(item, with_input=True)
                           for item in _list(raw['population'], 'population'))
        if type(raw['population_count']) is not int or raw['population_count'] != len(population):
            raise RuntimeError('Damaged persisted sample population count')
        selected = tuple(_list(raw['selected_job_ids'], 'selected jobs'))
        members.append(SampleMember(
            node, (selector['kind'], selector['value']), starting, population, selected,
            raw['starting_jobs_digest'], raw['population_digest'], raw['input_digest'],
        ))
    return SamplePlan(
        selection['node'], tuple(_list(selection['selectors'], 'selectors')),
        selection['seed'], tuple(_list(selection['status_filter'], 'status filter')),
        tuple(members), selection['shape'], selection['combined_digest'],
    ), selection['sample_id'], selection['full_starting_coverage']


def read_sample_admission_history(storage, connection, session_id, *, component, expected_shape):
    """Return one validated sample history bound to its exact selected session."""
    row = connection.execute(
        'SELECT command, selection_kind, start_component, details_json '
        'FROM execution_sessions WHERE session_id=?',
        (session_id,),
    ).fetchone()
    if row is None or row['command'] != 'run sample' or row['selection_kind'] != 'jobs':
        raise RuntimeError('Sample lifecycle requires an exact sampled selected session')
    components = storage._read_session_components(connection, session_id)
    component = tuple(component)
    if components != [component] or storage._stored_session_component(row['start_component']) != component:
        raise RuntimeError('Persisted sample scope differs from its lifecycle component')
    read_admitted_component_shape(connection, session_id, component, expected_shape)
    try:
        details = json.loads(row['details_json'])
    except (TypeError, json.JSONDecodeError) as error:
        raise RuntimeError('Damaged persisted sample session details') from error
    if not isinstance(details, dict) or 'selection' not in details:
        raise RuntimeError('Sample session lost its persisted selection')
    plan, sample_id, recorded_coverage = _decode_plan(details['selection'])
    try:
        snapshot = component_snapshot_from_shape(expected_shape)
    except ValueError as error:
        raise RuntimeError('Sample lifecycle has an invalid graph shape') from error
    if plan.shape != expected_shape or component not in snapshot.components:
        raise RuntimeError('Persisted sample graph shape differs from its lifecycle attempt')
    try:
        roots, canonical = plan.admission_record(
            sample_id=sample_id, component=component, expected_shape=expected_shape,
        )
    except (TypeError, ValueError) as error:
        raise RuntimeError('Damaged persisted sample admission') from error
    if canonical != details['selection']:
        raise RuntimeError('Persisted sample admission is not canonical')
    if recorded_coverage != plan.full_starting_coverage:
        raise RuntimeError('Persisted sample coverage disagrees with its population')
    if tuple(storage._read_session_job_roots(connection, session_id, components=components)) != roots:
        raise RuntimeError('Persisted sample roots differ from the selected session')
    return SampleAdmissionHistory(sample_id, plan, roots)


def sample_has_no_unprocessed_work(storage, connection, history):
    """Ignore unchanged excluded starting jobs when checking a fully selected population."""
    if not history.full_starting_coverage:
        return False
    for member in history.plan.members:
        starting = {job['job_id']: job for job in member.starting_jobs}
        eligible = {job['job_id'] for job in member.population}
        for row in connection.execute('SELECT job_id FROM jobs WHERE node_name=?', (member.node,)):
            observed = storage._read_job_owner_observation(connection, member.node, row['job_id'])
            if observed is None:
                raise RuntimeError('Sample completion lost a current job observation')
            if observed['active_execution_id'] is not None:
                return False
            if observed['status'] in ('done', 'skipped'):
                continue
            old = starting.get(row['job_id'])
            if (old is not None and row['job_id'] not in eligible
                    and all(observed[name] == old[name]
                            for name in ('job_instance_id', 'status', 'generation'))):
                continue
            return False
    return True
