"""Native sample admission retains one validated selection atomically."""

from __future__ import annotations

import os
import socket
from dataclasses import replace

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.models import Job, now
from micro_workflow_manager.processes import process_identity
from tests.test_090_component_session_settlement import _close, _rows


@pytest.fixture
def native_selection(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A')])
    storage = workflow.storage
    snapshot = workflow.topology.snapshot()
    storage.register_component_topology(snapshot)
    for node in ('A', 'B'):
        for job_id in (1, 2):
            storage.create_job(Job(node_name=node, job_id=job_id, params={'number': job_id}))
    try:
        yield storage, snapshot
    finally:
        _close(storage)


def _admit(storage, snapshot, builder, *, components=(('A', 'B'),)):
    pid = os.getpid()
    return storage.create_execution_session(
        'sample-admission', session_kind='main', command='run sample',
        start_component=('A', 'B'), selected_components=list(components),
        started_at=now(), hostname=socket.gethostname(), pid=pid,
        process_identity=process_identity(pid), sample_id='sample-identity',
        expected_shape=snapshot.shape_json, _reserved_sample_builder=builder,
    )


def _plan(storage, snapshot, connection, tokens=('A=1', 'B=1')):
    from micro_workflow_manager.storage.sample_planning import plan_sample

    return plan_sample(
        connection, storage.project_dir, snapshot, 'A', tokens,
        seed='fixed-seed', statuses=(), expected_population=None,
    )


def _input_files(storage):
    return {
        (node, job_id): storage.input_file(node, job_id).read_bytes()
        for node in ('A', 'B') for job_id in (1, 2)
    }


def test_sample_population_is_observed_after_reservation_and_retained_as_one_exact_plan(native_selection):
    storage, snapshot = native_selection
    before_component = storage.get_component_state(('A', 'B'))
    before_input = _input_files(storage)
    plans = []

    def build(connection):
        session = connection.execute(
            "SELECT status FROM execution_sessions WHERE session_id='sample-admission'",
        ).fetchone()
        assert session['status'] == 'running'
        reservations = [dict(row) for row in connection.execute('SELECT * FROM component_reservations')]
        assert reservations == [{
            'component_key': encode_component_key(('A', 'B')), 'session_id': 'sample-admission',
        }]
        plan = _plan(storage, snapshot, connection)
        plans.append(plan)
        return plan

    admitted = _admit(storage, snapshot, build)
    plan, = plans

    assert admitted['selection_kind'] == 'jobs'
    assert admitted['selected_jobs'] == list(plan.selected_jobs)
    assert admitted['details']['selection'] == plan.manifest('sample-identity')
    assert admitted['details']['selection']['full_starting_coverage'] is False
    assert admitted['details']['selection']['shape'] == snapshot.shape_json
    assert set(admitted['details']['selection']['members']) == {'A', 'B'}
    assert storage.get_live_main_session()['session_id'] == 'sample-admission'
    assert storage.get_component_reservation(('A', 'B')) == {
        'members': ('A', 'B'), 'session_id': 'sample-admission',
    }
    assert storage.get_component_state(('A', 'B')) == before_component
    assert _input_files(storage) == before_input
    roots = storage._read_session_job_roots(storage.db_connection(), 'sample-admission')
    assert list(roots) == list(plan.selected_job_instances)
    assert all(storage.get_job_status(node, job_id) == 'queued'
               for node in ('A', 'B') for job_id in (1, 2))


@pytest.mark.parametrize('failure', ['drift', 'empty', 'missing', 'duplicate', 'component'])
def test_refused_sample_admission_leaves_no_session_reservation_or_selection(native_selection, failure):
    storage, snapshot = native_selection
    before = _rows(storage)
    before_input = _input_files(storage)
    reached_reservation = []

    def build(connection):
        assert connection.execute('SELECT COUNT(*) FROM component_reservations').fetchone()[0] == 1
        reached_reservation.append(True)
        if failure == 'drift':
            raise RuntimeError('Sample population or input changed')
        if failure == 'empty':
            return _plan(storage, snapshot, connection, ('0',))
        plan = _plan(storage, snapshot, connection)
        first = plan.members[0]
        if failure == 'missing':
            return replace(plan, members=(replace(first, selected_job_ids=(99,)), *plan.members[1:]))
        if failure == 'duplicate':
            selected = first.selected_job_ids[0]
            return replace(plan, members=(replace(first, selected_job_ids=(selected, selected)), *plan.members[1:]))
        return replace(plan, members=plan.members[:1])

    with pytest.raises((RuntimeError, ValueError)):
        _admit(storage, snapshot, build)

    assert reached_reservation == [True]
    assert _rows(storage) == before
    assert _input_files(storage) == before_input


def test_reserved_sample_scope_is_rejected_before_the_private_reader_runs(native_selection):
    storage, snapshot = native_selection
    before = _rows(storage)
    before_input = _input_files(storage)
    called = []

    with pytest.raises(ValueError, match='exactly its starting component'):
        _admit(
            storage, snapshot, lambda connection: called.append(connection),
            components=(('A', 'B'), ('C',)),
        )

    assert called == []
    assert _rows(storage) == before
    assert _input_files(storage) == before_input


@pytest.mark.parametrize('write', ['component-reservation', 'selection-details', 'selected-jobs'])
def test_suppressed_sample_admission_write_rolls_back_every_record(native_selection, write):
    storage, snapshot = native_selection
    if write == 'selection-details':
        statement = (
            'CREATE TRIGGER suppress_admission BEFORE UPDATE OF selection_kind, details_json '
            "ON execution_sessions WHEN NEW.session_id='sample-admission' BEGIN "
            "UPDATE nodes SET status='failed' WHERE node_name='A'; SELECT RAISE(IGNORE); END"
        )
    else:
        table = 'component_reservations' if write == 'component-reservation' else 'session_jobs'
        statement = (
            f'CREATE TRIGGER suppress_admission BEFORE INSERT ON {table} BEGIN '
            "UPDATE nodes SET status='failed' WHERE node_name='A'; SELECT RAISE(IGNORE); END"
        )
    storage.submit_db_mutation(lambda connection: connection.execute(statement))
    before = _rows(storage)
    before_input = _input_files(storage)

    with pytest.raises(RuntimeError, match='reservation|retained|selected job'):
        _admit(storage, snapshot, lambda connection: _plan(storage, snapshot, connection))

    assert _rows(storage) == before
    assert _input_files(storage) == before_input
