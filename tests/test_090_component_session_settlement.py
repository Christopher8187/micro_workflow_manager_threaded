from __future__ import annotations

import os
import json
import socket
import time
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Event

import networkx as nx
import pytest

from micro_workflow_manager.models import Job, now
from micro_workflow_manager.session_liveness import process_identity
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.topology import ComponentTopology


def _close(storage):
    storage.db_mutation_barrier()
    deadline = time.perf_counter() + 10
    while storage.mutation_writer_diagnostics()['writer_alive']:
        assert time.perf_counter() < deadline, 'Mutation writer did not retire'
        time.sleep(0.01)
    storage.close_database_connections()


def _rows(storage):
    connection = storage.db_connection()
    tables = [row['name'] for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )]
    return {table: [dict(row) for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')]
            for table in tables}


def _outcome(topology, *, lifecycle='done', stability='stable', instability_origin=None):
    from micro_workflow_manager.storage.component_states import ComponentTerminalOutcome

    return ComponentTerminalOutcome(
        component=('A', 'B'), expected_shape=topology.shape_json,
        expected_alignment_generation=0, lifecycle=lifecycle, stability=stability,
        instability_origin=instability_origin,
    )


@pytest.mark.parametrize('publication', ['component', 'session'])
def test_component_publication_rolls_back_when_pending_record_cannot_be_removed(running_component, publication):
    storage, topology, generation, execution_id = running_component
    storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
    storage.submit_db_mutation(lambda connection: connection.execute(
        'CREATE TRIGGER suppress_pending_removal BEFORE DELETE ON pending_component_executions BEGIN '
        "UPDATE nodes SET status='failed' WHERE node_name='A'; SELECT RAISE(IGNORE); END",
    ))
    before = _rows(storage)
    with pytest.raises(RuntimeError, match='pending'):
        if publication == 'component':
            storage.finish_successful_component_execution('ordinary-result', _outcome(topology))
        else:
            storage.decide_execution_session_exit(
                'ordinary-result', outcome='done', finished_at=now(), component_outcomes=[_outcome(topology)],
            )
    assert _rows(storage) == before


def test_failed_exit_refuses_a_running_component_without_its_pending_record(running_component):
    storage, topology, generation, execution_id = running_component
    storage.finalize_job_execution('A', 1, generation, execution_id, 'failed')
    storage.submit_db_mutation(lambda connection: connection.execute(
        'DELETE FROM pending_component_executions WHERE session_id=?', ('ordinary-result',),
    ))
    before = _rows(storage)
    with pytest.raises(RuntimeError, match='running component'):
        storage.decide_execution_session_exit('ordinary-result', outcome='failed', finished_at=now())
    assert _rows(storage) == before


@pytest.mark.parametrize('damage', ['main-origin', 'invalid-ready'])
def test_failed_exit_refuses_malformed_pending_completion_without_erasing_it(running_component, damage):
    storage, topology, generation, execution_id = running_component
    storage.finalize_job_execution('A', 1, generation, execution_id, 'failed')

    def change(connection):
        if damage == 'main-origin':
            connection.execute(
                "UPDATE pending_component_executions SET completion_ready=1, stability='unstable', "
                "instability_origin='ordinary-result'",
            )
        else:
            connection.execute('PRAGMA ignore_check_constraints=ON')
            try:
                connection.execute('UPDATE pending_component_executions SET completion_ready=2')
            finally:
                connection.execute('PRAGMA ignore_check_constraints=OFF')

    storage.submit_db_mutation(change)
    before = _rows(storage)
    with pytest.raises(RuntimeError, match='pending'):
        storage.decide_execution_session_exit('ordinary-result', outcome='failed', finished_at=now())
    assert _rows(storage) == before


@pytest.mark.parametrize('publication', ['component', 'session'])
@pytest.mark.parametrize('changed', [False, True])
def test_ready_component_keeps_its_recorded_successful_lineage(running_component, publication, changed):
    storage, topology, generation, execution_id = running_component
    storage.create_execution_session(
        'interrupt-origin', session_kind='interrupt', command='run',
        start_component=('C',), selected_components=[('C',)],
        started_at=now(), hostname=socket.gethostname(), pid=os.getpid(),
        process_identity=process_identity(os.getpid()),
        expected_shape=topology.shape_json,
    )
    storage.submit_db_mutation(lambda connection: connection.execute(
        "UPDATE pending_component_executions SET completion_ready=1, stability='stable'",
    ))
    storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
    result = _outcome(
        topology, stability='unstable' if changed else 'stable',
        instability_origin='interrupt-origin' if changed else None,
    )
    before = _rows(storage)

    def publish():
        if publication == 'component':
            return storage.finish_successful_component_execution('ordinary-result', result)
        return storage.decide_execution_session_exit(
            'ordinary-result', outcome='done', finished_at=now(), component_outcomes=[result],
        )

    if changed:
        with pytest.raises(RuntimeError, match='recorded.*lineage'):
            publish()
        assert _rows(storage) == before
    else:
        publish()
        state = storage.get_component_state(('A', 'B'))
        assert (state['lifecycle'], state['stability'], state['instability_origin']) == ('done', 'stable', None)


@pytest.mark.parametrize('publication', ['component', 'session'])
@pytest.mark.parametrize('damage', ['shape', 'generation', 'missing'])
def test_component_publication_rechecks_its_pending_start_record(running_component, publication, damage):
    storage, topology, generation, execution_id = running_component
    storage.finalize_job_execution('A', 1, generation, execution_id, 'done')

    def change(connection):
        if damage == 'shape':
            shape_id = connection.execute(
                'INSERT INTO graph_shapes(shape_json) VALUES(?)',
                (topology.shape_json.replace('"C"', '"D"'),),
            ).lastrowid
            connection.execute(
                'INSERT INTO component_definitions(component_key, shape_id) VALUES(?, ?)',
                (json.dumps(['A', 'B']), shape_id),
            )
            connection.execute('UPDATE pending_component_executions SET shape_id=?', (shape_id,))
        elif damage == 'generation':
            connection.execute('UPDATE pending_component_executions SET alignment_generation=1')
        else:
            connection.execute('DELETE FROM pending_component_executions')

    storage.submit_db_mutation(change)
    before = _rows(storage)
    with pytest.raises(RuntimeError):
        if publication == 'component':
            storage.finish_successful_component_execution('ordinary-result', _outcome(topology))
        else:
            storage.decide_execution_session_exit(
                'ordinary-result', outcome='done', finished_at=now(), component_outcomes=[_outcome(topology)],
            )
    assert _rows(storage) == before


def _seed_retained_result(storage, component, *, lifecycle, stability, origin):
    key = json.dumps(list(component))

    def seed(connection):
        state = connection.execute(
            'SELECT shape_id, alignment_generation FROM component_states WHERE component_key=?',
            (key,),
        ).fetchone()
        assert state is not None
        connection.execute(
            'INSERT INTO component_successful_results '
            '(component_key, shape_id, alignment_generation, lifecycle, stability, instability_origin) '
            'VALUES(?, ?, ?, ?, ?, ?)',
            (key, state['shape_id'], state['alignment_generation'], lifecycle, stability, origin),
        )
        changed = connection.execute(
            'UPDATE component_states SET stability=?, instability_origin=?, retained_result_shape_id=?, '
            'retained_result_alignment_generation=? WHERE component_key=?',
            (stability, origin, state['shape_id'], state['alignment_generation'], key),
        ).rowcount
        assert changed == 1

    storage.submit_db_mutation(seed)


def test_component_publication_preserves_another_sessions_pending_record(running_component):
    storage, topology, generation, execution_id = running_component
    storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
    storage.create_execution_session(
        'retained-owner', session_kind='interrupt', command='run',
        start_component=('A', 'B'), selected_components=[('A', 'B')],
        started_at=now(), hostname=socket.gethostname(), pid=os.getpid(),
        process_identity=process_identity(os.getpid()), expected_shape=topology.shape_json,
    )
    storage.submit_db_mutation(lambda connection: connection.execute(
        'INSERT INTO pending_component_executions('
        'session_id, component_key, shape_id, starting_shape_id, alignment_generation, completion_ready, '
        'execution_kind, starting_lifecycle, starting_misaligned, stability, instability_origin) '
        'SELECT ?, component_key, shape_id, starting_shape_id, alignment_generation, completion_ready, '
        'execution_kind, starting_lifecycle, starting_misaligned, stability, instability_origin '
        'FROM pending_component_executions WHERE session_id=?',
        ('retained-owner', 'ordinary-result'),
    ))
    before = _rows(storage)
    assert storage.finish_successful_component_execution('ordinary-result', _outcome(topology)) is True
    expected = deepcopy(before)
    for row in expected['component_states']:
        if row['component_key'] == json.dumps(['A', 'B']):
            row.update(
                lifecycle='done', stability='stable', retained_result_shape_id=1,
                retained_result_alignment_generation=0,
            )
    expected['pending_component_executions'] = [
        row for row in expected['pending_component_executions'] if row['session_id'] == 'retained-owner'
    ]
    expected['component_successful_results'] = [{
        'component_key': json.dumps(['A', 'B']), 'shape_id': 1, 'alignment_generation': 0,
        'lifecycle': 'done', 'stability': 'stable', 'instability_origin': None,
    }]
    assert _rows(storage) == expected


@pytest.fixture
def running_component(tmp_path, request):
    storage = FileStorage._create_new_project_state(tmp_path)
    request.addfinalizer(lambda: _close(storage))
    topology = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'C')]), [('A', 'B')]).snapshot()
    storage.register_component_topology(topology)
    storage.create_execution_session(
        'ordinary-result', session_kind='main', command='run',
        start_component=('A', 'B'), selected_components=getattr(request, 'param', [('A', 'B')]),
        started_at=now(), hostname=socket.gethostname(),
        pid=os.getpid(), process_identity=process_identity(os.getpid()),
        expected_shape=topology.shape_json,
    )
    storage.reserve_execution_components('ordinary-result', expected_shape=topology.shape_json)
    storage.begin_queued_component_execution(
        'ordinary-result', ('A', 'B'), expected_shape=topology.shape_json,
        expected_alignment_generation=0, successful_lineage=('stable', None),
    )
    storage.create_job(Job(node_name='A', job_id=1, params={'value': 'preserved'}))
    generation, execution_id = storage.claim_job_execution(
        'A', 1, started_at='2026-09-06T08:00:00+00:00',
        session_id='ordinary-result', component=('A', 'B'),
    )
    return storage, topology, generation, execution_id


def test_component_result_and_session_exit_commit_together(running_component):
    storage, topology, generation, execution_id = running_component
    storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
    before_component = storage.get_component_state(('A', 'B'))
    before_unselected = storage.get_component_state(('C',))
    before_owner = storage.get_job_execution_owner(execution_id)
    before_events = storage.read_job_events('A', 1)

    from micro_workflow_manager.storage.component_states import ComponentTerminalOutcome
    outcome = ComponentTerminalOutcome(
        component=('A', 'B'), expected_shape=topology.shape_json,
        expected_alignment_generation=0, lifecycle='done', stability='stable',
        instability_origin=None,
    )
    decision = storage.decide_execution_session_exit(
        'ordinary-result', outcome='done', finished_at='2026-09-06T08:01:00+00:00',
        component_outcomes=[outcome],
    )

    assert decision == {'restarts': {}, 'released': 1}
    expected = {**before_component, 'lifecycle': 'done', 'stability': 'stable'}
    assert storage.get_component_state(('A', 'B')) == expected
    assert storage.get_component_state(('C',)) == before_unselected
    assert storage.get_execution_session('ordinary-result')['status'] == 'terminal'
    assert storage.get_component_reservation(('A', 'B')) is None
    assert storage.get_job_execution_owner(execution_id) == before_owner
    assert storage.read_job_events('A', 1) == before_events
    _close(storage)
    reopened = FileStorage(storage.project_dir)
    try:
        assert reopened.get_component_state(('A', 'B')) == expected
        assert reopened.get_execution_session('ordinary-result')['outcome'] == 'done'
        assert reopened.get_component_reservation(('A', 'B')) is None
    finally:
        _close(reopened)


@pytest.mark.parametrize('running_component', [[('A', 'B'), ('C',)]], indirect=True)
def test_successful_component_can_finish_while_its_session_remains_live(running_component):
    storage, topology, generation, execution_id = running_component
    storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
    before = _rows(storage)

    assert storage.finish_successful_component_execution('ordinary-result', _outcome(topology)) is True

    expected = deepcopy(before)
    component_row = next(row for row in expected['component_states'] if row['component_key'] == json.dumps(['A', 'B']))
    component_row.update(
        lifecycle='done', stability='stable', retained_result_shape_id=1,
        retained_result_alignment_generation=0,
    )
    expected['pending_component_executions'] = []
    expected['component_successful_results'] = [{
        'component_key': json.dumps(['A', 'B']), 'shape_id': 1, 'alignment_generation': 0,
        'lifecycle': 'done', 'stability': 'stable', 'instability_origin': None,
    }]
    assert _rows(storage) == expected
    observed = storage.read_component_states([('A', 'B'), ('C',)], expected_shape=topology.shape_json)
    assert observed[('A', 'B')]['lifecycle'] == 'done'
    assert observed[('C',)]['lifecycle'] == 'queued'
    assert storage.get_execution_session('ordinary-result')['status'] == 'running'
    assert storage.get_component_reservation(('A', 'B'))['session_id'] == 'ordinary-result'
    assert storage.get_component_reservation(('C',))['session_id'] == 'ordinary-result'


@pytest.mark.parametrize('obstruction', [
    'failed-result', 'queued-restart', 'active-job', 'reservation', 'generation', 'ignored-write',
])
def test_intermediate_component_success_preserves_state_when_refused(running_component, obstruction):
    storage, topology, generation, execution_id = running_component
    outcome = _outcome(topology)
    if obstruction == 'failed-result':
        outcome = _outcome(topology, lifecycle='failed', stability=None)
    if obstruction != 'active-job':
        storage.finalize_job_execution(
            'A', 1, generation, execution_id, 'failed' if obstruction == 'queued-restart' else 'done',
        )
    if obstruction == 'queued-restart':
        storage.request_owned_job_restarts(storage.plan_owned_job_restarts([('A', 1)]))
    elif obstruction == 'reservation':
        storage.release_execution_components('ordinary-result')
    elif obstruction == 'generation':
        storage.submit_db_mutation(lambda connection: connection.execute(
            'UPDATE component_states SET alignment_generation=1 WHERE component_key=?',
            (json.dumps(['A', 'B']),),
        ))
    elif obstruction == 'ignored-write':
        storage.submit_db_mutation(lambda connection: connection.execute(
            'CREATE TRIGGER suppress_component_success BEFORE UPDATE OF lifecycle ON component_states '
            "WHEN NEW.lifecycle='done' BEGIN "
            "UPDATE component_states SET alignment_generation=3 WHERE component_key='[\"C\"]'; "
            'SELECT RAISE(IGNORE); END',
        ))
    before = _rows(storage)
    with pytest.raises(ValueError if obstruction == 'failed-result' else RuntimeError):
        storage.finish_successful_component_execution('ordinary-result', outcome)
    assert _rows(storage) == before
    if obstruction == 'queued-restart':
        decision = storage.decide_execution_session_exit(
            'ordinary-result', outcome='failed', finished_at=now(),
            restart_attempts=[('A', 1, generation, execution_id)],
            component_outcomes=[_outcome(topology, lifecycle='failed', stability=None)],
        )
        assert list(decision['restarts']) == [('A', 1)]
        assert decision['released'] == 0
        assert _rows(storage) == before


@pytest.mark.parametrize('suppressed', ['session-finish', 'reservation-release'])
def test_component_settlement_rolls_back_a_suppressed_exit_step(running_component, suppressed):
    storage, topology, generation, execution_id = running_component
    storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
    event = ('UPDATE OF status ON execution_sessions' if suppressed == 'session-finish'
             else 'DELETE ON component_reservations')
    storage.submit_db_mutation(lambda connection: connection.execute(
        'CREATE TRIGGER suppress_exit BEFORE ' + event + ' BEGIN '
        "UPDATE component_states SET alignment_generation=3 WHERE component_key='[\"C\"]'; "
        'SELECT RAISE(IGNORE); END',
    ))
    before = _rows(storage)

    with pytest.raises(RuntimeError):
        storage.decide_execution_session_exit(
            'ordinary-result', outcome='done', finished_at='2026-09-06T08:01:00+00:00',
            component_outcomes=[_outcome(topology)],
        )

    assert _rows(storage) == before


@pytest.mark.parametrize('order', ['restart-first', 'terminal-first'])
def test_component_settlement_respects_the_accepted_restart_order(running_component, order):
    storage, topology, generation, execution_id = running_component
    storage.finalize_job_execution('A', 1, generation, execution_id, 'failed')
    targets = storage.plan_owned_job_restarts([('A', 1)])
    before_component = storage.get_component_state(('A', 'B'))
    before_session = storage.get_execution_session('ordinary-result')
    before_reservation = storage.get_component_reservation(('A', 'B'))
    if order == 'restart-first':
        storage.request_owned_job_restarts(targets)

    decision = storage.decide_execution_session_exit(
        'ordinary-result', outcome='failed', finished_at=now(),
        restart_attempts=[('A', 1, generation, execution_id)],
        component_outcomes=[_outcome(topology, lifecycle='failed', stability=None)],
    )
    if order == 'terminal-first':
        before = _rows(storage)
        with pytest.raises(RuntimeError):
            storage.request_owned_job_restarts(targets)
        assert _rows(storage) == before
        assert decision == {'restarts': {}, 'released': 1}
        assert storage.get_component_state(('A', 'B'))['lifecycle'] == 'failed'
        assert storage.get_component_reservation(('A', 'B')) is None
        return

    assert list(decision['restarts']) == [('A', 1)]
    assert decision['released'] == 0
    assert storage.get_component_state(('A', 'B')) == before_component
    assert storage.get_execution_session('ordinary-result') == before_session
    assert storage.get_component_reservation(('A', 'B')) == before_reservation
    successor_generation, successor_execution = storage.claim_job_execution(
        'A', 1, started_at=now(), session_id='ordinary-result', component=('A', 'B'),
        expected_restart=decision['restarts'][('A', 1)],
    )
    assert successor_generation == generation + 1
    assert successor_execution != execution_id
    storage.finalize_job_execution('A', 1, successor_generation, successor_execution, 'done')
    final = storage.decide_execution_session_exit(
        'ordinary-result', outcome='done', finished_at=now(),
        restart_attempts=[('A', 1, successor_generation, successor_execution)],
        component_outcomes=[_outcome(topology)],
    )
    assert final == {'restarts': {}, 'released': 1}
    assert storage.get_component_state(('A', 'B'))['lifecycle'] == 'done'
    assert storage.get_execution_session('ordinary-result')['outcome'] == 'done'


@pytest.mark.parametrize('status,active', [
    ('running', True), ('running', False), ('done', True),
    ('queued', False), ('failed', False), ('cancelled', False),
])
def test_component_completion_refuses_unfinished_jobs(running_component, status, active):
    storage, topology, generation, execution_id = running_component
    storage.submit_db_mutation(lambda connection: connection.execute(
        'UPDATE jobs SET status=?, active_execution_id=? WHERE node_name=? AND job_id=?',
        (status, execution_id if active else None, 'A', 1),
    ))
    before = _rows(storage)
    with pytest.raises(RuntimeError):
        storage.decide_execution_session_exit(
            'ordinary-result', outcome='done', finished_at=now(),
            component_outcomes=[_outcome(topology)],
        )
    assert _rows(storage) == before


@pytest.mark.parametrize('change', ['terminal', 'reservation', 'generation', 'shape', 'selection'])
def test_component_settlement_rechecks_state_at_its_write_boundary(running_component, monkeypatch, change):
    storage, topology, generation, execution_id = running_component
    storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
    other = FileStorage(storage.project_dir)
    awaiting_write, proceed = Event(), Event()
    submit = storage.submit_db_mutation

    def paused_submit(*args, **kwargs):
        awaiting_write.set()
        assert proceed.wait(10), 'Component settlement was not released'
        return submit(*args, **kwargs)

    try:
        with monkeypatch.context() as patch, ThreadPoolExecutor(max_workers=1) as pool:
            patch.setattr(storage, 'submit_db_mutation', paused_submit)
            future = pool.submit(
                storage.decide_execution_session_exit,
                'ordinary-result', outcome='done', finished_at=now(),
                component_outcomes=[_outcome(topology)],
            )
            try:
                assert awaiting_write.wait(5), 'Settlement did not reach its write boundary'
                if change == 'terminal':
                    other.finish_execution_session('ordinary-result', outcome='done', finished_at=now())
                elif change == 'reservation':
                    other.release_execution_components('ordinary-result')
                elif change == 'selection':
                    def remove_selection(connection):
                        connection.execute(
                            'DELETE FROM pending_component_executions WHERE session_id=?', ('ordinary-result',),
                        )
                        connection.execute(
                            'DELETE FROM session_components WHERE session_id=?', ('ordinary-result',),
                        )
                    other.submit_db_mutation(remove_selection)
                elif change == 'shape':
                    def corrupt_shape(connection):
                        connection.execute('DROP TRIGGER prevent_graph_shape_update')
                        connection.execute(
                            'UPDATE graph_shapes SET shape_json=?', (topology.shape_json.replace('"C"', '"D"'),),
                        )
                    other.submit_db_mutation(corrupt_shape)
                else:
                    other.submit_db_mutation(lambda connection: connection.execute(
                        'UPDATE component_states SET alignment_generation=1 WHERE component_key=?',
                        (json.dumps(['A', 'B']),),
                    ))
                before = _rows(other)
            finally:
                proceed.set()
            with pytest.raises(RuntimeError):
                future.result(timeout=15)
        assert _rows(storage) == before
    finally:
        proceed.set()
        _close(other)


@pytest.mark.parametrize('running_component', [[('A', 'B'), ('C',)]], indirect=True)
@pytest.mark.parametrize('obstruction', ['unfinished-job', 'suppressed-write'])
def test_component_settlement_rolls_back_the_whole_result_batch(running_component, obstruction):
    storage, topology, generation, execution_id = running_component
    storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
    storage.begin_queued_component_execution(
        'ordinary-result', ('C',), expected_shape=topology.shape_json,
        expected_alignment_generation=0, successful_lineage=('stable', None),
    )
    if obstruction == 'unfinished-job':
        storage.create_job(Job(node_name='C', job_id=1, params={}))
    else:
        storage.submit_db_mutation(lambda connection: connection.execute(
            'CREATE TRIGGER suppress_second_component BEFORE UPDATE OF lifecycle ON component_states '
            "WHEN OLD.component_key='[\"C\"]' AND NEW.lifecycle='done' BEGIN "
            "UPDATE nodes SET status='failed' WHERE node_name='A'; "
            'SELECT RAISE(IGNORE); END',
        ))
    before = _rows(storage)
    with pytest.raises(RuntimeError):
        storage.decide_execution_session_exit(
            'ordinary-result', outcome='done', finished_at=now(),
            component_outcomes=[_outcome(topology), replace(_outcome(topology), component=('C',))],
        )
    assert _rows(storage) == before


@pytest.mark.parametrize('running_component', [[('A', 'B'), ('C',)]], indirect=True)
@pytest.mark.parametrize('other_started', [False, True])
def test_component_result_batch_cannot_leave_an_owned_component_running(running_component, other_started):
    storage, topology, generation, execution_id = running_component
    storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
    if other_started:
        storage.begin_queued_component_execution(
            'ordinary-result', ('C',), expected_shape=topology.shape_json,
            expected_alignment_generation=0, successful_lineage=('stable', None),
        )
    before = _rows(storage)
    arguments = dict(outcome='done', finished_at=now(), component_outcomes=[_outcome(topology)])
    if other_started:
        with pytest.raises(RuntimeError, match='running component'):
            storage.decide_execution_session_exit('ordinary-result', **arguments)
        assert _rows(storage) == before
    else:
        assert storage.decide_execution_session_exit('ordinary-result', **arguments) == {'restarts': {}, 'released': 2}
        assert storage.get_component_state(('C',))['lifecycle'] == 'queued'


@pytest.mark.parametrize('running_component', [[('A', 'B'), ('C',)]], indirect=True)
def test_component_result_batch_preserves_work_transferred_to_another_session(running_component):
    storage, topology, generation, execution_id = running_component
    storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
    storage.begin_queued_component_execution(
        'ordinary-result', ('C',), expected_shape=topology.shape_json,
        expected_alignment_generation=0, successful_lineage=('stable', None),
    )
    storage.create_execution_session(
        'next-owner', session_kind='interrupt', command='run',
        start_component=('C',), selected_components=[('C',)], started_at=now(),
        hostname=socket.gethostname(), pid=os.getpid(), process_identity=process_identity(os.getpid()),
        expected_shape=topology.shape_json,
    )
    storage.submit_db_mutation(lambda connection: connection.execute(
        'UPDATE component_reservations SET session_id=? WHERE component_key=?',
        ('next-owner', json.dumps(['C'])),
    ))
    storage.create_job(Job(node_name='C', job_id=1, params={}))
    _, other_execution = storage.claim_job_execution(
        'C', 1, started_at=now(), session_id='next-owner', component=('C',),
    )
    before_component = storage.get_component_state(('C',))
    before_session = storage.get_execution_session('next-owner')
    before_reservation = storage.get_component_reservation(('C',))
    before_control = storage.read_job_control('C', 1)
    before_owner = storage.get_job_execution_owner(other_execution)
    before_events = storage.read_job_events('C', 1)

    assert storage.decide_execution_session_exit(
        'ordinary-result', outcome='done', finished_at=now(), component_outcomes=[_outcome(topology)],
    ) == {'restarts': {}, 'released': 1}

    assert storage.get_component_state(('C',)) == before_component
    assert storage.get_execution_session('next-owner') == before_session
    assert storage.get_component_reservation(('C',)) == before_reservation
    assert storage.read_job_control('C', 1) == before_control
    assert storage.get_job_execution_owner(other_execution) == before_owner
    assert storage.read_job_events('C', 1) == before_events


@pytest.mark.parametrize('mode', ['new-unstable', 'retained-unstable', 'replace-retained', 'failed'])
def test_component_settlement_preserves_or_clears_successful_lineage(running_component, mode):
    storage, topology, generation, execution_id = running_component
    storage.create_execution_session(
        'interrupt-origin', session_kind='interrupt', command='run',
        start_component=('C',), selected_components=[('C',)],
        started_at=now(), hostname=socket.gethostname(), pid=os.getpid(),
        process_identity=process_identity(os.getpid()),
        expected_shape=topology.shape_json,
    )
    lifecycle = 'failed' if mode == 'failed' else 'done'
    if mode in ('new-unstable', 'retained-unstable'):
        storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE pending_component_executions SET stability='unstable', instability_origin='interrupt-origin'",
        ))
    storage.finalize_job_execution('A', 1, generation, execution_id, lifecycle)
    if mode != 'new-unstable':
        _seed_retained_result(
            storage, ('A', 'B'), lifecycle='done', stability='unstable', origin='interrupt-origin',
        )
    stability = None if mode == 'failed' else 'stable' if mode == 'replace-retained' else 'unstable'
    origin = 'interrupt-origin' if stability == 'unstable' else None
    before = _rows(storage)
    arguments = dict(
        outcome=lifecycle, finished_at=now(), component_outcomes=[_outcome(
            topology, lifecycle=lifecycle, stability=stability, instability_origin=origin,
        )],
    )
    if mode == 'replace-retained':
        with pytest.raises(RuntimeError, match='retained'):
            storage.decide_execution_session_exit('ordinary-result', **arguments)
        assert _rows(storage) == before
        return
    assert storage.decide_execution_session_exit('ordinary-result', **arguments) == {'restarts': {}, 'released': 1}
    state = storage.get_component_state(('A', 'B'))
    assert (state['lifecycle'], state['stability'], state['instability_origin']) == (lifecycle, stability, origin)


@pytest.mark.parametrize('origin', ['missing-origin', 'ordinary-result'])
def test_component_settlement_requires_a_real_interrupt_origin(running_component, origin):
    storage, topology, generation, execution_id = running_component
    storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
    before = _rows(storage)
    with pytest.raises(RuntimeError, match='interrupt origin|recorded successful lineage'):
        storage.decide_execution_session_exit(
            'ordinary-result', outcome='done', finished_at=now(),
            component_outcomes=[_outcome(topology, stability='unstable', instability_origin=origin)],
        )
    assert _rows(storage) == before


@pytest.mark.parametrize('damage', ['duplicate', 'negative-generation', 'boolean-generation',
                                   'incomplete-lineage', 'unknown-lifecycle', 'failed-session-mismatch'])
def test_component_settlement_rejects_invalid_calculated_results(running_component, damage):
    storage, topology, generation, execution_id = running_component
    outcome = _outcome(topology)
    changes = {
        'negative-generation': {'expected_alignment_generation': -1},
        'boolean-generation': {'expected_alignment_generation': True},
        'incomplete-lineage': {'stability': 'unstable'},
        'unknown-lifecycle': {'lifecycle': 'waiting'},
        'failed-session-mismatch': {'lifecycle': 'failed', 'stability': None},
    }
    outcomes = [outcome, outcome] if damage == 'duplicate' else [replace(outcome, **changes[damage])]
    before = _rows(storage)
    with pytest.raises(ValueError):
        storage.decide_execution_session_exit(
            'ordinary-result', outcome='done', finished_at=now(), component_outcomes=outcomes,
        )
    assert _rows(storage) == before


@pytest.mark.parametrize('stage', ['ordinary-active', 'failed', 'accepted-queued', 'accepted-active', 'accepted-done'])
def test_ordinary_claim_waits_until_an_accepted_component_repair_finishes(running_component, stage):
    storage, topology, generation, execution_id = running_component
    storage.create_job(Job(node_name='A', job_id=2, params={}))
    claim = dict(started_at=now(), session_id='ordinary-result', component=('A', 'B'))
    if stage != 'ordinary-active':
        storage.finalize_job_execution('A', 1, generation, execution_id, 'failed')
    if stage.startswith('accepted-'):
        storage.request_owned_job_restarts(storage.plan_owned_job_restarts([('A', 1)]))
        before = _rows(storage)
        with pytest.raises(RuntimeError, match='Ordinary admission'):
            storage.claim_job_executions_batch('A', [1, 2], **claim)
        assert _rows(storage) == before
        if stage != 'accepted-queued':
            generation, execution_id = storage.claim_job_execution('A', 1, **claim)
            if stage == 'accepted-done':
                storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
    if stage in {'failed', 'accepted-queued', 'accepted-active'}:
        before = _rows(storage)
        with pytest.raises(RuntimeError, match='Ordinary admission'):
            storage.claim_job_execution('A', 2, **claim)
        assert _rows(storage) == before
    else:
        storage.claim_job_execution('A', 2, **claim)
        assert storage.get_job_status('A', 2) == 'running'


def test_full_component_pending_execution_refuses_sampled_result_without_mutation(running_component):
    storage, topology, generation, execution_id = running_component
    storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
    before = _rows(storage)
    with pytest.raises(ValueError, match='successful result'):
        storage.finish_successful_component_execution(
            'ordinary-result', replace(_outcome(topology), lifecycle='sampled'),
        )
    assert _rows(storage) == before


def test_full_component_session_exit_refuses_sampled_result_without_mutation(running_component):
    storage, topology, generation, execution_id = running_component
    storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
    before = _rows(storage)
    with pytest.raises(RuntimeError, match='Only selected execution may settle as sampled'):
        storage.decide_execution_session_exit(
            'ordinary-result', outcome='done', finished_at=now(),
            component_outcomes=[replace(_outcome(topology), lifecycle='sampled')],
        )
    assert _rows(storage) == before
