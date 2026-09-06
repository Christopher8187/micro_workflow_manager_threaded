from __future__ import annotations

import json
import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event

import networkx as nx
import pytest

from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.topology import ComponentTopology


def _close(storage):
    storage.db_mutation_barrier()
    deadline = time.perf_counter() + 10
    while storage.mutation_writer_diagnostics()['writer_alive']:
        assert time.perf_counter() < deadline, 'Mutation writer did not retire'
        time.sleep(0.01)
    storage.close_database_connections()


@pytest.fixture
def owned_component(tmp_path, request):
    storage = FileStorage._create_new_project_state(tmp_path)
    request.addfinalizer(lambda: _close(storage))
    topology = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'C')]), [('A', 'B')]).snapshot()
    storage.register_component_topology(topology)
    storage.create_execution_session(
        'ordinary-start', session_kind='main', command='run',
        start_component=('A', 'B'), selected_components=[('A', 'B')],
        started_at='2026-09-06T08:00:00+00:00', hostname='test-worker',
        pid=os.getpid(), process_identity='ordinary-start',
    )
    storage.reserve_execution_components('ordinary-start', expected_shape=topology.shape_json)
    return storage, topology


def _rows(storage):
    connection = storage.db_connection()
    tables = [row['name'] for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )]
    return {table: [dict(row) for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')]
            for table in tables}


def test_ordinary_component_start_changes_only_its_queued_lifecycle_and_reopens(owned_component):
    storage, topology = owned_component
    before = storage.get_component_state(('A', 'B'))
    assert before['lifecycle'] == 'queued'
    assert before['stability'] is None and before['instability_origin'] is None
    expected_rows = _rows(storage)
    component_rows = expected_rows['component_states']
    component = next(row for row in component_rows if json.loads(row['component_key']) == ['A', 'B'])
    component['lifecycle'] = 'running'

    assert storage.begin_queued_component_execution(
        'ordinary-start', ('A', 'B'), expected_shape=topology.shape_json,
        expected_alignment_generation=0,
    ) is True

    assert storage.get_component_state(('A', 'B')) == {**before, 'lifecycle': 'running'}
    assert _rows(storage) == expected_rows
    _close(storage)
    reopened = FileStorage(storage.project_dir)
    try:
        assert reopened.get_component_state(('A', 'B')) == {**before, 'lifecycle': 'running'}
        assert _rows(reopened) == expected_rows
    finally:
        _close(reopened)


@pytest.mark.parametrize('case', [
    'missing-session', 'terminal-session', 'missing-reservation', 'other-reservation',
    'unselected', 'missing-state', 'shape', 'generation',
    'running', 'failed', 'sampled', 'done', 'queued-lineage', 'queued-misaligned',
])
def test_component_start_refuses_ineligible_state_without_partial_changes(owned_component, case):
    storage, topology = owned_component
    key = json.dumps(['A', 'B'])
    session_id = 'ordinary-start'
    expected_shape = topology.shape_json
    expected_generation = 0
    if case == 'missing-session':
        session_id = 'missing'
    elif case == 'terminal-session':
        storage.finish_execution_session(
            'ordinary-start', outcome='done', finished_at='2026-09-06T08:01:00+00:00',
        )
    elif case == 'missing-reservation':
        storage.release_execution_components('ordinary-start')
    elif case == 'other-reservation':
        storage.create_execution_session(
            'other', session_kind='interrupt', command='run',
            start_component=('A', 'B'), selected_components=[('A', 'B')],
            started_at='2026-09-06T08:00:00+00:00', hostname='test-worker',
            pid=os.getpid(), process_identity='other',
        )
        storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE component_reservations SET session_id='other' WHERE component_key=?", (key,),
        ))
    elif case == 'unselected':
        storage.submit_db_mutation(lambda connection: connection.execute(
            'DELETE FROM session_components WHERE session_id=?', ('ordinary-start',),
        ))
    elif case == 'missing-state':
        storage.submit_db_mutation(lambda connection: connection.execute(
            'DELETE FROM component_states WHERE component_key=?', (key,),
        ))
    elif case == 'shape':
        expected_shape = topology.shape_json.replace('"C"', '"D"')
    elif case == 'generation':
        expected_generation = 1
    elif case in ('running', 'failed', 'sampled', 'done'):
        stability = 'stable' if case in ('sampled', 'done') else None
        storage.submit_db_mutation(lambda connection: connection.execute(
            'UPDATE component_states SET lifecycle=?, stability=? WHERE component_key=?',
            (case, stability, key),
        ))
    else:
        def damage(connection):
            connection.execute('PRAGMA ignore_check_constraints=ON')
            try:
                if case == 'queued-lineage':
                    connection.execute("UPDATE component_states SET stability='stable' WHERE component_key=?", (key,))
                else:
                    connection.execute('UPDATE component_states SET misaligned=1 WHERE component_key=?', (key,))
            finally:
                connection.execute('PRAGMA ignore_check_constraints=OFF')
        storage.submit_db_mutation(damage)
    before = _rows(storage)
    with pytest.raises(RuntimeError):
        storage.begin_queued_component_execution(
            session_id, ('A', 'B'), expected_shape=expected_shape,
            expected_alignment_generation=expected_generation,
        )
    assert _rows(storage) == before


@pytest.mark.parametrize('generation', [-1, True, '0', None])
def test_component_start_requires_an_exact_nonnegative_generation(owned_component, generation):
    storage, topology = owned_component
    before = _rows(storage)
    with pytest.raises(ValueError, match='generation'):
        storage.begin_queued_component_execution(
            'ordinary-start', ('A', 'B'), expected_shape=topology.shape_json,
            expected_alignment_generation=generation,
        )
    assert _rows(storage) == before


@pytest.mark.parametrize('failure', ['abort', 'ignore'])
def test_component_start_rolls_back_when_its_write_fails(owned_component, failure):
    storage, topology = owned_component
    statement = ("SELECT RAISE(ABORT, 'injected start write failure')"
                 if failure == 'abort' else 'SELECT RAISE(IGNORE)')
    storage.submit_db_mutation(lambda connection: connection.execute(
        "CREATE TRIGGER abort_component_start BEFORE UPDATE OF lifecycle ON component_states "
        "WHEN NEW.lifecycle='running' BEGIN "
        "UPDATE component_states SET alignment_generation=3 WHERE component_key='[\"C\"]'; "
        + statement + '; END',
    ))
    before = _rows(storage)
    error = sqlite3.IntegrityError if failure == 'abort' else RuntimeError
    message = 'injected start write failure' if failure == 'abort' else 'Component changed before start'
    with pytest.raises(error, match=message):
        storage.begin_queued_component_execution(
            'ordinary-start', ('A', 'B'), expected_shape=topology.shape_json,
            expected_alignment_generation=0,
        )
    assert _rows(storage) == before


def test_simultaneous_component_starts_commit_only_one_transition(owned_component):
    storage, topology = owned_component
    second = FileStorage(storage.project_dir)
    barrier = Barrier(2)

    def start(candidate):
        barrier.wait(timeout=10)
        try:
            candidate.begin_queued_component_execution(
                'ordinary-start', ('A', 'B'), expected_shape=topology.shape_json,
                expected_alignment_generation=0,
            )
        except RuntimeError as error:
            assert 'aligned queued component' in str(error)
            return 'refused'
        return 'started'

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(start, candidate) for candidate in (storage, second)]
            assert sorted(future.result(timeout=20) for future in futures) == ['refused', 'started']
        state = storage.get_component_state(('A', 'B'))
        assert (state['lifecycle'], state['stability'], state['instability_origin']) == ('running', None, None)
        assert storage.get_execution_session('ordinary-start')['status'] == 'running'
        assert storage.get_component_reservation(('A', 'B'))['session_id'] == 'ordinary-start'
    finally:
        _close(second)


@pytest.mark.parametrize('change', ['terminal', 'transferred', 'generation'])
def test_component_start_rechecks_state_when_its_mutation_is_submitted(owned_component, monkeypatch, change):
    storage, topology = owned_component
    other = FileStorage(storage.project_dir)
    other.create_execution_session(
        'next-owner', session_kind='interrupt', command='run',
        start_component=('A', 'B'), selected_components=[('A', 'B')],
        started_at='2026-09-06T08:00:00+00:00', hostname='test-worker',
        pid=os.getpid(), process_identity='next-owner',
    )
    awaiting_write, proceed = Event(), Event()
    submit = storage.submit_db_mutation

    def paused_submit(*args, **kwargs):
        awaiting_write.set()
        assert proceed.wait(10), 'The component start was not released'
        return submit(*args, **kwargs)

    try:
        with monkeypatch.context() as patch, ThreadPoolExecutor(max_workers=1) as pool:
            patch.setattr(storage, 'submit_db_mutation', paused_submit)
            future = pool.submit(
                storage.begin_queued_component_execution, 'ordinary-start', ('A', 'B'),
                expected_shape=topology.shape_json, expected_alignment_generation=0,
            )
            try:
                assert awaiting_write.wait(5), 'Component start did not reach its write boundary'
                if change == 'terminal':
                    other.finish_execution_session(
                        'ordinary-start', outcome='done', finished_at='2026-09-06T08:01:00+00:00',
                    )
                elif change == 'transferred':
                    assert other.release_execution_components('ordinary-start') == 1
                    assert other.reserve_execution_components('next-owner', expected_shape=topology.shape_json)
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
