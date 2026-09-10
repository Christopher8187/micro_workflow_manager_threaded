from __future__ import annotations

import os
import socket
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier, Event

import networkx as nx
import pytest

from micro_workflow_manager.models import Job
from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.processes import process_identity
from micro_workflow_manager.topology import ComponentTopology


def _close(storage):
    storage.db_mutation_barrier()
    deadline = time.perf_counter() + 10
    while storage.mutation_writer_diagnostics()['writer_alive']:
        assert time.perf_counter() < deadline, 'Mutation writer did not retire'
        time.sleep(0.01)
    storage.close_thread_connection()


def _session(storage, session_id, kind, component, snapshot):
    storage.create_execution_session(
        session_id, session_kind=kind, command='resume', start_component=component,
        selected_components=[component], started_at='2026-09-05T12:00:00+00:00',
        hostname=socket.gethostname(), pid=os.getpid(), process_identity=process_identity(os.getpid()),
        expected_shape=snapshot.shape_json,
    )


def _admit_interrupt_origin(storage, session_id, component, snapshot, *, release=True):
    _session(storage, session_id, 'interrupt', component, snapshot)
    assert storage.reserve_execution_components(session_id, expected_shape=snapshot.shape_json) is True
    if release:
        assert storage.release_execution_components(session_id) == 1
        assert storage.get_component_reservation(component) is None
    assert storage.db_connection().execute(
        'SELECT scope_admitted FROM execution_sessions WHERE session_id=?', (session_id,),
    ).fetchone()[0] == 1


def _seed_state(
    storage, component, *, lifecycle, stability, origin, generation, misaligned=0,
):
    key = encode_component_key(component)

    def seed(connection):
        row = connection.execute(
            'SELECT shape_id FROM component_states WHERE component_key=?', (key,),
        ).fetchone()
        assert row is not None
        retained = lifecycle in ('sampled', 'done') or (lifecycle == 'running' and stability is not None)
        if retained:
            result_lifecycle = lifecycle if lifecycle in ('sampled', 'done') else 'sampled'
            connection.execute(
                'INSERT INTO component_successful_results '
                '(component_key, shape_id, alignment_generation, lifecycle, stability, instability_origin) '
                'VALUES(?, ?, ?, ?, ?, ?)',
                (key, row['shape_id'], generation, result_lifecycle, stability, origin),
            )
        connection.execute(
            'UPDATE component_states SET lifecycle=?, stability=?, instability_origin=?, misaligned=?, '
            'alignment_generation=?, retained_result_shape_id=?, retained_result_alignment_generation=? '
            'WHERE component_key=?',
            (
                lifecycle, stability, origin, misaligned, generation,
                row['shape_id'] if retained else None, generation if retained else None, key,
            ),
        )

    storage.submit_db_mutation(seed)


def _replace_sampled_generation(connection, component, generation):
    key = encode_component_key(component)
    state = connection.execute(
        'SELECT shape_id, alignment_generation, retained_result_shape_id, '
        'retained_result_alignment_generation FROM component_states WHERE component_key=?',
        (key,),
    ).fetchone()
    assert state is not None
    retained = connection.execute(
        'SELECT lifecycle, stability, instability_origin FROM component_successful_results '
        'WHERE component_key=? AND shape_id=? AND alignment_generation=?',
        (key, state['retained_result_shape_id'], state['retained_result_alignment_generation']),
    ).fetchone()
    assert retained is not None and retained['lifecycle'] == 'sampled'
    assert connection.execute(
        'INSERT INTO component_successful_results '
        '(component_key, shape_id, alignment_generation, lifecycle, stability, instability_origin) '
        'VALUES(?, ?, ?, ?, ?, ?)',
        (key, state['shape_id'], generation, retained['lifecycle'], retained['stability'],
         retained['instability_origin']),
    ).rowcount == 1
    changed = connection.execute(
        'UPDATE component_states SET alignment_generation=?, retained_result_shape_id=shape_id, '
        'retained_result_alignment_generation=? WHERE component_key=? AND shape_id=? '
        'AND alignment_generation=? AND retained_result_shape_id=? '
        'AND retained_result_alignment_generation=?',
        (generation, generation, key, state['shape_id'], state['alignment_generation'],
         state['retained_result_shape_id'], state['retained_result_alignment_generation']),
    ).rowcount
    assert changed == 1
    return changed


def _other_rows(storage):
    tables = ('metadata', 'graph_shapes', 'component_definitions', 'execution_sessions',
              'session_components', 'session_jobs', 'component_reservations',
              'component_holds', 'component_successful_results', 'job_execution_owners',
              'jobs', 'nodes', 'job_events')
    return {
        table: [tuple(row) for row in storage.db_connection().execute(
            f'SELECT * FROM {table} ORDER BY rowid'
        )]
        for table in tables
    }


def _component_rows(storage):
    return {row['component_key']: dict(row) for row in storage.db_connection().execute(
        'SELECT * FROM component_states'
    )}


def _pending_rows(storage):
    return [dict(row) for row in storage.db_connection().execute(
        'SELECT pending.session_id, pending.component_key, shape.shape_json, '
        'pending.alignment_generation, starting_shape.shape_json AS starting_shape_json, '
        'pending.completion_ready, pending.execution_kind, pending.starting_lifecycle, '
        'pending.starting_misaligned, pending.stability, pending.instability_origin '
        'FROM pending_component_executions AS pending '
        'JOIN graph_shapes AS shape ON shape.shape_id=pending.shape_id '
        'JOIN graph_shapes AS starting_shape ON starting_shape.shape_id=pending.starting_shape_id '
        'ORDER BY pending.session_id, pending.component_key'
    )]


def _begin_sampled(storage, session_id, component, snapshot, generation, lineage):
    return storage.begin_sampled_component_execution(
        session_id, component, expected_shape=snapshot.shape_json,
        expected_alignment_generation=generation, successful_lineage=lineage,
    )


def _expected_pending(session_id, component, snapshot, generation, lineage):
    return [{
        'session_id': session_id,
        'component_key': encode_component_key(component),
        'shape_json': snapshot.shape_json,
        'alignment_generation': generation,
        'starting_shape_json': snapshot.shape_json,
        'completion_ready': 0,
        'execution_kind': 'resume',
        'starting_lifecycle': 'sampled',
        'starting_misaligned': 0,
        'stability': lineage[0],
        'instability_origin': lineage[1],
    }]


def test_sampled_resume_preserves_finished_interrupt_lineage_under_new_main_owner(tmp_path):
    component = ('A', 'B')
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'C')]), [('A', 'B')]).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(snapshot)
        storage.create_job(Job(node_name='A', job_id=1, params={'source': 'prior-input'}))
        _admit_interrupt_origin(storage, 'int-origin', component, snapshot, release=False)
        assert storage.finish_execution_session(
            'int-origin', outcome='done', finished_at='2026-09-05T12:01:00+00:00',
        ) is True
        assert storage.release_execution_components('int-origin') == 1
        assert storage.get_component_reservation(component) is None
        origin = storage.get_execution_session('int-origin')
        assert (origin['session_kind'], origin['status']) == ('interrupt', 'terminal')
        # Seed a native sampled result after its existing job was created.
        _seed_state(
            storage, component, lifecycle='sampled', stability='unstable', origin='int-origin',
            generation=7,
        )
        _session(storage, 'main-resume', 'main', component, snapshot)
        owner = storage.get_execution_session('main-resume')
        assert (owner['session_kind'], owner['status']) == ('main', 'running')
        assert storage.reserve_execution_components('main-resume', expected_shape=snapshot.shape_json) is True
        assert storage.get_component_reservation(component) == {
            'members': component, 'session_id': 'main-resume',
        }
        storage.set_node_status('A', 'done')
        output = storage.node_output_dir('A') / 'established.txt'
        output.write_bytes(b'established output')
        before = storage.get_component_state(component)
        assert before['lifecycle'] == 'sampled'
        other_component = storage.get_component_state(('C',))
        other_rows = _other_rows(storage)
        expected_rows = _component_rows(storage)
        expected_rows[encode_component_key(component)]['lifecycle'] = 'running'

        lineage = ('unstable', 'int-origin')
        assert _begin_sampled(storage, 'main-resume', component, snapshot, 7, lineage) is True

        expected = {**before, 'lifecycle': 'running'}
        assert storage.get_component_state(component) == expected
        assert expected['instability_origin'] == 'int-origin'
        assert expected['stability'] == 'unstable'
        assert expected['misaligned'] is False
        assert expected['alignment_generation'] == 7
        assert expected['shape_json'] == snapshot.shape_json
        assert storage.get_component_state(('C',)) == other_component
        assert _component_rows(storage) == expected_rows
        assert _other_rows(storage) == other_rows
        expected_pending = _expected_pending('main-resume', component, snapshot, 7, lineage)
        assert _pending_rows(storage) == expected_pending
        assert output.read_bytes() == b'established output'
    finally:
        _close(storage)

    reopened = FileStorage(tmp_path)
    try:
        assert reopened.get_component_state(component) == expected
        assert reopened.get_component_state(('C',)) == other_component
        assert _component_rows(reopened) == expected_rows
        assert _other_rows(reopened) == other_rows
        assert _pending_rows(reopened) == expected_pending
        assert output.read_bytes() == b'established output'
    finally:
        _close(reopened)


@pytest.mark.parametrize('failure,error,message', [
    ('ABORT', sqlite3.IntegrityError, 'injected resume failure'),
    ('IGNORE', RuntimeError, 'Component changed before start'),
])
def test_sampled_resume_rolls_back_suppressed_or_failed_update_and_can_retry(tmp_path, failure, error, message):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(snapshot)
        _session(storage, 'main-resume', 'main', ('A',), snapshot)
        assert storage.reserve_execution_components('main-resume', expected_shape=snapshot.shape_json) is True
        key = encode_component_key(('A',))
        _seed_state(
            storage, ('A',), lifecycle='sampled', stability='stable', origin=None, generation=7,
        )
        raise_sql = "RAISE(ABORT, 'injected resume failure')" if failure == 'ABORT' else 'RAISE(IGNORE)'
        storage.submit_db_mutation(lambda connection: connection.execute(
            'CREATE TRIGGER interrupt_resume BEFORE UPDATE OF lifecycle ON component_states '
            "WHEN NEW.lifecycle='running' BEGIN "
            "INSERT INTO metadata(key,value) VALUES('resume_test_marker','must roll back'); "
            f'SELECT {raise_sql}; END'
        ))
        before = _component_rows(storage), _other_rows(storage)

        with pytest.raises(error, match=message):
            _begin_sampled(storage, 'main-resume', ('A',), snapshot, 7, ('stable', None))

        assert (_component_rows(storage), _other_rows(storage)) == before
        assert _pending_rows(storage) == []
        storage.submit_db_mutation(lambda connection: connection.execute('DROP TRIGGER interrupt_resume'))
        assert _begin_sampled(
            storage, 'main-resume', ('A',), snapshot, 7, ('stable', None),
        ) is True
        expected = before[0]
        expected[key]['lifecycle'] = 'running'
        assert (_component_rows(storage), _other_rows(storage)) == (expected, before[1])
        assert _pending_rows(storage) == _expected_pending(
            'main-resume', ('A',), snapshot, 7, ('stable', None),
        )
    finally:
        _close(storage)

@pytest.mark.parametrize('ownership,error', [
    pytest.param('unknown-session', 'selected running session',
                 id='unknown-session-existing running session'),
    pytest.param('terminal-session', 'selected running session',
                 id='terminal-session-existing running session'),
    pytest.param('no-reservation', 'reservation', id='no-reservation-reservation'),
    pytest.param('other-owner', 'reservation', id='other-owner-reservation'),
    pytest.param('missing-selection', 'selected running session',
                 id='missing-selection-selected scope'),
])
def test_sampled_resume_refuses_invalid_session_ownership_without_mutation(tmp_path, ownership, error):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(snapshot)
        storage.create_job(Job(node_name='A', job_id=1, params={'source': 'prior-input'}))
        _seed_state(
            storage, ('A',), lifecycle='sampled', stability='stable', origin=None, generation=7,
        )
        _session(storage, 'main-resume', 'main', ('A',), snapshot)
        assert storage.reserve_execution_components('main-resume', expected_shape=snapshot.shape_json) is True
        actor = 'main-resume'
        if ownership == 'unknown-session':
            actor = 'unknown'
            assert storage.get_execution_session(actor) is None
        elif ownership == 'terminal-session':
            assert storage.finish_execution_session(
                actor, outcome='done', finished_at='2026-09-05T12:01:00+00:00',
            ) is True
        elif ownership in ('no-reservation', 'other-owner'):
            assert storage.release_execution_components(actor) == 1
            if ownership == 'other-owner':
                _session(storage, 'other-owner', 'interrupt', ('A',), snapshot)
                assert storage.reserve_execution_components(
                    'other-owner', expected_shape=snapshot.shape_json,
                ) is True
        else:
            # Damage the immutable selection while keeping its exact reservation.
            assert storage.submit_db_mutation(lambda connection: connection.execute(
                'DELETE FROM session_components WHERE session_id=?', (actor,),
            ).rowcount) == 1
        before = _component_rows(storage), _other_rows(storage)

        with pytest.raises(RuntimeError, match=error):
            _begin_sampled(storage, actor, ('A',), snapshot, 7, ('stable', None))

        assert (_component_rows(storage), _other_rows(storage)) == before
        assert _pending_rows(storage) == []
    finally:
        _close(storage)


@pytest.mark.parametrize('lifecycle,stability,misaligned,generation', [
    ('sampled', 'stable', 0, 8),
    ('sampled', 'stable', 1, 7),
    ('queued', None, 0, 7),
    ('done', 'stable', 0, 7),
    ('failed', None, 0, 7),
])
def test_sampled_resume_requires_current_aligned_sampled_state(
    tmp_path, lifecycle, stability, misaligned, generation,
):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(snapshot)
        storage.create_job(Job(node_name='A', job_id=1, params={'source': 'prior-input'}))
        _seed_state(
            storage, ('A',), lifecycle=lifecycle, stability=stability, origin=None,
            misaligned=misaligned, generation=generation,
        )
        _session(storage, 'main-resume', 'main', ('A',), snapshot)
        assert storage.reserve_execution_components('main-resume', expected_shape=snapshot.shape_json) is True
        before = _component_rows(storage), _other_rows(storage)

        with pytest.raises(RuntimeError, match='aligned sampled component at the expected generation'):
            _begin_sampled(storage, 'main-resume', ('A',), snapshot, 7, ('stable', None))

        assert (_component_rows(storage), _other_rows(storage)) == before
        assert _pending_rows(storage) == []
    finally:
        _close(storage)


@pytest.mark.parametrize('expected_generation,stored_generation', [
    (False, 0), (True, 1), (7.0, 7), ('7', 7), (None, 7), (-1, 7),
])
def test_sampled_resume_rejects_generation_coercion_before_mutation(
    tmp_path, expected_generation, stored_generation,
):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(snapshot)
        _seed_state(
            storage, ('A',), lifecycle='sampled', stability='stable', origin=None,
            generation=stored_generation,
        )
        _session(storage, 'main-resume', 'main', ('A',), snapshot)
        assert storage.reserve_execution_components('main-resume', expected_shape=snapshot.shape_json) is True
        before = _component_rows(storage), _other_rows(storage)

        with pytest.raises(ValueError, match='nonnegative integer'):
            _begin_sampled(
                storage, 'main-resume', ('A',), snapshot, expected_generation, ('stable', None),
            )

        assert (_component_rows(storage), _other_rows(storage)) == before
        assert _pending_rows(storage) == []
    finally:
        _close(storage)


@pytest.mark.parametrize('damage,error', [
    pytest.param('main-origin', 'Invalid component state',
                 id='main-origin-Invalid component state'),
    pytest.param('missing-origin', 'Invalid component state',
                 id='missing-origin-Invalid component state'),
    pytest.param('blank-origin', 'Invalid retained successful component result',
                 id='blank-origin-Invalid component state'),
    pytest.param('missing-lineage', 'Invalid component state',
                 id='missing-lineage-Invalid component state'),
    pytest.param('missing-shape', 'Session admitted graph shape is missing',
                 id='missing-shape-Incomplete component state or producing graph shape'),
    pytest.param('missing-definition', 'Session admitted shape has incomplete component definitions',
                 id='missing-definition-Unknown component'),
    pytest.param('missing-state', 'Component definition has no current state',
                 id='missing-state-Incomplete component state or producing graph shape'),
])
def test_sampled_resume_refuses_damaged_persisted_state_without_repair(tmp_path, damage, error):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(snapshot)
        _admit_interrupt_origin(storage, 'int-origin', ('A',), snapshot)
        _session(storage, 'main-resume', 'main', ('A',), snapshot)
        assert storage.reserve_execution_components('main-resume', expected_shape=snapshot.shape_json) is True
        storage.create_job(Job(node_name='A', job_id=1, params={'source': 'prior-input'}))
        key = encode_component_key(('A',))
        _seed_state(
            storage, ('A',), lifecycle='sampled', stability='unstable', origin='int-origin',
            generation=7,
        )
        # Deliberately corrupt only a disposable database, after valid owner setup.
        connection = sqlite3.connect(storage.state_database_path())
        try:
            connection.execute('PRAGMA foreign_keys=OFF')
            connection.execute('PRAGMA ignore_check_constraints=ON')
            if damage in ('main-origin', 'missing-origin', 'blank-origin'):
                origin = {'main-origin': 'main-resume', 'missing-origin': 'missing', 'blank-origin': '   '}[damage]
                if damage == 'blank-origin':
                    assert connection.execute(
                        'UPDATE execution_sessions SET session_id=? WHERE session_id=?', (origin, 'int-origin'),
                    ).rowcount == 1
                assert connection.execute(
                    'UPDATE component_states SET instability_origin=? WHERE component_key=?', (origin, key),
                ).rowcount == 1
            elif damage == 'missing-lineage':
                assert connection.execute(
                    'UPDATE component_states SET stability=NULL, instability_origin=NULL WHERE component_key=?',
                    (key,),
                ).rowcount == 1
            elif damage == 'missing-shape':
                assert connection.execute(
                    'DELETE FROM graph_shapes WHERE shape_id=(SELECT shape_id FROM component_definitions '
                    'WHERE component_key=?)', (key,),
                ).rowcount == 1
            elif damage == 'missing-definition':
                assert connection.execute(
                    'DELETE FROM component_definitions WHERE component_key=?', (key,),
                ).rowcount == 1
            else:
                assert connection.execute(
                    'DELETE FROM component_states WHERE component_key=?', (key,),
                ).rowcount == 1
            connection.commit()
        finally:
            connection.close()
        before = _component_rows(storage), _other_rows(storage)

        with pytest.raises(RuntimeError, match=error):
            _begin_sampled(storage, 'main-resume', ('A',), snapshot, 7, ('unstable', 'int-origin'))

        assert (_component_rows(storage), _other_rows(storage)) == before
        assert _pending_rows(storage) == []
    finally:
        _close(storage)


@pytest.mark.parametrize('stability,origin,actor_kind', [
    ('stable', None, 'main'),
    ('unstable', 'live-interrupt', 'main'),
    ('unstable', '  exact-interrupt  ', 'interrupt'),
])
def test_sampled_resume_preserves_lineage_and_rechecks_repeated_calls(tmp_path, stability, origin, actor_kind):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(snapshot)
        if origin is not None:
            _admit_interrupt_origin(storage, origin, ('A',), snapshot)
            assert storage.get_execution_session(origin)['status'] == 'running'
        _session(storage, 'resuming-owner', actor_kind, ('A',), snapshot)
        assert storage.reserve_execution_components('resuming-owner', expected_shape=snapshot.shape_json) is True
        key = encode_component_key(('A',))
        _seed_state(
            storage, ('A',), lifecycle='sampled', stability=stability, origin=origin, generation=7,
        )
        expected, others = _component_rows(storage), _other_rows(storage)
        expected[key]['lifecycle'] = 'running'

        lineage = (stability, origin)
        assert _begin_sampled(storage, 'resuming-owner', ('A',), snapshot, 7, lineage) is True
        assert (_component_rows(storage), _other_rows(storage)) == (expected, others)
        pending = _expected_pending('resuming-owner', ('A',), snapshot, 7, lineage)
        assert _pending_rows(storage) == pending
        with pytest.raises(RuntimeError, match='aligned sampled component'):
            _begin_sampled(storage, 'resuming-owner', ('A',), snapshot, 7, lineage)
        with pytest.raises(RuntimeError, match='aligned sampled component'):
            _begin_sampled(storage, 'resuming-owner', ('A',), snapshot, 8, lineage)
        assert (_component_rows(storage), _other_rows(storage)) == (expected, others)
        assert _pending_rows(storage) == pending
        assert storage.release_execution_components('resuming-owner') == 1
        after_release = _other_rows(storage)
        with pytest.raises(RuntimeError, match='reservation'):
            _begin_sampled(storage, 'resuming-owner', ('A',), snapshot, 7, lineage)
        assert (_component_rows(storage), _other_rows(storage)) == (expected, after_release)
        assert _pending_rows(storage) == pending
    finally:
        _close(storage)


@pytest.mark.parametrize('change,error', [
    pytest.param('terminal', 'selected running session', id='terminal-existing running session'),
    pytest.param('transferred', 'reservation', id='transferred-reservation'),
    pytest.param('generation', 'expected generation', id='generation-expected generation'),
])
def test_sampled_resume_rechecks_after_waiting_for_its_write_transaction(tmp_path, monkeypatch, change, error):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    other = FileStorage(tmp_path)
    awaiting_transaction, proceed = Event(), Event()
    try:
        storage.register_component_topology(snapshot)
        _session(storage, 'old-owner', 'main', ('A',), snapshot)
        _session(storage, 'next-owner', 'interrupt', ('A',), snapshot)
        assert storage.reserve_execution_components('old-owner', expected_shape=snapshot.shape_json) is True
        _seed_state(
            storage, ('A',), lifecycle='sampled', stability='stable', origin=None, generation=7,
        )
        original_transaction = storage.db_transaction

        @contextmanager
        def paused_transaction(*args, **kwargs):
            awaiting_transaction.set()
            assert proceed.wait(10), 'The test did not release the paused transaction'
            with original_transaction(*args, **kwargs) as connection:
                yield connection

        def attempt_resume():
            try:
                return _begin_sampled(
                    storage, 'old-owner', ('A',), snapshot, 7, ('stable', None),
                )
            finally:
                storage.close_thread_connection()

        monkeypatch.setattr(storage, 'db_transaction', paused_transaction)
        with ThreadPoolExecutor(max_workers=1) as executor:
            attempt = executor.submit(attempt_resume)
            try:
                assert awaiting_transaction.wait(5), 'Resume did not reach its write transaction'
                if change == 'terminal':
                    assert other.finish_execution_session(
                        'old-owner', outcome='done', finished_at='2026-09-05T12:01:00+00:00',
                    ) is True
                elif change == 'transferred':
                    assert other.release_execution_components('old-owner') == 1
                    assert other.reserve_execution_components('next-owner', expected_shape=snapshot.shape_json) is True
                else:
                    assert other.submit_db_mutation(
                        lambda connection: _replace_sampled_generation(connection, ('A',), 8)
                    ) == 1
                expected = _component_rows(other), _other_rows(other)
            finally:
                proceed.set()
            with pytest.raises(RuntimeError, match=error):
                attempt.result(timeout=15)

        assert (_component_rows(storage), _other_rows(storage)) == expected
        assert _pending_rows(storage) == []
    finally:
        proceed.set()
        _close(other)
        _close(storage)


def test_concurrent_sampled_resumes_change_exactly_one_component_once(tmp_path):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(snapshot)
        _session(storage, 'main-resume', 'main', ('A',), snapshot)
        assert storage.reserve_execution_components('main-resume', expected_shape=snapshot.shape_json) is True
        key = encode_component_key(('A',))
        _seed_state(
            storage, ('A',), lifecycle='sampled', stability='stable', origin=None, generation=7,
        )
        expected, others = _component_rows(storage), _other_rows(storage)
        expected[key]['lifecycle'] = 'running'
        start = Barrier(6)

        def attempt_resume():
            try:
                start.wait(timeout=10)
                try:
                    return _begin_sampled(
                        storage, 'main-resume', ('A',), snapshot, 7, ('stable', None),
                    )
                except RuntimeError as error:
                    return error
            finally:
                storage.close_thread_connection()

        with ThreadPoolExecutor(max_workers=6) as executor:
            attempts = [executor.submit(attempt_resume) for _ in range(6)]
            results = [attempt.result(timeout=15) for attempt in attempts]
        assert sum(result is True for result in results) == 1
        refused = [result for result in results if isinstance(result, RuntimeError)]
        assert len(refused) == 5
        assert all('aligned sampled component' in str(error) for error in refused)
        assert (_component_rows(storage), _other_rows(storage)) == (expected, others)
        assert _pending_rows(storage) == _expected_pending(
            'main-resume', ('A',), snapshot, 7, ('stable', None),
        )
    finally:
        _close(storage)


def test_sampled_resume_refuses_missing_native_session_without_changing_existing_work(tmp_path):
    storage = FileStorage(tmp_path)
    try:
        storage.create_job(Job(node_name='A', job_id=1, params={'source': 'prior-input'}))
        storage.set_node_status('A', 'done')
        output = storage.node_output_dir('A') / 'established.txt'
        output.write_bytes(b'established output')
        before = tuple(storage.db_connection().iterdump())

        with pytest.raises(RuntimeError, match='selected running session'):
            storage.begin_sampled_component_execution(
                'main-resume', ('A',), expected_shape='missing-native-shape',
                expected_alignment_generation=7, successful_lineage=('stable', None),
            )

        assert tuple(storage.db_connection().iterdump()) == before
        assert output.read_bytes() == b'established output'
        assert storage.db_connection().execute(
            "SELECT value FROM metadata WHERE key='database_schema_version'"
        ).fetchone()[0] == '9'
    finally:
        _close(storage)
