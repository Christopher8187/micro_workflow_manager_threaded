from __future__ import annotations

import os
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
from micro_workflow_manager.topology import ComponentTopology


def _close(storage):
    storage.db_mutation_barrier()
    deadline = time.perf_counter() + 10
    while storage.mutation_writer_diagnostics()['writer_alive']:
        assert time.perf_counter() < deadline, 'Mutation writer did not retire'
        time.sleep(0.01)
    storage.close_thread_connection()


def _session(storage, session_id, kind, component):
    storage.create_execution_session(
        session_id, session_kind=kind, command='resume', start_component=component,
        selected_components=[component], started_at='2026-09-05T12:00:00+00:00',
        hostname='worker.example', pid=os.getpid(), process_identity=session_id,
    )


def _other_rows(storage):
    tables = ('metadata', 'graph_shapes', 'component_definitions', 'execution_sessions',
              'session_components', 'session_jobs', 'component_reservations',
              'component_holds', 'job_execution_owners', 'jobs', 'nodes', 'job_events')
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


def test_sampled_resume_preserves_finished_interrupt_lineage_under_new_main_owner(tmp_path):
    component = ('A', 'B')
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'C')]), [('A', 'B')]).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(snapshot)
        _session(storage, 'int-origin', 'interrupt', component)
        assert storage.finish_execution_session(
            'int-origin', outcome='done', finished_at='2026-09-05T12:01:00+00:00',
        ) is True
        origin = storage.get_execution_session('int-origin')
        assert (origin['session_kind'], origin['status']) == ('interrupt', 'terminal')
        # No completion transition exists yet. Seed its established sampled result.
        assert storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE component_states SET lifecycle='sampled', stability='unstable', "
            "instability_origin='int-origin', alignment_generation=7 WHERE component_key=?",
            (encode_component_key(component),),
        ).rowcount) == 1
        _session(storage, 'main-resume', 'main', component)
        owner = storage.get_execution_session('main-resume')
        assert (owner['session_kind'], owner['status']) == ('main', 'running')
        assert storage.reserve_execution_components('main-resume', expected_shape=snapshot.shape_json) is True
        assert storage.get_component_reservation(component) == {
            'members': component, 'session_id': 'main-resume',
        }
        storage.create_job(Job(node_name='A', job_id=1, params={'source': 'prior-input'}))
        storage.set_node_status('A', 'done')
        output = storage.node_output_dir('A') / 'established.txt'
        output.write_bytes(b'established output')
        before = storage.get_component_state(component)
        assert before['lifecycle'] == 'sampled'
        other_component = storage.get_component_state(('C',))
        other_rows = _other_rows(storage)
        expected_rows = _component_rows(storage)
        expected_rows[encode_component_key(component)]['lifecycle'] = 'running'

        assert storage.begin_sampled_component_resume(
            'main-resume', component, expected_alignment_generation=7,
        ) is True

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
        assert output.read_bytes() == b'established output'
    finally:
        _close(storage)

    reopened = FileStorage(tmp_path)
    try:
        assert reopened.get_component_state(component) == expected
        assert reopened.get_component_state(('C',)) == other_component
        assert _component_rows(reopened) == expected_rows
        assert _other_rows(reopened) == other_rows
        assert output.read_bytes() == b'established output'
    finally:
        _close(reopened)


@pytest.mark.parametrize('failure,error,message', [
    ('ABORT', sqlite3.IntegrityError, 'injected resume failure'),
    ('IGNORE', RuntimeError, 'Sampled component changed before resume'),
])
def test_sampled_resume_rolls_back_suppressed_or_failed_update_and_can_retry(tmp_path, failure, error, message):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(snapshot)
        _session(storage, 'main-resume', 'main', ('A',))
        assert storage.reserve_execution_components('main-resume', expected_shape=snapshot.shape_json) is True
        key = encode_component_key(('A',))
        assert storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE component_states SET lifecycle='sampled', stability='stable', "
            'alignment_generation=7 WHERE component_key=?', (key,),
        ).rowcount) == 1
        raise_sql = "RAISE(ABORT, 'injected resume failure')" if failure == 'ABORT' else 'RAISE(IGNORE)'
        storage.submit_db_mutation(lambda connection: connection.execute(
            'CREATE TRIGGER interrupt_resume BEFORE UPDATE OF lifecycle ON component_states '
            "WHEN NEW.lifecycle='running' BEGIN "
            "INSERT INTO metadata(key,value) VALUES('resume_test_marker','must roll back'); "
            f'SELECT {raise_sql}; END'
        ))
        before = _component_rows(storage), _other_rows(storage)

        with pytest.raises(error, match=message):
            storage.begin_sampled_component_resume('main-resume', ('A',), expected_alignment_generation=7)

        assert (_component_rows(storage), _other_rows(storage)) == before
        storage.submit_db_mutation(lambda connection: connection.execute('DROP TRIGGER interrupt_resume'))
        assert storage.begin_sampled_component_resume(
            'main-resume', ('A',), expected_alignment_generation=7,
        ) is True
        expected = before[0]
        expected[key]['lifecycle'] = 'running'
        assert (_component_rows(storage), _other_rows(storage)) == (expected, before[1])
    finally:
        _close(storage)

@pytest.mark.parametrize('ownership,error', [
    ('unknown-session', 'existing running session'),
    ('terminal-session', 'existing running session'),
    ('no-reservation', 'reservation'),
    ('other-owner', 'reservation'),
    ('missing-selection', 'selected scope'),
])
def test_sampled_resume_refuses_invalid_session_ownership_without_mutation(tmp_path, ownership, error):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(snapshot)
        assert storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE component_states SET lifecycle='sampled', stability='stable', "
            "alignment_generation=7 WHERE component_key=?", (encode_component_key(('A',)),),
        ).rowcount) == 1
        _session(storage, 'main-resume', 'main', ('A',))
        assert storage.reserve_execution_components('main-resume', expected_shape=snapshot.shape_json) is True
        storage.create_job(Job(node_name='A', job_id=1, params={'source': 'prior-input'}))
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
                _session(storage, 'other-owner', 'interrupt', ('A',))
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
            storage.begin_sampled_component_resume(actor, ('A',), expected_alignment_generation=7)

        assert (_component_rows(storage), _other_rows(storage)) == before
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
        assert storage.submit_db_mutation(lambda connection: connection.execute(
            'UPDATE component_states SET lifecycle=?, stability=?, misaligned=?, '
            'alignment_generation=? WHERE component_key=?',
            (lifecycle, stability, misaligned, generation, encode_component_key(('A',))),
        ).rowcount) == 1
        _session(storage, 'main-resume', 'main', ('A',))
        assert storage.reserve_execution_components('main-resume', expected_shape=snapshot.shape_json) is True
        storage.create_job(Job(node_name='A', job_id=1, params={'source': 'prior-input'}))
        before = _component_rows(storage), _other_rows(storage)

        with pytest.raises(RuntimeError, match='aligned sampled component at the expected generation'):
            storage.begin_sampled_component_resume('main-resume', ('A',), expected_alignment_generation=7)

        assert (_component_rows(storage), _other_rows(storage)) == before
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
        assert storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE component_states SET lifecycle='sampled', stability='stable', "
            'alignment_generation=? WHERE component_key=?',
            (stored_generation, encode_component_key(('A',))),
        ).rowcount) == 1
        _session(storage, 'main-resume', 'main', ('A',))
        assert storage.reserve_execution_components('main-resume', expected_shape=snapshot.shape_json) is True
        before = _component_rows(storage), _other_rows(storage)

        with pytest.raises(ValueError, match='nonnegative integer'):
            storage.begin_sampled_component_resume(
                'main-resume', ('A',), expected_alignment_generation=expected_generation,
            )

        assert (_component_rows(storage), _other_rows(storage)) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('damage,error', [
    ('main-origin', 'Invalid component state'),
    ('missing-origin', 'Invalid component state'),
    ('blank-origin', 'Invalid component state'),
    ('missing-lineage', 'Invalid component state'),
    ('missing-shape', 'Incomplete component state or producing graph shape'),
    ('missing-definition', 'Unknown component'),
    ('missing-state', 'Incomplete component state or producing graph shape'),
])
def test_sampled_resume_refuses_damaged_persisted_state_without_repair(tmp_path, damage, error):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(snapshot)
        _session(storage, 'int-origin', 'interrupt', ('A',))
        _session(storage, 'main-resume', 'main', ('A',))
        assert storage.reserve_execution_components('main-resume', expected_shape=snapshot.shape_json) is True
        storage.create_job(Job(node_name='A', job_id=1, params={'source': 'prior-input'}))
        key = encode_component_key(('A',))
        assert storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE component_states SET lifecycle='sampled', stability='unstable', "
            "instability_origin='int-origin', alignment_generation=7 WHERE component_key=?", (key,),
        ).rowcount) == 1
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
            storage.begin_sampled_component_resume('main-resume', ('A',), expected_alignment_generation=7)

        assert (_component_rows(storage), _other_rows(storage)) == before
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
            _session(storage, origin, 'interrupt', ('A',))
            assert storage.get_execution_session(origin)['status'] == 'running'
        _session(storage, 'resuming-owner', actor_kind, ('A',))
        assert storage.reserve_execution_components('resuming-owner', expected_shape=snapshot.shape_json) is True
        key = encode_component_key(('A',))
        assert storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE component_states SET lifecycle='sampled', stability=?, instability_origin=?, "
            'alignment_generation=7 WHERE component_key=?', (stability, origin, key),
        ).rowcount) == 1
        expected, others = _component_rows(storage), _other_rows(storage)
        expected[key]['lifecycle'] = 'running'

        assert storage.begin_sampled_component_resume(
            'resuming-owner', ('A',), expected_alignment_generation=7,
        ) is True
        assert (_component_rows(storage), _other_rows(storage)) == (expected, others)
        assert storage.begin_sampled_component_resume(
            'resuming-owner', ('A',), expected_alignment_generation=7,
        ) is False
        with pytest.raises(RuntimeError, match='expected generation'):
            storage.begin_sampled_component_resume('resuming-owner', ('A',), expected_alignment_generation=8)
        assert (_component_rows(storage), _other_rows(storage)) == (expected, others)
        assert storage.release_execution_components('resuming-owner') == 1
        after_release = _other_rows(storage)
        with pytest.raises(RuntimeError, match='reservation'):
            storage.begin_sampled_component_resume('resuming-owner', ('A',), expected_alignment_generation=7)
        assert (_component_rows(storage), _other_rows(storage)) == (expected, after_release)
    finally:
        _close(storage)


@pytest.mark.parametrize('change,error', [
    ('terminal', 'existing running session'),
    ('transferred', 'reservation'),
    ('generation', 'expected generation'),
])
def test_sampled_resume_rechecks_after_waiting_for_its_write_transaction(tmp_path, monkeypatch, change, error):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    other = FileStorage(tmp_path)
    awaiting_transaction, proceed = Event(), Event()
    try:
        storage.register_component_topology(snapshot)
        _session(storage, 'old-owner', 'main', ('A',))
        _session(storage, 'next-owner', 'interrupt', ('A',))
        assert storage.reserve_execution_components('old-owner', expected_shape=snapshot.shape_json) is True
        assert storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE component_states SET lifecycle='sampled', stability='stable', "
            'alignment_generation=7 WHERE component_key=?', (encode_component_key(('A',)),),
        ).rowcount) == 1
        original_transaction = storage.db_transaction

        @contextmanager
        def paused_transaction(*args, **kwargs):
            awaiting_transaction.set()
            assert proceed.wait(10), 'The test did not release the paused transaction'
            with original_transaction(*args, **kwargs) as connection:
                yield connection

        def attempt_resume():
            try:
                return storage.begin_sampled_component_resume('old-owner', ('A',), expected_alignment_generation=7)
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
                    assert other.submit_db_mutation(lambda connection: connection.execute(
                        'UPDATE component_states SET alignment_generation=8 WHERE component_key=?',
                        (encode_component_key(('A',)),),
                    ).rowcount) == 1
                expected = _component_rows(other), _other_rows(other)
            finally:
                proceed.set()
            with pytest.raises(RuntimeError, match=error):
                attempt.result(timeout=15)

        assert (_component_rows(storage), _other_rows(storage)) == expected
    finally:
        proceed.set()
        _close(other)
        _close(storage)


def test_concurrent_sampled_resumes_change_exactly_one_component_once(tmp_path):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(snapshot)
        _session(storage, 'main-resume', 'main', ('A',))
        assert storage.reserve_execution_components('main-resume', expected_shape=snapshot.shape_json) is True
        key = encode_component_key(('A',))
        assert storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE component_states SET lifecycle='sampled', stability='stable', "
            'alignment_generation=7 WHERE component_key=?', (key,),
        ).rowcount) == 1
        expected, others = _component_rows(storage), _other_rows(storage)
        expected[key]['lifecycle'] = 'running'
        start = Barrier(6)

        def attempt_resume():
            try:
                start.wait(timeout=10)
                return storage.begin_sampled_component_resume('main-resume', ('A',), expected_alignment_generation=7)
            finally:
                storage.close_thread_connection()

        with ThreadPoolExecutor(max_workers=6) as executor:
            attempts = [executor.submit(attempt_resume) for _ in range(6)]
            results = [attempt.result(timeout=15) for attempt in attempts]
        assert sum(result is True for result in results) == 1
        assert sum(result is False for result in results) == 5
        assert (_component_rows(storage), _other_rows(storage)) == (expected, others)
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

        with pytest.raises(RuntimeError, match='existing running session'):
            storage.begin_sampled_component_resume('main-resume', ('A',), expected_alignment_generation=7)

        assert tuple(storage.db_connection().iterdump()) == before
        assert output.read_bytes() == b'established output'
        assert storage.db_connection().execute(
            "SELECT value FROM metadata WHERE key='database_schema_version'"
        ).fetchone()[0] == '5'
    finally:
        _close(storage)
