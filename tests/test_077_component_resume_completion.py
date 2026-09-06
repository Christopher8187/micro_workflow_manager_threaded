from __future__ import annotations

import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier, Event

import networkx as nx
import pytest

from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.models import Job
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
    return storage.create_execution_session(
        session_id, session_kind=kind, command='resume', start_component=component,
        selected_components=[component], started_at='2026-09-05T12:00:00+00:00',
        hostname='worker.example', pid=os.getpid(), process_identity=session_id,
    )


def _stored_rows(storage):
    connection = storage.db_connection()
    tables = [row['name'] for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )]
    return {
        table: [dict(row) for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')]
        for table in tables
    }


def test_resumed_sample_completion_preserves_exact_lineage_and_ownership_after_reopen(tmp_path):
    component = ('A', 'B')
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'C')]), [('A', 'B')]).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(snapshot)
        _session(storage, 'int-origin', 'interrupt', component)
        assert storage.finish_execution_session(
            'int-origin', outcome='done', finished_at='2026-09-05T12:01:00+00:00',
        ) is True
        key = encode_component_key(component)
        # Public sampling is not active. Seed only its established sampled result.
        assert storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE component_states SET lifecycle='sampled', stability='unstable', "
            "instability_origin='int-origin', alignment_generation=7 WHERE component_key=?", (key,),
        ).rowcount) == 1
        owner = _session(storage, 'main-resume', 'main', component)
        assert (owner['session_kind'], owner['status']) == ('main', 'running')
        assert storage.reserve_execution_components('main-resume', expected_shape=snapshot.shape_json) is True
        assert storage.begin_sampled_component_resume(
            'main-resume', component, expected_alignment_generation=7,
        ) is True
        storage.create_job(Job(node_name='A', job_id=1, params={'source': 'retained-input'}))
        storage.set_job_status('A', 1, 'done')
        storage.set_node_status('A', 'done')
        output = storage.node_output_dir('A') / 'established.txt'
        output.write_bytes(b'established output')
        before = storage.get_component_state(component)
        assert before['lifecycle'] == 'running'
        assert before['instability_origin'] == 'int-origin'
        assert storage.get_execution_session('int-origin')['status'] == 'terminal'
        assert storage.get_component_reservation(component)['session_id'] == 'main-resume'
        expected_rows = _stored_rows(storage)
        matching = [row for row in expected_rows['component_states'] if row['component_key'] == key]
        assert len(matching) == 1
        matching[0]['lifecycle'] = 'done'
        schema = [tuple(row) for row in storage.db_connection().execute(
            'SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name'
        )]

        assert storage.finish_sampled_component_resume(
            'main-resume', component, expected_shape=snapshot.shape_json,
            expected_alignment_generation=7,
        ) is True

        expected = {**before, 'lifecycle': 'done'}
        assert storage.get_component_state(component) == expected
        assert _stored_rows(storage) == expected_rows
        assert [tuple(row) for row in storage.db_connection().execute(
            'SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name'
        )] == schema
        assert output.read_bytes() == b'established output'
    finally:
        _close(storage)

    reopened = FileStorage(tmp_path)
    try:
        assert reopened.get_component_state(component) == expected
        assert _stored_rows(reopened) == expected_rows
        assert [tuple(row) for row in reopened.db_connection().execute(
            'SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name'
        )] == schema
        assert output.read_bytes() == b'established output'
    finally:
        _close(reopened)


def _running_sample(storage):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage.register_component_topology(snapshot)
    _session(storage, 'main-resume', 'main', ('A',))
    assert storage.reserve_execution_components('main-resume', expected_shape=snapshot.shape_json) is True
    assert storage.submit_db_mutation(lambda connection: connection.execute(
        "UPDATE component_states SET lifecycle='sampled', stability='stable', "
        "alignment_generation=7 WHERE component_key=?", (encode_component_key(('A',)),),
    ).rowcount) == 1
    assert storage.begin_sampled_component_resume(
        'main-resume', ('A',), expected_alignment_generation=7,
    ) is True
    storage.create_job(Job(node_name='A', job_id=1, params={'source': 'retained-input'}))
    return snapshot


@pytest.mark.parametrize('ownership,error', [
    ('unknown-session', 'existing running session'),
    ('terminal-session', 'existing running session'),
    ('no-reservation', 'reservation'),
    ('other-owner', 'reservation'),
    ('missing-selection', 'selected scope'),
])
def test_resumed_sample_completion_requires_current_session_ownership(tmp_path, ownership, error):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = _running_sample(storage)
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
            assert storage.submit_db_mutation(lambda connection: connection.execute(
                'DELETE FROM session_components WHERE session_id=?', (actor,),
            ).rowcount) == 1
        before = _stored_rows(storage)

        with pytest.raises(RuntimeError, match=error):
            storage.finish_sampled_component_resume(
                actor, ('A',), expected_shape=snapshot.shape_json,
                expected_alignment_generation=7,
            )

        assert _stored_rows(storage) == before
    finally:
        _close(storage)


def test_resumed_sample_completion_requires_the_captured_producing_shape(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = _running_sample(storage)
        other_shape = ComponentTopology(nx.DiGraph([('A', 'C')]), []).snapshot().shape_json
        assert other_shape != snapshot.shape_json
        before = _stored_rows(storage)

        with pytest.raises(RuntimeError, match='producing graph shape'):
            storage.finish_sampled_component_resume(
                'main-resume', ('A',), expected_shape=other_shape,
                expected_alignment_generation=7,
            )

        assert _stored_rows(storage) == before
        assert storage.finish_sampled_component_resume(
            'main-resume', ('A',), expected_shape=snapshot.shape_json,
            expected_alignment_generation=7,
        ) is True
        matching = [row for row in before['component_states']
                    if row['component_key'] == encode_component_key(('A',))]
        assert len(matching) == 1
        matching[0]['lifecycle'] = 'done'
        assert _stored_rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('lifecycle,stability,misaligned,generation', [
    pytest.param('running', 'stable', 0, 8, id='stale-generation'),
    pytest.param('running', None, 0, 7, id='no-retained-lineage'),
    pytest.param('sampled', 'stable', 0, 7, id='not-resumed'),
    pytest.param('done', 'stable', 0, 7, id='already-done'),
    pytest.param('queued', None, 0, 7, id='queued'),
    pytest.param('failed', None, 0, 7, id='failed'),
    pytest.param('running', 'stable', 1, 7, id='damaged-misaligned-running'),
])
def test_resumed_sample_completion_requires_current_result_bearing_running_state(
    tmp_path, lifecycle, stability, misaligned, generation,
):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = _running_sample(storage)
        connection = sqlite3.connect(storage.state_database_path())
        try:
            # Misaligned running is deliberately damaged state, not a supported transition.
            connection.execute('PRAGMA ignore_check_constraints=ON')
            assert connection.execute(
                'UPDATE component_states SET lifecycle=?, stability=?, misaligned=?, '
                'alignment_generation=? WHERE component_key=?',
                (lifecycle, stability, misaligned, generation, encode_component_key(('A',))),
            ).rowcount == 1
            connection.commit()
        finally:
            connection.close()
        before = _stored_rows(storage)

        with pytest.raises(RuntimeError):
            storage.finish_sampled_component_resume(
                'main-resume', ('A',), expected_shape=snapshot.shape_json,
                expected_alignment_generation=7,
            )

        assert _stored_rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('expected_generation,stored_generation', [
    (False, 0), (True, 1), (7.0, 7), ('7', 7), (None, 7), (-1, 7),
])
def test_resumed_sample_completion_rejects_generation_coercion(
    tmp_path, expected_generation, stored_generation,
):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = _running_sample(storage)
        assert storage.submit_db_mutation(lambda connection: connection.execute(
            'UPDATE component_states SET alignment_generation=? WHERE component_key=?',
            (stored_generation, encode_component_key(('A',))),
        ).rowcount) == 1
        before = _stored_rows(storage)

        with pytest.raises(ValueError, match='nonnegative integer'):
            storage.finish_sampled_component_resume(
                'main-resume', ('A',), expected_shape=snapshot.shape_json,
                expected_alignment_generation=expected_generation,
            )

        assert _stored_rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('stability,origin,actor_kind', [
    ('stable', None, 'main'),
    ('unstable', 'live-origin', 'main'),
    ('unstable', '  exact-origin  ', 'interrupt'),
])
def test_resumed_sample_completion_preserves_exact_lineage_and_refuses_repeat(
    tmp_path, stability, origin, actor_kind,
):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
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
        assert storage.begin_sampled_component_resume(
            'resuming-owner', ('A',), expected_alignment_generation=7,
        ) is True
        before = storage.get_component_state(('A',))
        expected_rows = _stored_rows(storage)
        matching = [row for row in expected_rows['component_states'] if row['component_key'] == key]
        assert len(matching) == 1
        matching[0]['lifecycle'] = 'done'

        assert storage.finish_sampled_component_resume(
            'resuming-owner', ('A',), expected_shape=snapshot.shape_json,
            expected_alignment_generation=7,
        ) is True
        assert storage.get_component_state(('A',)) == {**before, 'lifecycle': 'done'}
        assert _stored_rows(storage) == expected_rows
        with pytest.raises(RuntimeError, match='aligned running component'):
            storage.finish_sampled_component_resume(
                'resuming-owner', ('A',), expected_shape=snapshot.shape_json,
                expected_alignment_generation=7,
            )
        assert _stored_rows(storage) == expected_rows
    finally:
        _close(storage)


@pytest.mark.parametrize('damage', [
    'main-origin', 'missing-origin', 'blank-origin', 'invalid-stability',
    'missing-shape', 'missing-definition', 'missing-state',
])
def test_resumed_sample_completion_refuses_damaged_records_without_repair(tmp_path, damage):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = _running_sample(storage)
        _session(storage, 'int-origin', 'interrupt', ('A',))
        key = encode_component_key(('A',))
        assert storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE component_states SET stability='unstable', instability_origin='int-origin' "
            'WHERE component_key=?', (key,),
        ).rowcount) == 1
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
            elif damage == 'invalid-stability':
                assert connection.execute(
                    "UPDATE component_states SET stability='other' WHERE component_key=?", (key,),
                ).rowcount == 1
            elif damage == 'missing-shape':
                assert connection.execute(
                    'DELETE FROM graph_shapes WHERE shape_id=(SELECT shape_id FROM component_definitions '
                    'WHERE component_key=?)', (key,),
                ).rowcount == 1
            elif damage == 'missing-definition':
                assert connection.execute('DELETE FROM component_definitions WHERE component_key=?', (key,)).rowcount == 1
            else:
                assert connection.execute('DELETE FROM component_states WHERE component_key=?', (key,)).rowcount == 1
            connection.commit()
        finally:
            connection.close()
        before = _stored_rows(storage)

        with pytest.raises(RuntimeError, match='component'):
            storage.finish_sampled_component_resume(
                'main-resume', ('A',), expected_shape=snapshot.shape_json,
                expected_alignment_generation=7,
            )

        assert _stored_rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('failure,error,message', [
    ('ABORT', sqlite3.IntegrityError, 'injected completion failure'),
    ('IGNORE', RuntimeError, 'changed before completion'),
])
def test_resumed_sample_completion_rolls_back_failed_or_suppressed_update_and_can_retry(
    tmp_path, failure, error, message,
):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = _running_sample(storage)
        storage.submit_db_mutation(lambda connection: connection.execute(
            "CREATE TRIGGER interrupt_completion BEFORE UPDATE OF lifecycle ON component_states "
            "WHEN NEW.lifecycle='done' BEGIN "
            "INSERT INTO metadata(key,value) VALUES('completion-trigger-marker','unexpected'); "
            + ("SELECT RAISE(ABORT,'injected completion failure'); " if failure == 'ABORT'
               else 'SELECT RAISE(IGNORE); ') + 'END'
        ))
        before = _stored_rows(storage)

        with pytest.raises(error, match=message):
            storage.finish_sampled_component_resume(
                'main-resume', ('A',), expected_shape=snapshot.shape_json,
                expected_alignment_generation=7,
            )

        assert _stored_rows(storage) == before
        storage.submit_db_mutation(lambda connection: connection.execute('DROP TRIGGER interrupt_completion'))
        assert storage.finish_sampled_component_resume(
            'main-resume', ('A',), expected_shape=snapshot.shape_json,
            expected_alignment_generation=7,
        ) is True
        matching = [row for row in before['component_states']
                    if row['component_key'] == encode_component_key(('A',))]
        assert len(matching) == 1
        matching[0]['lifecycle'] = 'done'
        assert _stored_rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('change,error', [
    ('terminal', 'existing running session'),
    ('transferred', 'reservation'),
    ('generation', 'expected generation'),
])
def test_resumed_sample_completion_rechecks_after_waiting_for_its_transaction(
    tmp_path, monkeypatch, change, error,
):
    storage = FileStorage._create_new_project_state(tmp_path)
    other = FileStorage(tmp_path)
    awaiting_transaction, proceed = Event(), Event()
    try:
        snapshot = _running_sample(storage)
        _session(storage, 'next-owner', 'interrupt', ('A',))
        original_transaction = storage.db_transaction

        @contextmanager
        def paused_transaction(*args, **kwargs):
            awaiting_transaction.set()
            assert proceed.wait(10), 'The test did not release the paused transaction'
            with original_transaction(*args, **kwargs) as connection:
                yield connection

        def attempt_completion():
            try:
                return storage.finish_sampled_component_resume(
                    'main-resume', ('A',), expected_shape=snapshot.shape_json,
                    expected_alignment_generation=7,
                )
            finally:
                storage.close_thread_connection()

        monkeypatch.setattr(storage, 'db_transaction', paused_transaction)
        with ThreadPoolExecutor(max_workers=1) as executor:
            attempt = executor.submit(attempt_completion)
            try:
                assert awaiting_transaction.wait(5), 'Completion did not reach its write transaction'
                if change == 'terminal':
                    assert other.finish_execution_session(
                        'main-resume', outcome='done', finished_at='2026-09-05T12:01:00+00:00',
                    ) is True
                elif change == 'transferred':
                    assert other.release_execution_components('main-resume') == 1
                    assert other.reserve_execution_components('next-owner', expected_shape=snapshot.shape_json) is True
                else:
                    assert other.submit_db_mutation(lambda connection: connection.execute(
                        'UPDATE component_states SET alignment_generation=8 WHERE component_key=?',
                        (encode_component_key(('A',)),),
                    ).rowcount) == 1
                expected = _stored_rows(other)
            finally:
                proceed.set()
            with pytest.raises(RuntimeError, match=error):
                attempt.result(timeout=15)

        assert _stored_rows(storage) == expected
    finally:
        proceed.set()
        _close(other)
        _close(storage)


def test_concurrent_resumed_sample_completions_publish_once_and_refuse_other_attempts(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = _running_sample(storage)
        expected = _stored_rows(storage)
        matching = [row for row in expected['component_states']
                    if row['component_key'] == encode_component_key(('A',))]
        assert len(matching) == 1
        matching[0]['lifecycle'] = 'done'
        start = Barrier(6)

        def attempt_completion():
            try:
                start.wait(timeout=10)
                try:
                    return storage.finish_sampled_component_resume(
                        'main-resume', ('A',), expected_shape=snapshot.shape_json,
                        expected_alignment_generation=7,
                    )
                except RuntimeError as error:
                    return str(error)
            finally:
                storage.close_thread_connection()

        with ThreadPoolExecutor(max_workers=6) as executor:
            attempts = [executor.submit(attempt_completion) for _ in range(6)]
            results = [attempt.result(timeout=15) for attempt in attempts]
        assert sum(result is True for result in results) == 1
        expected_refusal = 'Completion requires an aligned running component with retained lineage at the expected generation'
        assert results.count(expected_refusal) == 5
        assert _stored_rows(storage) == expected
    finally:
        _close(storage)


def test_resumed_sample_completion_refuses_missing_native_session_and_preserves_existing_work(tmp_path):
    storage = FileStorage(tmp_path)
    try:
        storage.create_job(Job(node_name='A', job_id=1, params={'source': 'retained-input'}))
        storage.set_node_status('A', 'done')
        output = storage.node_output_dir('A') / 'established.txt'
        output.write_bytes(b'established output')
        before = tuple(storage.db_connection().iterdump())

        with pytest.raises(RuntimeError, match='existing running session'):
            storage.finish_sampled_component_resume(
                'main-resume', ('A',), expected_shape='unused-shape',
                expected_alignment_generation=7,
            )

        assert tuple(storage.db_connection().iterdump()) == before
        assert output.read_bytes() == b'established output'
        assert storage.db_connection().execute(
            "SELECT value FROM metadata WHERE key='database_schema_version'"
        ).fetchone()[0] == '5'
    finally:
        _close(storage)
