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


def test_running_component_failure_clears_lineage_and_preserves_ownership_after_reopen(tmp_path):
    component = ('A', 'B')
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'C')]), [('A', 'B')]).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        assert storage.register_component_topology(snapshot) is True
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
        owner = _session(storage, 'main-failure', 'main', component)
        assert (owner['session_kind'], owner['status']) == ('main', 'running')
        assert storage.reserve_execution_components(
            'main-failure', expected_shape=snapshot.shape_json,
        ) is True
        assert storage.begin_sampled_component_resume(
            'main-failure', component, expected_alignment_generation=7,
        ) is True

        storage.create_job(Job(node_name='A', job_id=1, params={'source': 'retained-input'}))
        storage.set_job_status('A', 1, 'failed')
        storage.set_node_status('A', 'failed')
        storage.db_mutation_barrier()
        output = storage.node_output_dir('A') / 'established.txt'
        output.write_bytes(b'established output')

        before = storage.get_component_state(component)
        assert before == {
            'members': component,
            'shape_json': snapshot.shape_json,
            'lifecycle': 'running',
            'stability': 'unstable',
            'instability_origin': 'int-origin',
            'misaligned': False,
            'alignment_generation': 7,
        }
        expected_owner = storage.get_execution_session('main-failure')
        expected_origin = storage.get_execution_session('int-origin')
        expected_reservation = storage.get_component_reservation(component)
        assert expected_owner['status'] == 'running'
        assert (expected_origin['session_kind'], expected_origin['status']) == (
            'interrupt', 'terminal',
        )
        assert expected_reservation == {
            'members': component,
            'session_id': 'main-failure',
        }

        expected_rows = _stored_rows(storage)
        matching = [
            row for row in expected_rows['component_states']
            if row['component_key'] == key
        ]
        assert len(matching) == 1
        matching[0]['lifecycle'] = 'failed'
        matching[0]['stability'] = None
        matching[0]['instability_origin'] = None
        schema = [tuple(row) for row in storage.db_connection().execute(
            'SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name'
        )]

        assert storage.fail_running_component(
            'main-failure', component, expected_shape=snapshot.shape_json,
            expected_alignment_generation=7,
        ) is True

        expected = {
            **before,
            'lifecycle': 'failed',
            'stability': None,
            'instability_origin': None,
        }
        assert storage.get_component_state(component) == expected
        assert storage.get_execution_session('main-failure') == expected_owner
        assert storage.get_execution_session('int-origin') == expected_origin
        assert storage.get_component_reservation(component) == expected_reservation
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
        assert reopened.get_execution_session('main-failure') == expected_owner
        assert reopened.get_execution_session('int-origin') == expected_origin
        assert reopened.get_component_reservation(component) == expected_reservation
        assert _stored_rows(reopened) == expected_rows
        assert [tuple(row) for row in reopened.db_connection().execute(
            'SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name'
        )] == schema
        assert output.read_bytes() == b'established output'
    finally:
        _close(reopened)


def _running_component(storage):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage.register_component_topology(snapshot)
    _session(storage, 'main-failure', 'main', ('A',))
    assert storage.reserve_execution_components('main-failure', expected_shape=snapshot.shape_json) is True
    # Public fresh activation is not present; seed only its owned running state.
    assert storage.submit_db_mutation(lambda connection: connection.execute(
        "UPDATE component_states SET lifecycle='running', alignment_generation=7 WHERE component_key=?",
        (encode_component_key(('A',)),),
    ).rowcount) == 1
    storage.create_job(Job(node_name='A', job_id=1, params={'source': 'retained-input'}))
    return snapshot


@pytest.mark.parametrize('ownership,error', [
    ('unknown-session', 'existing running session'),
    ('terminal-session', 'existing running session'),
    ('no-reservation', 'reservation'),
    ('other-owner', 'reservation'),
    ('missing-selection', 'selected scope'),
])
def test_running_component_failure_requires_current_session_ownership(tmp_path, ownership, error):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = _running_component(storage)
        actor = 'main-failure'
        if ownership == 'unknown-session':
            actor = 'unknown'
            assert storage.get_execution_session(actor) is None
        elif ownership == 'terminal-session':
            assert storage.finish_execution_session(
                actor, outcome='failed', finished_at='2026-09-05T12:01:00+00:00',
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
            storage.fail_running_component(
                actor, ('A',), expected_shape=snapshot.shape_json,
                expected_alignment_generation=7,
            )

        assert _stored_rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('lifecycle,stability,misaligned,generation,different_shape', [
    pytest.param('running', None, 0, 7, True, id='wrong-producing-shape'),
    pytest.param('running', None, 0, 8, False, id='stale-generation'),
    pytest.param('queued', None, 0, 7, False, id='queued'),
    pytest.param('sampled', 'stable', 0, 7, False, id='sampled'),
    pytest.param('done', 'stable', 0, 7, False, id='done'),
    pytest.param('failed', None, 0, 7, False, id='already-failed'),
    pytest.param('running', None, 1, 7, False, id='damaged-misaligned-running'),
])
def test_running_component_failure_requires_current_shape_and_aligned_running_state(
    tmp_path, lifecycle, stability, misaligned, generation, different_shape,
):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = _running_component(storage)
        expected_shape = snapshot.shape_json
        if different_shape:
            expected_shape = ComponentTopology(nx.DiGraph([('A', 'C')]), []).snapshot().shape_json
            assert expected_shape != snapshot.shape_json
        connection = sqlite3.connect(storage.state_database_path())
        try:
            # Misaligned running is damaged state and must not be repaired by failure publication.
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
            storage.fail_running_component(
                'main-failure', ('A',), expected_shape=expected_shape,
                expected_alignment_generation=7,
            )

        assert _stored_rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('expected_generation,stored_generation', [
    (False, 0), (True, 1), (7.0, 7), ('7', 7), (None, 7), (-1, 7),
])
def test_running_component_failure_rejects_generation_coercion(
    tmp_path, expected_generation, stored_generation,
):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = _running_component(storage)
        assert storage.submit_db_mutation(lambda connection: connection.execute(
            'UPDATE component_states SET alignment_generation=? WHERE component_key=?',
            (stored_generation, encode_component_key(('A',))),
        ).rowcount) == 1
        before = _stored_rows(storage)

        with pytest.raises(ValueError, match='nonnegative integer'):
            storage.fail_running_component(
                'main-failure', ('A',), expected_shape=snapshot.shape_json,
                expected_alignment_generation=expected_generation,
            )

        assert _stored_rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('stability,origin,actor_kind', [
    (None, None, 'main'),
    ('stable', None, 'main'),
    ('unstable', 'live-origin', 'main'),
    ('unstable', '  exact-origin  ', 'interrupt'),
])
def test_running_component_failure_clears_each_valid_lineage_and_refuses_repeat(
    tmp_path, stability, origin, actor_kind,
):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
        storage.register_component_topology(snapshot)
        if origin is not None:
            _session(storage, origin, 'interrupt', ('A',))
            assert storage.get_execution_session(origin)['status'] == 'running'
        _session(storage, 'failing-owner', actor_kind, ('A',))
        assert storage.reserve_execution_components('failing-owner', expected_shape=snapshot.shape_json) is True
        key = encode_component_key(('A',))
        assert storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE component_states SET lifecycle='running', stability=?, instability_origin=?, "
            'alignment_generation=7 WHERE component_key=?', (stability, origin, key),
        ).rowcount) == 1
        before = storage.get_component_state(('A',))
        expected_rows = _stored_rows(storage)
        matching = [row for row in expected_rows['component_states'] if row['component_key'] == key]
        assert len(matching) == 1
        matching[0].update(lifecycle='failed', stability=None, instability_origin=None)

        assert storage.fail_running_component(
            'failing-owner', ('A',), expected_shape=snapshot.shape_json,
            expected_alignment_generation=7,
        ) is True
        assert storage.get_component_state(('A',)) == {
            **before, 'lifecycle': 'failed', 'stability': None, 'instability_origin': None,
        }
        assert _stored_rows(storage) == expected_rows
        with pytest.raises(RuntimeError, match='aligned running component'):
            storage.fail_running_component(
                'failing-owner', ('A',), expected_shape=snapshot.shape_json,
                expected_alignment_generation=7,
            )
        assert _stored_rows(storage) == expected_rows
    finally:
        _close(storage)


@pytest.mark.parametrize('damage', [
    'main-origin', 'missing-origin', 'blank-origin', 'invalid-stability',
    'missing-shape', 'missing-definition', 'missing-state',
])
def test_running_component_failure_refuses_damaged_records_without_repair(tmp_path, damage):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = _running_component(storage)
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
                origin = {'main-origin': 'main-failure', 'missing-origin': 'missing', 'blank-origin': '   '}[damage]
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
            storage.fail_running_component(
                'main-failure', ('A',), expected_shape=snapshot.shape_json,
                expected_alignment_generation=7,
            )

        assert _stored_rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('failure,error,message', [
    ('ABORT', sqlite3.IntegrityError, 'injected failure publication error'),
    ('IGNORE', RuntimeError, 'changed before failure'),
])
def test_running_component_failure_rolls_back_failed_or_suppressed_update_and_can_retry(
    tmp_path, failure, error, message,
):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = _running_component(storage)
        storage.submit_db_mutation(lambda connection: connection.execute(
            "CREATE TRIGGER interrupt_failure BEFORE UPDATE OF lifecycle ON component_states "
            "WHEN NEW.lifecycle='failed' BEGIN "
            "INSERT INTO metadata(key,value) VALUES('failure-trigger-marker','unexpected'); "
            + ("SELECT RAISE(ABORT,'injected failure publication error'); " if failure == 'ABORT'
               else 'SELECT RAISE(IGNORE); ') + 'END'
        ))
        before = _stored_rows(storage)

        with pytest.raises(error, match=message):
            storage.fail_running_component(
                'main-failure', ('A',), expected_shape=snapshot.shape_json,
                expected_alignment_generation=7,
            )

        assert _stored_rows(storage) == before
        storage.submit_db_mutation(lambda connection: connection.execute('DROP TRIGGER interrupt_failure'))
        assert storage.fail_running_component(
            'main-failure', ('A',), expected_shape=snapshot.shape_json,
            expected_alignment_generation=7,
        ) is True
        matching = [row for row in before['component_states']
                    if row['component_key'] == encode_component_key(('A',))]
        assert len(matching) == 1
        matching[0].update(lifecycle='failed', stability=None, instability_origin=None)
        assert _stored_rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('change,error', [
    ('terminal', 'existing running session'),
    ('transferred', 'reservation'),
    ('generation', 'expected generation'),
])
def test_running_component_failure_rechecks_after_waiting_for_its_transaction(
    tmp_path, monkeypatch, change, error,
):
    storage = FileStorage._create_new_project_state(tmp_path)
    other = FileStorage(tmp_path)
    awaiting_transaction, proceed = Event(), Event()
    try:
        snapshot = _running_component(storage)
        _session(storage, 'next-owner', 'interrupt', ('A',))
        original_transaction = storage.db_transaction

        @contextmanager
        def paused_transaction(*args, **kwargs):
            awaiting_transaction.set()
            assert proceed.wait(10), 'The test did not release the paused transaction'
            with original_transaction(*args, **kwargs) as connection:
                yield connection

        def attempt_failure():
            try:
                return storage.fail_running_component(
                    'main-failure', ('A',), expected_shape=snapshot.shape_json,
                    expected_alignment_generation=7,
                )
            finally:
                storage.close_thread_connection()

        monkeypatch.setattr(storage, 'db_transaction', paused_transaction)
        with ThreadPoolExecutor(max_workers=1) as executor:
            attempt = executor.submit(attempt_failure)
            try:
                assert awaiting_transaction.wait(5), 'Failure publication did not reach its write transaction'
                if change == 'terminal':
                    assert other.finish_execution_session(
                        'main-failure', outcome='failed', finished_at='2026-09-05T12:01:00+00:00',
                    ) is True
                elif change == 'transferred':
                    assert other.release_execution_components('main-failure') == 1
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


def test_concurrent_running_component_failures_publish_once_and_refuse_other_attempts(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = _running_component(storage)
        expected = _stored_rows(storage)
        matching = [row for row in expected['component_states']
                    if row['component_key'] == encode_component_key(('A',))]
        assert len(matching) == 1
        matching[0].update(lifecycle='failed', stability=None, instability_origin=None)
        start = Barrier(6)

        def attempt_failure():
            try:
                start.wait(timeout=10)
                try:
                    return storage.fail_running_component(
                        'main-failure', ('A',), expected_shape=snapshot.shape_json,
                        expected_alignment_generation=7,
                    )
                except RuntimeError as error:
                    return str(error)
            finally:
                storage.close_thread_connection()

        with ThreadPoolExecutor(max_workers=6) as executor:
            attempts = [executor.submit(attempt_failure) for _ in range(6)]
            results = [attempt.result(timeout=15) for attempt in attempts]
        assert sum(result is True for result in results) == 1
        assert results.count('Failure requires an aligned running component at the expected generation') == 5
        assert _stored_rows(storage) == expected
    finally:
        _close(storage)


def test_completion_and_failure_compete_for_one_terminal_result_without_overwriting(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = _running_component(storage)
        _session(storage, 'int-origin', 'interrupt', ('A',))
        key = encode_component_key(('A',))
        assert storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE component_states SET stability='unstable', instability_origin='int-origin' "
            'WHERE component_key=?', (key,),
        ).rowcount) == 1
        expected = _stored_rows(storage)
        start = Barrier(2)

        def publish(outcome):
            try:
                start.wait(timeout=10)
                method = (storage.finish_sampled_component_resume if outcome == 'done'
                          else storage.fail_running_component)
                try:
                    return outcome, method(
                        'main-failure', ('A',), expected_shape=snapshot.shape_json,
                        expected_alignment_generation=7,
                    )
                except RuntimeError as error:
                    return outcome, str(error)
            finally:
                storage.close_thread_connection()

        with ThreadPoolExecutor(max_workers=2) as executor:
            attempts = [executor.submit(publish, outcome) for outcome in ('done', 'failed')]
            results = dict(attempt.result(timeout=15) for attempt in attempts)
        winners = [outcome for outcome, result in results.items() if result is True]
        assert len(winners) == 1
        winner = winners[0]
        loser = 'failed' if winner == 'done' else 'done'
        assert results[loser] == {
            'failed': 'Failure requires an aligned running component at the expected generation',
            'done': 'Completion requires an aligned running component with retained lineage at the expected generation',
        }[loser]
        matching = [row for row in expected['component_states'] if row['component_key'] == key]
        assert len(matching) == 1
        matching[0]['lifecycle'] = winner
        if winner == 'failed':
            matching[0].update(stability=None, instability_origin=None)
        assert _stored_rows(storage) == expected
    finally:
        _close(storage)


def test_running_component_failure_refuses_version4_and_preserves_existing_work(tmp_path):
    storage = FileStorage(tmp_path)
    try:
        storage.create_job(Job(node_name='A', job_id=1, params={'source': 'retained-input'}))
        storage.set_node_status('A', 'done')
        output = storage.node_output_dir('A') / 'established.txt'
        output.write_bytes(b'established output')
        before = tuple(storage.db_connection().iterdump())

        with pytest.raises(RuntimeError, match='session-capable database'):
            storage.fail_running_component(
                'main-failure', ('A',), expected_shape='unused-version4-shape',
                expected_alignment_generation=7,
            )

        assert tuple(storage.db_connection().iterdump()) == before
        assert output.read_bytes() == b'established output'
        assert storage.db_connection().execute(
            "SELECT value FROM metadata WHERE key='database_schema_version'"
        ).fetchone()[0] == '4'
    finally:
        _close(storage)
