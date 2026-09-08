from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace

import networkx as nx
import pytest

from micro_workflow_manager.models import Job
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.topology import ComponentTopology
import micro_workflow_manager.storage.execution_claims as execution_claims_module


STARTED_AT = '2026-09-05T12:00:00+00:00'


@pytest.mark.parametrize('batch', [False, True])
def test_native_claims_keep_owned_leases_events_and_release(tmp_path, batch):
    storage = _owned_storage(tmp_path)
    context = dict(session_id='int-17', component=('A', 'B'))
    try:
        if batch:
            claims = storage.claim_job_executions_batch(
                'A', [1, 2], started_at=STARTED_AT, **context,
                task_started_data={'task': 'extract'}, task_started_mask=[True, False],
            )
        else:
            claims = [storage.claim_job_execution('A', job_id, started_at=STARTED_AT, **context)
                      for job_id in [1, 2]]
        assert len({execution_id for _, execution_id in claims}) == 2
        for job_id, (generation, execution_id) in zip([1, 2], claims):
            assert generation == 0
            owner = storage.get_job_execution_owner(execution_id)
            assert owner['session_id'] == 'int-17' and owner['component'] == ('A', 'B')
            assert owner['created_by_execution_id'] is None
            control = storage.read_job_control('A', job_id)
            assert control['active_execution_id'] == execution_id
            assert control['active_pid'] == os.getpid()
            assert storage.get_job_status('A', job_id) == 'running'
            events = storage.read_job_events('A', job_id)
            started, = [event for event in events if event['event'] == 'started']
            assert (started['session_id'], started['execution_id'], started['component']) == (
                'int-17', execution_id, ['A', 'B'],
            )
            assert started['previous_status'] == 'queued' and started['status'] == 'running'
            task_events = [event for event in events if event['event'] == 'task_started']
            assert [event['task'] for event in task_events] == (['extract'] if batch and job_id == 1 else [])
            storage.release_unstarted_job_execution('A', job_id, generation, execution_id)
            assert storage.read_job_control('A', job_id)['active_execution_id'] is None
            assert storage.get_job_status('A', job_id) == 'queued'
            assert storage.get_job_execution_owner(execution_id) == owner
            assert len([event for event in storage.read_job_events('A', job_id) if event['event'] == 'started']) == 1
    finally:
        storage.close_database_connections()


def _owned_storage(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'C')]), [('A', 'B')]).snapshot()
    storage.register_component_topology(snapshot)
    for session_id, component in [('int-17', ('A', 'B')), ('int-18', ('C',))]:
        storage.create_execution_session(
            session_id, session_kind='interrupt', command='run', start_component=component,
            selected_components=[component], started_at=STARTED_AT, hostname='worker.example',
            pid=os.getpid(), process_identity='test-process', expected_shape=snapshot.shape_json,
        )
        storage.reserve_execution_components(session_id, expected_shape=snapshot.shape_json)
    for node in ['A', 'B', 'C']:
        for job_id in [1, 2]:
            storage.create_job(Job(node_name=node, job_id=job_id, params={}))
    return storage


@pytest.mark.parametrize('batch', [False, True])
def test_private_claims_commit_exact_execution_owners_and_matching_started_events(tmp_path, batch):
    storage = _owned_storage(tmp_path)
    observed = []
    for node, session_id, component in [('A', 'int-17', ('B', 'A')), ('C', 'int-18', ('C',))]:
        if batch:
            claims = storage.claim_job_executions_batch(
                node, [1, 2], started_at=STARTED_AT, session_id=session_id, component=component,
            )
        else:
            claims = [storage.claim_job_execution(
                node, job_id, started_at=STARTED_AT, session_id=session_id, component=component,
            ) for job_id in [1, 2]]
        for job_id, (generation, execution_id) in zip([1, 2], claims):
            expected_component = ('A', 'B') if node == 'A' else ('C',)
            owner = {
                'execution_id': execution_id, 'node_name': node, 'job_id': job_id,
                'generation': generation, 'session_id': session_id, 'component': expected_component,
                'job_instance_id': storage.read_job_instance_id(node, job_id),
                'shape_id': 1, 'alignment_generation': 0, 'created_by_execution_id': None,
            }
            assert storage.get_job_execution_owner(execution_id) == owner
            assert storage.read_job_control(node, job_id)['active_execution_id'] == execution_id
            assert storage.get_job_status(node, job_id) == 'running'
            started = [event for event in storage.read_job_events(node, job_id) if event['event'] == 'started']
            assert len(started) == 1
            assert started[0]['session_id'] == session_id
            assert started[0]['component'] == list(expected_component)
            assert started[0]['execution_id'] == execution_id
            observed.append((execution_id, owner))
    storage.close_database_connections()
    reopened = FileStorage(tmp_path)
    for execution_id, owner in observed:
        assert reopened.get_job_execution_owner(execution_id) == owner
    assert reopened.get_job_execution_owner('unknown') is None
    reopened.close_database_connections()


def _claim_rows(storage):
    connection = storage.db_connection()
    return {
        'jobs': [tuple(row) for row in connection.execute('SELECT * FROM jobs ORDER BY node_name, job_id')],
        'events': [tuple(row) for row in connection.execute('SELECT * FROM job_events ORDER BY event_id')],
        'owners': [tuple(row) for row in connection.execute('SELECT * FROM job_execution_owners ORDER BY execution_id')],
    }


@pytest.mark.parametrize('batch', [False, True])
@pytest.mark.parametrize('context', [{}, {'session_id': 'int-17'}, {'component': ('A', 'B')}])
def test_private_claims_require_both_explicit_owner_inputs_without_guessing(tmp_path, batch, context):
    storage = _owned_storage(tmp_path)
    before = _claim_rows(storage)

    with pytest.raises(RuntimeError, match='explicit session_id and component'):
        if batch:
            storage.claim_job_executions_batch('A', [1, 2], started_at=STARTED_AT, **context)
        else:
            storage.claim_job_execution('A', 1, started_at=STARTED_AT, **context)

    assert _claim_rows(storage) == before
    storage.close_database_connections()


@pytest.mark.parametrize('batch', [False, True])
@pytest.mark.parametrize(('condition', 'message'), [
    ('unknown', 'running session'),
    ('terminal', 'running session'),
    ('another-owner', 'reservation'),
    ('unreserved', 'reservation'),
    ('outside-component', 'member'),
    ('outside-selection', 'selected scope'),
])
def test_claims_require_a_running_exact_owner_and_eligible_component(tmp_path, batch, condition, message):
    storage = _owned_storage(tmp_path)
    session_id, component = 'int-17', ('A', 'B')
    if condition == 'unknown':
        session_id = 'int-99'
    elif condition == 'terminal':
        storage.finish_execution_session('int-17', outcome='done', finished_at='2026-09-05T12:01:00+00:00')
    elif condition == 'another-owner':
        session_id = 'int-18'
    elif condition == 'unreserved':
        storage.release_execution_components('int-17')
    elif condition == 'outside-component':
        session_id, component = 'int-18', ('C',)
    else:
        session_id = 'int-18'
        with sqlite3.connect(storage.state_database_path()) as connection:
            connection.execute("UPDATE component_reservations SET session_id='int-18' WHERE session_id='int-17'")
    before = _claim_rows(storage)

    with pytest.raises(RuntimeError, match=message):
        if batch:
            storage.claim_job_executions_batch('A', [1, 2], started_at=STARTED_AT, session_id=session_id, component=component)
        else:
            storage.claim_job_execution('A', 1, started_at=STARTED_AT, session_id=session_id, component=component)

    assert _claim_rows(storage) == before
    storage.close_database_connections()


@pytest.mark.parametrize('batch', [False, True])
@pytest.mark.parametrize('context', [
    {'session_id': 'int-17'}, {'component': ('A',)}, {'session_id': 'int-17', 'component': ('A',)},
])
def test_native_claims_refuse_changed_schema_without_changing_jobs(tmp_path, batch, context):
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.create_job(Job(node_name='A', job_id=1, params={}))
    storage.submit_db_mutation(lambda connection: connection.execute(
        "UPDATE metadata SET value='4' WHERE key='database_schema_version'",
    ))
    before = (storage.read_job_control('A', 1), storage.read_job_events('A', 1))

    with pytest.raises(RuntimeError, match='session-capable database'):
        if batch:
            storage.claim_job_executions_batch('A', [1], started_at=STARTED_AT, **context)
        else:
            storage.claim_job_execution('A', 1, started_at=STARTED_AT, **context)

    assert (storage.read_job_control('A', 1), storage.read_job_events('A', 1)) == before
    assert storage.get_job_status('A', 1) == 'queued'
    storage.close_database_connections()


@pytest.mark.parametrize('batch', [False, True])
@pytest.mark.parametrize('session_id', [' ', 17, []])
def test_claim_owner_id_must_be_nonempty_text_before_batch_collection(tmp_path, batch, session_id):
    storage = _owned_storage(tmp_path)
    before = _claim_rows(storage)

    with pytest.raises(ValueError, match='session_id'):
        if batch:
            storage.claim_job_executions_batch('A', [1], started_at=STARTED_AT, session_id=session_id, component=('A', 'B'))
        else:
            storage.claim_job_execution('A', 1, started_at=STARTED_AT, session_id=session_id, component=('A', 'B'))

    assert _claim_rows(storage) == before
    storage.close_database_connections()


@pytest.mark.parametrize('failure', ['owner', 'event'])
def test_failed_batch_rolls_back_leases_owners_and_started_events_together(tmp_path, failure):
    storage = _owned_storage(tmp_path)
    table = 'job_execution_owners' if failure == 'owner' else 'job_events'
    condition = 'NEW.job_id=2' + (" AND NEW.event='started'" if failure == 'event' else '')
    with sqlite3.connect(storage.state_database_path()) as connection:
        connection.execute(f'''
            CREATE TRIGGER fail_claim BEFORE INSERT ON {table} WHEN {condition}
            BEGIN SELECT RAISE(ABORT, 'injected claim failure'); END
        ''')
    before = _claim_rows(storage)

    with pytest.raises(sqlite3.IntegrityError, match='injected claim failure'):
        storage.claim_job_executions_batch('A', [1, 2], started_at=STARTED_AT,
                                          session_id='int-17', component=('A', 'B'))

    assert _claim_rows(storage) == before
    with sqlite3.connect(storage.state_database_path()) as connection:
        connection.execute('DROP TRIGGER fail_claim')
    claims = storage.claim_job_executions_batch('A', [1, 2], started_at=STARTED_AT,
                                              session_id='int-17', component=('A', 'B'))
    assert len(claims) == 2
    assert all(storage.get_job_execution_owner(execution_id)['session_id'] == 'int-17' for _, execution_id in claims)
    storage.close_database_connections()


def test_unstarted_release_and_job_recreation_keep_exact_execution_history(tmp_path):
    storage = _owned_storage(tmp_path)
    generation, execution_id = storage.claim_job_execution(
        'A', 1, started_at=STARTED_AT, session_id='int-17', component=('A', 'B'),
    )
    owner = storage.get_job_execution_owner(execution_id)
    storage.release_unstarted_job_execution('A', 1, generation, execution_id)
    assert storage.read_job_control('A', 1)['active_execution_id'] is None
    assert storage.get_job_status('A', 1) == 'queued'
    assert storage.get_job_execution_owner(execution_id) == owner
    events = storage.read_job_events('A', 1)
    assert storage.delete_job('A', 1, preserve_events=True)
    assert storage.get_job_execution_owner(execution_id) == owner
    assert storage.read_job_events('A', 1) == events
    storage.create_job(Job(node_name='A', job_id=1, params={'new': True}))
    _, next_execution = storage.claim_job_execution(
        'A', 1, started_at=STARTED_AT, session_id='int-17', component=('A', 'B'),
    )
    assert next_execution != execution_id
    assert storage.get_job_execution_owner(execution_id) == owner
    assert storage.get_job_execution_owner(next_execution)['session_id'] == 'int-17'
    storage.close_database_connections()


def test_large_claim_batch_checks_its_owner_once_and_commits_every_execution(tmp_path, monkeypatch):
    storage = _owned_storage(tmp_path)
    storage.create_jobs_batch([Job(node_name='A', job_id=job_id, params={}) for job_id in range(3, 601)])
    statements = []
    original_transaction = storage.db_transaction

    @contextmanager
    def traced_transaction(*args, **kwargs):
        with original_transaction(*args, **kwargs) as connection:
            connection.set_trace_callback(statements.append)
            try:
                yield connection
            finally:
                connection.set_trace_callback(None)

    monkeypatch.setattr(storage, 'db_transaction', traced_transaction)
    claims = storage.claim_job_executions_batch(
        'A', list(range(1, 601)), started_at=STARTED_AT, session_id='int-17', component=('B', 'A'),
    )

    owner_reads = [statement for statement in statements if 'FROM execution_sessions AS session' in statement]
    assert len(owner_reads) == 1
    assert len(claims) == len({execution_id for _, execution_id in claims}) == 600
    assert {generation for generation, _ in claims} == {0}
    owners = storage.db_connection().execute(
        "SELECT job_id, execution_id, session_id, component_key FROM job_execution_owners WHERE node_name='A' ORDER BY job_id",
    ).fetchall()
    assert [(row['job_id'], row['execution_id']) for row in owners] == [
        (job_id, execution_id) for job_id, (_, execution_id) in enumerate(claims, 1)
    ]
    assert {(row['session_id'], row['component_key']) for row in owners} == {('int-17', '["A", "B"]')}
    assert storage.db_connection().execute(
        "SELECT COUNT(*) FROM jobs WHERE node_name='A' AND status='running' AND active_execution_id IS NOT NULL",
    ).fetchone()[0] == 600
    assert storage.db_connection().execute(
        "SELECT COUNT(*) FROM job_events WHERE node_name='A' AND event='started'",
    ).fetchone()[0] == 600
    storage.close_database_connections()


@pytest.mark.parametrize(('session_id', 'component', 'message'), [
    ('int-18', ('A', 'B'), 'reservation'),
    ('int-17', ('C',), 'member'),
])
def test_simultaneous_single_claims_never_inherit_another_callers_context(
    tmp_path, monkeypatch, session_id, component, message,
):
    storage = _owned_storage(tmp_path)
    first_leader_waiting, second_leader_seen = Event(), Event()

    def collect_sleep(seconds):
        assert seconds == 0.001
        if not first_leader_waiting.is_set():
            first_leader_waiting.set()
            second_leader_seen.wait(3)
        else:
            second_leader_seen.set()

    # Control only the claim module's collection wait; real claims and SQLite
    # operations still run. A follower with an incorrect shared key cannot
    # signal a second leader and would inherit the first caller's valid owner.
    monkeypatch.setattr(execution_claims_module, 'time', SimpleNamespace(sleep=collect_sleep))
    with ThreadPoolExecutor(max_workers=2) as executor:
        accepted = executor.submit(storage.claim_job_execution, 'A', 1, started_at=STARTED_AT,
                                   session_id='int-17', component=('A', 'B'))
        assert first_leader_waiting.wait(5)
        refused = executor.submit(storage.claim_job_execution, 'A', 2, started_at=STARTED_AT,
                                  session_id=session_id, component=component)
        generation, execution_id = accepted.result(timeout=15)
        with pytest.raises(RuntimeError, match=message):
            refused.result(timeout=15)

    assert storage.get_job_execution_owner(execution_id) == {
        'execution_id': execution_id, 'node_name': 'A', 'job_id': 1, 'generation': generation,
        'session_id': 'int-17', 'component': ('A', 'B'),
        'job_instance_id': storage.read_job_instance_id('A', 1),
        'shape_id': 1, 'alignment_generation': 0, 'created_by_execution_id': None,
    }
    started = [event for event in storage.read_job_events('A', 1) if event['event'] == 'started']
    assert len(started) == 1
    assert (started[0]['execution_id'], started[0]['session_id'], started[0]['component']) == (
        execution_id, 'int-17', ['A', 'B'],
    )
    assert storage.read_job_control('A', 2)['active_execution_id'] is None
    assert storage.get_job_status('A', 2) == 'queued'
    assert not any(event['event'] == 'started' for event in storage.read_job_events('A', 2))
    assert len(_claim_rows(storage)['owners']) == 1
    storage.close_database_connections()


@pytest.mark.parametrize('change', ['terminal', 'released'])
def test_claim_rechecks_session_ownership_after_waiting_to_enter_its_write_transaction(tmp_path, monkeypatch, change):
    storage = _owned_storage(tmp_path)
    other = FileStorage(tmp_path)
    before = _claim_rows(storage)
    awaiting_transaction, proceed = Event(), Event()
    original_transaction = storage.db_transaction

    @contextmanager
    def paused_transaction(*args, **kwargs):
        awaiting_transaction.set()
        assert proceed.wait(10)
        with original_transaction(*args, **kwargs) as connection:
            yield connection

    monkeypatch.setattr(storage, 'db_transaction', paused_transaction)
    with ThreadPoolExecutor(max_workers=1) as executor:
        claim = executor.submit(storage.claim_job_executions_batch, 'A', [1, 2], started_at=STARTED_AT,
                                session_id='int-17', component=('A', 'B'))
        try:
            assert awaiting_transaction.wait(5)
            if change == 'terminal':
                other.finish_execution_session('int-17', outcome='done', finished_at='2026-09-05T12:01:00+00:00')
            else:
                other.release_execution_components('int-17')
        finally:
            proceed.set()
        with pytest.raises(RuntimeError, match='running session' if change == 'terminal' else 'reservation'):
            claim.result(timeout=15)

    assert _claim_rows(storage) == before
    other.close_database_connections()
    storage.close_database_connections()
