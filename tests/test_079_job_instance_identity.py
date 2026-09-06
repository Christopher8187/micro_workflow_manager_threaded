from __future__ import annotations

import json
import re
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock

import pytest
import networkx as nx

from micro_workflow_manager.models import Job
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.system import MicroWorkflow
from micro_workflow_manager.topology import ComponentTopology


def _close(storage):
    storage.db_mutation_barrier()
    deadline = time.perf_counter() + 10
    while storage.mutation_writer_diagnostics()['writer_alive']:
        assert time.perf_counter() < deadline, 'Mutation writer did not retire'
        time.sleep(0.01)
    storage.close_thread_connection()


def _raw_instance_id(storage, node_name, job_id):
    row = storage.db_connection().execute(
        'SELECT instance_id FROM job_instances WHERE node_name=? AND job_id=?',
        (node_name, job_id),
    ).fetchone()
    return None if row is None else row['instance_id']


def _assert_opaque_identity(value):
    assert type(value) is str
    assert value.strip()


def test_private_version5_job_identity_survives_refresh_and_changes_on_recreation(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.create_job(Job(node_name='A', job_id=1, params={'value': 'old'}))

        # This private reader is deliberately absent before the first RED.
        first = storage.read_job_instance_id('A', 1)
        _assert_opaque_identity(first)
        assert _raw_instance_id(storage, 'A', 1) == first
        assert storage.load_job('A', 1).params == {'value': 'old'}

        storage.set_job_status('A', 1, 'done')
        assert storage.get_job_status('A', 1) == 'done'
        refreshed = Job(node_name='A', job_id=1, params={'value': 'refreshed'})
        assert storage.ensure_job(refreshed) == refreshed
        assert storage.get_job_status('A', 1) == 'queued'
        assert storage.load_job('A', 1).params == {'value': 'refreshed'}
        assert storage.read_job_instance_id('A', 1) == first
        assert _raw_instance_id(storage, 'A', 1) == first

        assert storage.delete_job('A', 1) is True
        assert storage.job_exists('A', 1) is False
        assert storage.read_job_instance_id('A', 1) is None
        assert _raw_instance_id(storage, 'A', 1) is None

        storage.create_job(Job(node_name='A', job_id=1, params={'value': 'refreshed'}))
        second = storage.read_job_instance_id('A', 1)
        _assert_opaque_identity(second)
        assert second != first
        assert _raw_instance_id(storage, 'A', 1) == second
        assert storage.load_job('A', 1).params == {'value': 'refreshed'}
    finally:
        _close(storage)

    reopened = FileStorage(tmp_path)
    try:
        assert reopened.read_job_instance_id('A', 1) == second
        assert _raw_instance_id(reopened, 'A', 1) == second
        assert reopened.load_job('A', 1).params == {'value': 'refreshed'}
    finally:
        _close(reopened)


def _logical_state(storage):
    connection = storage.db_connection()
    schema = [tuple(row) for row in connection.execute(
        'SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name'
    )]
    tables = [row['name'] for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )]
    rows = {
        table: [tuple(row) for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')]
        for table in tables
    }
    return schema, rows


@pytest.mark.parametrize('damaged', [None, '', 'a' * 31, 'a' * 33, 'A' * 32,
                                    'g' * 32, ' ' * 32, b'1' * 32, 123,
                                    pytest.param('a' * 32 + '\0hidden', id='nul-suffix')])
def test_identity_reader_refuses_missing_or_invalid_value_without_repair(tmp_path, damaged):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.create_job(Job(node_name='A', job_id=1, params={'value': 'retained'}))
        _close(storage)
        with sqlite3.connect(storage.state_database_path()) as connection:
            connection.execute('PRAGMA ignore_check_constraints=ON')
            if damaged is None:
                connection.execute("DELETE FROM job_instances WHERE node_name='A' AND job_id=1")
            else:
                connection.execute(
                    "UPDATE job_instances SET instance_id=? WHERE node_name='A' AND job_id=1",
                    (damaged,),
                )
        before = _logical_state(storage)
        with pytest.raises(RuntimeError, match='Incomplete job instance identity'):
            storage.read_job_instance_id('A', 1)
        assert _logical_state(storage) == before
        assert storage.load_job('A', 1).params == {'value': 'retained'}
        assert storage.read_job_instance_id('Absent', 1) is None
    finally:
        _close(storage)


@pytest.mark.parametrize('invalid', [None, '', 'a' * 31, 'a' * 33, 'A' * 32,
                                    'g' * 32, ' ' * 32, b'1' * 32, 123,
                                    pytest.param('a' * 32 + '\0hidden', id='nul-suffix')])
def test_identity_schema_refuses_invalid_stored_values_without_changing_job(tmp_path, invalid):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.create_job(Job(node_name='A', job_id=1, params={'value': 'retained'}))
        before = _logical_state(storage)
        identity = storage.read_job_instance_id('A', 1)
        with pytest.raises(sqlite3.IntegrityError):
            storage.submit_db_mutation(lambda connection: connection.execute(
                "UPDATE job_instances SET instance_id=? WHERE node_name='A' AND job_id=1",
                (invalid,),
            ))
        assert _logical_state(storage) == before
        assert storage.read_job_instance_id('A', 1) == identity
    finally:
        _close(storage)


def _assert_fresh_process_refuses(storage):
    before = _logical_state(storage)
    sentinel = storage.project_dir / 'notes' / 'retained.txt'
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    sentinel.write_bytes(b'Unrelated project content remains unchanged.')
    saved = sentinel.read_bytes()
    _close(storage)
    script = '''
import sys
from pathlib import Path
from micro_workflow_manager.storage import FileStorage
try:
    storage = FileStorage(Path(sys.argv[1]))
except RuntimeError as error:
    assert 'Incomplete SQLite execution-session schema' in str(error), str(error)
else:
    storage.close_database_connections()
    raise AssertionError('Damaged job identity storage was accepted')
'''
    result = subprocess.run([sys.executable, '-c', script, str(storage.project_dir)],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _logical_state(storage) == before
    assert sentinel.read_bytes() == saved


@pytest.mark.parametrize('damage', ['missing', 'renamed', 'before-insert',
                                   'missing-insertion', 'missing-postcondition'])
def test_fresh_process_refuses_missing_or_changed_job_identity_trigger(tmp_path, damage):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.create_job(Job(node_name='A', job_id=1, params={'value': 'retained'}))
        _close(storage)
        with sqlite3.connect(storage.state_database_path()) as connection:
            original = connection.execute(
                "SELECT sql FROM sqlite_master WHERE name='create_job_instance'"
            ).fetchone()[0]
            connection.execute('DROP TRIGGER create_job_instance')
            if damage != 'missing':
                if damage == 'renamed':
                    changed = original.replace('create_job_instance', 'renamed_job_instance')
                elif damage == 'before-insert':
                    changed = original.replace('AFTER INSERT', 'BEFORE INSERT')
                elif damage == 'missing-insertion':
                    changed = re.sub(r'INSERT INTO job_instances.*?;', '', original, flags=re.S)
                else:
                    changed = re.sub(r"SELECT RAISE\(ABORT, 'Job instance identity was not created'\).*?;",
                                     '', original, flags=re.S)
                assert changed != original, 'Damage fixture did not alter the trigger'
                connection.execute(changed)
        _assert_fresh_process_refuses(storage)
    finally:
        _close(storage)


@pytest.mark.parametrize('damage,value', [
    ('missing', None), ('orphan', None), ('empty', ''), ('short', 'a' * 31),
    ('long', 'a' * 33), ('uppercase', 'A' * 32), ('nonhex', 'g' * 32),
    ('spaces', ' ' * 32), ('blob', b'1' * 32), ('number', 123),
    ('nul-suffix', 'a' * 32 + '\0hidden'),
])
def test_fresh_process_refuses_damaged_identity_rows_without_repair(tmp_path, damage, value):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.create_job(Job(node_name='A', job_id=1, params={'value': 'retained'}))
        _close(storage)
        with sqlite3.connect(storage.state_database_path()) as connection:
            connection.execute('PRAGMA foreign_keys=OFF')
            connection.execute('PRAGMA ignore_check_constraints=ON')
            if damage == 'missing':
                connection.execute("DELETE FROM job_instances WHERE node_name='A' AND job_id=1")
            elif damage == 'orphan':
                connection.execute(
                    'INSERT INTO job_instances(node_name,job_id,instance_id) VALUES(?,?,?)',
                    ('A', 2, '0123456789abcdef0123456789abcdef'),
                )
            else:
                connection.execute(
                    "UPDATE job_instances SET instance_id=? WHERE node_name='A' AND job_id=1", (value,),
                )
        _assert_fresh_process_refuses(storage)
    finally:
        _close(storage)


@pytest.mark.parametrize('damage', ['missing-table', 'address-key', 'foreign-key', 'cascade',
                                   'uniqueness', 'text-check', 'byte-check', 'hex-check'])
def test_fresh_process_refuses_changed_job_identity_table_declaration(tmp_path, damage):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.create_job(Job(node_name='A', job_id=1, params={'value': 'retained'}))
        _close(storage)
        with sqlite3.connect(storage.state_database_path()) as connection:
            original = connection.execute(
                "SELECT sql FROM sqlite_master WHERE name='job_instances'"
            ).fetchone()[0]
            retained = connection.execute('SELECT node_name,job_id,instance_id FROM job_instances').fetchall()
            connection.execute('DROP TABLE job_instances')
            if damage != 'missing-table':
                if damage == 'address-key':
                    changed = original.replace('PRIMARY KEY(node_name, job_id),', '')
                elif damage == 'foreign-key':
                    changed = re.sub(r',\s*FOREIGN KEY\(node_name, job_id\).*?ON DELETE CASCADE',
                                     '', original, flags=re.S)
                elif damage == 'cascade':
                    changed = original.replace('ON DELETE CASCADE', '')
                elif damage == 'uniqueness':
                    changed = original.replace(' UNIQUE', '')
                elif damage == 'text-check':
                    changed = original.replace("typeof(instance_id)='text' AND ", '')
                elif damage == 'byte-check':
                    changed = original.replace('AND length(CAST(instance_id AS BLOB))=32', '')
                else:
                    changed = original.replace("AND instance_id NOT GLOB '*[^0-9a-f]*'", '')
                assert changed != original, 'Damage fixture did not alter the table'
                connection.execute(changed)
                connection.executemany(
                    'INSERT INTO job_instances(node_name,job_id,instance_id) VALUES(?,?,?)', retained,
                )
        _assert_fresh_process_refuses(storage)
    finally:
        _close(storage)


def test_healthy_job_identities_reopen_in_a_fresh_process(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        for job_id in (1, 2):
            storage.create_job(Job(node_name='A', job_id=job_id, params={'value': job_id}))
        expected = [storage.read_job_instance_id('A', job_id) for job_id in (1, 2)]
        before = _logical_state(storage)
        _close(storage)
        script = '''
import json, sys
from pathlib import Path
from micro_workflow_manager.storage import FileStorage
storage = FileStorage(Path(sys.argv[1]))
try:
    assert [storage.load_job('A', job_id).params for job_id in (1, 2)] == [{'value': 1}, {'value': 2}]
    print(json.dumps([storage.read_job_instance_id('A', job_id) for job_id in (1, 2)]))
finally:
    storage.close_database_connections()
'''
        result = subprocess.run([sys.executable, '-c', script, str(tmp_path)],
                                capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stdout + result.stderr
        assert json.loads(result.stdout) == expected
        assert _logical_state(storage) == before
    finally:
        _close(storage)


def _private_workflow(path):
    initialized = FileStorage._create_new_project_state(path)
    _close(initialized)
    workflow = MicroWorkflow(project_dir=path)
    workflow.graph([])

    @workflow.task('A')
    def task(ctx, value=None):
        return value

    return workflow


def _identity_rows(storage):
    return {(row['node_name'], row['job_id']): row['instance_id']
            for row in storage.db_connection().execute('SELECT * FROM job_instances')}


@pytest.mark.parametrize('path', ['explicit', 'automatic', 'storage-automatic',
                                 'workflow-batch', 'storage-batch', 'defaults'])
def test_real_job_creation_paths_assign_distinct_identities_and_preserve_reuse(tmp_path, path):
    workflow = _private_workflow(tmp_path)
    storage = workflow.storage
    try:
        def create(value):
            if path == 'defaults':
                return workflow.create_jobs('A', number=3, params={'value': value})
            if path == 'workflow-batch':
                return workflow.add_jobs(None, 'A', [{'value': value}] * 3,
                                         idempotency_keys=['one', 'two', 'three'])
            if path == 'storage-batch':
                return storage.create_jobs_batch([
                    Job(node_name='A', job_id=job_id, params={'value': value}) for job_id in (1, 2, 3)
                ], idempotency_keys=['one', 'two', 'three'])
            if path == 'storage-automatic':
                return [storage.create_auto_id_job(
                    node_name='A', params={'value': value}, parent=None, producer_component=None,
                    job_kind=None, idempotency_key=key,
                ) for key in ('one', 'two', 'three')]
            return [workflow.add_job(
                None, 'A', job_id=job_id if path == 'explicit' else None,
                value=value, idempotency_key=key,
            ) for job_id, key in enumerate(('one', 'two', 'three'), 1)]

        jobs = create('initial')
        assert [job.job_id for job in jobs] == [1, 2, 3]
        identities = _identity_rows(storage)
        assert set(identities) == {('A', 1), ('A', 2), ('A', 3)}
        assert len(set(identities.values())) == 3
        assert all(re.fullmatch('[0-9a-f]{32}', identity) for identity in identities.values())
        if path == 'storage-batch':
            # This wrapper creates only new jobs. Reuse goes through its actual
            # stored idempotency lookup rather than a second duplicate insert.
            repeated = [storage.lookup_idempotent_job('A', key) for key in ('one', 'two', 'three')]
        else:
            repeated = create('refreshed')
        assert [job.job_id for job in repeated] == [1, 2, 3]
        assert _identity_rows(storage) == identities
        expected_value = 'refreshed' if path == 'defaults' else 'initial'
        assert [storage.load_job('A', job_id).params for job_id in (1, 2, 3)] == [
            {'value': expected_value}
        ] * 3
    finally:
        _close(storage)


def test_native_paste_reconciliation_retains_identity_and_removes_deleted_jobs(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.create_job(Job(node_name='A', job_id=1, params={'value': 'keep'}))
        storage.create_job(Job(node_name='A', job_id=2, params={'value': 'remove'}))
        first = storage.read_job_instance_id('A', 1)
        storage.set_job_status('A', 1, 'running')
        storage.input_file('A', 2).unlink()
        assert storage.reconcile_pasted_node_state('A') == {
            'requeued': 1, 'removed': 1, 'jobs': 1,
        }
        assert storage.read_job_instance_id('A', 1) == first
        assert storage.get_job_status('A', 1) == 'queued'
        assert storage.read_job_instance_id('A', 2) is None
        assert _identity_rows(storage) == {('A', 1): first}
    finally:
        _close(storage)


def test_clipboard_state_import_creates_new_destination_job_identities(tmp_path):
    source = FileStorage._create_new_project_state(tmp_path / 'source')
    destination = FileStorage._create_new_project_state(tmp_path / 'destination')
    try:
        source.create_job(Job(node_name='A', job_id=1, params={'value': 'source'}))
        destination.create_job(Job(node_name='A', job_id=1, params={'value': 'old destination'}))
        source_identity = source.read_job_instance_id('A', 1)
        old_destination_identity = destination.read_job_instance_id('A', 1)
        snapshot = source.export_node_state('A', tmp_path / 'clipboard.sqlite3')
        destination.import_node_state('A', snapshot)
        imported = destination.read_job_instance_id('A', 1)
        assert re.fullmatch('[0-9a-f]{32}', imported)
        assert imported not in (source_identity, old_destination_identity)
        assert _identity_rows(destination) == {('A', 1): imported}
        assert source.read_job_instance_id('A', 1) == source_identity
        # This storage API transfers database rows only; filesystem paste is a
        # separate operation and is not claimed by this identity assertion.
    finally:
        _close(source)
        _close(destination)


@pytest.mark.parametrize('path', ['selected-batch', 'whole-batch', 'node-jobs', 'node-state'])
def test_supported_deletion_paths_remove_only_deleted_job_identities(tmp_path, path):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        for node, job_id in [('A', 1), ('A', 2), ('B', 1)]:
            storage.create_job(Job(node_name=node, job_id=job_id, params={'value': node}))
        before = _identity_rows(storage)
        if path == 'selected-batch':
            assert storage.delete_jobs_batch('A', [1]) == 1
            expected = {('A', 2): before[('A', 2)], ('B', 1): before[('B', 1)]}
        else:
            if path == 'whole-batch':
                assert storage.delete_jobs_batch('A', [1, 2]) == 2
            elif path == 'node-jobs':
                storage.delete_node_jobs('A')
            else:
                storage.delete_node_state('A')
            expected = {('B', 1): before[('B', 1)]}
        assert _identity_rows(storage) == expected
        storage.create_job(Job(node_name='A', job_id=1, params={'value': 'recreated'}))
        assert storage.read_job_instance_id('A', 1) not in before.values()
        assert storage.read_job_instance_id('B', 1) == before[('B', 1)]
    finally:
        _close(storage)


def _reject_job_identity(storage, job_id):
    assert type(job_id) is int
    storage.submit_db_mutation(lambda connection: connection.execute(f'''
        CREATE TRIGGER reject_test_job_instance BEFORE INSERT ON job_instances
        WHEN NEW.node_name='A' AND NEW.job_id={job_id}
        BEGIN SELECT RAISE(ABORT, 'injected job identity failure'); END
    '''))


@pytest.mark.parametrize('fault', ['suppressed-identity', 'orphan-address'])
def test_native_creation_requires_a_new_identity_and_rolls_back_on_failure(tmp_path, fault):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.create_job(Job(node_name='A', job_id=1, params={'value': 'retained'}))
        if fault == 'suppressed-identity':
            storage.submit_db_mutation(lambda connection: connection.execute('''
                CREATE TRIGGER suppress_test_identity BEFORE INSERT ON job_instances
                WHEN NEW.node_name='A' AND NEW.job_id=2
                BEGIN SELECT RAISE(IGNORE); END
            '''))
        else:
            _close(storage)
            with sqlite3.connect(storage.state_database_path()) as connection:
                connection.execute('PRAGMA foreign_keys=OFF')
                connection.execute(
                    'INSERT INTO job_instances(node_name,job_id,instance_id) VALUES(?,?,?)',
                    ('A', 2, '0123456789abcdef0123456789abcdef'),
                )
        before = _logical_state(storage)
        with pytest.raises(sqlite3.IntegrityError):
            storage.create_job(Job(node_name='A', job_id=2, params={'value': 'new'}))
        assert _logical_state(storage) == before
        assert storage.job_exists('A', 2) is False
        assert not storage.job_base_dir('A', 2).exists()
        assert storage.load_job('A', 1).params == {'value': 'retained'}
    finally:
        _close(storage)


@pytest.mark.parametrize('path', ['storage-explicit', 'workflow-explicit', 'storage-batch', 'workflow-batch'])
def test_creation_failure_preserves_rows_and_cleans_only_new_payloads(tmp_path, path):
    workflow = _private_workflow(tmp_path)
    storage = workflow.storage
    try:
        storage.create_job(Job(node_name='A', job_id=1, params={'value': 'seed'}))
        storage.set_node_status('A', 'queued')
        batch = path.endswith('batch')
        _reject_job_identity(storage, 3 if batch else 2)
        before_schema, before_rows = _logical_state(storage)
        with pytest.raises(sqlite3.IntegrityError, match='injected job identity failure'):
            if path == 'storage-explicit':
                storage.create_job(Job(node_name='A', job_id=2, params={'value': 'new'}))
            elif path == 'workflow-explicit':
                workflow.add_job(None, 'A', job_id=2, value='new', idempotency_key='new-key')
            elif path == 'storage-batch':
                storage.create_jobs_batch([
                    Job(node_name='A', job_id=job_id, params={'value': 'new'}) for job_id in (2, 3)
                ], idempotency_keys=['key-two', 'key-three'])
            else:
                workflow.add_jobs(None, 'A', [{'value': 'two'}, {'value': 'three'}],
                                  idempotency_keys=['key-two', 'key-three'])
        if path == 'workflow-batch':
            # Workflow ID reservation commits before preparation/publication.
            assert before_rows['job_sequences'] == [('A', 2)]
            before_rows['job_sequences'] = [('A', 4)]
        assert _logical_state(storage) == (before_schema, before_rows)
        assert not storage.job_base_dir('A', 2).exists()
        assert not storage.job_base_dir('A', 3).exists()
        assert storage.load_job('A', 1).params == {'value': 'seed'}
    finally:
        _close(storage)


def test_grouped_auto_creation_rolls_back_members_and_cleans_staging_on_identity_failure(tmp_path, monkeypatch):
    storage = FileStorage._create_new_project_state(tmp_path)
    release_blocker, blocker_started, both_enqueued = Event(), Event(), Event()
    guard = Lock()
    enqueued = 0
    observed_groups = []
    blocker = None
    try:
        storage.create_job(Job(node_name='A', job_id=1, params={'value': 'seed'}))
        _reject_job_identity(storage, 3)
        before = _logical_state(storage)

        def block(connection):
            blocker_started.set()
            assert release_blocker.wait(10), 'Fixture did not release the writer blocker'

        blocker = storage.submit_db_mutation(block, wait=False, priority=0)
        assert blocker_started.wait(10), 'Writer blocker did not start'
        original_enqueue = storage._mutation_writer._enqueue
        original_apply = storage._apply_auto_job_publishes

        def observed_apply(connection, publishes):
            observed_groups.append(len(publishes))
            return original_apply(connection, publishes)

        def observed_enqueue(request):
            nonlocal enqueued
            grouped = request.group_key == ('auto-job-publish',)
            if grouped:
                request.group_operation = observed_apply
            original_enqueue(request)
            if grouped:
                with guard:
                    enqueued += 1
                    if enqueued == 2:
                        both_enqueued.set()

        monkeypatch.setattr(storage._mutation_writer, '_enqueue', observed_enqueue)

        def create(value):
            try:
                return storage.create_auto_id_job(
                    node_name='A', params={'value': value}, parent=None,
                    producer_component=None, job_kind=None, idempotency_key=str(value),
                )
            finally:
                storage.close_thread_connection()

        outcomes = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(create, value) for value in (2, 3)]
            try:
                assert both_enqueued.wait(10), 'Both actual publication requests were not enqueued'
            finally:
                release_blocker.set()
            for future in futures:
                try:
                    outcomes.append(future.result(timeout=10))
                except Exception as error:
                    outcomes.append(error)
        blocker.result(timeout=10)
        storage.db_mutation_barrier()
        assert observed_groups == [2], 'Fixture did not collect one two-member group'
        assert all(isinstance(result, sqlite3.IntegrityError)
                   and 'injected job identity failure' in str(result) for result in outcomes), outcomes
        assert _logical_state(storage) == before
        assert not storage.job_base_dir('A', 2).exists()
        assert not storage.job_base_dir('A', 3).exists()
        staged = tmp_path / '.mwf' / 'staged-jobs'
        assert not any(path.is_file() for path in staged.rglob('*'))
    finally:
        release_blocker.set()
        if blocker is not None:
            blocker.result(timeout=10)
        _close(storage)


def test_identity_survives_claim_terminal_restart_reset_and_sequence_updates(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        graph = nx.DiGraph()
        graph.add_node('A')
        snapshot = ComponentTopology(graph, []).snapshot()
        storage.register_component_topology(snapshot)
        storage.create_execution_session(
            'owner', session_kind='main', command='run', start_component=('A',),
            selected_components=[('A',)], started_at='2026-09-05T12:00:00',
            hostname='worker.example', pid=123, process_identity='owner-process',
        )
        assert storage.reserve_execution_components('owner', expected_shape=snapshot.shape_json)
        storage.create_job(Job(node_name='A', job_id=1, params={'value': 'retained'}))
        identity = storage.read_job_instance_id('A', 1)
        generation, execution = storage.claim_job_execution(
            'A', 1, started_at='2026-09-05T12:00:00', session_id='owner', component=('A',),
        )
        assert storage.get_job_status('A', 1) == 'running'
        assert storage.read_job_instance_id('A', 1) == identity
        storage.finalize_job_execution('A', 1, generation, execution, 'done')
        assert storage.get_job_status('A', 1) == 'done'
        assert storage.read_job_instance_id('A', 1) == identity
        restarted = storage.request_job_restart('A', 1)
        assert restarted['generation'] == generation + 1
        assert storage.get_job_status('A', 1) == 'queued'
        assert storage.read_job_instance_id('A', 1) == identity
        assert storage.finish_execution_session(
            'owner', outcome='stopped', finished_at='2026-09-05T12:01:00',
        )
        assert storage.release_execution_components('owner') == 1
        storage.set_job_status('A', 1, 'failed')
        assert storage.reset_jobs_for_run_batch('A', [1]) == 1
        assert storage.get_job_status('A', 1) == 'queued'
        storage.set_job_status('A', 1, 'done')
        assert storage.reset_nodes_for_run_batch(['A']) == 1
        assert storage.get_job_status('A', 1) == 'queued'
        storage.advance_job_sequence('A', 99)
        assert storage.rewind_job_sequence_to_available('A') == 2
        assert _identity_rows(storage) == {('A', 1): identity}
    finally:
        _close(storage)
