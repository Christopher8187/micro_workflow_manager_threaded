from __future__ import annotations

import os
import re
import shutil
import socket
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.cli.cleanup import reset_job_for_run
from micro_workflow_manager.models import Job, now
from micro_workflow_manager.processes import process_identity
from micro_workflow_manager.storage import FileStorage


def _close(storage):
    storage.db_mutation_barrier()
    deadline = time.monotonic() + 10
    while storage.mutation_writer_diagnostics()['writer_alive']:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    storage.close_database_connections()


@pytest.fixture
def workflow(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A')])
    workflow.storage.register_component_topology(workflow.topology.snapshot())
    for node in ('A', 'B'):
        workflow.storage.create_job(Job(node_name=node, job_id=1, params={'value': node}))
    try:
        yield workflow
    finally:
        _close(workflow.storage)


def _session(workflow, session_id):
    storage = workflow.storage
    storage.create_execution_session(
        session_id, session_kind='main', command='run', start_component=('A', 'B'),
        selected_components=[('A', 'B')], started_at=now(),
        hostname=socket.gethostname(), pid=os.getpid(), process_identity=process_identity(os.getpid()),
    )
    storage.reserve_execution_components(session_id, expected_shape=workflow.topology.snapshot().shape_json)


def _claim(storage, session_id, node='A'):
    return storage.claim_job_execution(
        node, 1, started_at=now(), session_id=session_id, component=('A', 'B'),
    )


def _finish(storage, session_id):
    storage.finish_execution_session(session_id, outcome='done', finished_at=now())
    storage.release_execution_components(session_id)


@pytest.mark.parametrize('status', ['done', 'failed', 'cancelled', 'skipped'])
def test_current_owner_survives_completion_trace_clearing_and_native_reopen(workflow, status):
    storage = workflow.storage
    assert storage.read_job_current_owner('A', 1) is None
    _session(workflow, 'original-session')
    generation, execution_id = _claim(storage, 'original-session')
    owner = storage.get_job_execution_owner(execution_id)
    assert storage.read_job_current_owner('A', 1) == owner
    storage.finalize_job_execution('A', 1, generation, execution_id, status)
    _finish(storage, 'original-session')
    assert storage.read_job_control('A', 1)['active_execution_id'] is None
    assert storage.read_job_current_owner('A', 1) == owner
    storage.clear_job_events('A', [1])
    assert storage.read_job_events('A', 1) == []
    assert storage.read_job_current_owner('A', 1) == owner
    _close(storage)
    reopened = FileStorage(storage.project_dir)
    try:
        assert reopened.read_job_current_owner('A', 1) == owner
        assert reopened.get_job_status('A', 1) == status
    finally:
        _close(reopened)


def test_latest_committed_unstarted_claim_is_last_owner_at_same_generation(workflow):
    storage = workflow.storage
    _session(workflow, 'first-session')
    first_generation, first_execution = _claim(storage, 'first-session')
    storage.release_unstarted_job_execution('A', 1, first_generation, first_execution)
    _finish(storage, 'first-session')
    assert storage.read_job_current_owner('A', 1)['session_id'] == 'first-session'
    _session(workflow, 'second-session')
    second_generation, second_execution = _claim(storage, 'second-session')
    storage.release_unstarted_job_execution('A', 1, second_generation, second_execution)
    _finish(storage, 'second-session')
    assert first_generation == second_generation
    assert first_execution != second_execution
    assert storage.read_job_current_owner('A', 1) == storage.get_job_execution_owner(second_execution)
    assert storage.read_job_current_owner('A', 1)['session_id'] == 'second-session'
    assert storage.get_job_execution_owner(first_execution)['session_id'] == 'first-session'


@pytest.mark.parametrize('prepare', ['nodes', 'jobs', 'cli-job'])
@pytest.mark.parametrize('keep_trace', [False, True])
def test_explicit_fresh_preparation_clears_only_selected_current_owner(workflow, prepare, keep_trace):
    storage = workflow.storage
    _session(workflow, 'finished-session')
    owners = {}
    for node in ('A', 'B'):
        generation, execution_id = _claim(storage, 'finished-session', node)
        storage.finalize_job_execution(node, 1, generation, execution_id, 'done')
        owners[node] = storage.get_job_execution_owner(execution_id)
    _finish(storage, 'finished-session')
    instance = storage.read_job_instance_id('A', 1)
    if prepare == 'nodes':
        storage.reset_nodes_for_run_batch(['A'], preserve_events=keep_trace)
    elif prepare == 'jobs':
        storage.reset_jobs_for_run_batch('A', [1], preserve_events=keep_trace)
    else:
        reset_job_for_run(storage.project_dir, workflow, 'A', 1, keep_trace=keep_trace)
    assert storage.read_job_current_owner('A', 1) is None
    assert storage.get_job_status('A', 1) == 'queued'
    assert storage.read_job_instance_id('A', 1) == instance
    assert storage.read_job_current_owner('B', 1) == owners['B']
    assert storage.get_job_execution_owner(owners['A']['execution_id']) == owners['A']
    _session(workflow, 'new-session')
    _, new_execution = _claim(storage, 'new-session')
    assert storage.read_job_current_owner('A', 1)['execution_id'] == new_execution
    assert storage.read_job_current_owner('A', 1)['session_id'] == 'new-session'


@pytest.mark.parametrize('delete', ['single', 'batch', 'node', 'state'])
def test_recreated_numeric_job_id_never_inherits_deleted_jobs_owner(workflow, delete):
    storage = workflow.storage
    _session(workflow, 'deleted-session')
    generation, execution_id = _claim(storage, 'deleted-session')
    storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
    _finish(storage, 'deleted-session')
    old_owner = storage.get_job_execution_owner(execution_id)
    old_instance = storage.read_job_instance_id('A', 1)
    if delete == 'single':
        storage.delete_job('A', 1, preserve_events=True)
    elif delete == 'batch':
        storage.delete_jobs_batch('A', [1], preserve_events=True)
    elif delete == 'node':
        storage.delete_node_jobs('A', preserve_events=True)
    else:
        shutil.rmtree(storage.node_dir('A'))
        storage.delete_node_state('A')
    assert storage.read_job_current_owner('A', 1) is None
    storage.create_job(Job(node_name='A', job_id=1, params={'value': 'new'}))
    assert storage.read_job_instance_id('A', 1) != old_instance
    assert storage.read_job_current_owner('A', 1) is None
    assert storage.get_job_execution_owner(execution_id) == old_owner


@pytest.mark.parametrize('damage', ['missing-column', 'nullable', 'type', 'length', 'bytes', 'hex'])
def test_native_reopen_refuses_weakened_claim_instance_declaration(workflow, damage):
    storage = workflow.storage
    _close(storage)
    with sqlite3.connect(storage.state_database_path()) as connection:
        original = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='job_execution_owners'"
        ).fetchone()[0]
        if damage == 'missing-column':
            changed = re.sub(r'job_instance_id TEXT NOT NULL.*?\),', '', original, flags=re.S)
        else:
            removed, replacement = {
                'nullable': ('job_instance_id TEXT NOT NULL', 'job_instance_id TEXT'),
                'type': ("typeof(job_instance_id)='text' AND ", ''),
                'length': ('length(job_instance_id)=32\n', '1\n'),
                'bytes': ('AND length(CAST(job_instance_id AS BLOB))=32', ''),
                'hex': ("AND job_instance_id NOT GLOB '*[^0-9a-f]*'", ''),
            }[damage]
            changed = original.replace(removed, replacement)
        assert changed != original
        connection.execute('DROP TABLE job_execution_owners')
        connection.execute(changed)
        before = list(connection.iterdump())
    with pytest.raises(RuntimeError, match='Incomplete SQLite execution-session schema'):
        FileStorage(storage.project_dir)
    with sqlite3.connect(storage.state_database_path()) as connection:
        assert list(connection.iterdump()) == before


@pytest.mark.parametrize('damage', ['missing', 'invalid'])
def test_claim_batch_refuses_invalid_instance_before_changing_any_job(workflow, damage):
    storage = workflow.storage
    _session(workflow, 'instance-damage-session')
    storage.create_job(Job(node_name='A', job_id=2, params={'value': 'second'}))
    connection = storage.db_connection()
    connection.execute('PRAGMA foreign_keys=OFF')
    connection.execute('PRAGMA ignore_check_constraints=ON')
    try:
        if damage == 'missing':
            connection.execute("DELETE FROM job_instances WHERE node_name='A' AND job_id=2")
        else:
            connection.execute("UPDATE job_instances SET instance_id='invalid' WHERE node_name='A' AND job_id=2")
    finally:
        connection.execute('PRAGMA foreign_keys=ON')
        connection.execute('PRAGMA ignore_check_constraints=OFF')
    before = list(connection.iterdump())
    with pytest.raises(RuntimeError, match='current job instances'):
        storage.claim_job_executions_batch(
            'A', [1, 2], started_at=now(), session_id='instance-damage-session', component=('A', 'B'),
        )
    assert list(connection.iterdump()) == before


def test_generation_restart_preserves_last_owner_until_the_next_claim(workflow):
    storage = workflow.storage
    _session(workflow, 'restart-session')
    generation, execution_id = _claim(storage, 'restart-session')
    owner = storage.read_job_current_owner('A', 1)
    storage.request_active_job_restart('A', 1, reason='replace generation')
    assert storage.read_job_control('A', 1)['generation'] == generation + 1
    assert storage.read_job_current_owner('A', 1) == owner
    next_generation, next_execution = _claim(storage, 'restart-session')
    assert next_generation == generation + 1
    assert storage.read_job_current_owner('A', 1)['execution_id'] == next_execution
    assert next_execution != execution_id


@pytest.mark.parametrize('operation', ['claim', 'reset-nodes', 'reset-jobs'])
def test_current_owner_update_failure_rolls_back_job_owner_and_events(workflow, operation):
    storage = workflow.storage
    _session(workflow, 'rollback-session')
    generation, execution_id = _claim(storage, 'rollback-session')
    if operation == 'claim':
        storage.release_unstarted_job_execution('A', 1, generation, execution_id)
    else:
        storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
    before_owner = storage.read_job_current_owner('A', 1)
    before_control = storage.read_job_control('A', 1)
    before_events = storage.read_job_events('A', 1)
    before_status = storage.get_job_status('A', 1)
    before_rows = [dict(row) for row in storage.db_connection().execute('SELECT * FROM job_execution_owners')]
    storage.db_connection().execute('''
        CREATE TRIGGER fail_owner_reference BEFORE UPDATE OF last_execution_id ON job_instances
        BEGIN SELECT RAISE(ABORT, 'injected owner reference failure'); END
    ''')
    try:
        with pytest.raises(sqlite3.IntegrityError, match='injected owner reference failure'):
            if operation == 'claim':
                _claim(storage, 'rollback-session')
            elif operation == 'reset-nodes':
                storage.reset_nodes_for_run_batch(['A'], preserve_events=False)
            else:
                storage.reset_jobs_for_run_batch('A', [1], preserve_events=False)
        assert storage.read_job_current_owner('A', 1) == before_owner
        assert storage.read_job_control('A', 1) == before_control
        assert storage.read_job_events('A', 1) == before_events
        assert storage.get_job_status('A', 1) == before_status
        assert [dict(row) for row in storage.db_connection().execute('SELECT * FROM job_execution_owners')] == before_rows
    finally:
        storage.db_connection().execute('DROP TRIGGER fail_owner_reference')


@pytest.mark.parametrize('damage', ['missing-owner', 'wrong-job', 'active-disagreement', 'missing-instance', 'invalid-instance'])
def test_damaged_current_owner_refuses_without_guessing_or_mutation(workflow, damage):
    storage = workflow.storage
    _session(workflow, 'damaged-session')
    generation, execution_id = _claim(storage, 'damaged-session')
    if damage != 'active-disagreement':
        storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
    connection = storage.db_connection()
    connection.execute('PRAGMA foreign_keys=OFF')
    connection.execute('PRAGMA ignore_check_constraints=ON')
    try:
        if damage == 'missing-owner':
            connection.execute('DELETE FROM job_execution_owners WHERE execution_id=?', (execution_id,))
        elif damage == 'wrong-job':
            connection.execute("UPDATE job_execution_owners SET node_name='B' WHERE execution_id=?", (execution_id,))
        elif damage == 'active-disagreement':
            connection.execute("UPDATE job_instances SET last_execution_id=NULL WHERE node_name='A'")
        elif damage == 'missing-instance':
            connection.execute("DELETE FROM job_instances WHERE node_name='A'")
        else:
            connection.execute("UPDATE job_instances SET instance_id='invalid' WHERE node_name='A'")
    finally:
        connection.execute('PRAGMA ignore_check_constraints=OFF')
        connection.execute('PRAGMA foreign_keys=ON')
    before = [dict(row) for row in connection.execute('SELECT * FROM jobs')]
    events = storage.read_job_events('A', 1)
    with pytest.raises(RuntimeError, match='ownership|instance'):
        storage.read_job_current_owner('A', 1)
    assert [dict(row) for row in connection.execute('SELECT * FROM jobs')] == before
    assert storage.read_job_events('A', 1) == events


def test_current_owner_refuses_retained_execution_of_a_deleted_job_instance(workflow):
    storage = workflow.storage
    _session(workflow, 'old-instance-session')
    generation, execution_id = _claim(storage, 'old-instance-session')
    storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
    _finish(storage, 'old-instance-session')
    old_instance = storage.read_job_instance_id('A', 1)
    old_owner = storage.get_job_execution_owner(execution_id)
    storage.delete_job('A', 1, preserve_events=True)
    storage.create_job(Job(node_name='A', job_id=1, params={'value': 'replacement'}))
    assert storage.read_job_instance_id('A', 1) != old_instance
    assert storage.read_job_current_owner('A', 1) is None
    storage.submit_db_mutation(lambda connection: connection.execute(
        "UPDATE job_instances SET last_execution_id=? WHERE node_name='A' AND job_id=1",
        (execution_id,),
    ))
    current_instance = storage.read_job_instance_id('A', 1)
    before_control = storage.read_job_control('A', 1)
    before_events = storage.read_job_events('A', 1)
    with pytest.raises(RuntimeError, match='instance|ownership'):
        storage.read_job_current_owner('A', 1)
    assert storage.get_job_status('A', 1) == 'queued'
    assert storage.load_job('A', 1).params == {'value': 'replacement'}
    assert storage.read_job_instance_id('A', 1) == current_instance
    assert storage.read_job_control('A', 1) == before_control
    assert storage.read_job_events('A', 1) == before_events
    assert storage.get_job_execution_owner(execution_id) == old_owner
    _session(workflow, 'replacement-session')
    before = list(storage.db_connection().iterdump())
    with pytest.raises(RuntimeError, match='ownership'):
        _claim(storage, 'replacement-session')
    assert list(storage.db_connection().iterdump()) == before
    # Restore the valid unclaimed pointer that this fixture deliberately damaged.
    storage.submit_db_mutation(lambda connection: connection.execute(
        "UPDATE job_instances SET last_execution_id=NULL WHERE node_name='A' AND job_id=1",
    ))
    _, replacement_execution = _claim(storage, 'replacement-session')
    replacement_owner = storage.read_job_current_owner('A', 1)
    assert replacement_owner['execution_id'] == replacement_execution
    assert replacement_owner['job_instance_id'] == current_instance
    assert replacement_owner['session_id'] == 'replacement-session'
    assert storage.get_job_execution_owner(execution_id) == old_owner


@pytest.mark.parametrize('batch', [False, True])
def test_a_new_claim_cannot_replace_an_existing_active_lease(workflow, batch):
    storage = workflow.storage
    _session(workflow, 'exclusive-session')
    _claim(storage, 'exclusive-session')
    before = list(storage.db_connection().iterdump())
    with pytest.raises(RuntimeError, match='already claimed|active execution'):
        if batch:
            storage.claim_job_executions_batch(
                'A', [1], started_at=now(), session_id='exclusive-session', component=('A', 'B'),
            )
        else:
            _claim(storage, 'exclusive-session')
    assert list(storage.db_connection().iterdump()) == before


def test_grouped_overlapping_claims_accept_one_batch_and_preserve_other_jobs(workflow, monkeypatch):
    storage = workflow.storage
    _session(workflow, 'exclusive-session')
    storage.create_job(Job(node_name='A', job_id=2, params={'value': 'unclaimed'}))
    instance = storage.read_job_instance_id('A', 1)
    waiting, release = Event(), Event()
    first_queued, both_queued = Event(), Event()
    count_lock = Lock()
    submitted = 0
    group_sizes = []
    original_submit = storage.submit_grouped_db_mutation

    def blocker(connection):
        waiting.set()
        assert release.wait(15)

    def queued_submit(key, item, operation, **kwargs):
        nonlocal submitted

        def observed_operation(connection, items):
            group_sizes.append(len(items))
            return operation(connection, items)

        kwargs['wait'] = False
        result = original_submit(key, item, observed_operation, **kwargs)
        with count_lock:
            submitted += 1
            first_queued.set()
            if submitted == 2:
                both_queued.set()
        return result.result(timeout=15)

    barrier = storage.submit_db_mutation(blocker, wait=False, priority=0)
    monkeypatch.setattr(storage, 'submit_grouped_db_mutation', queued_submit)
    with ThreadPoolExecutor(max_workers=2) as executor:
        try:
            assert waiting.wait(10)
            first = executor.submit(
                storage.claim_job_executions_batch, 'A', [1], started_at=now(),
                session_id='exclusive-session', component=('A', 'B'),
            )
            assert first_queued.wait(10)
            second = executor.submit(
                storage.claim_job_executions_batch, 'A', [1, 2], started_at=now(),
                session_id='exclusive-session', component=('A', 'B'),
            )
            assert both_queued.wait(10)
        finally:
            release.set()
        barrier.result(timeout=10)
        [(generation, execution_id)] = first.result(timeout=15)
        with pytest.raises(RuntimeError, match='already claimed|active execution'):
            second.result(timeout=15)
    assert group_sizes == [2]
    assert storage.read_job_instance_id('A', 1) == instance
    assert storage.read_job_control('A', 1)['active_execution_id'] == execution_id
    assert storage.read_job_current_owner('A', 1)['execution_id'] == execution_id
    assert storage.read_job_current_owner('A', 1)['generation'] == generation
    owners = list(storage.db_connection().execute('SELECT execution_id FROM job_execution_owners'))
    assert [row['execution_id'] for row in owners] == [execution_id]
    started = [row for row in storage.read_job_events('A', 1) if row['event'] == 'started']
    assert [row['execution_id'] for row in started] == [execution_id]
    assert storage.get_job_status('A', 2) == 'queued'
    assert storage.read_job_current_owner('A', 2) is None
    assert not any(row['event'] == 'started' for row in storage.read_job_events('A', 2))
