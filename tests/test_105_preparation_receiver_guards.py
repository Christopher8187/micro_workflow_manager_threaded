from __future__ import annotations

import os
import socket
from pathlib import Path
from types import SimpleNamespace

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.cli.destructive import execute_destructive_command
from micro_workflow_manager.errors import JobFailedError
from micro_workflow_manager.models import now
from micro_workflow_manager.processes import process_identity
from tests.test_090_component_session_settlement import _close, _rows


def _files(directory):
    return {path.relative_to(directory).as_posix(): path.read_bytes()
            for path in directory.rglob('*') if path.is_file()}


def _relevant_state(storage, root):
    rows = _rows(storage)
    assert {'preparation_receipts', 'receiver_mutation_guards'} <= rows.keys()
    for table in ('execution_sessions', 'session_components', 'session_jobs'):
        rows.pop(table, None)
    return rows, _files(root / 'node')


def _receiver_publication_state(storage, root, receiver='B'):
    rows = _rows(storage)
    return {
        'jobs': [row for row in rows['jobs'] if row['node_name'] == receiver],
        'job_sequences': [row for row in rows['job_sequences'] if row['node_name'] == receiver],
        'job_instances': [row for row in rows['job_instances'] if row['node_name'] == receiver],
        'job_execution_owners': [
            row for row in rows['job_execution_owners'] if row['node_name'] == receiver
        ],
        'job_events': [row for row in rows['job_events'] if row['node_name'] == receiver],
        'idempotency': [row for row in rows['idempotency'] if row['node_name'] == receiver],
        'default_job_specs': [
            row for row in rows['default_job_specs'] if row['node_name'] == receiver
        ],
        'managed_input_files': [
            row for row in rows['managed_input_files'] if row['receiver_node'] == receiver
        ],
        'managed_input_producers': [
            row for row in rows['managed_input_producers'] if row['receiver_node'] == receiver
        ],
        'input_publications': [
            row for row in rows['input_publications'] if row['receiver_node'] == receiver
        ],
        'nodes': [row for row in rows['nodes'] if row['node_name'] == receiver],
        'component_state': storage.get_component_state((receiver,)),
        'causes': [
            row for row in rows['component_misalignment_causes']
            if row['receiver_node'] == receiver
        ],
        'files': _files(root / 'node' / receiver),
    }


def _established_workflow(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('X', 'B')])
    publish = {'enabled': True}

    @workflow.task('A')
    def producer_a(ctx):
        if publish['enabled']:
            ctx.node('B').write_input('owned.txt', 'from A')
            ctx.node('B').add(origin='from-A')

    @workflow.task('X')
    def producer_x(ctx):
        if publish['enabled']:
            ctx.node('B').write_input('owned.txt', 'from X')
            ctx.node('B').add(origin='from-X')

    @workflow.task('B')
    def receiver(ctx, origin):
        ctx.write_output(origin + '.txt', origin)
        return origin

    workflow.start('A')
    workflow.start('X')
    workflow.start('B', origin='project')
    workflow.run()
    publish['enabled'] = False
    assert workflow.storage.get_component_state(('B',))['lifecycle'] == 'done'
    return workflow


def _native_receiver_owner(workflow, *, running):
    storage = workflow.storage
    snapshot = workflow.topology.snapshot()
    session_id = 'b' * 32
    identity = process_identity(os.getpid())
    assert identity
    storage.create_execution_session(
        session_id, session_kind='interrupt', command='run',
        start_component=('B',), selected_components=[('B',)], started_at=now(),
        hostname=socket.gethostname(), pid=os.getpid(), process_identity=identity,
        expected_shape=snapshot.shape_json,
    )
    storage.reserve_execution_components(session_id, expected_shape=snapshot.shape_json)
    if running:
        expected = storage.read_component_fresh_preparation(
            session_id, ('B',), expected_shape=snapshot.shape_json,
        )
        generation = storage.complete_component_fresh_preparation(
            session_id, ('B',), expected_state=expected,
        )
        assert storage.begin_queued_component_execution(
            session_id, ('B',), expected_shape=snapshot.shape_json,
            expected_alignment_generation=generation, successful_lineage=('stable', None),
        )
    return session_id


def _run_preparation(tmp_path, workflow, command):
    if command == 'reset':
        return execute_destructive_command(
            tmp_path, workflow, SimpleNamespace(command='reset', node='A', yes=True),
        )
    return workflow.run_node('A')


@pytest.mark.parametrize('command', ['reset', 'run'])
@pytest.mark.parametrize('receiver_state', ['reserved', 'running'])
def test_public_preparation_refuses_owned_excluded_receiver_before_work_changes(
    tmp_path, command, receiver_state,
):
    workflow = _established_workflow(tmp_path)
    storage = workflow.storage
    try:
        owner = _native_receiver_owner(workflow, running=receiver_state == 'running')
        before = _relevant_state(storage, tmp_path)
        with pytest.raises(RuntimeError) as caught:
            _run_preparation(tmp_path, workflow, command)
        assert owner in str(caught.value)
        if command == 'run':
            assert 'B' in str(caught.value)
        assert _relevant_state(storage, tmp_path) == before
    finally:
        _close(storage)


def _install_guard(storage, *, operation_id='c' * 32, session_id=None):
    identity = process_identity(os.getpid())
    assert identity
    storage.submit_db_mutation(lambda connection: connection.execute(
        'INSERT INTO receiver_mutation_guards '
        '(receiver_node, operation_id, session_id, owner_pid, process_identity, hostname) '
        'VALUES(?,?,?,?,?,?)',
        ('B', operation_id, session_id, os.getpid(), identity, socket.gethostname()),
    ))
    return operation_id


@pytest.mark.parametrize('command', ['reset', 'run'])
def test_public_preparation_refuses_existing_excluded_receiver_guard(tmp_path, command):
    workflow = _established_workflow(tmp_path)
    storage = workflow.storage
    try:
        operation_id = _install_guard(storage)
        before = _relevant_state(storage, tmp_path)
        with pytest.raises(RuntimeError) as caught:
            _run_preparation(tmp_path, workflow, command)
        assert 'B' in str(caught.value) and operation_id in str(caught.value)
        assert _relevant_state(storage, tmp_path) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('publication', [
    'input', 'direct-single-job', 'auto-job', 'batch-jobs', 'resolving-batch-jobs',
])
def test_held_receiver_guard_refuses_new_managed_publication(tmp_path, publication):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage

    @workflow.task('A')
    def producer(ctx):
        if publication == 'input':
            ctx.node('B').write_input('blocked.txt', 'must not publish')
        elif publication == 'direct-single-job':
            ctx.node('B').add(job_id=1, origin='must-not-publish')
        elif publication == 'auto-job':
            ctx.node('B').add(origin='must-not-publish')
        elif publication == 'batch-jobs':
            workflow.create_jobs('B', number=2, params={'origin': 'must-not-publish'})
        else:
            ctx.node('B').add_many([
                {'origin': 'must-not-publish'}, {'origin': 'must-not-publish-either'},
            ])

    @workflow.task('B')
    def receiver(ctx, origin):
        return origin

    workflow.start('A')
    storage.register_component_topology(workflow.topology.snapshot())
    try:
        operation_id = _install_guard(storage)
        before = _receiver_publication_state(storage, tmp_path)
        with pytest.raises(JobFailedError) as caught:
            workflow.run_node('A')
        assert isinstance(caught.value.__cause__, RuntimeError)
        assert 'B' in str(caught.value.__cause__) and operation_id in str(caught.value.__cause__)
        assert _receiver_publication_state(storage, tmp_path) == before
        guard = storage.db_connection().execute(
            'SELECT operation_id FROM receiver_mutation_guards WHERE receiver_node=?', ('B',),
        ).fetchone()
        assert guard['operation_id'] == operation_id
    finally:
        _close(storage)


@pytest.mark.parametrize('publication', [
    'direct-single-job', 'auto-job', 'batch-jobs', 'resolving-batch-jobs',
])
def test_held_receiver_guard_allows_idempotent_job_reuse_without_receiver_changes(
    tmp_path, publication,
):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    returned = []

    @workflow.task('B')
    def receiver(ctx, origin):
        return origin

    if publication == 'batch-jobs':
        existing = workflow.create_jobs('B', params={'origin': 'kept'})[0]
    else:
        existing = workflow.add_job(
            None, 'B', job_id=1, idempotency_key='keep', origin='kept',
        )

    @workflow.task('A')
    def producer(ctx):
        if publication == 'direct-single-job':
            returned.append(ctx.node('B').add(
                job_id=2, idempotency_key='keep', origin='ignored',
            ))
        elif publication == 'auto-job':
            returned.append(ctx.node('B').add(idempotency_key='keep', origin='ignored'))
        elif publication == 'batch-jobs':
            returned.extend(workflow.create_jobs('B', params={'origin': 'kept'}))
        else:
            returned.extend(ctx.node('B').add_many(
                [{'origin': 'ignored'}], idempotency_keys=['keep'],
            ))

    workflow.start('A')
    storage.register_component_topology(workflow.topology.snapshot())
    try:
        operation_id = _install_guard(storage)
        before = _receiver_publication_state(storage, tmp_path)
        workflow.run_node('A')
        assert [(job.node_name, job.job_id, job.params) for job in returned] == [
            ('B', existing.job_id, {'origin': 'kept'}),
        ]
        assert _receiver_publication_state(storage, tmp_path) == before
        guard = storage.db_connection().execute(
            'SELECT operation_id FROM receiver_mutation_guards WHERE receiver_node=?', ('B',),
        ).fetchone()
        assert guard['operation_id'] == operation_id
    finally:
        _close(storage)


def test_grouped_auto_publication_refuses_guarded_receiver_without_touching_its_rows(
    tmp_path, monkeypatch,
):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event, Lock

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('A', 'C')])
    storage = workflow.storage
    snapshot = workflow.topology.snapshot()
    storage.register_component_topology(snapshot)
    entered, queued, release, lock = Event(), Event(), Event(), Lock()
    submitted, applied = [], []
    original_submit = storage.submit_grouped_db_mutation
    original_apply = storage._apply_auto_job_publishes

    def submit(*args, **kwargs):
        future = original_submit(*args, **kwargs)
        with lock:
            submitted.append(True)
            if len(submitted) == 2:
                queued.set()
        return future

    def apply(connection, publishes):
        applied.append(tuple(sorted(item.provisional.node_name for item in publishes)))
        return original_apply(connection, publishes)

    def hold_writer(connection):
        entered.set()
        assert release.wait(15)

    def publish(receiver):
        try:
            return storage.create_auto_id_job(
                node_name=receiver, params={'origin': receiver}, parent=None,
                producer_component=None, job_kind=None, expected_shape=snapshot.shape_json,
            )
        except RuntimeError as error:
            return error
        finally:
            storage.close_thread_connection()

    try:
        operation_id = _install_guard(storage)
        before_b = _receiver_publication_state(storage, tmp_path)
        blocker = storage.submit_db_mutation(hold_writer, priority=0, wait=False)
        assert entered.wait(15)
        monkeypatch.setattr(storage, 'submit_grouped_db_mutation', submit)
        monkeypatch.setattr(storage, '_apply_auto_job_publishes', apply)
        with ThreadPoolExecutor(max_workers=2) as executor:
            pending = {receiver: executor.submit(publish, receiver) for receiver in ('B', 'C')}
            assert queued.wait(15)
            release.set()
            blocker.result(timeout=15)
            outcomes = {receiver: future.result(timeout=15) for receiver, future in pending.items()}
        assert applied == [('B', 'C')]
        assert isinstance(outcomes['B'], RuntimeError)
        assert 'B' in str(outcomes['B']) and operation_id in str(outcomes['B'])
        assert _receiver_publication_state(storage, tmp_path) == before_b
        guard = storage.db_connection().execute(
            'SELECT operation_id FROM receiver_mutation_guards WHERE receiver_node=?', ('B',),
        ).fetchone()
        assert guard['operation_id'] == operation_id
        assert (outcomes['C'].node_name, outcomes['C'].job_id, outcomes['C'].params) == (
            'C', 1, {'origin': 'C'},
        )
        assert storage.list_job_ids('C') == [1]
        assert storage.next_job_id('C') == 2
        assert storage.input_file('C', 1).is_file()
    finally:
        release.set()
        _close(storage)


def test_held_receiver_guard_refuses_new_component_reservation(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    snapshot = workflow.topology.snapshot()
    storage.register_component_topology(snapshot)
    identity = process_identity(os.getpid())
    assert identity
    session_id = 'd' * 32
    try:
        operation_id = _install_guard(storage)
        storage.create_execution_session(
            session_id, session_kind='interrupt', command='run',
            start_component=('B',), selected_components=[('B',)], started_at=now(),
            hostname=socket.gethostname(), pid=os.getpid(), process_identity=identity,
            expected_shape=snapshot.shape_json,
        )
        with pytest.raises(RuntimeError) as caught:
            storage.reserve_execution_components(session_id, expected_shape=snapshot.shape_json)
        assert 'B' in str(caught.value) and operation_id in str(caught.value)
        assert storage.get_component_reservation(('B',)) is None
    finally:
        _close(storage)


def test_public_preparation_refuses_changed_later_component_before_first_unit_mutates(
    tmp_path, monkeypatch,
):
    from micro_workflow_manager.workflow import preparation as workflow_preparation

    workflow = _established_workflow(tmp_path)
    storage = workflow.storage
    before_a = _receiver_publication_state(storage, tmp_path, 'A')
    before_x = storage.get_component_state(('X',))
    before_b = _receiver_publication_state(storage, tmp_path)
    rows = _rows(storage)
    before_preparation_rows = {
        table: rows[table]
        for table in ('preparation_receipts', 'receiver_mutation_guards')
    }
    read_footprint = workflow_preparation.read_preparation_footprint
    changed = []

    def read_then_change_x(*args, **kwargs):
        footprint = read_footprint(*args, **kwargs)
        session_id, _components, shape = workflow.execution_session_context
        expected = storage.read_component_fresh_preparation(
            session_id, ('X',), expected_shape=shape,
        )
        changed.append(storage.complete_component_fresh_preparation(
            session_id, ('X',), expected_state=expected,
        ))
        return footprint

    monkeypatch.setattr(workflow_preparation, 'read_preparation_footprint', read_then_change_x)
    try:
        with pytest.raises(RuntimeError, match='changed'):
            workflow.run_concurrently(['A', 'X'])
        assert changed == [before_x['alignment_generation'] + 1]
        assert _receiver_publication_state(storage, tmp_path, 'A') == before_a
        assert storage.get_component_state(('X',)) == dict(
            before_x, lifecycle='queued', stability=None, instability_origin=None,
            misaligned=False, alignment_generation=before_x['alignment_generation'] + 1,
        )
        assert _receiver_publication_state(storage, tmp_path) == before_b
        rows = _rows(storage)
        assert {
            table: rows[table]
            for table in ('preparation_receipts', 'receiver_mutation_guards')
        } == before_preparation_rows
    finally:
        _close(storage)


def test_later_selected_failure_preserves_its_publications_after_first_producer_commits(
    tmp_path, monkeypatch,
):
    workflow = _established_workflow(tmp_path)
    storage = workflow.storage
    jobs = {storage.load_job('B', job_id).params['origin']: job_id
            for job_id in storage.list_job_ids('B')}
    before_a = storage.get_component_state(('A',))
    before_x = storage.get_component_state(('X',))
    before_b = storage.get_component_state(('B',))
    x_job = (
        storage.read_job_control('B', jobs['from-X']),
        storage.read_job_current_owner('B', jobs['from-X']),
        storage.read_job_events('B', jobs['from-X']),
        _files(storage.job_base_dir('B', jobs['from-X'])),
    )
    x_input = tmp_path / 'node' / 'B' / 'input' / 'X' / 'owned.txt'
    x_input_owner = storage.read_node_input_owner('B', 'X/owned.txt')
    output = _files(storage.node_output_dir('B'))
    x_job_dir = storage.job_base_dir('B', jobs['from-X'])
    preparation_trash = tmp_path / '.mwf' / 'preparation-trash'
    rename = Path.rename
    original = OSError('later selected component preparation failed')
    injected = False

    def fail_x_staging(path, target):
        nonlocal injected
        target = Path(target)
        if (not injected and path == x_job_dir
                and target.is_relative_to(preparation_trash)):
            injected = True
            raise original
        return rename(path, target)

    monkeypatch.setattr(Path, 'rename', fail_x_staging)
    try:
        with pytest.raises(OSError) as caught:
            workflow.run_concurrently(['A', 'X'])
        assert caught.value is original
        assert injected
        assert storage.get_component_state(('A',)) == dict(
            before_a, lifecycle='queued', stability=None, instability_origin=None,
            misaligned=False, alignment_generation=before_a['alignment_generation'] + 1,
        )
        assert storage.get_component_state(('X',)) == before_x
        assert storage.get_component_state(('B',)) == dict(before_b, misaligned=True)
        assert storage.read_node_input_owner('B', 'A/owned.txt') is None
        assert not (tmp_path / 'node' / 'B' / 'input' / 'A' / 'owned.txt').exists()
        assert not storage.job_exists('B', jobs['from-A'])
        assert storage.read_node_input_owner('B', 'X/owned.txt') == x_input_owner
        assert x_input.read_text() == 'from X'
        assert (
            storage.read_job_control('B', jobs['from-X']),
            storage.read_job_current_owner('B', jobs['from-X']),
            storage.read_job_events('B', jobs['from-X']),
            _files(storage.job_base_dir('B', jobs['from-X'])),
        ) == x_job
        assert _files(storage.node_output_dir('B')) == output
        cause, = storage.read_component_misalignment_causes(('B',))
        assert cause['preparation_kind'] == 'preparation-removal'
        assert cause['producer_node'] == 'A' and cause['producer_job_id'] == 1
    finally:
        _close(storage)
