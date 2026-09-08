from __future__ import annotations

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.component_identity import encode_component_key
from tests.test_090_component_session_settlement import _close


def _replace_done_result_with_sampled(storage, component):
    key = encode_component_key(component)

    def replace(connection):
        state = connection.execute(
            'SELECT lifecycle, stability, instability_origin, shape_id, alignment_generation, '
            'retained_result_shape_id, retained_result_alignment_generation '
            'FROM component_states WHERE component_key=?', (key,),
        ).fetchone()
        assert state is not None
        assert (state['lifecycle'], state['stability'], state['instability_origin']) == (
            'done', 'stable', None,
        )
        assert (
            state['retained_result_shape_id'], state['retained_result_alignment_generation'],
        ) == (state['shape_id'], state['alignment_generation'])
        result = connection.execute(
            "UPDATE component_successful_results SET lifecycle='sampled' "
            "WHERE component_key=? AND shape_id=? AND alignment_generation=? "
            "AND lifecycle='done' AND stability='stable' AND instability_origin IS NULL",
            (key, state['shape_id'], state['alignment_generation']),
        ).rowcount
        current = connection.execute(
            "UPDATE component_states SET lifecycle='sampled' "
            "WHERE component_key=? AND shape_id=? AND alignment_generation=? "
            "AND lifecycle='done' AND stability='stable' AND instability_origin IS NULL",
            (key, state['shape_id'], state['alignment_generation']),
        ).rowcount
        assert (result, current) == (1, 1)

    storage.submit_db_mutation(replace)


@pytest.mark.parametrize('operation', ['auto', 'explicit', 'prepared'])
@pytest.mark.parametrize('interruptions', [1, 3])
def test_interrupted_publication_wait_drains_the_writer_and_preserves_committed_input(tmp_path, monkeypatch, operation, interruptions):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from micro_workflow_manager.models import Job

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('B', 'A')])
    storage = workflow.storage

    @workflow.task('B')
    def consume(ctx, value):
        return value

    workflow.start('B', value='established result')
    entered, draining, release = Event(), Event(), Event()
    try:
        workflow.run()
        before = storage.get_component_state(('B',))
        job = Job(node_name='B', job_id=2, params={'value': 'committed despite interrupted wait'})
        if operation == 'prepared':
            storage.prepare_jobs_batch([job])

        def hold_writer(connection):
            entered.set()
            assert release.wait(15)

        blocker = storage.submit_db_mutation(hold_writer, priority=0, wait=False)
        assert entered.wait(15)
        method = 'submit_grouped_db_mutation' if operation == 'auto' else 'submit_db_mutation'
        original_submit = getattr(storage, method)
        errors = [KeyboardInterrupt(f'interrupted publication wait {number}') for number in range(interruptions)]
        raised = []

        def submit(*args, **kwargs):
            future = original_submit(*args, **kwargs)
            original_result = future.result

            def result(*result_args, **result_kwargs):
                if len(raised) < len(errors):
                    error = errors[len(raised)]
                    raised.append(error)
                    raise error
                draining.set()
                return original_result(*result_args, **result_kwargs)

            future.result = result
            return future

        monkeypatch.setattr(storage, method, submit)

        def publish():
            try:
                if operation == 'auto':
                    storage.create_auto_id_job(node_name='B', params=job.params, parent=None,
                                               producer_component=None, job_kind=None, idempotency_key='interrupted')
                elif operation == 'explicit':
                    storage.create_job(job, idempotency_key='interrupted')
                else:
                    storage.commit_prepared_job_resolving_idempotency(job, idempotency_key='interrupted')
            except KeyboardInterrupt as error:
                return error
            finally:
                storage.close_thread_connection()

        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(publish)
            assert draining.wait(15)
            assert not pending.done()
            release.set()
            blocker.result(timeout=15)
            assert pending.result(timeout=15) is errors[0]
        monkeypatch.setattr(storage, method, original_submit)
        assert raised == errors
        assert storage.load_job('B', 2).params == job.params
        assert storage.get_job_status('B', 2) == 'queued'
        assert storage.read_job_instance_id('B', 2) is not None
        assert storage.lookup_idempotent_job('B', 'interrupted').job_id == 2
        assert len([event for event in storage.read_job_events('B', 2) if event['event'] == 'created']) == 1
        assert storage.next_job_id('B') == 3
        assert storage.get_component_state(('B',)) == dict(before, misaligned=True)
        assert storage.read_component_misalignment_causes(('B',)) == [{
            'receiver_node': 'B', 'alignment_generation': before['alignment_generation'],
            'producer_node': None, 'producer_job_id': None,
            'arrival_kind': 'managed-job', 'job_id': 2,
        }]
    finally:
        release.set()
        _close(storage)


@pytest.mark.parametrize('first', ['managed-input', 'managed-job'])
def test_input_and_job_arrivals_share_the_same_first_cause(tmp_path, first):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    later = False

    @workflow.task('A')
    def produce(ctx):
        if later:
            actions = {
                'managed-input': lambda: ctx.node('B').write_input('first.txt', 'new input'),
                'managed-job': lambda: ctx.node('B').add(value='new job'),
            }
            actions[first]()
            cause = storage.read_component_misalignment_causes(('B',))
            actions['managed-job' if first == 'managed-input' else 'managed-input']()
            assert storage.read_component_misalignment_causes(('B',)) == cause

    @workflow.task('B')
    def consume(ctx, value):
        return value

    workflow.start('A')
    workflow.start('B', value='established result')
    try:
        workflow.run()
        before = storage.get_component_state(('B',))
        output = storage.output_file('B', 1).read_bytes()
        later = True
        workflow.run_node('A')
        assert storage.read_component_misalignment_causes(('B',)) == [{
            'receiver_node': 'B', 'alignment_generation': before['alignment_generation'],
            'producer_node': 'A', 'producer_job_id': 1, 'arrival_kind': first,
            **({'path': 'A/first.txt'} if first == 'managed-input' else {'job_id': 2}),
        }]
        assert storage.get_component_state(('B',)) == dict(before, misaligned=True)
        assert storage.load_job('B', 2).params == {'value': 'new job'}
        assert (tmp_path / 'node' / 'B' / 'input' / 'A' / 'first.txt').read_text() == 'new input'
        assert storage.output_file('B', 1).read_bytes() == output
    finally:
        _close(storage)


@pytest.mark.parametrize('receivers', [('B', 'B'), ('B', 'C')])
def test_process_arrivals_choose_one_durable_cause_per_receiver(tmp_path, receivers):
    import subprocess
    import sys
    import time

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('B', 'C'), ('C', 'B')])
    storage = workflow.storage

    @workflow.task('B')
    @workflow.task('C')
    def consume(ctx, value):
        return value

    for node in ('B', 'C'):
        workflow.start(node, value='established result')
    processes = []
    try:
        workflow.run()
        before = storage.get_component_state(('B', 'C'))
        child = '''
import sys
import time
from pathlib import Path
from micro_workflow_manager.storage import FileStorage
from tests.test_090_component_session_settlement import _close
root, node, marker = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
storage = FileStorage(root)
try:
    (root / marker).write_text('ready')
    deadline = time.monotonic() + 15
    while not (root / 'publish-go').exists():
        assert time.monotonic() < deadline
        time.sleep(0.01)
    job = storage.create_auto_id_job(node_name=node, params={'value': marker}, parent=None,
                                     producer_component=None, job_kind=None)
    assert storage.load_job(node, job.job_id).params == job.params
finally:
    _close(storage)
'''
        for position, receiver in enumerate(receivers):
            processes.append(subprocess.Popen(
                [sys.executable, '-c', child, str(tmp_path), receiver, f'ready-{position}'],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0),
            ))
        deadline = time.monotonic() + 15
        while not all((tmp_path / f'ready-{position}').exists() for position in range(2)):
            for process in processes:
                assert process.poll() is None, process.communicate(timeout=10)
            assert time.monotonic() < deadline
            time.sleep(0.01)
        # Both processes admit the unchanged native schema before this test
        # installs temporary guards. The guards remain active for both writes.
        storage.submit_db_mutation(lambda connection: connection.execute('''
            CREATE TRIGGER refuse_repeated_alignment BEFORE UPDATE OF misaligned ON component_states
            WHEN OLD.misaligned=1 BEGIN SELECT RAISE(ABORT, 'Repeated alignment update'); END
        '''))
        storage.submit_db_mutation(lambda connection: connection.execute('''
            CREATE TRIGGER refuse_repeated_cause BEFORE INSERT ON component_misalignment_causes
            WHEN EXISTS(SELECT 1 FROM component_misalignment_causes
                        WHERE receiver_node=NEW.receiver_node AND alignment_generation=NEW.alignment_generation)
            BEGIN SELECT RAISE(ABORT, 'Repeated cause insertion'); END
        '''))
        (tmp_path / 'publish-go').write_text('go')
        for process in processes:
            stdout, stderr = process.communicate(timeout=20)
            assert process.returncode == 0, stdout + stderr

        causes = storage.read_component_misalignment_causes(('B', 'C'))
        assert causes == [{
            'receiver_node': receiver, 'alignment_generation': before['alignment_generation'],
            'producer_node': None, 'producer_job_id': None,
            'arrival_kind': 'managed-job', 'job_id': 2,
        } for receiver in sorted(set(receivers))]
        assert storage.get_component_state(('B', 'C')) == dict(before, misaligned=True)
        for receiver in set(receivers):
            assert len(storage.list_job_ids(receiver)) == 1 + receivers.count(receiver)
            workflow.start(receiver, value='local continuation')
        assert storage.read_component_misalignment_causes(('B', 'C')) == causes
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=10)
        _close(storage)


@pytest.mark.parametrize('damage', [
    'missing', 'kind', 'job-zero', 'job-text', 'instance-case', 'instance-short',
    'path', 'missing-producer', 'different-producer', 'current-creator', 'shape',
])
def test_reopened_job_cause_reader_refuses_damaged_history_without_rewriting_it(tmp_path, damage):
    import sqlite3
    from micro_workflow_manager.storage import FileStorage
    from tests.test_090_component_session_settlement import _rows

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('B', 'A')])
    storage = workflow.storage

    @workflow.task('B')
    def consume(ctx, value):
        return value

    workflow.start('B', value='established result')
    try:
        workflow.run()
        receiver_owner = storage.read_job_current_owner('B', 1)
        workflow.start('B', value='late job')
        assert len(storage.read_component_misalignment_causes(('B',))) == 1
    finally:
        _close(storage)

    with sqlite3.connect(tmp_path / '.mwf' / 'state.sqlite3') as connection:
        connection.execute('PRAGMA foreign_keys=OFF')
        connection.execute('PRAGMA ignore_check_constraints=ON')
        if damage == 'missing':
            connection.execute('DELETE FROM component_misalignment_causes')
        elif damage == 'current-creator':
            connection.execute("UPDATE job_instances SET created_by_execution_id=? WHERE node_name='B' AND job_id=2",
                               (receiver_owner['execution_id'],))
        else:
            column, value = {
                'kind': ('arrival_kind', 'unknown'),
                'job-zero': ('receiver_job_id', 0),
                'job-text': ('receiver_job_id', 'invalid'),
                'instance-case': ('receiver_job_instance_id', 'A' * 32),
                'instance-short': ('receiver_job_instance_id', 'a' * 31),
                'path': ('relative_path', 'B/unexpected.txt'),
                'missing-producer': ('producer_execution_id', 'missing-producer'),
                'different-producer': ('producer_execution_id', receiver_owner['execution_id']),
                'shape': ('shape_id', 'missing-shape'),
            }[damage]
            connection.execute('UPDATE component_misalignment_causes SET ' + column + '=?', (value,))

    reopened = FileStorage(tmp_path)
    try:
        before = _rows(reopened)
        with pytest.raises(RuntimeError, match='[Mm]isalignment'):
            reopened.read_component_misalignment_causes(('B',))
        assert _rows(reopened) == before
    finally:
        _close(reopened)


def test_failed_auto_caller_cannot_remove_a_later_prepared_directory(tmp_path, monkeypatch):
    import sqlite3
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from micro_workflow_manager.models import Job

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('B', 'A')])
    storage = workflow.storage

    @workflow.task('B')
    def consume(ctx, value):
        return value

    workflow.start('B', value='established result')
    failed, release = Event(), Event()
    try:
        workflow.run()
        storage.submit_db_mutation(lambda connection: connection.execute('''
            CREATE TRIGGER refuse_auto_arrival BEFORE INSERT ON component_misalignment_causes
            BEGIN SELECT RAISE(ABORT, 'Auto publication failure'); END
        '''))
        original_submit = storage.submit_grouped_db_mutation
        delivered = []

        def submit(*args, **kwargs):
            future = original_submit(*args, **kwargs)
            original_result = future.result

            def result(*result_args, **result_kwargs):
                try:
                    return original_result(*result_args, **result_kwargs)
                except sqlite3.IntegrityError as error:
                    if not delivered:
                        delivered.append(error)
                        failed.set()
                        assert release.wait(15)
                    raise

            future.result = result
            return future

        monkeypatch.setattr(storage, 'submit_grouped_db_mutation', submit)

        def publish():
            try:
                workflow.start('B', value='failed auto input')
            except sqlite3.IntegrityError as error:
                return error
            finally:
                storage.close_thread_connection()

        prepared = Job(node_name='B', job_id=2, params={'value': 'new prepared input'})
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(publish)
            assert failed.wait(15)
            try:
                storage.prepare_jobs_batch([prepared])
                prepared_before_delivery = True
            except FileExistsError:
                prepared_before_delivery = False
            release.set()
            assert pending.result(timeout=15) is delivered[0]
        if not prepared_before_delivery:
            storage.prepare_jobs_batch([prepared])
        path = storage.input_file('B', 2)
        assert path.is_file(), 'Failed auto caller removed the newer prepared input'
        storage.submit_db_mutation(lambda connection: connection.execute('DROP TRIGGER refuse_auto_arrival'))
        assert storage.commit_prepared_job_resolving_idempotency(prepared) == (True, 2)
        assert storage.load_job('B', 2).params == prepared.params
    finally:
        release.set()
        _close(storage)


@pytest.mark.parametrize('receivers,fail_group', [(('B', 'B'), False), (('B', 'C'), False), (('B', 'C'), True)])
def test_grouped_arrivals_keep_one_cause_per_receiver_and_rollback_together(tmp_path, monkeypatch, receivers, fail_group):
    import sqlite3
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event, Lock
    from tests.test_090_component_session_settlement import _rows

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('B', 'C'), ('C', 'B')])
    storage = workflow.storage

    @workflow.task('B')
    @workflow.task('C')
    def consume(ctx, value):
        return value

    for node in ('B', 'C'):
        workflow.start(node, value='established result')
    release = Event()
    try:
        workflow.run()
        established = storage.get_component_state(('B', 'C'))
        storage.submit_db_mutation(lambda connection: connection.execute('''
            CREATE TRIGGER refuse_repeated_alignment BEFORE UPDATE OF misaligned ON component_states
            WHEN OLD.misaligned=1 BEGIN SELECT RAISE(ABORT, 'Repeated alignment update'); END
        '''))
        storage.submit_db_mutation(lambda connection: connection.execute('''
            CREATE TRIGGER refuse_repeated_cause BEFORE INSERT ON component_misalignment_causes
            WHEN EXISTS(SELECT 1 FROM component_misalignment_causes
                        WHERE receiver_node=NEW.receiver_node AND alignment_generation=NEW.alignment_generation)
            BEGIN SELECT RAISE(ABORT, 'Repeated cause insertion'); END
        '''))
        if fail_group:
            storage.submit_db_mutation(lambda connection: connection.execute('''
                CREATE TRIGGER refuse_group_receiver BEFORE INSERT ON component_misalignment_causes
                WHEN NEW.receiver_node='C' BEGIN SELECT RAISE(ABORT, 'Grouped receiver failure'); END
            '''))
        before = _rows(storage)
        files = {path.relative_to(tmp_path): path.read_bytes()
                 for path in (tmp_path / 'node').rglob('*') if path.is_file()}
        entered, queued, lock = Event(), Event(), Lock()
        submitted, applied = [], []
        original_submit = storage.submit_grouped_db_mutation

        def submit(group_key, item, operation, **options):
            def observe(connection, items):
                applied.append([item.provisional.node_name for item in items])
                return operation(connection, items)
            result = original_submit(group_key, item, observe, **options)
            with lock:
                submitted.append(item.provisional.node_name)
                if len(submitted) == 2:
                    queued.set()
            return result

        def hold_writer(connection):
            entered.set()
            assert release.wait(15), 'Grouped publication writer was not released'

        blocker = storage.submit_db_mutation(hold_writer, priority=0, wait=False)
        assert entered.wait(15)
        monkeypatch.setattr(storage, 'submit_grouped_db_mutation', submit)

        def publish(item):
            position, receiver = item
            try:
                return workflow.add_job(None, receiver, idempotency_key=f'route-{position}', value=position)
            except sqlite3.IntegrityError as error:
                return error
            finally:
                storage.close_thread_connection()

        with ThreadPoolExecutor(max_workers=2) as executor:
            pending = [executor.submit(publish, item) for item in enumerate(receivers)]
            assert queued.wait(15)
            release.set()
            blocker.result(timeout=15)
            outcomes = [future.result(timeout=15) for future in pending]
        assert len(applied) == 1 and sorted(applied[0]) == sorted(receivers)
        monkeypatch.setattr(storage, 'submit_grouped_db_mutation', original_submit)
        if fail_group:
            assert all(isinstance(value, sqlite3.IntegrityError) for value in outcomes)
            assert all('Grouped receiver failure' in str(value) for value in outcomes)
            assert _rows(storage) == before
            assert {path.relative_to(tmp_path): path.read_bytes()
                    for path in (tmp_path / 'node').rglob('*') if path.is_file()} == files
            storage.submit_db_mutation(lambda connection: connection.execute('DROP TRIGGER refuse_group_receiver'))
            outcomes = [workflow.add_job(None, receiver, value='retry') for receiver in receivers]
        else:
            after = _rows(storage)
            for position, receiver in enumerate(receivers):
                reused = workflow.add_job(None, receiver, idempotency_key=f'route-{position}', value='ignored')
                assert reused.job_id == outcomes[position].job_id
                assert reused.params == outcomes[position].params
            assert _rows(storage) == after

        causes = storage.read_component_misalignment_causes(('B', 'C'))
        assert causes == [{
            'receiver_node': receiver, 'alignment_generation': established['alignment_generation'],
            'producer_node': None, 'producer_job_id': None,
            'arrival_kind': 'managed-job', 'job_id': 2,
        } for receiver in sorted(set(receivers))]
        assert storage.get_component_state(('B', 'C')) == dict(established, misaligned=True)
        for receiver in receivers:
            workflow.add_job(None, receiver, value='later arrival')
        assert storage.read_component_misalignment_causes(('B', 'C')) == causes
    finally:
        release.set()
        _close(storage)


@pytest.mark.parametrize('operation', ['auto', 'explicit'])
def test_single_publication_preserves_another_prepared_job(tmp_path, operation):
    from micro_workflow_manager.models import Job
    from tests.test_090_component_session_settlement import _rows

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('B', 'A')])
    storage = workflow.storage

    @workflow.task('B')
    def consume(ctx, value):
        return value

    workflow.start('B', value='established result')
    try:
        workflow.run()
        prepared = Job(node_name='B', job_id=2, params={'value': 'prepared first'})
        storage.prepare_jobs_batch([prepared])
        before = _rows(storage)
        payload = storage.input_file('B', 2).read_bytes()
        with pytest.raises(FileExistsError):
            workflow.add_job(None, 'B', job_id=2 if operation == 'explicit' else None,
                             value='competing single job')
        assert _rows(storage) == before
        assert storage.input_file('B', 2).read_bytes() == payload
        assert storage.commit_prepared_job_resolving_idempotency(prepared) == (True, 2)
        assert storage.load_job('B', 2).params == prepared.params
        assert len(storage.read_component_misalignment_causes(('B',))) == 1
    finally:
        _close(storage)


@pytest.mark.parametrize('other_state', ['committed', 'prepared'])
@pytest.mark.parametrize('batch_size', [2, 8])
def test_failed_batch_preserves_another_publishers_directory(tmp_path, other_state, batch_size):
    from micro_workflow_manager.models import Job
    from tests.test_090_component_session_settlement import _rows

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('B', 'A')])
    storage = workflow.storage

    @workflow.task('B')
    def consume(ctx, value):
        return value

    workflow.start('B', value='established result')
    try:
        workflow.run()
        other = Job(node_name='B', job_id=2, params={'value': 'other publisher'})
        if other_state == 'committed':
            storage.create_job(other, idempotency_key='other publisher')
        else:
            storage.prepare_jobs_batch([other])
        before = _rows(storage)
        files = {path.relative_to(tmp_path): path.read_bytes()
                 for path in (tmp_path / 'node').rglob('*') if path.is_file()}
        jobs = [Job(node_name='B', job_id=job_id, params={'value': 'losing publisher'})
                for job_id in [*range(3, batch_size + 2), 2]]

        with pytest.raises(FileExistsError):
            storage.create_jobs_batch(jobs)

        assert _rows(storage) == before
        assert {path.relative_to(tmp_path): path.read_bytes()
                for path in (tmp_path / 'node').rglob('*') if path.is_file()} == files
        if other_state == 'committed':
            assert storage.lookup_idempotent_job('B', 'other publisher').params == other.params
        else:
            assert storage.commit_prepared_job_resolving_idempotency(other) == (True, 2)
        assert storage.load_job('B', 2).params == other.params
    finally:
        _close(storage)


def test_competing_storage_publishers_keep_the_winners_input(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier, Event, Lock
    from micro_workflow_manager.models import Job

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('B', 'A')])
    storage = workflow.storage

    @workflow.task('B')
    def consume(ctx, value):
        return value

    workflow.start('B', value='established result')
    try:
        workflow.run()
        control = storage.read_job_control('B', 1)
        output = storage.output_file('B', 1).read_bytes()
        exists, write = storage.job_exists, storage.atomic_write_json
        barrier, lock = Barrier(2), Lock()
        first_written, second_written, first_finished = Event(), Event(), Event()
        observations = []

        def concurrent_exists(node_name, job_id):
            result = exists(node_name, job_id)
            with lock:
                pause = node_name == 'B' and job_id == 2 and not result and len(observations) < 2
                if pause:
                    observations.append(job_id)
            if pause:
                barrier.wait(timeout=10)
            return result

        def overlapping_write(path, data):
            # Force overlap if a publisher writes the final payload before SQL
            # chooses a winner. Staged publication never enters this branch.
            if path == tmp_path / 'node' / 'B' / 'jobs' / '2' / 'input.json':
                if data['value'] == 'first':
                    write(path, data)
                    first_written.set()
                    assert second_written.wait(10)
                else:
                    assert first_written.wait(10)
                    write(path, data)
                    second_written.set()
                    assert first_finished.wait(10)
            else:
                write(path, data)

        monkeypatch.setattr(storage, 'job_exists', concurrent_exists)
        monkeypatch.setattr(storage, 'atomic_write_json', overlapping_write)

        def publish(value):
            try:
                return storage.create_job(Job(node_name='B', job_id=2, params={'value': value}))
            except ValueError as error:
                return error
            finally:
                if value == 'first':
                    first_finished.set()
                storage.close_thread_connection()

        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = list(executor.map(publish, ['first', 'second']))

        winners = [value for value in outcomes if isinstance(value, Job)]
        errors = [value for value in outcomes if isinstance(value, ValueError)]
        assert len(winners) == len(errors) == 1
        assert 'already exists' in str(errors[0])
        assert storage.load_job('B', 2).params == winners[0].params
        assert len([event for event in storage.read_job_events('B', 2) if event['event'] == 'created']) == 1
        assert len(storage.read_component_misalignment_causes(('B',))) == 1
        assert storage.read_job_control('B', 1) == control
        assert storage.output_file('B', 1).read_bytes() == output
    finally:
        _close(storage)


def test_simultaneous_explicit_requests_with_same_key_return_one_job(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier, Lock

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('B', 'A')])
    storage = workflow.storage

    @workflow.task('B')
    def consume(ctx, value):
        return value

    workflow.start('B', value='established result')
    try:
        workflow.run()
        before = storage.get_component_state(('B',))
        control = storage.read_job_control('B', 1)
        output = storage.output_file('B', 1).read_bytes()
        lookup = storage.lookup_idempotent_job
        barrier, lock = Barrier(2), Lock()
        observations = []

        def concurrent_lookup(node_name, key):
            result = lookup(node_name, key)
            with lock:
                pause = result is None and len(observations) < 2
                if pause:
                    observations.append(key)
            if pause:
                barrier.wait(timeout=10)
            return result

        monkeypatch.setattr(storage, 'lookup_idempotent_job', concurrent_lookup)

        def publish(value):
            try:
                job = workflow.add_job(None, 'B', job_id=2, idempotency_key='shared', value=value)
                return job, storage.read_job_instance_id('B', job.job_id)
            finally:
                storage.close_thread_connection()

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(publish, ['first', 'second']))

        assert observations == ['shared', 'shared']
        assert [job.job_id for job, _ in results] == [2, 2]
        assert results[0][1] == results[1][1] and results[0][1] is not None
        assert results[0][0].params == results[1][0].params == storage.load_job('B', 2).params
        assert storage.list_job_ids('B') == [1, 2]
        assert len([event for event in storage.read_job_events('B', 2) if event['event'] == 'created']) == 1
        assert storage.get_component_state(('B',)) == dict(before, misaligned=True)
        assert storage.read_component_misalignment_causes(('B',)) == [{
            'receiver_node': 'B', 'alignment_generation': before['alignment_generation'],
            'producer_node': None, 'producer_job_id': None,
            'arrival_kind': 'managed-job', 'job_id': 2,
        }]
        assert storage.read_job_control('B', 1) == control
        assert storage.output_file('B', 1).read_bytes() == output
    finally:
        _close(storage)


@pytest.mark.parametrize('operation', ['auto', 'explicit', 'batch'])
def test_late_routed_job_marks_completed_receiver_without_replacing_its_result(tmp_path, operation):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    publish_later = False
    created, producing = [], []

    @workflow.task('A')
    def produce(ctx):
        if publish_later:
            producing.append(storage.read_job_current_owner('A', 1))
            if operation == 'batch':
                created.extend(ctx.node('B').add_many([{'value': 'new work'}, {'value': 'more work'}]))
            elif operation == 'explicit':
                created.append(ctx.node('B').add(job_id=2, value='new work'))
            else:
                created.append(ctx.node('B').add(value='new work'))

    @workflow.task('B')
    def consume(ctx, value):
        return value

    workflow.start('A')
    workflow.start('B', value='established result')
    try:
        workflow.run()
        before = storage.get_component_state(('B',))
        control = storage.read_job_control('B', 1)
        events = storage.read_job_events('B', 1)
        owner = storage.read_job_current_owner('B', 1)
        output = storage.output_file('B', 1).read_bytes()
        publish_later = True

        workflow.run_node('A')

        assert [(job.node_name, job.job_id) for job in created] == (
            [('B', 2), ('B', 3)] if operation == 'batch' else [('B', 2)]
        )
        assert storage.get_job_status('B', 2) == 'queued'
        assert storage.read_job_current_owner('B', 2) is None
        assert storage.load_job('B', 2).params == {'value': 'new work'}
        assert storage.get_component_state(('B',)) == dict(before, misaligned=True)
        assert storage.read_component_misalignment_causes(('B',)) == [{
            'receiver_node': 'B', 'alignment_generation': before['alignment_generation'],
            'producer_node': 'A', 'producer_job_id': 1,
            'arrival_kind': 'managed-job', 'job_id': 2,
        }]
        instance = storage.db_connection().execute(
            "SELECT created_by_execution_id FROM job_instances WHERE node_name='B' AND job_id=2",
        ).fetchone()
        assert instance['created_by_execution_id'] == producing[0]['execution_id']
        assert storage.read_job_control('B', 1) == control
        assert storage.read_job_events('B', 1) == events
        assert storage.read_job_current_owner('B', 1) == owner
        assert storage.output_file('B', 1).read_bytes() == output
        assert storage.get_component_reservation(('B',)) is None
    finally:
        _close(storage)


@pytest.mark.parametrize('operation,failure', [
    ('auto', 'state'), ('auto', 'cause'), ('auto', 'event'), ('auto', 'key'), ('auto', 'sequence'),
    ('explicit', 'state'), ('explicit', 'cause'), ('explicit', 'event'), ('explicit', 'key'), ('explicit', 'sequence'),
    ('batch', 'state'), ('batch', 'cause'), ('batch', 'event'), ('batch', 'key'),
    ('defaults', 'state'), ('defaults', 'cause'), ('defaults', 'event'), ('defaults', 'sequence'),
])
def test_failed_job_arrival_rolls_back_publication_and_restores_input_files(tmp_path, operation, failure):
    import sqlite3
    from tests.test_090_component_session_settlement import _rows

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('B', 'A')])
    storage = workflow.storage

    @workflow.task('B')
    def consume(ctx, value):
        return value

    workflow.start('B', value='established result')
    try:
        workflow.run()
        trigger = {
            'state': 'UPDATE OF misaligned ON component_states',
            'cause': 'INSERT ON component_misalignment_causes',
            'event': 'INSERT ON job_events',
            'key': 'INSERT ON idempotency',
            'sequence': 'UPDATE ON job_sequences',
        }[failure]
        storage.submit_db_mutation(lambda connection: connection.execute(
            'CREATE TRIGGER refuse_job_publication BEFORE ' + trigger +
            " BEGIN SELECT RAISE(ABORT, 'injected job publication failure'); END"
        ))
        before = _rows(storage)
        if operation == 'batch':
            # Batch IDs are reserved before payload preparation; those addresses
            # remain consumed even when the subsequent publication rolls back.
            for row in before['job_sequences']:
                if row['node_name'] == 'B':
                    row['next_job_id'] += 2
        files = {path.relative_to(tmp_path): path.read_bytes()
                 for path in (tmp_path / 'node').rglob('*') if path.is_file()}
        try:
            with pytest.raises(sqlite3.IntegrityError, match='injected job publication failure'):
                if operation == 'auto':
                    workflow.start('B', idempotency_key='new-job', value='new work')
                elif operation == 'explicit':
                    workflow.start('B', job_id=2, idempotency_key='new-job', value='new work')
                elif operation == 'batch':
                    workflow.add_jobs(None, 'B', [{'value': 'new work'}, {'value': 'more work'}],
                                      idempotency_keys=['new-job', 'more-work'])
                else:
                    workflow.create_jobs('B', start_job_id=2, number=2, params={'value': 'new work'})
            assert _rows(storage) == before
            assert {path.relative_to(tmp_path): path.read_bytes()
                    for path in (tmp_path / 'node').rglob('*') if path.is_file()} == files
            assert storage.read_component_misalignment_causes(('B',)) == []
        finally:
            storage.submit_db_mutation(lambda connection: connection.execute('DROP TRIGGER refuse_job_publication'))
        retry = workflow.start('B', value='accepted retry')
        assert storage.read_component_misalignment_causes(('B',))[0]['job_id'] == retry.job_id
    finally:
        _close(storage)


@pytest.mark.parametrize('communicating', [False, True])
def test_initial_arrival_does_not_hide_first_later_arrival_at_established_result(tmp_path, communicating):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A')] if communicating else [('A', 'B')])
    storage = workflow.storage
    members = ('A', 'B') if communicating else ('B',)
    observed = []

    @workflow.task('A')
    def produce(ctx):
        before = storage.get_component_state(members)
        assert before['lifecycle'] == ('running' if communicating else 'queued')
        ctx.node('B').add()
        assert storage.get_component_state(members) == before
        assert storage.read_component_misalignment_causes(members) == []
        observed.append(before['alignment_generation'])

    @workflow.task('B')
    def consume(ctx):
        return 'result'

    workflow.start('A')
    try:
        workflow.run()
        established = storage.get_component_state(members)
        assert established['lifecycle'] == 'done'
        assert established['alignment_generation'] == observed[0]
        workflow.start('B')
        assert storage.get_component_state(members) == dict(established, misaligned=True)
        assert storage.read_component_misalignment_causes(members) == [{
            'receiver_node': 'B', 'alignment_generation': established['alignment_generation'],
            'producer_node': None, 'producer_job_id': None,
            'arrival_kind': 'managed-job', 'job_id': 2,
        }]
    finally:
        _close(storage)


@pytest.mark.parametrize('lifecycle', ['sampled', 'failed'])
def test_job_arrival_keeps_terminal_result_and_full_preparation_repairs_alignment(tmp_path, lifecycle):
    from micro_workflow_manager.errors import JobFailedError

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('B', 'A')])
    storage = workflow.storage
    fail = lifecycle == 'failed'

    @workflow.task('B')
    def consume(ctx):
        if fail:
            raise ValueError('receiver failure')
        return 'result'

    workflow.start('B')
    try:
        if fail:
            with pytest.raises(JobFailedError) as caught:
                workflow.run_node('B')
            assert isinstance(caught.value.__cause__, ValueError)
            assert str(caught.value.__cause__) == 'receiver failure'
        else:
            workflow.run_node('B')
            _replace_done_result_with_sampled(storage, ('B',))
        before = storage.get_component_state(('B',))
        control = storage.read_job_control('B', 1)
        output = storage.output_file('B', 1).read_bytes()
        workflow.start('B')
        assert storage.get_component_state(('B',)) == dict(before, misaligned=True)
        first = [{
            'receiver_node': 'B', 'alignment_generation': before['alignment_generation'],
            'producer_node': None, 'producer_job_id': None,
            'arrival_kind': 'managed-job', 'job_id': 2,
        }]
        assert storage.read_component_misalignment_causes(('B',)) == first
        assert storage.read_job_control('B', 1) == control
        assert storage.output_file('B', 1).read_bytes() == output
        fail = False
        workflow.run_node('B')
        repaired = storage.get_component_state(('B',))
        assert repaired['lifecycle'] == 'done' and not repaired['misaligned']
        assert repaired['alignment_generation'] == before['alignment_generation'] + 1
        assert storage.read_component_misalignment_causes(('B',)) == []
        workflow.start('B')
        assert storage.get_component_state(('B',)) == dict(repaired, misaligned=True)
        assert storage.read_component_misalignment_causes(('B',)) == [dict(
            first[0], alignment_generation=repaired['alignment_generation'], job_id=3,
        )]
    finally:
        _close(storage)


def test_mixed_batch_records_first_new_job_and_retains_its_cause_after_reuse(tmp_path):
    from micro_workflow_manager.storage import FileStorage
    from tests.test_090_component_session_settlement import _rows

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    phase = 'idle'
    returned = []

    @workflow.task('A')
    def produce(ctx):
        if phase == 'reuse':
            returned.extend(ctx.node('B').add_many([{'value': 'ignored'}], idempotency_keys=['keep']))
        elif phase == 'mixed':
            returned.extend(ctx.node('B').add_many(
                [{'value': 'ignored'}, {'value': 'new work'}, {'value': 'more work'}],
                idempotency_keys=['keep', 'new', 'more'],
            ))

    @workflow.task('B')
    def consume(ctx, value):
        return value

    workflow.start('A')
    workflow.add_job(None, 'B', value='established result', idempotency_key='keep')
    try:
        workflow.run()
        before = storage.get_component_state(('B',))
        phase = 'reuse'
        workflow.run_job('A', 1, ignore_readiness=True)
        assert [job.job_id for job in returned] == [1]
        assert returned[0].params == {'value': 'established result'}
        assert storage.get_component_state(('B',)) == before
        assert storage.read_component_misalignment_causes(('B',)) == []

        returned.clear()
        phase = 'mixed'
        workflow.run_job('A', 1, ignore_readiness=True)
        assert [job.job_id for job in returned] == [1, 2, 3]
        expected = [{
            'receiver_node': 'B', 'alignment_generation': before['alignment_generation'],
            'producer_node': 'A', 'producer_job_id': 1,
            'arrival_kind': 'managed-job', 'job_id': 2,
        }]
        assert storage.get_component_state(('B',)) == dict(before, misaligned=True)
        assert storage.read_component_misalignment_causes(('B',)) == expected
        old_instance = storage.read_job_instance_id('B', 2)
        state = _rows(storage)
        reused = workflow.add_jobs(None, 'B', [{'value': 'ignored'}, {'value': 'ignored'}],
                                   idempotency_keys=['new', 'more'])
        assert [job.job_id for job in reused] == [2, 3]
        assert _rows(storage) == state
        storage.clear_job_events('A')
        storage.clear_job_events('B')
        storage.delete_job('B', 2)
        workflow.start('B', job_id=2, value='replacement from project')
        assert storage.read_job_instance_id('B', 2) != old_instance
        assert storage.read_component_misalignment_causes(('B',)) == expected
        stored = storage.db_connection().execute(
            "SELECT receiver_job_instance_id FROM component_misalignment_causes WHERE receiver_node='B'",
        ).fetchone()
        assert stored['receiver_job_instance_id'] == old_instance
        _close(storage)
        reopened = FileStorage(tmp_path)
        try:
            assert reopened.read_component_misalignment_causes(('B',)) == expected
        finally:
            _close(reopened)
    finally:
        _close(storage)


@pytest.mark.parametrize('operation', ['auto', 'explicit', 'batch', 'defaults'])
def test_publication_keeps_original_error_when_cleanup_cannot_read_commit_state(tmp_path, monkeypatch, operation):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('B', 'A')])
    storage = workflow.storage

    @workflow.task('B')
    def consume(ctx, value):
        return value

    workflow.start('B', value='established result')
    failure = RuntimeError('injected original job notification failure')
    notified = []
    original_notify = storage.notify_state_change
    original_connect = storage._new_db_connection

    def fail_after_job_commit():
        if not notified and storage.job_exists('B', 2):
            notified.append(True)
            raise failure
        return original_notify()

    def refuse_cleanup_connection():
        if notified:
            raise OSError('injected cleanup connection failure')
        return original_connect()

    try:
        workflow.run()
        with monkeypatch.context() as patch:
            patch.setattr(storage, 'notify_state_change', fail_after_job_commit)
            patch.setattr(storage, '_new_db_connection', refuse_cleanup_connection)
            with pytest.raises(RuntimeError) as caught:
                if operation == 'auto':
                    workflow.start('B', value='new work')
                elif operation == 'explicit':
                    workflow.start('B', job_id=2, value='new work')
                elif operation == 'batch':
                    workflow.add_jobs(None, 'B', [{'value': 'new work'}])
                else:
                    workflow.create_jobs('B', start_job_id=2, params={'value': 'new work'})
        assert caught.value is failure
        assert any('injected cleanup connection failure' in note for note in failure.__notes__)
        assert storage.load_job('B', 2).params == {'value': 'new work'}
        assert storage.read_component_misalignment_causes(('B',))[0]['job_id'] == 2
    finally:
        _close(storage)


@pytest.mark.parametrize('operation', ['auto', 'explicit', 'batch', 'defaults'])
def test_job_arrival_refuses_changed_current_shape_before_mutation(tmp_path, operation):
    from tests.test_090_component_session_settlement import _rows

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('B', 'A')])
    storage = workflow.storage

    @workflow.task('B')
    def consume(ctx, value):
        return value

    workflow.start('B', value='established result')
    try:
        workflow.run()
        # The original B -> A topology has singleton components. Adding the
        # reverse edge changes B's actual component to {A, B}; an unrelated
        # global DAG change would correctly retain B's singleton membership.
        workflow.graph([('A', 'B')])
        before = _rows(storage)
        files = {path.relative_to(tmp_path): path.read_bytes()
                 for path in (tmp_path / 'node').rglob('*') if path.is_file()}

        with pytest.raises(RuntimeError, match='membership|preparation'):
            if operation == 'auto':
                workflow.start('B', value='new work')
            elif operation == 'explicit':
                workflow.start('B', job_id=2, value='new work')
            elif operation == 'batch':
                workflow.add_jobs(None, 'B', [{'value': 'new work'}, {'value': 'more work'}])
            else:
                workflow.create_jobs('B', start_job_id=2, number=2, params={'value': 'new work'})

        assert _rows(storage) == before
        assert {path.relative_to(tmp_path): path.read_bytes()
                for path in (tmp_path / 'node').rglob('*') if path.is_file()} == files
    finally:
        _close(storage)


@pytest.mark.parametrize('operation', ['auto', 'explicit', 'batch', 'defaults', 'prepared'])
def test_notification_failure_preserves_committed_jobs_and_their_input_files(tmp_path, monkeypatch, operation):
    import json
    from micro_workflow_manager.models import Job

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('B', 'A')])
    storage = workflow.storage

    @workflow.task('B')
    def consume(ctx, value):
        return value

    workflow.start('B', value='established result')
    try:
        workflow.run()
        before = storage.get_component_state(('B',))
        control = storage.read_job_control('B', 1)
        output = storage.output_file('B', 1).read_bytes()
        notified = []
        original = storage.notify_state_change

        def fail_after_job_commit():
            if not notified and storage.job_exists('B', 2):
                notified.append(True)
                raise RuntimeError('injected notification after job commit')
            return original()

        with monkeypatch.context() as patch:
            patch.setattr(storage, 'notify_state_change', fail_after_job_commit)
            with pytest.raises(RuntimeError, match='injected notification after job commit'):
                if operation == 'auto':
                    workflow.start('B', value='new work')
                elif operation == 'explicit':
                    workflow.start('B', job_id=2, idempotency_key='late-explicit-job', value='new work')
                elif operation == 'batch':
                    workflow.add_jobs(None, 'B', [{'value': 'new work'}, {'value': 'new work'}])
                elif operation == 'defaults':
                    workflow.create_jobs('B', start_job_id=2, number=2, params={'value': 'new work'})
                else:
                    job = Job(node_name='B', job_id=2, params={'value': 'new work'})
                    storage.prepare_jobs_batch([job])
                    storage.commit_prepared_job_resolving_idempotency(job)

        assert notified == [True]
        ids = [2, 3] if operation in ('batch', 'defaults') else [2]
        for job_id in ids:
            assert storage.job_exists('B', job_id)
            path = tmp_path / 'node' / 'B' / 'jobs' / str(job_id) / 'input.json'
            assert path.is_file(), 'Committed job input was removed after notification failed'
            assert json.loads(path.read_text(encoding='utf-8')) == {'value': 'new work'}
            assert storage.read_job_instance_id('B', job_id) is not None
            assert storage.get_job_status('B', job_id) == 'queued'
            assert len([event for event in storage.read_job_events('B', job_id)
                        if event['event'] == 'created']) == 1
        assert storage.next_job_id('B') == max(ids) + 1
        if operation == 'explicit':
            reused = storage.lookup_idempotent_job('B', 'late-explicit-job')
            assert reused is not None and reused.job_id == 2
        assert storage.get_component_state(('B',)) == dict(before, misaligned=True)
        assert storage.read_component_misalignment_causes(('B',)) == [{
            'receiver_node': 'B', 'alignment_generation': before['alignment_generation'],
            'producer_node': None, 'producer_job_id': None,
            'arrival_kind': 'managed-job', 'job_id': 2,
        }]
        assert storage.read_job_control('B', 1) == control
        assert storage.output_file('B', 1).read_bytes() == output
    finally:
        _close(storage)


@pytest.mark.parametrize('operation', ['start', 'explicit', 'batch', 'defaults', 'prepared', 'ensure'])
def test_project_created_jobs_record_unknown_producer_at_completed_receiver(tmp_path, operation):
    from micro_workflow_manager.models import Job

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('B', 'A')])
    storage = workflow.storage

    @workflow.task('B')
    def consume(ctx, value):
        return value

    workflow.start('B', value='established result')
    try:
        workflow.run()
        before = storage.get_component_state(('B',))
        control = storage.read_job_control('B', 1)
        output = storage.output_file('B', 1).read_bytes()
        if operation == 'start':
            workflow.start('B', value='new work')
        elif operation == 'explicit':
            workflow.add_job(None, 'B', job_id=2, value='new work')
        elif operation == 'batch':
            workflow.add_jobs(None, 'B', [{'value': 'new work'}, {'value': 'more work'}])
        elif operation == 'defaults':
            workflow.create_jobs('B', start_job_id=2, number=2, params={'value': 'new work'})
        else:
            job = Job(node_name='B', job_id=2, params={'value': 'new work'})
            if operation == 'ensure':
                storage.ensure_job(job)
            else:
                storage.prepare_jobs_batch([job])
                assert storage.commit_prepared_job_resolving_idempotency(job) == (True, 2)

        assert storage.get_job_status('B', 2) == 'queued'
        assert storage.load_job('B', 2).params == {'value': 'new work'}
        assert storage.read_job_current_owner('B', 2) is None
        assert storage.get_component_state(('B',)) == dict(before, misaligned=True)
        assert storage.read_component_misalignment_causes(('B',)) == [{
            'receiver_node': 'B', 'alignment_generation': before['alignment_generation'],
            'producer_node': None, 'producer_job_id': None,
            'arrival_kind': 'managed-job', 'job_id': 2,
        }]
        assert storage.read_job_control('B', 1) == control
        assert storage.output_file('B', 1).read_bytes() == output
        assert storage.get_component_reservation(('B',)) is None
    finally:
        _close(storage)


@pytest.mark.parametrize('operation', ['start', 'add_job'])
def test_task_can_create_outside_component_job_without_direct_edge(tmp_path, operation):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('B', 'A')])
    storage = workflow.storage
    publish_later = False
    created, creators = [], []

    @workflow.task('A')
    def produce(ctx):
        if publish_later:
            creators.append(ctx.execution_id)
            if operation == 'start':
                created.append(workflow.start('B', value='new work'))
            else:
                created.append(workflow.add_job(None, 'B', value='new work'))

    @workflow.task('B')
    def consume(ctx, value):
        return value

    workflow.start('A')
    workflow.start('B', value='established result')
    try:
        workflow.run()
        before = storage.get_component_state(('B',))
        control = storage.read_job_control('B', 1)
        events = storage.read_job_events('B', 1)
        owner = storage.read_job_current_owner('B', 1)
        output = storage.output_file('B', 1).read_bytes()
        publish_later = True

        workflow.run_node('A')

        assert len(created) == len(creators) == 1
        job, = created
        assert (job.node_name, job.job_id, job.parent) == ('B', 2, None)
        assert storage.get_job_status('B', 2) == 'queued'
        assert storage.read_job_current_owner('B', 2) is None
        assert storage.load_job('B', 2).params == {'value': 'new work'}
        assert job.producer_component == ('A',) and job.job_kind == 'dag'
        instance = storage.db_connection().execute(
            "SELECT created_by_execution_id FROM job_instances WHERE node_name='B' AND job_id=2",
        ).fetchone()
        assert instance['created_by_execution_id'] == creators[0]
        after = storage.get_component_state(('B',))
        assert {key: value for key, value in after.items() if key != 'misaligned'} == {
            key: value for key, value in before.items() if key != 'misaligned'
        }
        assert after['misaligned']
        assert storage.read_component_misalignment_causes(('B',)) == [{
            'receiver_node': 'B', 'alignment_generation': before['alignment_generation'],
            'producer_node': 'A', 'producer_job_id': 1,
            'arrival_kind': 'managed-job', 'job_id': 2,
        }]
        assert storage.read_job_control('B', 1) == control
        assert storage.read_job_events('B', 1) == events
        assert storage.read_job_current_owner('B', 1) == owner
        assert storage.output_file('B', 1).read_bytes() == output
        assert storage.get_component_reservation(('B',)) is None
    finally:
        _close(storage)
