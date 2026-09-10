from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Event, Thread

import pytest

from micro_workflow_manager import MicroWorkflow, NodeRouter, cli
from micro_workflow_manager.models import Job, now
from micro_workflow_manager.processes import process_identity
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage import execution_restart
from micro_workflow_manager.storage.restart_receipts import (
    require_restart_pre_state,
    validate_restart_manifest,
    validate_restart_terminal_decision,
)


def _close(storage):
    storage.db_mutation_barrier()
    deadline = time.monotonic() + 10
    while storage.mutation_writer_diagnostics()['writer_alive']:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    storage.close_database_connections()


def _workflow(tmp_path, monkeypatch, *, prior_owner=False):
    monkeypatch.chdir(tmp_path)
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A'), ('B', 'C')])
    storage = workflow.storage
    snapshot = workflow.topology.snapshot()
    storage.register_component_topology(snapshot)
    for node in ('A', 'B'):
        storage.create_job(Job(node_name=node, job_id=1, params={'value': node}))
    if prior_owner:
        storage.create_job(Job(node_name='A', job_id=2, params={'value': 'old owner'}))
        storage.create_execution_session(
            'earlier-interrupt', session_kind='interrupt', command='run',
            start_component=('A', 'B'), selected_components=[('A', 'B')],
            started_at=now(), hostname=socket.gethostname(), pid=os.getpid(),
            process_identity=process_identity(os.getpid()), expected_shape=snapshot.shape_json,
        )
        storage.reserve_execution_components('earlier-interrupt', expected_shape=snapshot.shape_json)
        earlier = storage.claim_job_execution(
            'A', 2, started_at=now(), session_id='earlier-interrupt', component=('A', 'B'),
        )
        storage.finalize_job_execution('A', 2, *earlier, 'failed')
        assert storage.get_job_status('A', 2) == 'failed'
        assert storage.read_job_control('A', 2)['active_execution_id'] is None
        storage.output_file('A', 2).write_bytes(b'earlier output\n')
        assert storage.finish_execution_session(
            'earlier-interrupt', outcome='failed', finished_at=now(),
        ) is True
        storage.release_execution_components('earlier-interrupt')
    for session_id, kind, component in [
        ('unrelated-main', 'main', ('C',)), ('job-interrupt', 'interrupt', ('A', 'B')),
    ]:
        storage.create_execution_session(
            session_id, session_kind=kind, command='run', start_component=component,
            selected_components=[component], started_at=now(), hostname=socket.gethostname(),
            pid=os.getpid(), process_identity=process_identity(os.getpid()),
            expected_shape=snapshot.shape_json,
        )
        storage.reserve_execution_components(session_id, expected_shape=snapshot.shape_json)
        if session_id == 'job-interrupt':
            state = storage.get_component_state(component)
            assert state['lifecycle'] == 'queued'
            assert storage.begin_queued_component_execution(
                session_id, component, expected_shape=snapshot.shape_json,
                expected_alignment_generation=state['alignment_generation'],
                successful_lineage=('stable', None), expected_parent_states={},
            ) is True
    try:
        yield workflow
    finally:
        _close(storage)


@pytest.fixture
def workflow(tmp_path, monkeypatch):
    yield from _workflow(tmp_path, monkeypatch)


@pytest.fixture
def workflow_with_prior_owner(tmp_path, monkeypatch):
    yield from _workflow(tmp_path, monkeypatch, prior_owner=True)


@pytest.mark.parametrize('entry', ['run_jobs', 'run_node_jobs'])
@pytest.mark.parametrize('runner', ['direct', 'threaded'])
def test_finite_output_failure_keeps_accepted_restart(tmp_path, monkeypatch, entry, runner):
    workflow = MicroWorkflow(tmp_path, runner=runner, persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    router = NodeRouter('A', runner=runner, max_threads=1)
    calls, result = [], {}
    at_decision, release_decision = Event(), Event()
    output_error = OSError('selected output could not be saved')

    @router.task
    def work(ctx):
        calls.append((ctx.job_id, ctx.execution_generation))
        return (ctx.job_id, ctx.execution_generation)

    workflow.include_routers(router)
    for _ in range(3):
        workflow.add_job(None, 'A')
    write_output = storage.write_output

    def fail_first_output(node, job_id, output):
        if node == 'A' and job_id == 1 and output['generation'] == 0:
            raise output_error
        return write_output(node, job_id, output)

    monkeypatch.setattr(storage, 'write_output', fail_first_output)
    decide = storage.decide_execution_session_exit

    def pause_failed_decision(*args, **kwargs):
        if kwargs.get('outcome') == 'failed' and not at_decision.is_set():
            at_decision.set()
            assert release_decision.wait(30)
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, 'decide_execution_session_exit', pause_failed_decision)

    def run():
        try:
            if entry == 'run_jobs':
                result['value'] = workflow.run_jobs('A', [2, 1, 3])
            else:
                result['value'] = workflow.run_node_jobs('A', [storage.load_job('A', n) for n in (2, 1, 3)])
        except BaseException as error:
            result['error'] = error

    thread = Thread(target=run, name='native-finite-output-restart', daemon=True)
    thread.start()
    try:
        assert at_decision.wait(20), result
        assert calls == [(2, 0), (1, 0)]
        assert storage.get_job_status('A', 2) == 'done'
        peer_output = storage.output_file('A', 2).read_bytes()
        peer_events = storage.read_job_events('A', 2)
        peer_owner = storage.read_job_current_owner('A', 2)
        owner = storage.read_job_current_owner('A', 1)
        session = storage.get_execution_session(owner['session_id'])
        assert session['status'] == 'running'
        command = subprocess.run(
            [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '1'],
            cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
            capture_output=True, text=True, timeout=20,
        )
        assert command.returncode == 0, command.stdout + command.stderr
        assert storage.get_job_status('A', 1) == 'queued'
        assert storage.current_job_generation('A', 1) == 1
        assert storage.read_job_current_owner('A', 1) == owner
        release_decision.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        assert result.get('error') is output_error
        assert calls == [(2, 0), (1, 0), (1, 1)]
        assert [storage.get_job_status('A', n) for n in (1, 2, 3)] == ['done', 'done', 'queued']
        assert storage.output_file('A', 2).read_bytes() == peer_output
        assert storage.read_job_events('A', 2) == peer_events
        assert storage.read_job_current_owner('A', 2) == peer_owner
        assert storage.read_job_current_owner('A', 3) is None
        assert not any(event['event'] == 'started' for event in storage.read_job_events('A', 3))
        assert not storage.output_file('A', 3).exists()
        replacement = storage.read_job_current_owner('A', 1)
        assert replacement['session_id'] == owner['session_id']
        assert replacement['job_instance_id'] == owner['job_instance_id']
        assert replacement['generation'] == 1 and replacement['execution_id'] != owner['execution_id']
        assert storage.read_job_control('A', 1)['active_execution_id'] is None
        assert storage.read_json(storage.output_file('A', 1))['generation'] == 1
        assert len([event for event in storage.read_job_events('A', 1) if event['event'] == 'done']) == 1
        sessions = storage.list_execution_sessions()
        assert len(sessions) == 1 and sessions[0]['outcome'] == 'failed'
        assert sessions[0]['selected_jobs'] == session['selected_jobs']
        assert storage.get_component_reservation(('A',)) is None
        assert storage.list_jobs('A', status='running') == []
    finally:
        release_decision.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        _close(storage)


@pytest.mark.parametrize('reporter_name', ['InlineStatsReporter', 'InlineMonitorReporter'])
def test_selected_job_failure_remains_primary_when_session_reporter_cleanup_fails(tmp_path, monkeypatch, reporter_name):
    from micro_workflow_manager import monitor
    from micro_workflow_manager.errors import JobFailedError

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    router = NodeRouter('A', runner='direct')

    @router.task
    def work(ctx):
        raise ValueError('selected handler failed')

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    reporter_type = getattr(monitor, reporter_name)
    stop = reporter_type.stop_periodic
    cleanup_error = OSError('selected session reporter cleanup failed')

    def stop_and_fail(reporter):
        stop(reporter)
        raise cleanup_error

    monkeypatch.setattr(reporter_type, 'stop_periodic', stop_and_fail)
    try:
        with pytest.raises(JobFailedError, match='Job A/1 failed') as observed:
            workflow.run_jobs('A', [1])
        assert observed.value.__cause__ is cleanup_error
        session = workflow.storage.list_execution_sessions()[0]
        assert session['status'] == 'terminal'
        assert session['outcome'] == 'failed'
        assert 'Job A/1 failed' in session['failures'][0]['error']
        assert workflow.storage.get_component_reservation(('A',)) is None
        assert workflow.storage.get_job_status('A', 1) == 'failed'
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('entry', ['run_node', 'run_queued_node_jobs'])
@pytest.mark.parametrize('failure', ['configuration', 'interrupt'])
def test_queued_preparation_failure_preserves_unclaimed_job_and_finishes_node(tmp_path, monkeypatch, entry, failure):
    workflow = MicroWorkflow(tmp_path, runner='threaded', persist_graph=False)
    workflow.graph([('A', 'B')])
    router = NodeRouter('A', runner='threaded')
    calls = []

    @router.task
    def work(ctx):
        calls.append(ctx.job_id)
        return 'must remain queued'

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    storage = workflow.storage
    before_control = storage.read_job_control('A', 1)
    before_events = storage.read_job_events('A', 1)
    before_files = _snapshot(storage)[1]
    interrupted = KeyboardInterrupt('interrupted during runner preparation')
    if failure == 'configuration':
        monkeypatch.setenv('MWF_THREADED_JOB_PREFETCH_WORKERS', 'invalid')
        error_type = ValueError
        error_message = 'MWF_THREADED_JOB_PREFETCH_WORKERS must be an integer >= 1'
    else:
        def interrupt_preparation(*args, **kwargs):
            raise interrupted
        monkeypatch.setattr(workflow, 'make_runner', interrupt_preparation)
        error_type = KeyboardInterrupt
        error_message = 'interrupted during runner preparation'
    try:
        with pytest.raises(error_type, match=error_message) as observed:
            getattr(workflow, entry)('A')
        if failure == 'interrupt':
            assert observed.value is interrupted
        assert calls == []
        assert storage.get_job_status('A', 1) == 'queued'
        assert storage.read_job_current_owner('A', 1) is None
        assert storage.read_job_control('A', 1) == before_control
        if entry == 'run_node':
            assert [{key: value for key, value in event.items() if key != 'time'}
                    for event in storage.read_job_events('A', 1)] == [
                {'event': 'queued', 'previous_status': 'queued', 'status': 'queued'},
            ]
        else:
            assert storage.read_job_events('A', 1) == before_events
        assert _snapshot(storage)[1] == before_files
        sessions = storage.list_execution_sessions()
        assert len(sessions) == 1
        assert sessions[0]['status'] == 'terminal'
        assert sessions[0]['outcome'] == 'failed'
        assert error_message in sessions[0]['failures'][0]['error']
        assert storage.get_component_reservation(('A',)) is None
        assert storage.get_node_status('A') == 'failed'
    finally:
        _close(storage)


def test_finishing_selected_failure_cannot_overwrite_a_new_sessions_node_state(tmp_path, monkeypatch):
    from micro_workflow_manager.errors import JobFailedError
    from micro_workflow_manager.monitor import InlineStatsReporter

    first = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    second = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    for workflow in (first, second):
        workflow.graph([('A', 'B')])
    failed_router = NodeRouter('A', runner='direct')
    succeeding_router = NodeRouter('A', runner='direct')

    @failed_router.task
    def fail(ctx):
        raise ValueError('old selected call failed')

    @succeeding_router.task
    def succeed(ctx):
        return 'new session result'

    first.include_routers(failed_router)
    second.include_routers(succeeding_router)
    first.add_job(None, 'A')
    cleanup_started, release_cleanup = Event(), Event()
    stop = InlineStatsReporter.stop_periodic
    result = {}

    def pause_old_cleanup(reporter):
        if reporter.workflow is first:
            cleanup_started.set()
            assert release_cleanup.wait(30)
        stop(reporter)

    monkeypatch.setattr(InlineStatsReporter, 'stop_periodic', pause_old_cleanup)

    def run_first():
        try:
            first.run_jobs('A', [1])
        except BaseException as error:
            result['error'] = error

    thread = Thread(target=run_first, name='old-selected-finalization', daemon=True)
    thread.start()
    try:
        assert cleanup_started.wait(20), result
        old_session = first.storage.list_execution_sessions()[0]
        assert old_session['status'] == 'terminal'
        assert first.storage.get_component_reservation(('A',)) is None
        assert second.run_jobs('A', [1]) == ['new session result']
        newest = second.storage.read_job_current_owner('A', 1)
        assert newest['session_id'] != old_session['session_id']
        assert second.storage.get_node_status('A') == 'done'
        release_cleanup.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        assert isinstance(result.get('error'), JobFailedError), result
        assert first.storage.get_node_status('A') == 'done'
        assert first.storage.get_job_status('A', 1) == 'done'
        assert first.storage.read_job_current_owner('A', 1) == newest
        sessions = {session['session_id']: session for session in first.storage.list_execution_sessions()}
        assert sessions[old_session['session_id']]['outcome'] == 'failed'
        assert sessions[newest['session_id']]['outcome'] == 'done'
    finally:
        release_cleanup.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        _close(first.storage)
        _close(second.storage)


@pytest.mark.parametrize('status', ['running', 'failed', 'cancelled'])
def test_restart_job_cli_uses_its_exact_native_owner_and_preserves_other_work(workflow, status, capsys):
    storage = workflow.storage
    generation, execution_id = storage.claim_job_execution(
        'A', 1, started_at=now(), session_id='job-interrupt', component=('A', 'B'),
    )
    if status != 'running':
        storage.finalize_job_execution('A', 1, generation, execution_id, status)
    output = storage.output_file('A', 1)
    output.write_text(json.dumps({'status': status, 'value': 'prior output'}), encoding='utf-8')
    owner = storage.read_job_current_owner('A', 1)
    sessions = storage.list_execution_sessions()
    reservations = [dict(row) for row in storage.db_connection().execute('SELECT * FROM component_reservations')]
    peer = storage.read_job_control('B', 1), storage.read_job_events('B', 1)
    revision = storage.job_restart_revision()
    capsys.readouterr()

    # No stored graph path is configured. The second-terminal control resolves
    # native ownership directly and does not import a graph or start a scheduler.
    assert cli.main(['restart', 'A', 'job', '1']) == 0
    text = capsys.readouterr().out
    assert 'job-interrupt' in text
    assert 'unrelated-main' not in text
    assert storage.list_execution_sessions() == sessions
    assert [dict(row) for row in storage.db_connection().execute('SELECT * FROM component_reservations')] == reservations
    assert (storage.read_job_control('B', 1), storage.read_job_events('B', 1)) == peer
    assert storage.get_job_status('A', 1) == 'queued'
    assert storage.read_job_control('A', 1)['generation'] == generation + 1
    assert storage.read_job_control('A', 1)['active_execution_id'] is None
    assert storage.read_job_current_owner('A', 1) == owner
    assert storage.load_job('A', 1).params == {'value': 'A'}
    assert storage.job_restart_revision() == revision + 1
    assert not output.exists()
    assert not storage.job_execution_is_current('A', 1, generation, execution_id)
    events = storage.read_job_events('A', 1)
    assert [event['event'] for event in events[-2:]] == ['queued', 'restart_requested']
    assert events[-1]['session_id'] == 'job-interrupt'
    assert events[-1]['execution_id'] == execution_id


def _claim_with_output(storage, node='A', job_id=1):
    lease = storage.claim_job_execution(
        node, job_id, started_at=now(), session_id='job-interrupt', component=('A', 'B'),
    )
    storage.output_file(node, job_id).write_bytes(b'prior output\n')
    return lease


def _snapshot(storage):
    storage.db_mutation_barrier()
    files = {}
    for node in ('A', 'B'):
        for job_id in storage.list_job_ids(node):
            directory = storage.job_base_dir(node, job_id)
            files[node, job_id] = {
                path.relative_to(directory).as_posix(): None if path.is_dir() else path.read_bytes()
                for path in directory.rglob('*')
            }
    return list(storage.db_connection().iterdump()), files


def _restart_business_snapshot(storage):
    database, files = _snapshot(storage)
    database = [
        statement for statement in database
        if not statement.startswith('INSERT INTO "restart_receipts"')
    ]
    return database, files


def _restart_receipt(storage, targets, state):
    [receipt] = [
        dict(row) for row in storage.db_connection().execute(
            'SELECT * FROM restart_receipts ORDER BY operation_id',
        )
    ]
    manifest, files = validate_restart_manifest(storage.project_dir, receipt)
    assert receipt['state'] == state
    assert manifest['session_id'] == 'job-interrupt'
    assert manifest['owned'] is True
    assert manifest['reason'] == 'second-terminal restart'
    assert manifest['requested_by_pid'] == os.getpid()
    assert [
        (item['node'], item['job_id'], item['target'])
        for item in manifest['targets']
    ] == [
        (target['node'], target['job_id'], json.loads(json.dumps(target)))
        for target in sorted(targets, key=lambda item: (item['node'], item['job_id']))
    ]
    assert [
        (item['node'], item['job_id']) for item in manifest['database']
    ] == sorted((target['node'], target['job_id']) for target in targets)
    if state == 'committed':
        validate_restart_terminal_decision(manifest, receipt)
    else:
        require_restart_pre_state(storage.db_connection(), manifest)
        if state == 'aborted':
            validate_restart_terminal_decision(manifest, receipt)
    return receipt, manifest, files


def _fail_second_restart_writer(storage, monkeypatch, error):
    submit = storage.submit_db_mutation
    calls = []

    def submit_with_one_failure(operation, *args, **kwargs):
        calls.append(operation)
        if len(calls) == 2:
            original_operation = operation

            def fail_after_operation(connection):
                original_operation(connection)
                raise error

            operation = fail_after_operation
        return submit(operation, *args, **kwargs)

    monkeypatch.setattr(storage, 'submit_db_mutation', submit_with_one_failure)
    return calls


def _change(storage, sql, values=()):
    with storage.db_transaction() as connection:
        connection.execute(sql, values)


@pytest.mark.parametrize('damage', [
    'queued', 'done', 'no-owner', 'wrong-instance', 'wrong-selected-scope',
    'terminal-session', 'reused-pid', 'missing-reservation', 'foreign-reservation',
])
def test_native_restart_refuses_ineligible_or_damaged_owner_without_changing_work(workflow, damage, capsys, monkeypatch):
    storage = workflow.storage
    generation, execution = _claim_with_output(storage)
    if damage == 'queued':
        storage.release_unstarted_job_execution('A', 1, generation, execution)
    elif damage == 'done':
        storage.finalize_job_execution('A', 1, generation, execution, 'done')
    elif damage == 'no-owner':
        _change(storage, "UPDATE jobs SET active_execution_id=NULL WHERE node_name='A'")
        _change(storage, "UPDATE job_instances SET last_execution_id=NULL WHERE node_name='A'")
    elif damage == 'wrong-instance':
        _change(storage, 'UPDATE job_execution_owners SET job_instance_id=? WHERE execution_id=?', ('f' * 32, execution))
    elif damage == 'wrong-selected-scope':
        _change(storage, 'UPDATE job_execution_owners SET session_id=? WHERE execution_id=?', ('unrelated-main', execution))
    elif damage == 'terminal-session':
        storage.finish_execution_session('job-interrupt', outcome='done', finished_at=now())
    elif damage == 'reused-pid':
        _change(storage, "UPDATE execution_sessions SET process_identity='another-process' WHERE session_id='job-interrupt'")
    elif damage == 'missing-reservation':
        storage.release_execution_components('job-interrupt')
    elif damage == 'foreign-reservation':
        _change(storage, "UPDATE component_reservations SET session_id='unrelated-main' WHERE session_id='job-interrupt'")
    before = _snapshot(storage)
    after_startup = []
    if damage == 'reused-pid':
        from micro_workflow_manager.cli import restart as restart_module

        # The ownership reader refuses without mutation. Applied startup must
        # first recover this abandoned session, then refuse its queued job.
        with pytest.raises(RuntimeError):
            storage.plan_owned_job_restarts([('A', 1)])
        assert _snapshot(storage) == before
        recover = restart_module.recover_before_mutation
        owner = storage.read_job_current_owner('A', 1)
        peer = storage.read_job_control('B', 1), storage.read_job_events('B', 1)
        unrelated = storage.get_execution_session('unrelated-main')
        events = storage.read_job_events('A', 1)

        def observe_recovery(root):
            recover(root)
            control = storage.read_job_control('A', 1)
            assert storage.get_job_status('A', 1) == 'queued'
            assert control['generation'] == generation + 1
            assert control['active_execution_id'] is None
            assert storage.read_job_current_owner('A', 1) == owner
            assert not storage.output_file('A', 1).exists()
            recovered_events = storage.read_job_events('A', 1)
            assert recovered_events[:-2] == events
            assert [event['event'] for event in recovered_events[-2:]] == ['recovered', 'queued']
            session = storage.get_execution_session('job-interrupt')
            assert session['status'] == 'terminal' and session['outcome'] == 'failed'
            assert storage.get_component_state(('A', 'B'))['lifecycle'] == 'failed'
            assert storage.get_component_reservation(('A', 'B')) is None
            assert (storage.read_job_control('B', 1), storage.read_job_events('B', 1)) == peer
            assert storage.get_execution_session('unrelated-main') == unrelated
            after_startup.append(_snapshot(storage))

        monkeypatch.setattr(restart_module, 'recover_before_mutation', observe_recovery)
    capsys.readouterr()

    assert cli.main(['restart', 'A', 'job', '1']) == 1

    if damage == 'reused-pid':
        assert len(after_startup) == 1
        assert _snapshot(storage) == after_startup[0]
    else:
        assert _snapshot(storage) == before
    assert 'Restarted' not in capsys.readouterr().out


@pytest.mark.parametrize('change', ['status', 'generation', 'owner', 'instance', 'session', 'reservation'])
def test_restart_rechecks_state_after_cli_preflight(workflow, change, monkeypatch, capsys):
    storage = workflow.storage
    generation, execution = _claim_with_output(storage)
    apply = FileStorage.request_owned_job_restarts
    expected = []

    def change_before_apply(target_storage, targets, **kwargs):
        if change == 'status':
            storage.finalize_job_execution('A', 1, generation, execution, 'failed')
        elif change == 'generation':
            restarted, = apply(storage, storage.plan_owned_job_restarts([('A', 1)]))
            assert restarted['generation'] == generation + 1
            storage.output_file('A', 1).write_bytes(b'new generation output\n')
        elif change == 'owner':
            assert storage.release_unstarted_job_execution('A', 1, generation, execution)
            replacement = storage.claim_job_execution(
                'A', 1, started_at=now(), session_id='job-interrupt', component=('A', 'B'),
            )
            assert replacement[0] == generation and replacement[1] != execution
        elif change == 'instance':
            storage.delete_job('A', 1)
            storage.create_job(Job(node_name='A', job_id=1, params={'value': 'replacement'}))
            _claim_with_output(storage)
        elif change == 'session':
            storage.finish_execution_session('job-interrupt', outcome='done', finished_at=now())
        elif change == 'reservation':
            storage.release_execution_components('job-interrupt')
        expected.append(_snapshot(storage))
        return apply(target_storage, targets, **kwargs)

    monkeypatch.setattr(FileStorage, 'request_owned_job_restarts', change_before_apply)
    capsys.readouterr()

    assert cli.main(['restart', 'A', 'job', '1']) == 1

    assert len(expected) == 1
    assert _snapshot(storage) == expected[0]
    assert 'Restarted' not in capsys.readouterr().out


@pytest.mark.parametrize('failure', ['second-event', 'second-output', 'commit'])
def test_native_restart_batch_restores_all_work_on_synchronous_failure(workflow, failure, monkeypatch):
    storage = workflow.storage
    storage.create_job(Job(node_name='A', job_id=2, params={'value': 'second'}))
    _claim_with_output(storage)
    _claim_with_output(storage, job_id=2)
    targets = storage.plan_owned_job_restarts([('A', 1), ('A', 2)])
    if failure == 'second-event':
        _change(storage, "CREATE TRIGGER fail_restart_event BEFORE INSERT ON job_events "
                "WHEN NEW.job_id=2 AND NEW.event='restart_requested' "
                "BEGIN SELECT RAISE(ABORT, 'injected restart event failure'); END")
    elif failure == 'second-output':
        move = execution_restart._move_restart_output
        second_output = storage.output_file('A', 2)

        def fail_second_output(path, destination):
            if path == second_output:
                raise PermissionError('injected output staging failure')
            return move(path, destination)

        monkeypatch.setattr(execution_restart, '_move_restart_output', fail_second_output)
    before = _restart_business_snapshot(storage)
    writer_calls = None
    if failure == 'commit':
        writer_calls = _fail_second_restart_writer(
            storage, monkeypatch, RuntimeError('injected restart commit failure'),
        )

    with pytest.raises(Exception, match='injected'):
        storage.request_owned_job_restarts(targets)

    assert _restart_business_snapshot(storage) == before
    receipt, manifest, files = _restart_receipt(storage, targets, 'aborted')
    assert json.loads(receipt['decision_json'])['details'] == {'restored': True}
    assert files.has_private_path() is False
    for item in manifest['targets']:
        output = item['output']
        assert output['original'] is not None
        assert not (storage.project_dir / output['saved']).exists()
    if failure == 'commit':
        assert len(writer_calls) == 3


def test_explicit_restart_batch_advances_every_target_in_one_request(workflow, capsys):
    storage = workflow.storage
    storage.create_job(Job(node_name='A', job_id=2, params={'value': 'second'}))
    first = _claim_with_output(storage)
    second = _claim_with_output(storage, job_id=2)
    storage.finalize_job_execution('A', 2, *second, 'failed')
    peer = storage.read_job_control('B', 1), storage.read_job_events('B', 1)
    revision = storage.job_restart_revision()
    capsys.readouterr()

    assert cli.main(['restart', 'A', 'jobs', '1-2']) == 0

    text = capsys.readouterr().out
    assert text.count('in session job-interrupt') == 2
    assert storage.job_restart_revision() == revision + 1
    for job_id, lease in ((1, first), (2, second)):
        assert storage.get_job_status('A', job_id) == 'queued'
        assert storage.read_job_control('A', job_id)['generation'] == lease[0] + 1
        assert storage.read_job_current_owner('A', job_id)['execution_id'] == lease[1]
        assert not storage.output_file('A', job_id).exists()
        assert [event['event'] for event in storage.read_job_events('A', job_id)[-2:]] == ['queued', 'restart_requested']
    assert (storage.read_job_control('B', 1), storage.read_job_events('B', 1)) == peer


def test_restart_retains_older_last_owner_after_requeue_and_cancellation(workflow, capsys):
    storage = workflow.storage
    generation, execution = _claim_with_output(storage)
    owner = storage.read_job_current_owner('A', 1)
    storage.request_job_restart('A', 1)
    workflow.cancel_job('A', 1)
    assert storage.get_job_status('A', 1) == 'cancelled'
    assert storage.read_job_control('A', 1)['generation'] == generation + 1
    capsys.readouterr()

    assert cli.main(['restart', 'A', 'job', '1']) == 0

    assert storage.read_job_control('A', 1)['generation'] == generation + 2
    assert storage.read_job_current_owner('A', 1) == owner
    assert storage.get_node_status('A') == 'running'
    assert not list(storage.job_base_dir('A', 1).glob('.restart-*'))
    replacement_generation, replacement_execution = storage.claim_job_execution(
        'A', 1, started_at=now(), session_id='job-interrupt', component=('A', 'B'),
    )
    replacement = storage.read_job_current_owner('A', 1)
    assert replacement_generation == generation + 2
    assert replacement_execution != execution
    assert replacement['session_id'] == owner['session_id']
    assert replacement['component'] == owner['component']
    assert replacement['job_instance_id'] == owner['job_instance_id']
    assert replacement['generation'] == generation + 2


@pytest.mark.parametrize('failure', ['cleanup', 'state-notification', 'queue-notification'])
def test_restart_reports_postcommit_failures_without_restoring_old_output(workflow, failure, monkeypatch):
    storage = workflow.storage
    generation, execution = _claim_with_output(storage)
    targets = storage.plan_owned_job_restarts([('A', 1)])
    if failure == 'cleanup':
        discard = execution_restart._discard_restart_output

        def fail_staged_cleanup(path):
            if path.name.startswith('.restart-'):
                raise PermissionError('injected staged cleanup failure')
            return discard(path)

        monkeypatch.setattr(execution_restart, '_discard_restart_output', fail_staged_cleanup)
    else:
        def fail_notification(*args, **kwargs):
            raise RuntimeError('injected notification failure')

        method = 'notify_state_change' if failure == 'state-notification' else 'notify_queue_change'
        monkeypatch.setattr(storage, method, fail_notification)

    [result] = storage.request_owned_job_restarts(targets)

    assert result['generation'] == generation + 1
    assert result['warnings']
    assert all('Restart committed' in warning and 'requires attention' in warning for warning in result['warnings'])
    assert storage.get_job_status('A', 1) == 'queued'
    assert storage.read_job_current_owner('A', 1)['execution_id'] == execution
    assert not storage.output_file('A', 1).exists()
    assert [event['event'] for event in storage.read_job_events('A', 1)[-2:]] == ['queued', 'restart_requested']
    saved = list(storage.job_base_dir('A', 1).glob('.restart-*'))
    if failure == 'cleanup':
        assert len(saved) == 1
        assert saved[0].read_bytes() == b'prior output\n'
    else:
        assert saved == []


def test_restart_zero_row_update_rolls_back_every_target(workflow):
    storage = workflow.storage
    _claim_with_output(storage)
    _claim_with_output(storage, node='B')
    targets = storage.plan_owned_job_restarts([('A', 1), ('B', 1)])
    _change(storage, "CREATE TRIGGER skip_restart BEFORE UPDATE ON jobs "
            "WHEN OLD.node_name='B' AND NEW.restart_requested_at IS NOT NULL "
            "BEGIN SELECT RAISE(IGNORE); END")
    before = _restart_business_snapshot(storage)

    with pytest.raises(RuntimeError, match='Restart state changed'):
        storage.request_owned_job_restarts(targets)

    assert _restart_business_snapshot(storage) == before
    receipt, manifest, files = _restart_receipt(storage, targets, 'aborted')
    assert json.loads(receipt['decision_json'])['details'] == {'restored': True}
    assert files.has_private_path() is False
    assert all(not (storage.project_dir / item['output']['saved']).exists()
               for item in manifest['targets'])


def test_opposite_order_restart_requests_commit_once_without_deadlock(workflow):
    storage = workflow.storage
    _claim_with_output(storage)
    _claim_with_output(storage, node='B')
    targets = storage.plan_owned_job_restarts([('A', 1), ('B', 1)])
    barrier = Barrier(2)

    def restart(order):
        try:
            barrier.wait(timeout=10)
            return storage.request_owned_job_restarts(order)
        except RuntimeError as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(restart, targets)
        second = pool.submit(restart, targets[::-1])
        results = [first.result(timeout=15), second.result(timeout=15)]

    assert sum(isinstance(result, list) for result in results) == 1
    assert sum(isinstance(result, RuntimeError) for result in results) == 1
    for target in targets:
        node = target['node']
        assert storage.read_job_control(node, 1)['generation'] == target['generation'] + 1
        assert len([event for event in storage.read_job_events(node, 1) if event['event'] == 'restart_requested']) == 1
        assert not list(storage.job_base_dir(node, 1).glob('.restart-*'))
    receipt, _, files = _restart_receipt(storage, targets, 'committed')
    assert json.loads(receipt['decision_json'])['details']['restart_revision'] == 1
    assert files.has_private_path() is False


def test_restart_cli_reports_retained_output_when_rollback_restoration_fails(workflow, monkeypatch, capsys):
    storage = workflow.storage
    _claim_with_output(storage)
    before_database = []
    planned_targets = []
    apply = FileStorage.request_owned_job_restarts

    def apply_with_failure(target_storage, targets, **kwargs):
        _change(storage, "CREATE TRIGGER fail_restart BEFORE INSERT ON job_events "
                "WHEN NEW.event='restart_requested' "
                "BEGIN SELECT RAISE(ABORT, 'injected restart event failure'); END")
        before_database.extend(_restart_business_snapshot(storage)[0])
        planned_targets.extend(targets)
        return apply(target_storage, targets, **kwargs)

    monkeypatch.setattr(FileStorage, 'request_owned_job_restarts', apply_with_failure)
    move = execution_restart._move_restart_output

    def fail_restoration(path, destination):
        if path.name.startswith('.restart-'):
            raise PermissionError('injected output restoration failure')
        return move(path, destination)

    monkeypatch.setattr(execution_restart, '_move_restart_output', fail_restoration)
    capsys.readouterr()

    assert cli.main(['restart', 'A', 'job', '1']) == 1

    assert _restart_business_snapshot(storage)[0] == before_database
    [saved] = list(storage.job_base_dir('A', 1).glob('.restart-*'))
    assert saved.read_bytes() == b'prior output\n'
    receipt, manifest, files = _restart_receipt(storage, planned_targets, 'prepared')
    assert receipt['decision_json'] is None and receipt['decision_digest'] is None
    assert files.has_private_path() is True
    assert storage.project_dir / manifest['targets'][0]['output']['saved'] == saved
    text = capsys.readouterr().err
    assert 'injected restart event failure' in text
    assert 'restoration failed' in text
    assert str(saved) in text


def test_restart_wakes_the_affected_node_only_after_commit(workflow):
    storage = workflow.storage
    _claim_with_output(storage)
    targets = storage.plan_owned_job_restarts([('A', 1)])
    observed = []
    peer_wakes = []
    unsubscribe = storage.subscribe_queue_changes(
        lambda: observed.append((storage.get_job_status('A', 1), storage.output_file('A', 1).exists())),
        node_name='A',
    )
    unsubscribe_peer = storage.subscribe_queue_changes(lambda: peer_wakes.append(True), node_name='B')
    try:
        storage.request_owned_job_restarts(targets)
    finally:
        unsubscribe()
        unsubscribe_peer()

    assert observed == [('queued', False)]
    assert peer_wakes == []


@pytest.mark.parametrize('failed_only', [False, True])
def test_restart_node_selects_the_persisted_component_and_only_eligible_jobs(workflow, failed_only, capsys):
    storage = workflow.storage
    running = _claim_with_output(storage)
    failed = _claim_with_output(storage, node='B')
    terminal = []
    for job_id, status in ((2, 'done'), (3, 'cancelled')):
        storage.create_job(Job(node_name='B', job_id=job_id, params={'status': status}))
        terminal.append((job_id, status, _claim_with_output(storage, node='B', job_id=job_id)))
    storage.finalize_job_execution('B', 1, *failed, 'failed')
    for job_id, status, lease in terminal:
        storage.finalize_job_execution('B', job_id, *lease, status)
    storage.create_job(Job(node_name='B', job_id=4, params={'status': 'queued'}))
    untouched = {
        job_id: (storage.read_job_control('B', job_id), storage.read_job_events('B', job_id))
        for job_id in (2, 4)
    }
    running_before = storage.read_job_control('A', 1), storage.read_job_events('A', 1)
    sessions = storage.list_execution_sessions()
    reservations = [dict(row) for row in storage.db_connection().execute('SELECT * FROM component_reservations')]
    capsys.readouterr()

    assert cli.main(['restart', 'A'] + (['failed'] if failed_only else [])) == 0

    text = capsys.readouterr().out
    assert 'job-interrupt' in text and 'unrelated-main' not in text
    assert storage.list_execution_sessions() == sessions
    assert [dict(row) for row in storage.db_connection().execute('SELECT * FROM component_reservations')] == reservations
    assert storage.get_job_status('B', 1) == 'queued'
    assert storage.get_job_status('B', 3) == 'queued'
    assert storage.read_job_control('B', 1)['generation'] == failed[0] + 1
    assert {job_id: (storage.read_job_control('B', job_id), storage.read_job_events('B', job_id))
            for job_id in (2, 4)} == untouched
    if failed_only:
        assert (storage.read_job_control('A', 1), storage.read_job_events('A', 1)) == running_before
        assert storage.output_file('A', 1).read_bytes() == b'prior output\n'
    else:
        assert storage.get_job_status('A', 1) == 'queued'
        assert storage.read_job_control('A', 1)['generation'] == running[0] + 1


def test_restart_node_without_eligible_jobs_is_a_native_noop(workflow, capsys):
    storage = workflow.storage
    before = _snapshot(storage)
    capsys.readouterr()

    assert cli.main(['restart', 'A']) == 0

    text = capsys.readouterr().out
    assert 'job-interrupt' in text and '{A, B}' in text
    assert 'No matching jobs' in text
    assert _snapshot(storage) == before


def test_restart_restoration_failure_preserves_error_without_python311_add_note(workflow, monkeypatch):
    storage = workflow.storage
    _claim_with_output(storage)
    _claim_with_output(storage, node='B')
    targets = storage.plan_owned_job_restarts([('A', 1), ('B', 1)])
    before = _restart_business_snapshot(storage)[0]

    class Python310Error(RuntimeError):
        def __getattribute__(self, name):
            if name == 'add_note':
                raise AttributeError('Python 3.10 has no add_note')
            return super().__getattribute__(name)

    original_error = Python310Error('injected original commit failure')
    writer_calls = _fail_second_restart_writer(storage, monkeypatch, original_error)
    move = execution_restart._move_restart_output
    attempted = []

    def fail_restoration(path, destination):
        if path.name.startswith('.restart-'):
            attempted.append(path)
            raise PermissionError('injected restoration failure')
        return move(path, destination)

    monkeypatch.setattr(execution_restart, '_move_restart_output', fail_restoration)

    with pytest.raises(Python310Error, match='injected original commit failure') as captured:
        storage.request_owned_job_restarts(targets)

    assert captured.value is original_error
    assert len(writer_calls) == 2
    assert _restart_business_snapshot(storage)[0] == before
    receipt, _, files = _restart_receipt(storage, targets, 'prepared')
    assert receipt['decision_json'] is None and receipt['decision_digest'] is None
    assert files.has_private_path() is True
    assert len(attempted) == 2
    for saved in attempted:
        assert saved.read_bytes() == b'prior output\n'
        assert any(str(saved) in note for note in captured.value.__notes__)


@pytest.mark.parametrize('runner', ['direct', 'threaded', 'process'])
def test_second_terminal_restart_replaces_execution_within_the_same_native_session(tmp_path, monkeypatch, runner):
    from tests.test_active_job_restart import make_restart_project, wait_until

    make_restart_project(tmp_path, monkeypatch)
    storage = FileStorage(tmp_path)
    result = {}

    def run():
        try:
            result['code'] = cli.main(['runfrom', 'A', '--runner', runner])
        except BaseException as error:
            result['error'] = error

    thread = Thread(target=run, name='native-restart-workflow', daemon=True)
    release = tmp_path / 'node' / 'A' / 'input' / 'release_old.flag'
    thread.start()
    try:
        wait_until(lambda: (tmp_path / 'node' / 'A' / 'input' / 'old_started.flag').exists(), timeout=20)
        owner = storage.read_job_current_owner('A', 1)
        session_id = owner['session_id']
        assert storage.get_execution_session(session_id)['status'] == 'running'
        environment = dict(os.environ)
        environment['PYTHONPATH'] = str(Path(__file__).resolve().parents[1])
        command = subprocess.run(
            [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '1'],
            cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=30,
        )
        assert command.returncode == 0, command.stdout + command.stderr
        assert session_id in command.stdout
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        assert result == {'code': 0}
        session = storage.get_execution_session(session_id)
        assert session['status'] == 'terminal' and session['outcome'] == 'done'
        assert [row['session_id'] for row in storage.list_execution_sessions()] == [session_id]
        replacement = storage.read_job_current_owner('A', 1)
        assert replacement['session_id'] == session_id
        assert replacement['execution_id'] != owner['execution_id']
        assert replacement['job_instance_id'] == owner['job_instance_id']
        assert replacement['generation'] == owner['generation'] + 1
        assert storage.get_job_status('A', 1) == 'done'
        assert storage.list_job_ids('B') == [1]
        assert storage.load_job('B', 1).params == {'value': 'fresh'}
        assert storage.get_job_status('B', 1) == 'done'
        assert (tmp_path / 'node' / 'B' / 'output' / 'received.txt').read_text(encoding='utf-8') == 'fresh'
        release.write_text('release', encoding='utf-8')
        time.sleep(0.25)
        assert not (tmp_path / 'node' / 'A' / 'output' / 'stale.txt').exists()
        assert storage.list_job_ids('B') == [1]
        assert not (tmp_path / '.mwf' / 'run.json').exists()
    finally:
        release.write_text('release', encoding='utf-8')
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        _close(storage)


@pytest.mark.parametrize('mode', ['job', 'jobs', 'component', 'failed'])
def test_restart_dry_run_preserves_stored_work_and_names_exact_owner(workflow, mode, capsys):
    storage = workflow.storage
    _claim_with_output(storage)
    storage.create_job(Job(node_name='A', job_id=2, params={'value': 'second'}))
    second = _claim_with_output(storage, job_id=2)
    storage.finalize_job_execution('A', 2, *second, 'failed')
    arguments = {'job': ['job', '1'], 'jobs': ['jobs', '1-2'], 'component': [], 'failed': ['failed']}[mode]
    before = _snapshot(storage)
    capsys.readouterr()

    assert cli.main(['restart', 'A', *arguments, '--dry-run']) == 0

    text = capsys.readouterr().out
    assert 'Restart dry run' in text and 'job-interrupt' in text
    assert 'unrelated-main' not in text
    assert '{A, B}' in text
    assert _snapshot(storage) == before


@pytest.mark.parametrize('change', ['target-finished', 'new-target', 'reservation-released'])
def test_component_restart_rechecks_the_entire_selection_before_changing_work(workflow, change, monkeypatch, capsys):
    storage = workflow.storage
    _claim_with_output(storage)
    second = _claim_with_output(storage, node='B') if change == 'target-finished' else None
    apply = FileStorage.request_owned_job_restarts
    expected = []

    def change_before_apply(target_storage, targets, **kwargs):
        if change == 'target-finished':
            storage.finalize_job_execution('B', 1, *second, 'done')
        elif change == 'new-target':
            _claim_with_output(storage, node='B')
        else:
            storage.release_execution_components('job-interrupt')
        expected.append(_snapshot(storage))
        return apply(target_storage, targets, **kwargs)

    monkeypatch.setattr(FileStorage, 'request_owned_job_restarts', change_before_apply)
    capsys.readouterr()

    assert cli.main(['restart', 'A']) == 1

    assert len(expected) == 1
    assert _snapshot(storage) == expected[0]
    assert 'Restarted' not in capsys.readouterr().out


@pytest.mark.parametrize('form', ['component', 'jobs'])
def test_restart_refuses_the_entire_selection_when_one_job_has_another_last_owner(
    workflow_with_prior_owner, form, capsys,
):
    storage = workflow_with_prior_owner.storage
    _claim_with_output(storage)
    before = _snapshot(storage)
    capsys.readouterr()

    assert cli.main(['restart', 'A'] + (['jobs', '1-2'] if form == 'jobs' else [])) == 1

    assert _snapshot(storage) == before
    text = capsys.readouterr()
    assert 'earlier-interrupt' in text.err
    assert 'Restarted' not in text.out


@pytest.mark.parametrize('status', ['queued', 'done', 'missing'])
def test_explicit_restart_refuses_all_jobs_when_a_later_job_is_ineligible(workflow, status, capsys):
    storage = workflow.storage
    _claim_with_output(storage)
    if status != 'missing':
        storage.create_job(Job(node_name='A', job_id=2, params={'status': status}))
        if status == 'done':
            second = _claim_with_output(storage, job_id=2)
            storage.finalize_job_execution('A', 2, *second, 'done')
    before = _snapshot(storage)
    capsys.readouterr()

    assert cli.main(['restart', 'A', 'jobs', '1-2']) == 1

    assert _snapshot(storage) == before
    assert 'Restarted' not in capsys.readouterr().out


@pytest.mark.parametrize('failed_only', [False, True])
def test_empty_component_restart_refuses_if_work_becomes_eligible_after_preflight(workflow, failed_only, monkeypatch, capsys):
    storage = workflow.storage
    lease = _claim_with_output(storage) if failed_only else None
    plan = FileStorage.plan_owned_component_restart
    expected = []

    def make_work_eligible(target_storage, node, **kwargs):
        result = plan(target_storage, node, **kwargs)
        assert result['targets'] == []
        if failed_only:
            storage.finalize_job_execution('A', 1, *lease, 'failed')
        else:
            _claim_with_output(storage)
        expected.append(_snapshot(storage))
        return result

    monkeypatch.setattr(FileStorage, 'plan_owned_component_restart', make_work_eligible)
    capsys.readouterr()

    assert cli.main(['restart', 'A'] + (['failed'] if failed_only else [])) == 1

    assert len(expected) == 1
    assert _snapshot(storage) == expected[0]
    assert 'No matching jobs' not in capsys.readouterr().out


def test_component_restart_refuses_running_job_without_an_active_execution(workflow, capsys):
    storage = workflow.storage
    generation, execution = _claim_with_output(storage)
    storage.release_unstarted_job_execution('A', 1, generation, execution)
    _change(storage, "UPDATE jobs SET status='running' WHERE node_name='A' AND job_id=1")
    before = _snapshot(storage)
    capsys.readouterr()

    assert cli.main(['restart', 'A']) == 1

    assert _snapshot(storage) == before
    text = capsys.readouterr()
    assert 'Recovery requires an exact active owner' in text.err
    assert 'No matching jobs' not in text.out


@pytest.mark.parametrize('entry', ['run_node', 'run_jobs', 'run_node_jobs'])
def test_node_execution_preserves_python_results_and_selection(tmp_path, entry):
    workflow = MicroWorkflow(tmp_path, runner='threaded', persist_graph=False)
    workflow.graph([('A', 'B')])
    router = NodeRouter('A', runner='threaded', max_threads=2)
    values = {job_id: object() for job_id in (1, 2, 3)}

    @router.task
    def work(ctx):
        return values[ctx.job_id]

    workflow.include_routers(router)
    for _ in range(3):
        workflow.add_job(None, 'A')
    try:
        if entry == 'run_node':
            result = workflow.run_node('A')
            selected = [1, 2, 3]
        elif entry == 'run_jobs':
            result = workflow.run_jobs('A', [3, 1])
            selected = [3, 1]
        else:
            result = workflow.run_node_jobs('A', [workflow.storage.load_job('A', n) for n in (3, 1)])
            selected = [3, 1]
        assert len(result) == len(selected)
        assert all(value is values[job_id] for value, job_id in zip(result, selected))
        if entry != 'run_node':
            assert workflow.storage.get_job_status('A', 2) == 'queued'
    finally:
        _close(workflow.storage)


def test_node_failure_still_stops_admission_without_an_explicit_restart(tmp_path):
    from micro_workflow_manager.errors import JobFailedError

    workflow = MicroWorkflow(tmp_path, runner='threaded', persist_graph=False)
    workflow.graph([('A', 'B')])
    router = NodeRouter('A', runner='threaded', max_threads=1)
    started = []

    @router.task
    def work(ctx):
        started.append(ctx.job_id)
        raise ValueError('unrepaired failure')

    workflow.include_routers(router)
    for _ in range(3):
        workflow.add_job(None, 'A')
    try:
        with pytest.raises(JobFailedError, match='Job A/1 failed'):
            workflow.run_node('A')
        assert started == [1]
        assert [workflow.storage.get_job_status('A', n) for n in (1, 2, 3)] == ['failed', 'queued', 'queued']
        assert workflow.storage.list_execution_sessions()[0]['outcome'] == 'failed'
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('entry', ['run_node', 'run_jobs', 'run_node_jobs', 'run_node_cancelled_restart'])
def test_failed_job_restart_is_readmitted_before_its_live_session_finishes(tmp_path, monkeypatch, entry):
    from tests.test_active_job_restart import wait_until

    monkeypatch.chdir(tmp_path)
    workflow = MicroWorkflow(tmp_path, runner='threaded', persist_graph=False)
    workflow.graph([('A', 'B')])
    peer_started, release_peer, replacement_started = Event(), Event(), Event()
    router = NodeRouter('A', runner='threaded', max_threads=2)
    values = {job_id: object() for job_id in (1, 2, 3)}
    starts = []

    @router.task
    def work(ctx):
        starts.append((ctx.job_id, ctx.execution_generation))
        if ctx.job_id == 1:
            if ctx.execution_generation == 0:
                assert peer_started.wait(15)
                raise ValueError('first attempt failed')
            replacement_started.set()
        elif ctx.job_id == 2:
            peer_started.set()
            assert release_peer.wait(30)
        return values[ctx.job_id]

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'A')
    storage = workflow.storage
    result = {}

    def run():
        try:
            if entry in {'run_node', 'run_node_cancelled_restart'}:
                result['value'] = workflow.run_node('A')
            elif entry == 'run_jobs':
                result['value'] = workflow.run_jobs('A', [2, 1])
            else:
                result['value'] = workflow.run_node_jobs('A', [storage.load_job('A', n) for n in (2, 1)])
        except BaseException as error:
            result['error'] = repr(error)

    thread = Thread(target=run, name='native-failed-restart', daemon=True)
    thread.start()
    try:
        wait_until(lambda: storage.get_job_status('A', 1) == 'failed', timeout=20)
        owner = storage.read_job_current_owner('A', 1)
        assert storage.get_execution_session(owner['session_id'])['status'] == 'running'
        assert storage.get_job_status('A', 2) == 'running'
        environment = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
        command = subprocess.run(
            [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '1'],
            cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=20,
        )
        assert command.returncode == 0, command.stdout + command.stderr
        replacement_generation = owner['generation'] + 1
        if entry == 'run_node_cancelled_restart':
            workflow.cancel_job('A', 1)
            assert storage.get_job_status('A', 1) == 'cancelled'
            command = subprocess.run(command.args, cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=20)
            assert command.returncode == 0, command.stdout + command.stderr
            replacement_generation += 1
        release_peer.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        assert replacement_started.is_set(), {
            'result': result, 'job': storage.read_job_control('A', 1),
            'session': storage.get_execution_session(owner['session_id']),
        }
        assert 'error' not in result
        whole_node = entry in {'run_node', 'run_node_cancelled_restart'}
        expected = [1, 2, 3] if whole_node else [2, 1]
        assert len(result['value']) == len(expected)
        assert all(value is values[job_id] for value, job_id in zip(result['value'], expected))
        assert sorted(starts) == sorted([(1, 0), (1, replacement_generation), (2, 0)] + ([(3, 0)] if whole_node else []))
        if not whole_node:
            assert storage.get_job_status('A', 3) == 'queued'
        assert storage.get_job_status('A', 1) == 'done'
        replacement = storage.read_job_current_owner('A', 1)
        assert replacement['session_id'] == owner['session_id']
        assert replacement['job_instance_id'] == owner['job_instance_id']
        assert replacement['generation'] == replacement_generation
        assert [session['session_id'] for session in storage.list_execution_sessions()] == [owner['session_id']]
        assert storage.get_execution_session(owner['session_id'])['outcome'] == 'done'
    finally:
        release_peer.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        _close(storage)


def test_accepted_restart_runs_even_when_another_job_remains_failed(tmp_path, monkeypatch):
    from micro_workflow_manager.errors import JobFailedError
    from tests.test_active_job_restart import wait_until

    monkeypatch.chdir(tmp_path)
    workflow = MicroWorkflow(tmp_path, runner='threaded', persist_graph=False)
    workflow.graph([('A', 'B')])
    peer_started, release_peer, replacement_started = Event(), Event(), Event()
    router = NodeRouter('A', runner='threaded', max_threads=3)
    starts = []

    @router.task
    def work(ctx):
        starts.append((ctx.job_id, ctx.execution_generation))
        if ctx.job_id in (1, 2):
            if ctx.execution_generation == 0:
                assert peer_started.wait(15)
                raise ValueError('initial failure')
            replacement_started.set()
        elif ctx.job_id == 3:
            peer_started.set()
            assert release_peer.wait(30)
        return ctx.job_id

    workflow.include_routers(router)
    for _ in range(4):
        workflow.add_job(None, 'A')
    storage, result = workflow.storage, {}

    def run():
        try:
            result['value'] = workflow.run_node('A')
        except BaseException as error:
            result['error'] = error

    thread = Thread(target=run, name='native-partial-restart', daemon=True)
    thread.start()
    try:
        wait_until(lambda: all(storage.get_job_status('A', n) == 'failed' for n in (1, 2)), timeout=20)
        owner = storage.read_job_current_owner('A', 1)
        environment = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
        command = subprocess.run(
            [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '1'],
            cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=20,
        )
        assert command.returncode == 0, command.stdout + command.stderr
        release_peer.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        assert replacement_started.is_set(), result
        assert isinstance(result.get('error'), JobFailedError), result
        assert 'A/2' in str(result['error'])
        assert sorted(starts) == [(1, 0), (1, 1), (2, 0), (3, 0)]
        assert [storage.get_job_status('A', n) for n in (1, 2, 3, 4)] == ['done', 'failed', 'done', 'queued']
        assert storage.read_job_current_owner('A', 1)['session_id'] == owner['session_id']
        assert storage.get_execution_session(owner['session_id'])['outcome'] == 'failed'
    finally:
        release_peer.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        _close(storage)


@pytest.mark.parametrize('peer_result', ['value', 'unpickleable'])
def test_process_failed_restart_retains_peer_results_and_exact_selection(tmp_path, monkeypatch, peer_result):
    from tests.test_active_job_restart import wait_until

    monkeypatch.chdir(tmp_path)
    source = tmp_path / 'src'
    behaviors = source / 'node_behavior'
    behaviors.mkdir(parents=True)
    graph = source / 'graph.py'
    graph.write_text("EDGES = [('A', 'B')]\n", encoding='utf-8')
    (behaviors / 'A.py').write_text(f'PEER_RESULT = {peer_result!r}\n' + '''
import time
from micro_workflow_manager import NodeRouter
router = NodeRouter('A', runner='process', max_threads=2)
@router.task
def work(ctx):
    peer = ctx.input_path('peer-started')
    release = ctx.input_path('release-peer')
    if ctx.job_id == 1 and ctx.execution_generation == 0:
        deadline = time.monotonic() + 20
        while not peer.exists():
            assert time.monotonic() < deadline
            time.sleep(0.01)
        raise ValueError('failed process attempt')
    if ctx.job_id == 2:
        peer.write_text('started', encoding='utf-8')
        deadline = time.monotonic() + 40
        while not release.exists():
            assert time.monotonic() < deadline
            time.sleep(0.01)
        if PEER_RESULT == 'unpickleable':
            return lambda: ctx.job_id
    return (ctx.job_id, b'python-value')
''', encoding='utf-8')
    workflow = MicroWorkflow(tmp_path, runner='process', process_graph_path=graph, persist_graph=False)
    workflow.graph([('A', 'B')])
    workflow.include_node_dir(behaviors)
    for _ in range(3):
        workflow.add_job(None, 'A')
    storage, result = workflow.storage, {}

    def run():
        try:
            result['value'] = workflow.run_jobs('A', [2, 1])
        except BaseException as error:
            result['error'] = error

    thread = Thread(target=run, name='native-process-failed-restart', daemon=True)
    thread.start()
    release = tmp_path / 'node' / 'A' / 'input' / 'release-peer'
    try:
        wait_until(lambda: storage.get_job_status('A', 1) == 'failed', timeout=25)
        owner = storage.read_job_current_owner('A', 1)
        assert storage.get_job_status('A', 2) == 'running'
        environment = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
        command = subprocess.run(
            [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '1'],
            cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=20,
        )
        assert command.returncode == 0, command.stdout + command.stderr
        release.write_text('release', encoding='utf-8')
        thread.join(timeout=25)
        assert not thread.is_alive(), result
        assert [storage.get_job_status('A', n) for n in (1, 2, 3)] == ['done', 'done', 'queued']
        if peer_result == 'unpickleable':
            assert isinstance(result.get('error'), TypeError), result
            assert 'results must be pickleable' in str(result['error'])
        else:
            assert result == {'value': [(2, b'python-value'), (1, b'python-value')]}
        for job_id, expected in ((1, 2), (2, 1), (3, 0)):
            events = [event for event in storage.read_job_events('A', job_id) if event['event'] == 'started']
            assert len(events) == expected
        replacement = storage.read_job_current_owner('A', 1)
        assert replacement['session_id'] == owner['session_id']
        assert replacement['job_instance_id'] == owner['job_instance_id']
        assert replacement['generation'] == owner['generation'] + 1
        assert storage.get_execution_session(owner['session_id'])['outcome'] == (
            'failed' if peer_result == 'unpickleable' else 'done'
        )
        assert storage.get_component_reservation(('A',)) is None
    finally:
        release.write_text('release', encoding='utf-8')
        thread.join(timeout=25)
        assert not thread.is_alive(), result
        _close(storage)


def test_process_submit_failure_retains_an_accepted_restart_and_the_original_error(tmp_path, monkeypatch):
    from concurrent.futures import ProcessPoolExecutor
    from tests.test_active_job_restart import wait_until

    monkeypatch.chdir(tmp_path)
    source = tmp_path / 'src'
    behaviors = source / 'node_behavior'
    behaviors.mkdir(parents=True)
    graph = source / 'graph.py'
    graph.write_text("EDGES = [('A', 'B')]\n", encoding='utf-8')
    (behaviors / 'A.py').write_text(
        "from micro_workflow_manager import NodeRouter\n"
        "router = NodeRouter('A', runner='process', max_threads=2)\n"
        "@router.task\n"
        "def work(ctx):\n"
        "    if ctx.job_id == 1 and ctx.execution_generation == 0:\n"
        "        raise ValueError('failed before the next submission')\n"
        "    return (ctx.job_id, ctx.execution_generation)\n", encoding='utf-8',
    )
    workflow = MicroWorkflow(tmp_path, runner='process', process_graph_path=graph, persist_graph=False)
    workflow.graph([('A', 'B')])
    workflow.include_node_dir(behaviors)
    for _ in range(3):
        workflow.add_job(None, 'A')
    submit_waiting, release_submit = Event(), Event()
    original_submit = ProcessPoolExecutor.submit
    submit_calls = []
    submission_error = OSError('executor refused the second submission')
    result = {}

    def gated_submit(executor, *args, **kwargs):
        submit_calls.append(True)
        if len(submit_calls) == 2:
            submit_waiting.set()
            assert release_submit.wait(30)
            raise submission_error
        return original_submit(executor, *args, **kwargs)

    monkeypatch.setattr(ProcessPoolExecutor, 'submit', gated_submit)

    def run():
        try:
            workflow.run_jobs('A', [1, 2])
        except BaseException as error:
            result['error'] = error

    thread = Thread(target=run, name='native-process-submit-failure', daemon=True)
    thread.start()
    storage = workflow.storage
    try:
        assert submit_waiting.wait(20), result
        wait_until(lambda: storage.get_job_status('A', 1) == 'failed', timeout=20)
        owner = storage.read_job_current_owner('A', 1)
        environment = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
        command = subprocess.run(
            [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '1'],
            cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=20,
        )
        assert command.returncode == 0, command.stdout + command.stderr
        release_submit.set()
        thread.join(timeout=25)
        assert not thread.is_alive(), result
        assert [storage.get_job_status('A', n) for n in (1, 2, 3)] == ['done', 'queued', 'queued']
        assert result.get('error') is submission_error, result
        for job_id, count in ((1, 2), (2, 0), (3, 0)):
            assert len([event for event in storage.read_job_events('A', job_id)
                        if event['event'] == 'started']) == count
        replacement = storage.read_job_current_owner('A', 1)
        assert replacement['session_id'] == owner['session_id']
        assert replacement['job_instance_id'] == owner['job_instance_id']
        assert replacement['generation'] == owner['generation'] + 1
        assert storage.get_execution_session(owner['session_id'])['outcome'] == 'failed'
        assert storage.get_component_reservation(('A',)) is None
    finally:
        release_submit.set()
        thread.join(timeout=25)
        assert not thread.is_alive(), result
        _close(storage)


@pytest.mark.parametrize('mode', ['explicit', 'sample'])
def test_cli_selected_run_continues_without_repeating_selection_or_reset(tmp_path, mode):
    from tests.test_active_job_restart import wait_until

    source = tmp_path / 'src'
    behaviors = source / 'node_behavior'
    behaviors.mkdir(parents=True)
    (source / 'graph.py').write_text('''
import time
from pathlib import Path
from micro_workflow_manager.workflow.node_execution_group import NodeExecutionGroup
EDGES = [('A', 'B')]
root = Path(__file__).resolve().parent.parent
original = NodeExecutionGroup._prepare_replacements
def pause_after_absent_successor(group):
    found = original(group)
    checked, release = root / 'cli-exit-checked', root / 'cli-exit-release'
    if not found and not checked.exists():
        checked.write_text('checked', encoding='utf-8')
        deadline = time.monotonic() + 40
        while not release.exists():
            assert time.monotonic() < deadline
            time.sleep(0.01)
    return found
NodeExecutionGroup._prepare_replacements = pause_after_absent_successor
''', encoding='utf-8')
    (behaviors / 'A.py').write_text(
        "from micro_workflow_manager import NodeRouter\n"
        "router = NodeRouter('A', runner='direct')\n"
        "router.create_job(number=3, params={})\n"
        "@router.task\n"
        "def work(ctx):\n"
        "    if ctx.job_id == 2 and ctx.execution_generation == 0:\n"
        "        raise ValueError('selected CLI first attempt failed')\n"
        "    return (ctx.job_id, ctx.execution_generation)\n", encoding='utf-8',
    )
    environment = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
    command_prefix = [sys.executable, '-m', 'micro_workflow_manager']
    for args in (['init'], ['graph', 'src/graph.py', '--runner', 'direct']):
        setup = subprocess.run(
            command_prefix + args, cwd=tmp_path, env=environment,
            capture_output=True, text=True, timeout=25,
        )
        assert setup.returncode == 0, setup.stdout + setup.stderr
    if mode == 'sample':
        prior = subprocess.run(
            command_prefix + ['run', 'A', 'job', '3'], cwd=tmp_path, env=environment,
            capture_output=True, text=True, timeout=25,
        )
        assert prior.returncode == 0, prior.stdout + prior.stderr
    storage = FileStorage(tmp_path)
    prior_sessions = {session['session_id'] for session in storage.list_execution_sessions()}
    unselected_events = storage.read_job_events('A', 3)
    unselected_owner = storage.read_job_current_owner('A', 3)
    args = (['run', 'A', 'jobs', '1', '2'] if mode == 'explicit' else
            ['run', 'A', 'sample', '2', '--seed', 'exit-order', '--status', 'queued'])
    process = subprocess.Popen(
        command_prefix + args, cwd=tmp_path, env=environment,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    release = tmp_path / 'cli-exit-release'
    try:
        wait_until(lambda: (tmp_path / 'cli-exit-checked').exists() or process.poll() is not None, timeout=25)
        assert process.poll() is None, process.communicate(timeout=5)
        owner = storage.read_job_current_owner('A', 2)
        session = storage.get_execution_session(owner['session_id'])
        assert session['status'] == 'running'
        assert session['selected_jobs'] == [('A', 1), ('A', 2)]
        if mode == 'sample':
            assert session['details']['selection']['members']['A']['population_count'] == 2
            assert session['details']['selection']['members']['A']['selected_job_ids'] == [1, 2]
        peer_output = storage.output_file('A', 1).read_bytes()
        peer_events = storage.read_job_events('A', 1)
        restart = subprocess.run(
            command_prefix + ['restart', 'A', 'job', '2'], cwd=tmp_path, env=environment,
            capture_output=True, text=True, timeout=25,
        )
        assert restart.returncode == 0, restart.stdout + restart.stderr
        release.write_text('release', encoding='utf-8')
        stdout, stderr = process.communicate(timeout=25)
        assert process.returncode == 0, stdout + stderr
        label = 'sample jobs' if mode == 'sample' else 'jobs'
        assert stdout.count(f'Ran {label} for A:') == 1
        assert stdout.endswith(f'Ran {label} for A:\n  A/1\n  A/2\n')
        assert storage.output_file('A', 1).read_bytes() == peer_output
        assert storage.read_job_events('A', 1) == peer_events
        assert [storage.get_job_status('A', n) for n in (1, 2, 3)] == [
            'done', 'done', 'done' if mode == 'sample' else 'queued',
        ]
        assert storage.read_job_events('A', 3) == unselected_events
        assert storage.read_job_current_owner('A', 3) == unselected_owner
        assert len([event for event in storage.read_job_events('A', 2) if event['event'] == 'started']) == 2
        replacement = storage.read_job_current_owner('A', 2)
        assert replacement['generation'] == owner['generation'] + 1
        assert replacement['job_instance_id'] == owner['job_instance_id']
        assert replacement['session_id'] == owner['session_id']
        sessions = storage.list_execution_sessions()
        assert {item['session_id'] for item in sessions} == prior_sessions | {owner['session_id']}
        final = storage.get_execution_session(owner['session_id'])
        assert final['outcome'] == 'done'
        assert final['selected_jobs'] == [('A', 1), ('A', 2)]
        assert final['details'] == session['details']
        assert storage.get_component_reservation(('A',)) is None
    finally:
        release.write_text('release', encoding='utf-8')
        if process.poll() is None:
            try:
                process.communicate(timeout=25)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=10)
                raise
        _close(storage)


def test_process_result_transport_failure_is_not_retried(tmp_path):
    source = tmp_path / 'src'
    behaviors = source / 'node_behavior'
    behaviors.mkdir(parents=True)
    graph = source / 'graph.py'
    graph.write_text("EDGES = [('A', 'B')]\n", encoding='utf-8')
    (behaviors / 'A.py').write_text(
        "from micro_workflow_manager import NodeRouter\n"
        "router = NodeRouter('A', runner='process', max_threads=1)\n"
        "@router.task\n"
        "def work(ctx):\n"
        "    return lambda: ctx.job_id\n", encoding='utf-8',
    )
    workflow = MicroWorkflow(tmp_path, runner='process', process_graph_path=graph, persist_graph=False)
    workflow.graph([('A', 'B')])
    workflow.include_node_dir(behaviors)
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'A')
    try:
        with pytest.raises(TypeError, match='results must be pickleable'):
            workflow.run_node('A')
        assert [workflow.storage.get_job_status('A', n) for n in (1, 2)] == ['done', 'queued']
        assert len([event for event in workflow.storage.read_job_events('A', 1) if event['event'] == 'started']) == 1
        assert workflow.storage.list_execution_sessions()[0]['outcome'] == 'failed'
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('change', ['recreate', 'generation', 'owner', 'status', 'restart_marker'])
@pytest.mark.parametrize('entry', ['run_node', 'run_jobs'])
def test_restart_admission_refuses_target_changes_after_payload_reload(tmp_path, monkeypatch, change, entry):
    from tests.test_active_job_restart import wait_until

    monkeypatch.chdir(tmp_path)
    workflow = MicroWorkflow(tmp_path, runner='threaded', persist_graph=False)
    workflow.graph([('A', 'B')])
    peer_started, release_peer, recreated = Event(), Event(), Event()
    router = NodeRouter('A', runner='threaded', max_threads=2)

    @router.task
    def work(ctx):
        if ctx.job_id == 1:
            assert peer_started.wait(15)
            raise ValueError('initial failure')
        peer_started.set()
        assert release_peer.wait(30)
        return 'peer'

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'A')
    storage, result = workflow.storage, {}
    load_job = storage.load_job
    changed_job = []

    def target_state():
        return (storage.read_job_instance_id('A', 1), storage.read_job_control('A', 1),
                storage.get_job_status('A', 1), storage.read_job_current_owner('A', 1),
                load_job('A', 1).params, storage.output_file('A', 1).exists())

    def reload_then_recreate(node, job_id):
        job = load_job(node, job_id)
        if node == 'A' and job_id == 1 and storage.current_job_generation(node, job_id) > 0 and not recreated.is_set():
            if change == 'recreate':
                storage.delete_job(node, job_id, preserve_events=True)
                storage.create_job(Job(node_name=node, job_id=job_id, params={'value': 'new job'}))
            elif change == 'generation':
                _change(storage, "UPDATE jobs SET generation=generation+1 WHERE node_name='A' AND job_id=1")
            elif change == 'owner':
                _change(storage, "UPDATE job_instances SET last_execution_id=NULL WHERE node_name='A' AND job_id=1")
            elif change == 'status':
                _change(storage, "UPDATE jobs SET status='done' WHERE node_name='A' AND job_id=1")
            else:
                _change(storage, "UPDATE jobs SET restart_requested_at=NULL WHERE node_name='A' AND job_id=1")
            changed_job.append(target_state())
            recreated.set()
        return job

    monkeypatch.setattr(storage, 'load_job', reload_then_recreate)

    def run():
        try:
            result['value'] = workflow.run_jobs('A', [2, 1]) if entry == 'run_jobs' else workflow.run_node('A')
        except BaseException as error:
            result['error'] = repr(error)

    thread = Thread(target=run, name='native-recreated-restart', daemon=True)
    thread.start()
    try:
        wait_until(lambda: storage.get_job_status('A', 1) == 'failed', timeout=20)
        environment = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
        command = subprocess.run(
            [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '1'],
            cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=20,
        )
        assert command.returncode == 0, command.stdout + command.stderr
        release_peer.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        assert recreated.is_set()
        assert 'error' in result
        assert target_state() == changed_job[0]
    finally:
        release_peer.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        _close(storage)


@pytest.mark.parametrize('order', ['restart_first', 'terminal_first', 'restart_superseded'])
@pytest.mark.parametrize('runner', ['direct', 'threaded'])
@pytest.mark.parametrize('entry', ['run_jobs', 'run_node_jobs', 'run_node', 'run_queued_node_jobs'])
def test_restart_and_session_exit_have_one_durable_decision(tmp_path, monkeypatch, order, runner, entry):
    monkeypatch.chdir(tmp_path)
    workflow = MicroWorkflow(tmp_path, runner=runner, persist_graph=False)
    workflow.graph([('A', 'B')])
    router = NodeRouter('A', runner=runner, max_threads=2)
    ready, decision_ready, release_decision = Barrier(2), Event(), Event()
    job_count = 130 if (entry, runner, order) == ('run_node', 'threaded', 'restart_first') else 3
    values = {job_id: object() for job_id in range(1, job_count + 1)}
    completed_value, replacement_value = values[1], values[2]
    starts, result = [], {}
    whole_node = entry in {'run_node', 'run_queued_node_jobs'}
    selected = list(range(1, job_count + 1)) if whole_node else ([1, 2] if runner == 'direct' else [2, 1])

    @router.task
    def work(ctx):
        starts.append((ctx.job_id, ctx.execution_generation))
        if ctx.job_id >= 3:
            return values[ctx.job_id]
        if ctx.execution_generation == 0:
            if runner == 'threaded':
                ready.wait(timeout=15)
            if ctx.job_id == 2:
                raise ValueError('first attempt fails before session exit')
            return completed_value
        return replacement_value

    workflow.include_routers(router)
    for _ in range(job_count):
        workflow.add_job(None, 'A')
    storage = workflow.storage
    decide = storage.decide_execution_session_exit
    load_job = storage.load_job
    superseded = []

    def load_then_replace_again(node, job_id):
        job = load_job(node, job_id)
        if (order == 'restart_superseded' and node == 'A' and job_id == 2
                and storage.current_job_generation(node, job_id) == 1 and not superseded):
            superseded.append(True)
            workflow.cancel_job(node, job_id)
            assert storage.get_job_status(node, job_id) == 'cancelled'
            second = subprocess.run(
                [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '2'],
                cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=20,
            )
            assert second.returncode == 0, second.stdout + second.stderr
        return job

    monkeypatch.setattr(storage, 'load_job', load_then_replace_again)

    def pause_before_failed_decision(*args, **kwargs):
        if kwargs.get('outcome') == 'failed' and not decision_ready.is_set():
            decision_ready.set()
            assert release_decision.wait(30)
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, 'decide_execution_session_exit', pause_before_failed_decision)

    def run():
        try:
            if entry == 'run_jobs':
                result['value'] = workflow.run_jobs('A', selected)
            elif entry == 'run_node_jobs':
                result['value'] = workflow.run_node_jobs('A', [storage.load_job('A', n) for n in selected])
            elif entry == 'run_node':
                result['value'] = workflow.run_node('A')
            else:
                result['value'] = workflow.run_queued_node_jobs('A')
        except BaseException as error:
            result['error'] = repr(error)

    thread = Thread(target=run, name='native-session-exit-restart', daemon=True)
    thread.start()
    try:
        assert decision_ready.wait(20), result
        owner = storage.read_job_current_owner('A', 2)
        assert storage.get_execution_session(owner['session_id'])['status'] == 'running'
        assert storage.get_component_reservation(('A',))['session_id'] == owner['session_id']
        assert storage.get_job_status('A', 1) == 'done'
        original_starts = list(starts)
        if order == 'terminal_first':
            release_decision.set()
            thread.join(timeout=20)
            assert not thread.is_alive(), result
            assert storage.get_execution_session(owner['session_id'])['status'] == 'terminal'
        before = _snapshot(storage)
        environment = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
        command = subprocess.run(
            [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '2'],
            cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=20,
        )
        if order == 'terminal_first':
            assert command.returncode == 1, command.stdout + command.stderr
            assert _snapshot(storage) == before
            assert 'error' in result
            assert sorted(starts) == sorted(original_starts)
            return
        assert command.returncode == 0, command.stdout + command.stderr
        assert storage.get_execution_session(owner['session_id'])['status'] == 'running'
        assert storage.get_component_reservation(('A',))['session_id'] == owner['session_id']
        release_decision.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        assert 'error' not in result, result
        assert len(result['value']) == len(selected)
        assert all(value is values[job_id] for value, job_id in zip(result['value'], selected))
        generation = 2 if order == 'restart_superseded' else 1
        assert sorted(starts) == [(1, 0), (2, 0), (2, generation)] + (
            [(job_id, 0) for job_id in range(3, job_count + 1)] if whole_node else []
        )
        assert bool(superseded) == (order == 'restart_superseded')
        assert storage.get_job_status('A', 3) == ('done' if whole_node else 'queued')
        sessions = storage.list_execution_sessions()
        assert len(sessions) == 1
        assert sessions[0]['session_id'] == owner['session_id']
        assert sessions[0]['outcome'] == 'done'
        assert storage.get_component_reservation(('A',)) is None
    finally:
        release_decision.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        _close(storage)
