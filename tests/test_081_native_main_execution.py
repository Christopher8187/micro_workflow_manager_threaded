from __future__ import annotations

import time
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event

import pytest

from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager import cli
from micro_workflow_manager.cli.run_orchestration import run_nodes
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.errors import JobFailedError
from micro_workflow_manager.monitor import InlineMonitorReporter, InlineStatsReporter
from micro_workflow_manager.storage import FileStorage


def _close(storage):
    storage.db_mutation_barrier()
    deadline = time.perf_counter() + 10
    while storage.mutation_writer_diagnostics()['writer_alive']:
        assert time.perf_counter() < deadline, 'Mutation writer did not retire'
        time.sleep(0.01)
    storage.close_database_connections()


@pytest.mark.parametrize('runner', ['direct', 'api'])
def test_ordinary_main_owns_real_component_claims_and_releases_its_scope(tmp_path, runner):
    workflow = MicroWorkflow(tmp_path, runner=runner, persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A')])
    observed = []

    def work(ctx):
        storage = ctx.system.storage
        main = storage.get_live_main_session()
        assert main is not None
        owner = storage.get_job_execution_owner(ctx.execution_id)
        assert owner['session_id'] == main['session_id']
        assert owner['component'] == ('A', 'B')
        assert storage.get_component_reservation(('A', 'B')) == {
            'members': ('A', 'B'), 'session_id': main['session_id'],
        }
        assert not (tmp_path / '.mwf' / 'run.json').exists()
        observed.append((ctx.current_node, ctx.execution_id, main['session_id']))
        return ctx.current_node

    for name in ('A', 'B'):
        router = NodeRouter(name, runner=runner, max_threads=2)
        router.task(work)
        workflow.include_routers(router)
        workflow.add_job(None, name)

    try:
        assert run_nodes(workflow, ['A', 'B'], 'A') == 0
        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 1
        main = sessions[0]
        assert main['session_kind'] == 'main'
        assert main['command'] == 'run'
        assert main['start_component'] == ('A', 'B')
        assert main['selected_components'] == [('A', 'B')]
        assert main['selected_jobs'] == []
        assert main['status'] == 'terminal'
        assert main['outcome'] == 'done'
        assert main['failures'] == []
        assert workflow.storage.get_live_main_session() is None
        assert workflow.storage.get_component_reservation(('A', 'B')) is None
        assert {node for node, _, _ in observed} == {'A', 'B'}
        for node, execution_id, session_id in observed:
            assert session_id == main['session_id']
            assert workflow.storage.get_job_status(node, 1) == 'done'
            events = [event for event in workflow.storage.read_job_events(node, 1)
                      if event['event'] == 'started']
            assert len(events) == 1
            assert events[0]['execution_id'] == execution_id
            assert events[0]['session_id'] == session_id
            assert events[0]['component'] == ['A', 'B']
        assert not (tmp_path / '.mwf' / 'run.json').exists()
    finally:
        _close(workflow.storage)


def test_process_workers_claim_for_the_parent_native_main_session(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    source = tmp_path / 'src'
    behaviors = source / 'node_behavior'
    behaviors.mkdir(parents=True)
    (source / 'graph.py').write_text("EDGES = [('A', 'B'), ('B', 'A')]\n", encoding='utf-8')
    for name in ('A', 'B'):
        (behaviors / f'{name}.py').write_text(
            'from micro_workflow_manager import NodeRouter\n'
            f'router = NodeRouter({name!r}, runner="process", max_threads=1)\n'
            'router.create_job(params={})\n'
            '@router.task\n'
            'def work(ctx):\n'
            '    storage = ctx.system.storage\n'
            '    owner = storage.get_job_execution_owner(ctx.execution_id)\n'
            '    main = storage.get_live_main_session()\n'
            '    assert main is not None\n'
            '    assert owner["session_id"] == main["session_id"]\n'
            '    assert owner["component"] == ("A", "B")\n'
            '    return main["session_id"]\n',
            encoding='utf-8',
        )
    assert cli.main(['init']) == 0
    assert cli.main(['graph', 'src/graph.py', '--runner', 'process']) == 0
    assert cli.main(['run', 'A']) == 0
    storage = FileStorage(tmp_path)
    try:
        sessions = storage.list_execution_sessions()
        assert len(sessions) == 1
        main = sessions[0]
        assert main['outcome'] == 'done'
        assert main['selected_components'] == [('A', 'B')]
        assert storage.get_component_reservation(('A', 'B')) is None
        for node in ('A', 'B'):
            assert storage.get_job_status(node, 1) == 'done'
            events = [event for event in storage.read_job_events(node, 1) if event['event'] == 'started']
            assert len(events) == 1
            owner = storage.get_job_execution_owner(events[0]['execution_id'])
            assert owner['session_id'] == main['session_id']
            assert owner['component'] == ('A', 'B')
        assert not (tmp_path / '.mwf' / 'run.json').exists()
    finally:
        _close(storage)


@pytest.mark.parametrize('fault', ['reservation', 'runtime-limits'])
def test_failed_native_main_startup_releases_owned_state_and_allows_retry(tmp_path, monkeypatch, fault):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    ran = []
    router = NodeRouter('A', runner='direct')

    @router.task
    def work(ctx):
        ran.append(ctx.execution_id)
        return 'done'

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    workflow.storage.set_thread_override('A', 5)
    method = 'reserve_execution_components' if fault == 'reservation' else 'bind_thread_overrides_to_run'
    original = getattr(workflow.storage, method)

    def fail_after_change(*args, **kwargs):
        original(*args, **kwargs)
        raise OSError('injected native session startup failure')

    try:
        with monkeypatch.context() as patch:
            patch.setattr(workflow.storage, method, fail_after_change)
            with pytest.raises(OSError, match='injected native session startup failure'):
                run_nodes(workflow, ['A'], 'A')
        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 1
        failed = sessions[0]
        assert failed['status'] == 'terminal'
        assert failed['outcome'] == 'failed'
        assert 'injected native session startup failure' in failed['failures'][0]['error']
        assert workflow.storage.get_live_main_session() is None
        assert workflow.storage.get_component_reservation(('A',)) is None
        assert ran == []
        with pytest.raises(RuntimeError, match='No active execution session owns node A'):
            workflow.execution_claim_context('A')
        assert workflow.storage.read_thread_overrides() == ({'A': 5} if fault == 'reservation' else {})
        assert run_nodes(workflow, ['A'], 'A') == 0
        assert len(ran) == 1
        owner = workflow.storage.get_job_execution_owner(ran[0])
        assert owner['session_id'] != failed['session_id']
        assert workflow.storage.get_execution_session(owner['session_id'])['outcome'] == 'done'
        assert workflow.storage.read_thread_overrides() == {}
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('second_start', ['A', 'C'])
def test_another_process_cannot_start_a_second_native_main_before_preparation(tmp_path, second_start):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    edges = [('A', 'B'), ('B', 'A'), ('C', 'D'), ('D', 'C')]
    workflow.graph(edges)
    admitted = Event()
    release = Event()
    for name in ('A', 'B'):
        router = NodeRouter(name, runner='direct')
        router.task(lambda ctx: 'done')
        workflow.include_routers(router)
        workflow.add_job(None, name)

    def prepare():
        admitted.set()
        assert release.wait(30), 'The competing-process check did not release the first main'

    code = '''
import sys
from pathlib import Path
from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.cli.run_orchestration import run_nodes
root = Path(sys.argv[1])
workflow = MicroWorkflow(root, runner='direct', persist_graph=False)
workflow.graph([('A', 'B'), ('B', 'A'), ('C', 'D'), ('D', 'C')])
start = sys.argv[2]
scope = ['A', 'B'] if start == 'A' else ['C', 'D']
run_nodes(workflow, scope, start, prepare=lambda: (root / 'second-preparation.txt').write_text('ran'))
'''
    pool = ThreadPoolExecutor(max_workers=1)
    future = pool.submit(run_nodes, workflow, ['A', 'B'], 'A', prepare=prepare)
    try:
        assert admitted.wait(10), 'The first native main never reached preparation'
        first = workflow.storage.get_live_main_session()
        assert first is not None
        result = subprocess.run(
            [sys.executable, '-c', code, str(tmp_path), second_start],
            capture_output=True, text=True, timeout=20,
        )
        assert result.returncode != 0
        assert 'already active' in result.stderr
        assert not (tmp_path / 'second-preparation.txt').exists()
        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 1
        assert sessions[0]['session_id'] == first['session_id']
        assert sessions[0]['selected_components'] == [('A', 'B')]
        assert workflow.storage.get_component_reservation(('A', 'B'))['session_id'] == first['session_id']
        assert workflow.storage.get_component_reservation(('C', 'D')) is None
    finally:
        release.set()
        try:
            assert future.result(timeout=30) == 0
        finally:
            pool.shutdown(wait=True)
            _close(workflow.storage)


def test_native_heartbeat_cannot_change_a_finished_session_and_reuse_gets_a_new_owner(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner='threaded', persist_graph=False)
    workflow.graph([('A', 'B')])
    router = NodeRouter('A', runner='threaded', max_threads=5)
    admitted = Event()
    release_task = Event()
    stale_heartbeat = Event()
    release_heartbeat = Event()
    heartbeat_finished = Event()
    original = workflow.storage.heartbeat_execution_session
    calls = 0
    owners = []
    heartbeat_times = []
    delayed_outcomes = []

    def heartbeat(session_id, heartbeat_at):
        nonlocal calls
        calls += 1
        heartbeat_times.append(heartbeat_at)
        delayed = calls == 2
        if delayed:
            stale_heartbeat.set()
            assert release_heartbeat.wait(15), 'The late heartbeat was not released'
        try:
            changed = original(session_id, heartbeat_at)
            if delayed:
                delayed_outcomes.append(changed)
            return changed
        finally:
            if delayed:
                heartbeat_finished.set()

    @router.task
    def work(ctx):
        owners.append(ctx.system.storage.get_job_execution_owner(ctx.execution_id))
        if ctx.current_job.job_id == 1:
            admitted.set()
            assert release_task.wait(15), 'The test did not release the first handler'
        return 'done'

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    workflow.storage.set_thread_override('A', 2)
    workflow.storage.set_api_total_limit(3)
    monkeypatch.setattr(workflow.storage, 'heartbeat_execution_session', heartbeat)
    pool = ThreadPoolExecutor(max_workers=1)
    future = pool.submit(run_nodes, workflow, ['A'], 'A')
    try:
        assert admitted.wait(10)
        first = workflow.storage.get_live_main_session()
        assert first is not None
        assert workflow.effective_max_threads('A') == 2
        assert workflow.api_total_limit_override() == 3
        assert stale_heartbeat.wait(10), 'The supervisor did not schedule two native heartbeats'
        live = workflow.storage.get_execution_session(first['session_id'])
        # The initial read can already include the first published heartbeat.
        # The second call is still held before publication at this point.
        assert live['heartbeat_at'] == heartbeat_times[0]
        release_task.set()
        assert future.result(timeout=10) == 0
        terminal = workflow.storage.get_execution_session(first['session_id'])
        assert terminal['status'] == 'terminal'
        assert workflow.storage.get_component_reservation(('A',)) is None
        release_heartbeat.set()
        assert heartbeat_finished.wait(10)
        assert delayed_outcomes == [False]
        assert workflow.storage.get_execution_session(first['session_id']) == terminal
        assert workflow.storage.read_thread_overrides() == {}
        assert workflow.storage.read_api_total_limit() is None
        workflow.add_job(None, 'A')
        assert workflow.run_job('A', 2) == 'done'
        assert len(owners) == 2
        assert owners[0]['session_id'] == first['session_id']
        assert owners[1]['session_id'] != first['session_id']
        assert [owner['job_id'] for owner in owners] == [1, 2]
        assert workflow.storage.get_execution_session(first['session_id']) == terminal
        second = workflow.storage.get_execution_session(owners[1]['session_id'])
        assert second['outcome'] == 'done'
        assert second['selected_jobs'] == [('A', 2)]
        assert workflow.storage.get_component_reservation(('A',)) is None
        with pytest.raises(RuntimeError, match='No active execution session owns node A'):
            workflow.execution_claim_context('A')
        assert workflow.storage.get_live_main_session() is None
        assert not (tmp_path / '.mwf' / 'run.json').exists()
    finally:
        release_task.set()
        release_heartbeat.set()
        try:
            future.result(timeout=15)
        finally:
            pool.shutdown(wait=True)
            _close(workflow.storage)


def test_handler_failure_keeps_exact_claim_history_and_finishes_native_main(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    router = NodeRouter('A', runner='direct')

    @router.task
    def work(ctx):
        raise ValueError('native task failure')

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    try:
        with pytest.raises(JobFailedError):
            run_nodes(workflow, ['A'], 'A')
        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 1
        failed = sessions[0]
        assert failed['status'] == 'terminal'
        assert failed['outcome'] == 'failed'
        assert failed['failures']
        assert workflow.storage.get_job_status('A', 1) == 'failed'
        events = [event for event in workflow.storage.read_job_events('A', 1) if event['event'] == 'started']
        assert len(events) == 1
        owner = workflow.storage.get_job_execution_owner(events[0]['execution_id'])
        assert owner['session_id'] == failed['session_id']
        assert owner['component'] == ('A',)
        assert workflow.storage.get_component_reservation(('A',)) is None
        assert workflow.storage.get_live_main_session() is None
        with pytest.raises(RuntimeError, match='No active execution session owns node A'):
            workflow.execution_claim_context('A')
        assert not (tmp_path / '.mwf' / 'run.json').exists()
    finally:
        _close(workflow.storage)


def test_failure_with_broken_repr_preserves_original_error_and_failed_session(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])

    class BrokenReprError(Exception):
        def __repr__(self):
            raise ValueError('exception repr is broken')

    failure = BrokenReprError('native preparation failed')

    def prepare():
        raise failure

    try:
        with pytest.raises(BrokenReprError) as observed:
            run_nodes(workflow, ['A'], 'A', prepare=prepare)
        assert observed.value is failure
        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 1
        assert sessions[0]['status'] == 'terminal'
        assert sessions[0]['outcome'] == 'failed'
        assert sessions[0]['failures']
        assert workflow.storage.get_component_reservation(('A',)) is None
        assert workflow.storage.get_live_main_session() is None
    finally:
        _close(workflow.storage)


def test_failed_session_record_read_during_creation_cannot_leave_a_running_main(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    original = workflow.storage._execution_session_from_row
    observed = []

    def read_and_fail(connection, row):
        record = original(connection, row)
        observed.append(record['session_id'])
        raise OSError('injected session record read failure')

    try:
        with monkeypatch.context() as patch:
            patch.setattr(workflow.storage, '_execution_session_from_row', read_and_fail)
            with pytest.raises(OSError, match='injected session record read failure'):
                run_nodes(workflow, ['A'], 'A')
        assert observed, 'The persisted session record read was not reached'
        assert all(session['status'] == 'terminal' for session in workflow.storage.list_execution_sessions())
        assert workflow.storage.get_live_main_session() is None
        assert workflow.storage.get_component_reservation(('A',)) is None
        with pytest.raises(RuntimeError, match='No active execution session owns node A'):
            workflow.execution_claim_context('A')
        assert run_nodes(workflow, ['A'], 'A') == 0
    finally:
        _close(workflow.storage)


def test_retrying_terminal_publication_preserves_the_failed_outcome(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    original = workflow.storage.decide_execution_session_exit
    interrupted = False
    attempts = []
    preparation_error = ValueError('the original preparation failed')

    def finish(session_id, **kwargs):
        nonlocal interrupted
        attempts.append((session_id, kwargs))
        if not interrupted:
            interrupted = True
            raise OSError('injected terminal publication failure')
        return original(session_id, **kwargs)

    def prepare():
        raise preparation_error

    monkeypatch.setattr(workflow.storage, 'decide_execution_session_exit', finish)
    try:
        with pytest.raises(ValueError, match='the original preparation failed') as observed:
            run_nodes(workflow, ['A'], 'A', prepare=prepare)
        assert observed.value is preparation_error
        assert any('injected terminal publication failure' in note for note in observed.value.__notes__)
        assert interrupted
        assert len(attempts) == 1
        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 1
        assert sessions[0]['status'] == 'running'
        assert workflow.storage.get_component_reservation(('A',))['session_id'] == sessions[0]['session_id']
        # An explicit retry uses the retained failed decision; unwinding does not retry it.
        session_id, request = attempts[0]
        workflow.storage.decide_execution_session_exit(session_id, **request)
        assert len(attempts) == 2
        sessions = workflow.storage.list_execution_sessions()
        assert sessions[0]['status'] == 'terminal'
        assert sessions[0]['outcome'] == 'failed'
        assert 'the original preparation failed' in sessions[0]['failures'][0]['error']
        assert workflow.storage.get_component_reservation(('A',)) is None
        assert workflow.storage.get_live_main_session() is None
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('reporter_type', [InlineStatsReporter, InlineMonitorReporter])
def test_reporter_teardown_failure_cannot_leave_native_main_running(tmp_path, monkeypatch, reporter_type):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    original = reporter_type.stop_periodic

    def stop_and_fail(reporter):
        original(reporter)
        raise OSError('injected reporter teardown failure')

    try:
        with monkeypatch.context() as patch:
            patch.setattr(reporter_type, 'stop_periodic', stop_and_fail)
            with pytest.raises(OSError, match='injected reporter teardown failure'):
                run_nodes(workflow, ['A'], 'A')
        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 1
        assert sessions[0]['status'] == 'terminal'
        assert sessions[0]['outcome'] == 'done'
        assert workflow.storage.get_component_reservation(('A',)) is None
        assert workflow.storage.get_live_main_session() is None
        with pytest.raises(RuntimeError, match='No active execution session owns node A'):
            workflow.execution_claim_context('A')
        assert run_nodes(workflow, ['A'], 'A') == 0
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('reporter_type', [InlineStatsReporter, InlineMonitorReporter])
def test_reporter_cleanup_preserves_an_existing_body_failure(tmp_path, monkeypatch, reporter_type):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    original = reporter_type.stop_periodic
    failure = ValueError('the preparation body failed')

    def stop_and_fail(reporter):
        original(reporter)
        raise OSError('the diagnostic reporter also failed')

    def prepare():
        raise failure

    monkeypatch.setattr(reporter_type, 'stop_periodic', stop_and_fail)
    try:
        with pytest.raises(ValueError, match='the preparation body failed') as observed:
            run_nodes(workflow, ['A'], 'A', prepare=prepare)
        assert observed.value is failure
        assert isinstance(observed.value.__cause__, OSError)
        assert str(observed.value.__cause__) == 'the diagnostic reporter also failed'
        session = workflow.storage.list_execution_sessions()[0]
        assert session['status'] == 'terminal'
        assert session['outcome'] == 'failed'
        assert 'the preparation body failed' in session['failures'][0]['error']
        assert workflow.storage.get_component_reservation(('A',)) is None
        assert workflow.storage.get_live_main_session() is None
        assert not workflow.storage.heartbeat_execution_session(session['session_id'], '2100-01-01T00:00:00+00:00')
        assert workflow.storage.get_execution_session(session['session_id']) == session
        with pytest.raises(RuntimeError, match='No active execution session owns node A'):
            workflow.execution_claim_context('A')
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('change', ['edges', 'autostart', 'unchanged'])
def test_process_workers_require_the_admitted_graph_shape(tmp_path, monkeypatch, change):
    monkeypatch.chdir(tmp_path)
    source = tmp_path / 'src'
    behaviors = source / 'node_behavior'
    behaviors.mkdir(parents=True)
    graph = source / 'graph.py'
    graph.write_text("EDGES = [('A', 'B')]\n", encoding='utf-8')
    for name in ('A', 'B'):
        (behaviors / f'{name}.py').write_text(
            'from micro_workflow_manager import NodeRouter\n'
            f'router = NodeRouter({name!r}, runner="process", max_threads=1)\n'
            'router.create_job(params={})\n'
            '@router.task\n'
            'def work(ctx):\n'
            '    assert ctx.system.component_id(ctx.current_node) == ("A", "B")\n'
            '    (ctx.system.storage.project_dir / "handler-ran.txt").write_text("ran")\n'
            '    return ctx.current_node\n'
            + ('def declared_autostart(ctx):\n    ctx.node("B").add(autostart=True)\n' if name == 'A' else ''),
            encoding='utf-8',
        )
    assert cli.main(['init']) == 0
    assert cli.main(['graph', 'src/graph.py', '--runner', 'process']) == 0
    workflow = load_workflow(tmp_path)
    assert workflow.component_id('A') == ('A', 'B')

    def prepare():
        if change == 'edges':
            graph.write_text("EDGES = [('A', 'B'), ('B', 'A')]\n", encoding='utf-8')
        elif change == 'autostart':
            behavior = behaviors / 'A.py'
            behavior.write_text(behavior.read_text(encoding='utf-8').replace('autostart=True', 'autostart=False'), encoding='utf-8')

    try:
        if change == 'unchanged':
            assert run_nodes(workflow, ['A', 'B'], 'A', prepare=prepare) == 0
        else:
            with pytest.raises(Exception):
                run_nodes(workflow, ['A', 'B'], 'A', prepare=prepare)
            assert not (tmp_path / 'handler-ran.txt').exists()
            for node in ('A', 'B'):
                assert workflow.storage.get_job_status(node, 1) == 'queued'
                assert not any(event['event'] == 'started' for event in workflow.storage.read_job_events(node, 1))
        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 1
        assert sessions[0]['status'] == 'terminal'
        assert sessions[0]['outcome'] == ('done' if change == 'unchanged' else 'failed')
        assert workflow.storage.get_component_reservation(('A', 'B')) is None
        assert workflow.storage.get_live_main_session() is None
    finally:
        _close(workflow.storage)


def test_next_main_waits_for_terminal_reservations_to_be_released(tmp_path, monkeypatch):
    first = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    second = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    for workflow in (first, second):
        workflow.graph([('A', 'B')])
    terminal = Event()
    release = Event()
    second_attempt = Event()
    second_entered = Event()
    original_decision = first.storage.decide_execution_session_exit
    original_lock = second.storage.interprocess_lock

    def gated_decision(session_id, **kwargs):
        decision = original_decision(session_id, **kwargs)
        assert first.storage.get_execution_session(session_id)['status'] == 'terminal'
        assert first.storage.get_component_reservation(('A',)) is None
        terminal.set()
        assert release.wait(20), 'The terminal teardown check did not release its gate'
        return decision

    @contextmanager
    def observe_admission(name, **kwargs):
        if name == 'active-run-state':
            second_attempt.set()
        with original_lock(name, **kwargs):
            if name == 'active-run-state':
                second_entered.set()
            yield

    monkeypatch.setattr(first.storage, 'decide_execution_session_exit', gated_decision)
    monkeypatch.setattr(second.storage, 'interprocess_lock', observe_admission)
    pool = ThreadPoolExecutor(max_workers=2)
    first_future = pool.submit(run_nodes, first, ['A'], 'A')
    second_future = None
    try:
        assert terminal.wait(10), 'The first session did not reach terminal teardown'
        second_future = pool.submit(run_nodes, second, ['A'], 'A')
        assert second_attempt.wait(10), 'The second session did not attempt admission'
        assert not second_entered.wait(0.5), 'Admission overtook terminal reservation cleanup'
        assert len(first.storage.list_execution_sessions()) == 1
        release.set()
        assert first_future.result(timeout=10) == 0
        assert second_future.result(timeout=10) == 0
        sessions = first.storage.list_execution_sessions()
        assert len(sessions) == 2
        assert all(session['status'] == 'terminal' and session['outcome'] == 'done' for session in sessions)
        assert first.storage.get_component_reservation(('A',)) is None
    finally:
        release.set()
        pool.shutdown(wait=True, cancel_futures=True)
        _close(first.storage)
        _close(second.storage)


@pytest.mark.parametrize('removed_count', [1, 2])
def test_missing_reserved_scope_is_reported_during_terminal_cleanup(tmp_path, monkeypatch, removed_count):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])

    original = workflow.storage.decide_execution_session_exit

    def finish(session_id, **kwargs):
        session_id = workflow.storage.get_live_main_session()['session_id']
        assert all(workflow.storage.get_component_state((node,))['lifecycle'] == 'done' for node in ('A', 'B'))
        with workflow.storage.db_transaction() as connection:
            keys = connection.execute(
                'SELECT component_key FROM component_reservations WHERE session_id=? ORDER BY component_key',
                (session_id,),
            ).fetchall()
            assert len(keys) == 2
            connection.executemany('DELETE FROM component_reservations WHERE session_id=? AND component_key=?',
                                   [(session_id, row['component_key']) for row in keys[:removed_count]])
        return original(session_id, **kwargs)

    try:
        monkeypatch.setattr(workflow.storage, 'decide_execution_session_exit', finish)
        with pytest.raises(RuntimeError, match='reservation cleanup.*expected 2'):
            run_nodes(workflow, ['A', 'B'], 'A')
        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 1
        assert sessions[0]['status'] == 'terminal'
        assert workflow.storage.get_live_main_session() is None
        assert workflow.storage.get_component_reservation(('A',)) is None
        assert workflow.storage.get_component_reservation(('B',)) is None
        with pytest.raises(RuntimeError, match='No active execution session owns node A'):
            workflow.execution_claim_context('A')
    finally:
        _close(workflow.storage)


def test_session_selection_uses_the_scheduler_component_order(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('Z', 'A'), ('X', 'Y')])
    selected_nodes = ['A', 'Y', 'X', 'Z']
    expected = workflow.execution_components(selected_nodes)
    assert expected.index(('Z',)) < expected.index(('A',))
    assert expected.index(('X',)) < expected.index(('Y',))
    try:
        assert run_nodes(workflow, selected_nodes, 'Z') == 0
        session = workflow.storage.list_execution_sessions()[0]
        assert session['selected_components'] == expected
        assert all(workflow.storage.get_component_reservation(component) is None for component in expected)
    finally:
        _close(workflow.storage)
