from __future__ import annotations

import os
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import pytest

from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.errors import JobFailedError
from micro_workflow_manager.storage import FileStorage
from tests.test_090_component_session_settlement import _close, _rows


@pytest.mark.parametrize('runner', ['direct', 'threaded'])
def test_parent_completion_is_durable_before_child_execution(tmp_path, runner):
    workflow = MicroWorkflow(tmp_path, runner=runner, persist_graph=False)
    workflow.graph([('A', 'B')])
    parent, child = NodeRouter('A', runner=runner), NodeRouter('B', runner=runner)
    storage = workflow.storage
    observations = []

    @parent.task
    def parent_work(ctx):
        observations.append(('parent', storage.get_component_state(('A',))))
        return 'parent-result'

    @child.task
    def child_work(ctx):
        owner = storage.read_job_current_owner('B', ctx.job_id)
        observations.append(('child', {
            'parent': storage.get_component_state(('A',)),
            'child': storage.get_component_state(('B',)),
            'session': storage.get_execution_session(owner['session_id']),
            'parent_reservation': storage.get_component_reservation(('A',)),
            'child_reservation': storage.get_component_reservation(('B',)),
        }))
        return 'child-result'

    workflow.include_routers(parent, child)
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'B')
    try:
        assert workflow.run() == ['A', 'B']
        assert [name for name, _ in observations] == ['parent', 'child']
        assert observations[0][1]['lifecycle'] == 'running'
        observed = observations[1][1]
        assert (observed['parent']['lifecycle'], observed['parent']['stability']) == ('done', 'stable')
        assert observed['child']['lifecycle'] == 'running'
        assert observed['session']['status'] == 'running'
        session_id = observed['session']['session_id']
        assert observed['parent_reservation']['session_id'] == session_id
        assert observed['child_reservation']['session_id'] == session_id
        assert storage.get_execution_session(session_id)['outcome'] == 'done'
        for component in [('A',), ('B',)]:
            state = storage.get_component_state(component)
            assert (state['lifecycle'], state['stability'], state['instability_origin']) == ('done', 'stable', None)
            assert storage.get_component_reservation(component) is None
        outputs = {node: storage.output_file(node, 1).read_bytes() for node in ('A', 'B')}
        events = {node: storage.read_job_events(node, 1) for node in ('A', 'B')}
        assert json.loads(outputs['A'])['result_repr'] == "'parent-result'"
        assert json.loads(outputs['B'])['result_repr'] == "'child-result'"
        assert all(len([event for event in rows if event['event'] == 'started']) == 1 for rows in events.values())
    finally:
        _close(storage)
    reopened = FileStorage(tmp_path)
    try:
        assert reopened.get_execution_session(session_id)['status'] == 'terminal'
        for node in ('A', 'B'):
            assert reopened.get_component_state((node,))['lifecycle'] == 'done'
            assert reopened.get_component_reservation((node,)) is None
            assert reopened.output_file(node, 1).read_bytes() == outputs[node]
            assert reopened.read_job_events(node, 1) == events[node]
    finally:
        _close(reopened)


@pytest.mark.parametrize('parent_done', [False, True])
@pytest.mark.parametrize('predicate_allows', [False, True])
def test_custom_readiness_can_only_narrow_native_component_admission(tmp_path, parent_done, predicate_allows):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    parent, child = NodeRouter('A', runner='direct'), NodeRouter('B', runner='direct')
    storage = workflow.storage
    calls, checks = [], []

    @parent.task
    def parent_work(ctx):
        calls.append('A')
        return 'parent result'

    @child.task
    def child_work(ctx):
        calls.append('B')
        return 'child result'

    def ready_check(node):
        checks.append(node)
        return predicate_allows

    workflow.include_routers(parent, child)
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'B')
    try:
        if parent_done:
            workflow.run_component({'A'})
        parent_events = storage.read_job_events('A', 1)
        child_events = storage.read_job_events('B', 1)
        calls.clear()
        expected = ['B'] if parent_done and predicate_allows else []
        assert workflow.run_concurrently(nodes=['B'], ready_check=ready_check) == expected
        assert calls == expected
        assert checks == [] if not parent_done else checks and set(checks) == {'B'}
        assert storage.read_job_events('A', 1) == parent_events
        assert storage.get_component_state(('A',))['lifecycle'] == ('done' if parent_done else 'queued')
        assert storage.get_component_state(('B',))['lifecycle'] == ('done' if expected else 'queued')
        assert storage.get_job_status('B', 1) == ('done' if expected else 'queued')
        if not expected:
            assert storage.read_job_events('B', 1) == child_events
            assert not storage.output_file('B', 1).exists()
        session = storage.list_execution_sessions()[-1]
        assert (session['status'], session['outcome']) == ('terminal', 'done')
        assert storage.get_component_reservation(('B',)) is None
    finally:
        _close(storage)


def test_child_admission_refuses_a_parent_changed_after_observation(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    other = FileStorage(tmp_path)
    parent, child = NodeRouter('A', runner='direct'), NodeRouter('B', runner='direct')
    parent_called, child_begin, observed, proceed = Event(), Event(), Event(), Event()
    trace_errors, child_calls = [], []
    begin_component = workflow.begin_full_component_execution

    def begin_child(component, **kwargs):
        if tuple(sorted(component)) == ('B',):
            child_begin.set()
        try:
            return begin_component(component, **kwargs)
        finally:
            child_begin.clear()

    monkeypatch.setattr(workflow, 'begin_full_component_execution', begin_child)

    def trace(sql):
        if (sql == 'RELEASE SAVEPOINT mwf_component_observation'
                and parent_called.is_set() and child_begin.is_set() and not observed.is_set()):
            observed.set()
            if not proceed.wait(10):
                trace_errors.append('Parent change did not release admission')

    @parent.task
    def parent_work(ctx):
        parent_called.set()
        return 'parent-result'

    @child.task
    def child_work(ctx):
        child_calls.append(ctx.job_id)
        return 'child-result'

    workflow.include_routers(parent, child)
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'B')

    def run():
        storage.db_connection().set_trace_callback(trace)
        try:
            return workflow.run()
        finally:
            storage.db_connection().set_trace_callback(None)

    child_events = storage.read_job_events('B', 1)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(run)
            try:
                assert observed.wait(5), 'Child did not observe its successful parent'
                assert other.get_component_state(('A',))['lifecycle'] == 'done'
                other.submit_db_mutation(lambda connection: connection.execute(
                    "UPDATE component_states SET lifecycle='failed', stability=NULL "
                    "WHERE component_key='[\"A\"]'",
                ))
            finally:
                proceed.set()
            with pytest.raises(RuntimeError, match='parent.*changed'):
                future.result(timeout=15)
        assert not trace_errors
        assert child_calls == []
        assert storage.get_component_state(('B',))['lifecycle'] == 'queued'
        assert storage.read_job_events('B', 1) == child_events
        assert storage.get_component_reservation(('B',)) is None
    finally:
        proceed.set()
        _close(other)
        _close(storage)


def test_failed_parent_settles_with_session_and_leaves_child_unstarted(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    parent, child = NodeRouter('A', runner='direct'), NodeRouter('B', runner='direct')
    observations, child_calls = [], []

    @parent.task
    def parent_work(ctx):
        observations.append(storage.get_component_state(('A',)))
        raise ValueError('parent failure')

    @child.task
    def child_work(ctx):
        child_calls.append(ctx.job_id)
        return 'child-result'

    workflow.include_routers(parent, child)
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'B')
    child_events = storage.read_job_events('B', 1)
    try:
        with pytest.raises(JobFailedError):
            workflow.run()
        assert observations[0]['lifecycle'] == 'running'
        assert child_calls == []
        failed = storage.get_component_state(('A',))
        assert (failed['lifecycle'], failed['stability'], failed['instability_origin']) == ('failed', None, None)
        assert storage.get_component_state(('B',))['lifecycle'] == 'queued'
        assert storage.get_job_status('A', 1) == 'failed'
        assert storage.get_job_status('B', 1) == 'queued'
        assert storage.read_job_events('B', 1) == child_events
        session, = storage.list_execution_sessions()
        assert (session['status'], session['outcome']) == ('terminal', 'failed')
        assert storage.get_component_reservation(('A',)) is None
        assert storage.get_component_reservation(('B',)) is None
    finally:
        _close(storage)


@pytest.mark.parametrize('restart_first', [True, False])
def test_component_failure_and_restart_share_one_terminal_decision(tmp_path, monkeypatch, restart_first):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    parent, child = NodeRouter('A', runner='direct'), NodeRouter('B', runner='direct')
    at_decision, proceed = Event(), Event()
    calls, child_observations = [], []

    @parent.task
    def parent_work(ctx):
        calls.append(('A', ctx.execution_generation))
        if ctx.execution_generation == 0:
            raise ValueError('restartable parent failure')
        return 'parent-result'

    @child.task
    def child_work(ctx):
        calls.append(('B', ctx.execution_generation))
        child_observations.append(storage.get_component_state(('A',)))
        return 'child-result'

    workflow.include_routers(parent, child)
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'B')
    decide = storage.decide_execution_session_exit

    def ordered_decision(*args, **kwargs):
        if kwargs['outcome'] != 'failed' or at_decision.is_set():
            return decide(*args, **kwargs)
        if restart_first:
            at_decision.set()
            assert proceed.wait(20)
            return decide(*args, **kwargs)
        result = decide(*args, **kwargs)
        at_decision.set()
        assert proceed.wait(20)
        return result

    monkeypatch.setattr(storage, 'decide_execution_session_exit', ordered_decision)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(workflow.run)
            try:
                assert at_decision.wait(10), 'Parent did not reach its terminal decision'
                session, = storage.list_execution_sessions()
                expected_lifecycle = 'running' if restart_first else 'failed'
                assert storage.get_component_state(('A',))['lifecycle'] == expected_lifecycle
                assert storage.get_component_state(('B',))['lifecycle'] == 'queued'
                assert session['status'] == ('running' if restart_first else 'terminal')
                for component in [('A',), ('B',)]:
                    assert storage.get_component_reservation(component) == (
                        {'members': component, 'session_id': session['session_id']} if restart_first else None
                    )
                command = subprocess.run(
                    [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '1'],
                    cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
                    capture_output=True, text=True, timeout=15,
                )
                assert (command.returncode == 0) is restart_first, command.stdout + command.stderr
            finally:
                proceed.set()
            if restart_first:
                future.result(timeout=15)
            else:
                with pytest.raises(JobFailedError):
                    future.result(timeout=15)
        assert calls == ([('A', 0), ('A', 1), ('B', 0)] if restart_first else [('A', 0)])
        session, = storage.list_execution_sessions()
        assert (session['status'], session['outcome']) == ('terminal', 'done' if restart_first else 'failed')
        if restart_first:
            observed, = child_observations
            assert (observed['lifecycle'], observed['stability']) == ('done', 'stable')
        for component in [('A',), ('B',)]:
            assert storage.get_component_reservation(component) is None
    finally:
        proceed.set()
        _close(storage)


@pytest.mark.parametrize('entry', ['run_node', 'run_queued_node_jobs'])
@pytest.mark.parametrize('runner', ['direct', 'threaded'])
def test_full_singleton_call_preserves_ordered_values_and_component_result(tmp_path, entry, runner):
    workflow = MicroWorkflow(tmp_path, runner=runner, persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    router = NodeRouter('A', runner=runner, max_threads=2)
    observations = []

    @router.task
    def work(ctx):
        observations.append(storage.get_component_state(('A',))['lifecycle'])
        return {'job': ctx.job_id}

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'A')
    try:
        result = getattr(workflow, entry)('A')
        assert result == [{'job': 1}, {'job': 2}]
        assert observations == ['running', 'running']
        state = storage.get_component_state(('A',))
        assert (state['lifecycle'], state['stability'], state['instability_origin']) == ('done', 'stable', None)
        session, = storage.list_execution_sessions()
        assert (session['status'], session['outcome']) == ('terminal', 'done')
        assert storage.get_component_reservation(('A',)) is None
    finally:
        _close(storage)


def test_task_parented_full_component_uses_the_shared_session(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'C'), ('B', 'C')])
    storage = workflow.storage
    first, nested = NodeRouter('A', runner='direct'), NodeRouter('B', runner='direct')
    observations = []

    @first.task
    def first_work(ctx):
        owner = storage.get_job_execution_owner(ctx.execution_id)
        workflow.run_component({'B'})
        observations.append(('returned', owner['session_id'], {
            'first': storage.get_component_state(('A',))['lifecycle'],
            'nested': storage.get_component_state(('B',))['lifecycle'],
            'session': storage.get_execution_session(owner['session_id'])['status'],
        }))
        return 'first-result'

    @nested.task
    def nested_work(ctx):
        owner = storage.get_job_execution_owner(ctx.execution_id)
        observations.append(('nested', owner['session_id'], storage.get_component_state(('B',))['lifecycle']))
        return 'nested-result'

    workflow.include_routers(first, nested)
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'B')
    try:
        workflow.run()
        session, = storage.list_execution_sessions()
        assert observations == [
            ('nested', session['session_id'], 'running'),
            ('returned', session['session_id'], {'first': 'running', 'nested': 'done', 'session': 'running'}),
        ]
        assert (session['status'], session['outcome']) == ('terminal', 'done')
        for component in [('A',), ('B',)]:
            assert storage.get_component_state(component)['lifecycle'] == 'done'
            assert storage.get_component_reservation(component) is None
    finally:
        _close(storage)


def test_task_parented_component_failure_settles_every_begun_component(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'C'), ('B', 'C')])
    storage = workflow.storage
    first, nested = NodeRouter('A', runner='direct'), NodeRouter('B', runner='direct')
    calls = []

    @first.task
    def first_work(ctx):
        calls.append('A')
        workflow.run_component({'B'})
        return 'unreachable'

    @nested.task
    def nested_work(ctx):
        calls.append('B')
        raise ValueError('nested component failed')

    workflow.include_routers(first, nested)
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'B')
    try:
        with pytest.raises(JobFailedError):
            workflow.run()
        assert calls == ['A', 'B']
        session, = storage.list_execution_sessions()
        assert (session['status'], session['outcome']) == ('terminal', 'failed')
        for component in [('A',), ('B',)]:
            state = storage.get_component_state(component)
            assert (state['lifecycle'], state['stability'], state['instability_origin']) == ('failed', None, None)
            assert storage.get_component_reservation(component) is None
        assert storage.get_component_state(('C',))['lifecycle'] == 'queued'
    finally:
        _close(storage)


@pytest.mark.parametrize('entry', ['run_component', 'run_node', 'run_queued_node_jobs', 'run', 'run_concurrently'])
def test_same_component_nested_full_call_borrows_the_running_lifecycle(tmp_path, entry):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    router = NodeRouter('A', runner='direct')
    observations = []

    @router.task
    def work(ctx):
        observations.append((ctx.job_id, storage.get_component_state(('A',))['lifecycle']))
        if ctx.job_id == 1:
            workflow.add_job(None, 'A')
            if entry == 'run_component':
                workflow.run_component({'A'})
            elif entry == 'run':
                workflow.run()
            elif entry == 'run_concurrently':
                workflow.run_concurrently(['A'])
            else:
                assert getattr(workflow, entry)('A') == [2]
            observations.append(('returned', storage.get_component_state(('A',))['lifecycle']))
        return ctx.job_id

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    try:
        if entry == 'run':
            workflow.run()
        else:
            workflow.run_component({'A'})
        assert observations == [(1, 'running'), (2, 'running'), ('returned', 'running')]
        state = storage.get_component_state(('A',))
        assert (state['lifecycle'], state['stability'], state['instability_origin']) == ('done', 'stable', None)
        session, = storage.list_execution_sessions()
        assert (session['status'], session['outcome']) == ('terminal', 'done')
        for job_id in (1, 2):
            starts = [event for event in storage.read_job_events('A', job_id) if event['event'] == 'started']
            assert len(starts) == 1
            assert storage.get_job_status('A', job_id) == 'done'
    finally:
        _close(storage)


def test_failed_session_publication_retains_the_original_job_error(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    router = NodeRouter('A', runner='direct')
    original_error = ValueError('original task failure')
    before_decision = []

    @router.task
    def work(ctx):
        raise original_error

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    decide = storage.decide_execution_session_exit

    def changed_pending_record(*args, **kwargs):
        if not before_decision:
            storage.submit_db_mutation(lambda connection: connection.execute(
                'UPDATE pending_component_executions SET alignment_generation=1',
            ))
            before_decision.append(_rows(storage))
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, 'decide_execution_session_exit', changed_pending_record)
    try:
        with pytest.raises(JobFailedError) as caught:
            workflow.run_component({'A'})
        assert caught.value.__cause__ is original_error
        assert any('Execution session exit failed' in note for note in caught.value.__notes__)
        before_decision[0]['advisory_locks'] = []
        assert _rows(storage) == before_decision[0]
        session, = storage.list_execution_sessions()
        assert session['status'] == 'running'
        assert storage.get_component_reservation(('A',))['session_id'] == session['session_id']
        assert storage.get_component_state(('A',))['lifecycle'] == 'running'
    finally:
        _close(storage)


@pytest.mark.parametrize('entry', ['run_component', 'run_node', 'run_queued_node_jobs', 'run_concurrently'])
@pytest.mark.parametrize('parent_fails', [False, True])
def test_selected_job_nested_full_call_defers_completion_until_its_parent_finishes(tmp_path, entry, parent_fails):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    router = NodeRouter('A', runner='direct')
    observations = []

    @router.task
    def work(ctx):
        if ctx.job_id == 1:
            observations.append(('selected', storage.get_component_state(('A',))['lifecycle']))
            workflow.add_job(None, 'A')
            if entry == 'run_component':
                workflow.run_component({'A'})
            elif entry == 'run_concurrently':
                workflow.run_concurrently(['A'])
            else:
                assert getattr(workflow, entry)('A') == [2]
            observations.append(('returned', storage.get_component_state(('A',))['lifecycle']))
            assert storage.get_job_status('A', 1) == 'running'
            if parent_fails:
                raise ValueError('selected parent failed after nested completion')
        else:
            observations.append(('nested', storage.get_component_state(('A',))['lifecycle']))
        return ctx.job_id

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    try:
        if parent_fails:
            with pytest.raises(JobFailedError):
                workflow.run_job('A', 1)
        else:
            assert workflow.run_job('A', 1) == 1
        assert observations == [('selected', 'queued'), ('nested', 'running'), ('returned', 'running')]
        state = storage.get_component_state(('A',))
        assert (state['lifecycle'], state['stability'], state['instability_origin']) == (
            ('failed', None, None) if parent_fails else ('done', 'stable', None)
        )
        session, = storage.list_execution_sessions()
        assert session['selected_jobs'] == [('A', 1)]
        assert (session['status'], session['outcome']) == ('terminal', 'failed' if parent_fails else 'done')
        assert storage.get_component_reservation(('A',)) is None
        assert storage.get_job_status('A', 1) == ('failed' if parent_fails else 'done')
        assert storage.get_job_status('A', 2) == 'done'
    finally:
        _close(storage)


@pytest.mark.parametrize('restart_nested,catch_failure', [(False, False), (True, False), (True, True)])
def test_process_nested_failure_settles_worker_started_components_and_survives_reopen(tmp_path, monkeypatch, restart_nested, catch_failure):
    monkeypatch.chdir(tmp_path)
    source = tmp_path / 'src'
    behaviors = source / 'node_behavior'
    behaviors.mkdir(parents=True)
    graph = source / 'graph.py'
    graph.write_text("EDGES = [('A', 'C'), ('B', 'C')]\n", encoding='utf-8')
    (behaviors / 'A.py').write_text('''
import json
import os
from micro_workflow_manager import NodeRouter
from micro_workflow_manager.errors import JobFailedError
router = NodeRouter('A', runner='process', max_threads=1)
@router.task
def work(ctx):
    context = ctx.system.execution_session_context
    ctx.input_path('worker.json').write_text(json.dumps({
        'pid': os.getpid(), 'context_length': len(context), 'session_id': context[0],
        'lifecycle': ctx.system.storage.get_component_state(('A',))['lifecycle'],
    }), encoding='utf-8')
    try:
        ctx.system.run_component({'B'})
    except JobFailedError:
        if not CATCH_FAILURE:
            raise
    return 'retained parent result'
'''.replace('CATCH_FAILURE', repr(catch_failure)), encoding='utf-8')
    (behaviors / 'B.py').write_text('''
from micro_workflow_manager import NodeRouter
router = NodeRouter('B', runner='direct')
@router.task
def work(ctx):
    assert ctx.system.storage.get_component_state(('B',))['lifecycle'] == 'running'
    if ctx.execution_generation == 0:
        raise ValueError('nested worker component failed')
    return 'accepted nested successor'
''', encoding='utf-8')
    (behaviors / 'C.py').write_text('''
from micro_workflow_manager import NodeRouter
router = NodeRouter('C', runner='direct')
@router.task
def work(ctx):
    assert ctx.system.storage.get_component_state(('A',))['lifecycle'] == 'done'
    assert ctx.system.storage.get_component_state(('B',))['lifecycle'] == 'done'
    assert CATCH_FAILURE, 'Child of a failed component must not start'
    return 'selected descendant'
'''.replace('CATCH_FAILURE', repr(catch_failure)), encoding='utf-8')
    workflow = MicroWorkflow(tmp_path, runner='direct', process_graph_path=graph, persist_graph=False)
    workflow.graph([('A', 'C'), ('B', 'C')])
    workflow.include_node_dir(behaviors)
    for node in ('A', 'B', 'C'):
        workflow.add_job(None, node)
    storage = workflow.storage
    child_events = storage.read_job_events('C', 1)
    at_decision, proceed = Event(), Event()
    decide = storage.decide_execution_session_exit

    def pause_decision(*args, **kwargs):
        if restart_nested and kwargs['outcome'] == ('done' if catch_failure else 'failed') and not at_decision.is_set():
            at_decision.set()
            assert proceed.wait(20)
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, 'decide_execution_session_exit', pause_decision)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(workflow.run)
            try:
                if restart_nested:
                    assert at_decision.wait(15)
                    command = subprocess.run(
                        [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'B', 'job', '1'],
                        cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
                        capture_output=True, text=True, timeout=15,
                    )
                    assert command.returncode == 0, command.stdout + command.stderr
            finally:
                proceed.set()
            if catch_failure:
                assert future.result(timeout=20) == ['A', 'C']
            else:
                with pytest.raises(JobFailedError, match='Job A/1 failed'):
                    future.result(timeout=20)
        session, = storage.list_execution_sessions()
        session_id = session['session_id']
        worker = json.loads((tmp_path / 'node' / 'A' / 'input' / 'worker.json').read_text(encoding='utf-8'))
        assert worker['pid'] != os.getpid()
        assert worker == {
            'pid': worker['pid'], 'context_length': 3,
            'session_id': session_id, 'lifecycle': 'running',
        }
        assert (session['status'], session['outcome']) == ('terminal', 'done' if catch_failure else 'failed')
        expected_states = {
            'A': 'done' if catch_failure else 'failed',
            'B': 'done' if restart_nested else 'failed',
            'C': 'done' if catch_failure else 'queued',
        }
        for node in ('A', 'B'):
            state = storage.get_component_state((node,))
            expected = expected_states[node]
            assert (state['lifecycle'], state['stability'], state['instability_origin']) == (
                expected, 'stable' if expected == 'done' else None, None,
            )
            assert storage.get_job_status(node, 1) == expected
            assert storage.read_job_current_owner(node, 1)['session_id'] == session_id
        assert storage.get_component_state(('C',))['lifecycle'] == expected_states['C']
        assert storage.get_job_status('C', 1) == expected_states['C']
        if not catch_failure:
            assert storage.read_job_events('C', 1) == child_events
        events = {node: storage.read_job_events(node, 1) for node in ('A', 'B', 'C')}
        outputs = {node: storage.output_file(node, 1).read_bytes() for node in ('A', 'B')}
        for node in ('A', 'B'):
            expected_starts = 2 if node == 'B' and restart_nested else 1
            assert len([event for event in events[node] if event['event'] == 'started']) == expected_starts
        if restart_nested:
            assert json.loads(outputs['B'])['result_repr'] == "'accepted nested successor'"
    finally:
        proceed.set()
        _close(storage)
    reopened = FileStorage(tmp_path)
    try:
        assert reopened.get_execution_session(session_id) == session
        for node in ('A', 'B', 'C'):
            assert reopened.get_component_state((node,))['lifecycle'] == expected_states[node]
            assert reopened.get_component_reservation((node,)) is None
            assert reopened.read_job_events(node, 1) == events[node]
        for node in ('A', 'B'):
            assert reopened.output_file(node, 1).read_bytes() == outputs[node]
    finally:
        _close(reopened)


@pytest.mark.parametrize('entry', ['run_jobs', 'run_node_jobs'])
@pytest.mark.parametrize('runner', ['direct', 'threaded'])
def test_finite_selected_jobs_leave_other_work_and_full_component_lifecycle_queued(tmp_path, entry, runner):
    workflow = MicroWorkflow(tmp_path, runner=runner, persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    router = NodeRouter('A', runner=runner)
    calls = []

    @router.task
    def work(ctx):
        calls.append(ctx.job_id)
        assert storage.get_component_state(('A',))['lifecycle'] == 'queued'
        return {'selected': ctx.job_id}

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'A')
    untouched_events = storage.read_job_events('A', 2)
    try:
        selected = [1] if entry == 'run_jobs' else [storage.load_job('A', 1)]
        assert getattr(workflow, entry)('A', selected) == [{'selected': 1}]
        assert calls == [1]
        assert storage.get_job_status('A', 1) == 'done'
        assert storage.get_job_status('A', 2) == 'queued'
        assert storage.read_job_events('A', 2) == untouched_events
        assert not storage.output_file('A', 2).exists()
        state = storage.get_component_state(('A',))
        assert (state['lifecycle'], state['stability'], state['instability_origin']) == ('queued', None, None)
        session, = storage.list_execution_sessions()
        assert session['selected_jobs'] == [('A', 1)]
        assert (session['status'], session['outcome']) == ('terminal', 'done')
        assert storage.get_component_reservation(('A',)) is None
    finally:
        _close(storage)


@pytest.mark.parametrize('entry', ['run_node', 'run_queued_node_jobs'])
def test_full_empty_singleton_finishes_before_its_child_can_start(tmp_path, entry):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    router = NodeRouter('B', runner='direct')
    observations = []

    @router.task
    def work(ctx):
        observations.append(storage.get_component_state(('A',)))
        return 'child-result'

    workflow.include_routers(router)
    workflow.add_job(None, 'B')
    try:
        assert getattr(workflow, entry)('A') == []
        state = storage.get_component_state(('A',))
        assert (state['lifecycle'], state['stability'], state['instability_origin']) == ('done', 'stable', None)
        assert workflow.run_node('B') == ['child-result']
        assert observations == [state]
        assert storage.get_component_state(('B',))['lifecycle'] == 'done'
        assert all((session['status'], session['outcome']) == ('terminal', 'done')
                   for session in storage.list_execution_sessions())
    finally:
        _close(storage)


@pytest.mark.parametrize('entry', ['run', 'run_concurrently'])
@pytest.mark.parametrize('runner', ['direct', 'threaded'])
def test_full_dag_completes_empty_parents_before_starting_their_children(tmp_path, entry, runner):
    workflow = MicroWorkflow(tmp_path, runner=runner, persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    router = NodeRouter('B', runner=runner)
    observations = []

    @router.task
    def work(ctx):
        parent = storage.get_component_state(('A',))
        observations.append((parent['lifecycle'], parent['stability'], parent['instability_origin']))
        assert storage.get_component_state(('B',))['lifecycle'] == 'running'
        return 'child-result'

    workflow.include_routers(router)
    workflow.add_job(None, 'B')
    try:
        assert getattr(workflow, entry)() == ['B']
        assert observations == [('done', 'stable', None)]
        states = {node: storage.get_component_state((node,)) for node in ('A', 'B')}
        assert all(state['lifecycle'] == 'done' for state in states.values())
        events = storage.read_job_events('B', 1)
        assert len([event for event in events if event['event'] == 'started']) == 1
        assert getattr(workflow, entry)() == []
        assert storage.read_job_events('B', 1) == events
        for node in ('A', 'B'):
            assert storage.get_component_state((node,)) == states[node]
            assert storage.get_component_reservation((node,)) is None
        sessions = storage.list_execution_sessions()
        assert len(sessions) == 2
        assert all((session['status'], session['outcome']) == ('terminal', 'done') for session in sessions)
    finally:
        _close(storage)


def test_accepted_restart_runs_before_a_damaged_terminal_receipt_is_rejected(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    router = NodeRouter('A', runner='direct')
    at_decision, proceed = Event(), Event()
    calls = []

    @router.task
    def work(ctx):
        calls.append(ctx.execution_generation)
        if ctx.execution_generation == 0:
            raise ValueError('restartable first attempt')
        assert storage.get_component_state(('A',))['lifecycle'] == 'running'
        return 'accepted successor'

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    decide = storage.decide_execution_session_exit

    def pause_decision(*args, **kwargs):
        if kwargs['outcome'] == 'failed' and not at_decision.is_set():
            at_decision.set()
            assert proceed.wait(20)
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, 'decide_execution_session_exit', pause_decision)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(workflow.run_component, {'A'})
            try:
                assert at_decision.wait(10)
                command = subprocess.run(
                    [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '1'],
                    cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
                    capture_output=True, text=True, timeout=15,
                )
                assert command.returncode == 0, command.stdout + command.stderr
                storage.submit_db_mutation(lambda connection: connection.execute(
                    'UPDATE pending_component_executions SET alignment_generation=1',
                ))
            finally:
                proceed.set()
            with pytest.raises((RuntimeError, JobFailedError)):
                future.result(timeout=15)
        assert calls == [0, 1]
        assert storage.get_job_status('A', 1) == 'done'
        assert json.loads(storage.output_file('A', 1).read_text(encoding='utf-8'))['result_repr'] == "'accepted successor'"
        session, = storage.list_execution_sessions()
        assert session['status'] == 'running'
        assert storage.get_component_state(('A',))['lifecycle'] == 'running'
        assert storage.get_component_reservation(('A',))['session_id'] == session['session_id']
        assert len([event for event in storage.read_job_events('A', 1) if event['event'] == 'started']) == 2
    finally:
        proceed.set()
        _close(storage)


@pytest.mark.parametrize('restart_first', [True, False])
@pytest.mark.parametrize('nested_jobs', [1, 2])
def test_nested_component_restart_repairs_only_the_accepted_job_and_preserves_parent_failure(tmp_path, monkeypatch, restart_first, nested_jobs):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'C'), ('B', 'C')])
    storage = workflow.storage
    parent, nested = NodeRouter('A', runner='direct'), NodeRouter('B', runner='direct')
    at_decision, proceed = Event(), Event()
    calls, nested_errors, replacement_observations = [], [], []

    @parent.task
    def parent_work(ctx):
        calls.append(('A', ctx.execution_generation))
        try:
            workflow.run_component({'B'})
        except JobFailedError as error:
            nested_errors.append(error)
            raise

    @nested.task
    def nested_work(ctx):
        assert ctx.job_id == 1, 'Ordinary nested work must stay stopped while its parent fails'
        calls.append(('B', ctx.execution_generation))
        if ctx.execution_generation == 0:
            raise ValueError('nested first attempt failed')
        owner = storage.get_job_execution_owner(ctx.execution_id)
        replacement_observations.append((
            storage.get_execution_session(owner['session_id'])['status'],
            storage.get_component_state(('A',))['lifecycle'],
            storage.get_component_state(('B',))['lifecycle'],
            storage.get_component_reservation(('B',))['session_id'] == owner['session_id'],
        ))
        return 'nested replacement'

    workflow.include_routers(parent, nested)
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'B')
    if nested_jobs == 2:
        workflow.add_job(None, 'B')
    untouched_events = storage.read_job_events('B', 2) if nested_jobs == 2 else None
    decide = storage.decide_execution_session_exit

    def ordered_decision(*args, **kwargs):
        if kwargs['outcome'] != 'failed' or at_decision.is_set():
            return decide(*args, **kwargs)
        if restart_first:
            at_decision.set()
            assert proceed.wait(20)
            return decide(*args, **kwargs)
        result = decide(*args, **kwargs)
        at_decision.set()
        assert proceed.wait(20)
        return result

    monkeypatch.setattr(storage, 'decide_execution_session_exit', ordered_decision)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(workflow.run)
            try:
                assert at_decision.wait(10)
                parent_output = storage.output_file('A', 1).read_bytes()
                parent_events = storage.read_job_events('A', 1)
                command = subprocess.run(
                    [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'B', 'job', '1'],
                    cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
                    capture_output=True, text=True, timeout=15,
                )
                assert (command.returncode == 0) is restart_first, command.stdout + command.stderr
            finally:
                proceed.set()
            with pytest.raises(JobFailedError, match='Job A/1 failed') as caught:
                future.result(timeout=15)
        assert caught.value.__cause__ is nested_errors[0]
        assert calls == [('A', 0), ('B', 0)] + ([('B', 1)] if restart_first else [])
        assert replacement_observations == ([('running', 'running', 'running', True)] if restart_first else [])
        assert storage.output_file('A', 1).read_bytes() == parent_output
        assert storage.read_job_events('A', 1) == parent_events
        assert storage.get_job_status('A', 1) == 'failed'
        assert storage.get_job_status('B', 1) == ('done' if restart_first else 'failed')
        assert storage.get_component_state(('A',))['lifecycle'] == 'failed'
        result = storage.get_component_state(('B',))
        assert (result['lifecycle'], result['stability'], result['instability_origin']) == (
            ('done', 'stable', None) if restart_first and nested_jobs == 1 else ('failed', None, None)
        )
        if nested_jobs == 2:
            assert storage.get_job_status('B', 2) == 'queued'
            assert storage.read_job_events('B', 2) == untouched_events
            assert not storage.output_file('B', 2).exists()
        assert storage.get_component_state(('C',))['lifecycle'] == 'queued'
        session, = storage.list_execution_sessions()
        assert (session['status'], session['outcome']) == ('terminal', 'failed')
        for component in [('A',), ('B',), ('C',)]:
            assert storage.get_component_reservation(component) is None
    finally:
        proceed.set()
        _close(storage)


@pytest.mark.parametrize('successful_generation', [1, 2])
@pytest.mark.parametrize('nested_jobs', [1, 2])
def test_caught_nested_failure_can_restart_at_clean_exit_and_continue_selected_descendants(tmp_path, monkeypatch, successful_generation, nested_jobs):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'C'), ('B', 'C')])
    storage = workflow.storage
    parent, nested, child = (NodeRouter(name, runner='direct') for name in ('A', 'B', 'C'))
    at_decisions = [Event() for _ in range(successful_generation)]
    proceeds = [Event() for _ in range(successful_generation)]
    decisions = []
    calls = []

    @parent.task
    def parent_work(ctx):
        calls.append(('A', ctx.execution_generation))
        try:
            workflow.run_component({'B'})
        except JobFailedError:
            pass
        return 'retained parent result'

    @nested.task
    def nested_work(ctx):
        calls.append(('B' if ctx.job_id == 1 else 'B/2', ctx.execution_generation))
        if ctx.job_id == 1 and ctx.execution_generation < successful_generation:
            raise ValueError('caught nested failure')
        return 'nested successor'

    @child.task
    def child_work(ctx):
        calls.append(('C', ctx.execution_generation))
        assert storage.get_component_state(('A',))['lifecycle'] == 'done'
        assert storage.get_component_state(('B',))['lifecycle'] == 'done'
        return 'selected descendant'

    workflow.include_routers(parent, nested, child)
    for node in ('A', 'B', 'C'):
        workflow.add_job(None, node)
    if nested_jobs == 2:
        workflow.add_job(None, 'B')
    decide = storage.decide_execution_session_exit

    def pause_clean_exit(*args, **kwargs):
        if len(decisions) < successful_generation:
            position = len(decisions)
            decisions.append(kwargs['outcome'])
            at_decisions[position].set()
            assert proceeds[position].wait(20)
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, 'decide_execution_session_exit', pause_clean_exit)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(workflow.run)
            try:
                for position, at_decision in enumerate(at_decisions):
                    assert at_decision.wait(10)
                    if position == 0:
                        parent_output = storage.output_file('A', 1).read_bytes()
                        parent_events = storage.read_job_events('A', 1)
                    assert storage.get_job_status('C', 1) == 'queued'
                    command = subprocess.run(
                        [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'B', 'job', '1'],
                        cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
                        capture_output=True, text=True, timeout=15,
                    )
                    assert command.returncode == 0, command.stdout + command.stderr
                    proceeds[position].set()
            finally:
                for proceed in proceeds:
                    proceed.set()
            assert future.result(timeout=15) == ['A', 'C']
        assert decisions == ['done'] + ['failed'] * (successful_generation - 1)
        assert calls == [('A', 0)] + [('B', generation) for generation in range(successful_generation + 1)] + (
            [('B/2', 0)] if nested_jobs == 2 else []
        ) + [('C', 0)]
        assert storage.output_file('A', 1).read_bytes() == parent_output
        assert storage.read_job_events('A', 1) == parent_events
        session, = storage.list_execution_sessions()
        assert (session['status'], session['outcome']) == ('terminal', 'done')
        for node in ('A', 'B', 'C'):
            assert storage.get_component_state((node,))['lifecycle'] == 'done'
            assert storage.get_component_reservation((node,)) is None
        if nested_jobs == 2:
            assert storage.get_job_status('B', 2) == 'done'
            assert len([event for event in storage.read_job_events('B', 2) if event['event'] == 'started']) == 1
    finally:
        for proceed in proceeds:
            proceed.set()
        for session in storage.list_execution_sessions():
            workflow.scheduler_supervisor.stop_run_heartbeat(session['session_id'])
        _close(storage)


@pytest.mark.parametrize('repair_nested', [False, True])
def test_mixed_parent_and_nested_restarts_stop_ordinary_work_until_nested_repair_succeeds(tmp_path, monkeypatch, repair_nested):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'C'), ('B', 'C')])
    storage = workflow.storage
    parent, nested, child = (NodeRouter(name, runner='direct') for name in ('A', 'B', 'C'))
    at_decision, proceed = Event(), Event()
    at_second_decision, proceed_second = Event(), Event()
    calls = []

    @parent.task
    def parent_work(ctx):
        calls.append(('A', ctx.job_id, ctx.execution_generation))
        if ctx.job_id == 1 and ctx.execution_generation == 0:
            workflow.run_component({'B'})
        return {'retained': ctx.job_id}

    @nested.task
    def nested_work(ctx):
        calls.append(('B', ctx.job_id, ctx.execution_generation))
        if ctx.execution_generation < 2:
            raise ValueError('nested component still fails')
        return 'nested repaired'

    @child.task
    def child_work(ctx):
        assert repair_nested
        assert storage.get_component_state(('A',))['lifecycle'] == 'done'
        assert storage.get_component_state(('B',))['lifecycle'] == 'done'
        calls.append(('C', ctx.job_id, ctx.execution_generation))
        return 'child result'

    workflow.include_routers(parent, nested, child)
    for node in ('A', 'A', 'B', 'C'):
        workflow.add_job(None, node)
    untouched_events = storage.read_job_events('A', 2)
    decide = storage.decide_execution_session_exit

    def pause_failure(*args, **kwargs):
        if kwargs['outcome'] == 'failed' and not at_decision.is_set():
            at_decision.set()
            assert proceed.wait(20)
        elif repair_nested and kwargs['outcome'] == 'failed' and not at_second_decision.is_set():
            at_second_decision.set()
            assert proceed_second.wait(20)
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, 'decide_execution_session_exit', pause_failure)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(workflow.run)
            try:
                assert at_decision.wait(10)
                assert storage.get_job_status('A', 2) == 'queued'
                for node in ('A', 'B'):
                    command = subprocess.run(
                        [sys.executable, '-m', 'micro_workflow_manager', 'restart', node, 'job', '1'],
                        cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
                        capture_output=True, text=True, timeout=15,
                    )
                    assert command.returncode == 0, command.stdout + command.stderr
                proceed.set()
                if repair_nested:
                    assert at_second_decision.wait(10)
                    assert storage.get_job_status('A', 1) == 'done'
                    assert storage.get_job_status('A', 2) == 'queued'
                    command = subprocess.run(
                        [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'B', 'job', '1'],
                        cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
                        capture_output=True, text=True, timeout=15,
                    )
                    assert command.returncode == 0, command.stdout + command.stderr
            finally:
                proceed.set()
                proceed_second.set()
            if repair_nested:
                assert future.result(timeout=15) == ['A', 'C']
            else:
                with pytest.raises(JobFailedError, match='Job B/1 failed'):
                    future.result(timeout=15)
        assert calls == [('A', 1, 0), ('B', 1, 0), ('B', 1, 1), ('A', 1, 1)] + (
            [('B', 1, 2), ('A', 2, 0), ('C', 1, 0)] if repair_nested else []
        )
        assert storage.get_job_status('A', 1) == 'done'
        assert storage.get_job_status('A', 2) == ('done' if repair_nested else 'queued')
        if not repair_nested:
            assert storage.read_job_events('A', 2) == untouched_events
            assert not storage.output_file('A', 2).exists()
        assert json.loads(storage.output_file('A', 1).read_text(encoding='utf-8'))['result_repr'] == "{'retained': 1}"
        assert storage.get_component_state(('A',))['lifecycle'] == ('done' if repair_nested else 'failed')
        assert storage.get_component_state(('B',))['lifecycle'] == ('done' if repair_nested else 'failed')
        assert storage.get_component_state(('C',))['lifecycle'] == ('done' if repair_nested else 'queued')
        session, = storage.list_execution_sessions()
        assert (session['status'], session['outcome']) == ('terminal', 'done' if repair_nested else 'failed')
        for component in [('A',), ('B',), ('C',)]:
            assert storage.get_component_reservation(component) is None
    finally:
        proceed.set()
        proceed_second.set()
        _close(storage)


@pytest.mark.parametrize('successful_generation', [1, 2])
def test_clean_exit_does_not_reuse_a_sibling_restart_consumed_during_another_repair(tmp_path, monkeypatch, successful_generation):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'C'), ('B', 'C'), ('D', 'C')])
    storage = workflow.storage
    parent, first, second, child = (NodeRouter(name, runner='direct') for name in ('A', 'B', 'D', 'C'))
    gates = [Event() for _ in range(successful_generation)]
    releases = [Event() for _ in range(successful_generation)]
    decisions, calls = [], []

    @parent.task
    def parent_work(ctx):
        calls.append(('A', ctx.execution_generation))
        for component in ('B', 'D'):
            try:
                workflow.run_component({component})
            except JobFailedError:
                pass
        return 'retained parent'

    @first.task
    def first_work(ctx):
        calls.append(('B', ctx.execution_generation))
        if ctx.execution_generation < successful_generation:
            raise ValueError('first nested component needs another repair')
        return 'first repaired'

    @second.task
    def second_work(ctx):
        calls.append(('D', ctx.execution_generation))
        if ctx.execution_generation == 0:
            raise ValueError('second nested component needs one repair')
        return 'second repaired'

    @child.task
    def child_work(ctx):
        calls.append(('C', ctx.execution_generation))
        assert all(storage.get_component_state((node,))['lifecycle'] == 'done' for node in ('A', 'B', 'D'))
        return 'selected descendant'

    workflow.include_routers(parent, first, second, child)
    for node in ('A', 'B', 'D', 'C'):
        workflow.add_job(None, node)
    decide = storage.decide_execution_session_exit

    def pause_decision(*args, **kwargs):
        if len(decisions) < successful_generation:
            position = len(decisions)
            decisions.append(kwargs['outcome'])
            gates[position].set()
            assert releases[position].wait(20)
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, 'decide_execution_session_exit', pause_decision)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(workflow.run)
            try:
                for position, nodes in enumerate([('B', 'D')] + [('B',)] * (successful_generation - 1)):
                    assert gates[position].wait(10)
                    if position == 0:
                        parent_output = storage.output_file('A', 1).read_bytes()
                        parent_events = storage.read_job_events('A', 1)
                    for node in nodes:
                        command = subprocess.run(
                            [sys.executable, '-m', 'micro_workflow_manager', 'restart', node, 'job', '1'],
                            cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
                            capture_output=True, text=True, timeout=15,
                        )
                        assert command.returncode == 0, command.stdout + command.stderr
                    releases[position].set()
            finally:
                for release in releases:
                    release.set()
            assert future.result(timeout=15) == ['A', 'C']
        assert decisions == ['done'] + ['failed'] * (successful_generation - 1)
        assert sorted(calls) == [('A', 0)] + [('B', generation) for generation in range(successful_generation + 1)] + [('C', 0), ('D', 0), ('D', 1)]
        assert calls[-1] == ('C', 0)
        assert storage.output_file('A', 1).read_bytes() == parent_output
        assert storage.read_job_events('A', 1) == parent_events
        session, = storage.list_execution_sessions()
        assert (session['status'], session['outcome']) == ('terminal', 'done')
        for node, expected_starts in [('A', 1), ('B', successful_generation + 1), ('D', 2), ('C', 1)]:
            assert storage.get_component_state((node,))['lifecycle'] == 'done'
            assert storage.get_component_reservation((node,)) is None
            assert len([event for event in storage.read_job_events(node, 1) if event['event'] == 'started']) == expected_starts
    finally:
        for release in releases:
            release.set()
        _close(storage)


@pytest.mark.parametrize('ordinary_fails', [False, True])
def test_all_accepted_nested_repairs_finish_before_any_ordinary_remainder(tmp_path, monkeypatch, ordinary_fails):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'C'), ('B', 'C'), ('D', 'C')])
    storage = workflow.storage
    parent, child = NodeRouter('A', runner='direct'), NodeRouter('C', runner='direct')
    nested = [NodeRouter(node, runner='direct') for node in ('B', 'D')]
    at_decision, proceed = Event(), Event()
    calls, ordinary_calls = [], []
    ordinary_error = ValueError('ordinary nested work failed after both repairs')

    @parent.task
    def parent_work(ctx):
        calls.append(('A', ctx.job_id, ctx.execution_generation))
        for node in ('B', 'D'):
            try:
                workflow.run_component({node})
            except JobFailedError:
                pass
        return 'retained parent'

    def install_nested_task(router, node):
        @router.task
        def nested_work(ctx):
            calls.append((node, ctx.job_id, ctx.execution_generation))
            if ctx.job_id == 1:
                if ctx.execution_generation == 0:
                    raise ValueError('nested component needs repair')
                return 'repaired'
            assert all(storage.get_job_status(node, 1) == 'done' for node in ('B', 'D')), (
                'Every accepted repair must finish before ordinary nested work starts'
            )
            ordinary_calls.append(node)
            if ordinary_fails:
                raise ordinary_error
            return 'ordinary remainder'

    for router, node in zip(nested, ('B', 'D')):
        install_nested_task(router, node)

    @child.task
    def child_work(ctx):
        calls.append(('C', ctx.job_id, ctx.execution_generation))
        assert all(storage.get_component_state((node,))['lifecycle'] == 'done' for node in ('A', 'B', 'D'))
        return 'selected descendant'

    workflow.include_routers(parent, *nested, child)
    for node in ('A', 'B', 'D', 'C', 'B', 'D'):
        workflow.add_job(None, node)
    untouched_events = {node: storage.read_job_events(node, 2) for node in ('B', 'D')}
    decide = storage.decide_execution_session_exit

    def pause_decision(*args, **kwargs):
        if not at_decision.is_set():
            assert kwargs['outcome'] == 'done'
            at_decision.set()
            assert proceed.wait(20)
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, 'decide_execution_session_exit', pause_decision)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(workflow.run)
            try:
                assert at_decision.wait(10)
                parent_output = storage.output_file('A', 1).read_bytes()
                parent_events = storage.read_job_events('A', 1)
                for node in ('B', 'D'):
                    command = subprocess.run(
                        [sys.executable, '-m', 'micro_workflow_manager', 'restart', node, 'job', '1'],
                        cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
                        capture_output=True, text=True, timeout=15,
                    )
                    assert command.returncode == 0, command.stdout + command.stderr
            finally:
                proceed.set()
            if ordinary_fails:
                with pytest.raises(JobFailedError) as caught:
                    future.result(timeout=15)
                assert caught.value.__cause__ is ordinary_error
            else:
                assert future.result(timeout=15) == ['A', 'C']
        assert calls[:3] == [('A', 1, 0), ('B', 1, 0), ('D', 1, 0)]
        assert sorted(calls[3:5]) == [('B', 1, 1), ('D', 1, 1)]
        assert storage.output_file('A', 1).read_bytes() == parent_output
        assert storage.read_job_events('A', 1) == parent_events
        assert storage.get_component_state(('A',))['lifecycle'] == 'done'
        assert len(ordinary_calls) == (1 if ordinary_fails else 2)
        for node in ('B', 'D'):
            assert storage.get_job_status(node, 1) == 'done'
            assert len([event for event in storage.read_job_events(node, 1) if event['event'] == 'started']) == 2
            assert storage.get_component_state((node,))['lifecycle'] == ('failed' if ordinary_fails else 'done')
            if ordinary_fails and node not in ordinary_calls:
                assert storage.get_job_status(node, 2) == 'queued'
                assert storage.read_job_events(node, 2) == untouched_events[node]
                assert not storage.output_file(node, 2).exists()
            else:
                assert storage.get_job_status(node, 2) == ('failed' if ordinary_fails else 'done')
        assert storage.get_component_state(('C',))['lifecycle'] == ('queued' if ordinary_fails else 'done')
        assert len(calls) == (6 if ordinary_fails else 8)
        session, = storage.list_execution_sessions()
        assert (session['status'], session['outcome']) == ('terminal', 'failed' if ordinary_fails else 'done')
        for node in ('A', 'B', 'D', 'C'):
            assert storage.get_component_reservation((node,)) is None
    finally:
        proceed.set()
        _close(storage)


@pytest.mark.parametrize('entry', ['run_job', 'run_jobs', 'run_node_jobs'])
def test_selected_parent_retains_its_python_value_when_nested_same_component_work_restarts(tmp_path, monkeypatch, entry):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    router = NodeRouter('A', runner='direct')
    value = {'retained': object()}
    at_decision, proceed = Event(), Event()
    calls = []

    @router.task
    def work(ctx):
        calls.append((ctx.job_id, ctx.execution_generation))
        if ctx.job_id == 1:
            assert sum(job_id == 1 for job_id, _ in calls) == 1, 'Selected parent replayed without an accepted restart'
            workflow.add_job(None, 'A')
            try:
                workflow.run_component({'A'})
            except JobFailedError:
                pass
            return value
        if ctx.execution_generation == 0:
            raise ValueError('nested same-component job failed')
        return 'accepted nested result'

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    decide = storage.decide_execution_session_exit

    def pause_clean_exit(*args, **kwargs):
        if kwargs['outcome'] == 'done' and not at_decision.is_set():
            at_decision.set()
            assert proceed.wait(20)
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, 'decide_execution_session_exit', pause_clean_exit)
    selection = 1 if entry == 'run_job' else [1] if entry == 'run_jobs' else [storage.load_job('A', 1)]
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(getattr(workflow, entry), 'A', selection)
            try:
                assert at_decision.wait(10)
                parent_output = storage.output_file('A', 1).read_bytes()
                parent_events = storage.read_job_events('A', 1)
                command = subprocess.run(
                    [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '2'],
                    cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
                    capture_output=True, text=True, timeout=15,
                )
                assert command.returncode == 0, command.stdout + command.stderr
            finally:
                proceed.set()
            result = future.result(timeout=15)
            if entry == 'run_job':
                assert result is value
            else:
                assert len(result) == 1 and result[0] is value
        assert calls == [(1, 0), (2, 0), (2, 1)]
        assert storage.output_file('A', 1).read_bytes() == parent_output
        assert storage.read_job_events('A', 1) == parent_events
        assert json.loads(storage.output_file('A', 2).read_text(encoding='utf-8'))['result_repr'] == "'accepted nested result'"
        assert storage.get_component_state(('A',))['lifecycle'] == 'done'
        assert storage.get_component_state(('B',))['lifecycle'] == 'queued'
        session, = storage.list_execution_sessions()
        assert session['selected_jobs'] == [('A', 1)]
        assert (session['status'], session['outcome']) == ('terminal', 'done')
        assert storage.get_component_reservation(('A',)) is None
    finally:
        proceed.set()
        _close(storage)


@pytest.mark.parametrize('entry', ['run_component', 'run_concurrently', 'run_node', 'run_queued_node_jobs'])
@pytest.mark.parametrize('runner', ['direct', 'api'])
def test_full_component_retains_successful_parent_when_same_component_nested_work_restarts(tmp_path, monkeypatch, entry, runner):
    workflow = MicroWorkflow(tmp_path, runner=runner, persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    router = NodeRouter('A', runner=runner, max_threads=1)
    at_decision, proceed = Event(), Event()
    calls = []
    value = {'retained': object()}

    @router.task
    def work(ctx):
        calls.append((ctx.job_id, ctx.execution_generation))
        if ctx.job_id == 1:
            assert calls.count((1, 0)) == 1, 'Successful parent must not be replayed'
            workflow.add_job(None, 'A')
            try:
                workflow.run_component({'A'})
            except JobFailedError:
                pass
            return value
        if ctx.execution_generation == 0:
            raise ValueError('nested same-component job failed')
        return 'accepted nested result'

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    decide = storage.decide_execution_session_exit

    def pause_exit(*args, **kwargs):
        if not at_decision.is_set():
            at_decision.set()
            assert proceed.wait(20)
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, 'decide_execution_session_exit', pause_exit)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            argument = {'A'} if entry == 'run_component' else ['A'] if entry == 'run_concurrently' else 'A'
            future = pool.submit(getattr(workflow, entry), argument)
            try:
                assert at_decision.wait(10)
                parent_output = storage.output_file('A', 1).read_bytes()
                parent_events = storage.read_job_events('A', 1)
                command = subprocess.run(
                    [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '2'],
                    cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
                    capture_output=True, text=True, timeout=15,
                )
                assert command.returncode == 0, command.stdout + command.stderr
            finally:
                proceed.set()
            result = future.result(timeout=15)
            if entry in {'run_component', 'run_concurrently'}:
                assert result == ['A']
            else:
                assert len(result) == 1 and result[0] is value
        assert calls == [(1, 0), (2, 0), (2, 1)]
        assert storage.output_file('A', 1).read_bytes() == parent_output
        assert storage.read_job_events('A', 1) == parent_events
        assert storage.get_component_state(('A',))['lifecycle'] == 'done'
        assert storage.get_component_state(('B',))['lifecycle'] == 'queued'
        session, = storage.list_execution_sessions()
        assert session['selected_jobs'] == []
        assert (session['status'], session['outcome']) == ('terminal', 'done')
        assert storage.get_component_reservation(('A',)) is None
    finally:
        proceed.set()
        _close(storage)


@pytest.mark.parametrize('entry', ['run_component', 'run_concurrently', 'run_node', 'run_queued_node_jobs'])
@pytest.mark.parametrize('runner, leading_jobs', [('direct', ()), ('api', ()), ('api', (7, 5))])
@pytest.mark.parametrize('accept_restart', [False, True])
def test_caught_same_component_failure_keeps_ordinary_remainder_queued_until_repair(
    tmp_path, monkeypatch, entry, runner, leading_jobs, accept_restart,
):
    workflow = MicroWorkflow(tmp_path, runner=runner, persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    router = NodeRouter('A', runner=runner, max_threads=1)
    at_decision, proceed = Event(), Event()
    calls = []
    value = {'retained': object()}
    leading_values = {job_id: object() for job_id in leading_jobs}

    @router.task
    def work(ctx):
        calls.append((ctx.job_id, ctx.execution_generation))
        if ctx.job_id in leading_values:
            return leading_values[ctx.job_id]
        if ctx.job_id == 1:
            assert calls.count((1, 0)) == 1, 'Successful parent must not be replayed'
            workflow.add_job(None, 'A', job_id=2)
            workflow.add_job(None, 'A', job_id=3)
            try:
                workflow.run_component({'A'})
            except JobFailedError:
                pass
            return value
        if ctx.job_id == 2:
            if ctx.execution_generation == 0:
                raise ValueError('nested same-component job failed')
            return 'accepted nested result'
        assert storage.get_job_status('A', 2) == 'done', 'Ordinary work started before nested repair'
        return 'ordinary remainder'

    workflow.include_routers(router)
    for job_id in [*leading_jobs, 1]:
        workflow.add_job(None, 'A', job_id=job_id)
    decide = storage.decide_execution_session_exit

    def pause_exit(*args, **kwargs):
        if not at_decision.is_set():
            at_decision.set()
            assert proceed.wait(20)
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, 'decide_execution_session_exit', pause_exit)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            argument = {'A'} if entry == 'run_component' else ['A'] if entry == 'run_concurrently' else 'A'
            future = pool.submit(getattr(workflow, entry), argument)
            try:
                assert at_decision.wait(10)
                parent_output = storage.output_file('A', 1).read_bytes()
                parent_events = storage.read_job_events('A', 1)
                assert calls == [(job_id, 0) for job_id in leading_jobs] + [(1, 0), (2, 0)]
                assert storage.get_job_status('A', 3) == 'queued'
                remainder_events = storage.read_job_events('A', 3)
                if accept_restart:
                    command = subprocess.run(
                        [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '2'],
                        cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
                        capture_output=True, text=True, timeout=15,
                    )
                    assert command.returncode == 0, command.stdout + command.stderr
            finally:
                proceed.set()
            if not accept_restart:
                with pytest.raises(RuntimeError, match='unfinished'):
                    future.result(timeout=15)
            else:
                result = future.result(timeout=15)
                if entry in {'run_component', 'run_concurrently'}:
                    assert result == ['A']
                else:
                    expected_values = [*leading_values.values(), value]
                    assert len(result) == len(expected_values)
                    assert all(actual is expected for actual, expected in zip(result, expected_values))
        expected_calls = [(job_id, 0) for job_id in leading_jobs] + [(1, 0), (2, 0)]
        assert calls == expected_calls + ([(2, 1), (3, 0)] if accept_restart else [])
        assert storage.output_file('A', 1).read_bytes() == parent_output
        assert storage.read_job_events('A', 1) == parent_events
        assert storage.get_job_status('A', 3) == ('done' if accept_restart else 'queued')
        if accept_restart:
            assert json.loads(storage.output_file('A', 3).read_text(encoding='utf-8'))['result_repr'] == "'ordinary remainder'"
        else:
            assert storage.read_job_events('A', 3) == remainder_events
            assert not storage.output_file('A', 3).exists()
        outcome = 'done' if accept_restart else 'failed'
        assert storage.get_component_state(('A',))['lifecycle'] == outcome
        session, = storage.list_execution_sessions()
        assert (session['status'], session['outcome']) == ('terminal', outcome)
        assert storage.get_component_reservation(('A',)) is None
    finally:
        proceed.set()
        _close(storage)


@pytest.mark.parametrize('fault', ['task', 'output', 'terminal'])
def test_same_component_nested_failure_does_not_retire_its_active_parent(tmp_path, monkeypatch, fault):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    router = NodeRouter('A', runner='direct')
    parent_failure = ValueError('parent independently failed after catching its nested job')
    calls, observations = [], []
    injected = []
    write_output = storage.write_output
    finalize = storage.finalize_job_execution

    def fail_output(node, job_id, data, **kwargs):
        if fault == 'output' and node == 'A' and job_id == 2 and not injected:
            injected.append(True)
            raise OSError('nested output write failed')
        return write_output(node, job_id, data, **kwargs)

    def fail_terminal(node, job_id, *args, **kwargs):
        if fault == 'terminal' and node == 'A' and job_id == 2 and not injected:
            injected.append(True)
            raise OSError('nested terminal write failed')
        return finalize(node, job_id, *args, **kwargs)

    monkeypatch.setattr(storage, 'write_output', fail_output)
    monkeypatch.setattr(storage, 'finalize_job_execution', fail_terminal)

    @router.task
    def work(ctx):
        calls.append(ctx.job_id)
        if ctx.job_id == 1:
            assert calls.count(1) == 1, 'Active parent was replayed without an accepted restart'
            workflow.add_job(None, 'A')
            try:
                workflow.run_component({'A'})
            except Exception:
                owner = storage.read_job_owner_observation('A', 1)
                observations.append((owner['status'], owner['active_execution_id'] == ctx.execution_id))
            raise parent_failure
        if fault == 'task':
            raise ValueError('nested same-component job failed')
        return 'child output available for reconciliation'

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    try:
        with pytest.raises(JobFailedError) as caught:
            workflow.run_job('A', 1)
        assert observations == [('running', True)]
        assert calls == [1, 2]
        assert bool(injected) is (fault != 'task')
        assert caught.value.__cause__ is parent_failure
        assert storage.get_job_status('A', 2) == ('done' if fault == 'terminal' else 'failed')
        assert storage.read_job_owner_observation('A', 2)['active_execution_id'] is None
        assert storage.get_component_state(('A',))['lifecycle'] == 'failed'
        assert storage.get_component_reservation(('A',)) is None
        session, = storage.list_execution_sessions()
        assert (session['status'], session['outcome']) == ('terminal', 'failed')
        assert len([event for event in storage.read_job_events('A', 1) if event['event'] == 'started']) == 1
    finally:
        _close(storage)
