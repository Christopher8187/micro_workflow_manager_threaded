from __future__ import annotations

import pytest
from contextlib import contextmanager

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.errors import InvalidGraphError
from micro_workflow_manager.workflow.preparation import prepare_fresh_components
from tests.test_090_component_session_settlement import _rows
from tests.test_093_native_cli_readiness import _node_files
from tests.test_090_component_session_settlement import _close


@pytest.mark.parametrize('runner', ['direct', 'api'])
@pytest.mark.parametrize('entry', ['run', 'run_concurrently', 'run_node', 'run_component'])
def test_independent_full_runs_prepare_again_and_preserve_root_inputs(tmp_path, runner, entry):
    workflow = MicroWorkflow(tmp_path, runner=runner, persist_graph=False)
    workflow.graph([('A', 'A')])
    storage = workflow.storage
    observed = []

    @workflow.task('A')
    def work(ctx, value):
        observed.append(value)
        owner = storage.read_job_current_owner('A', ctx.job_id)
        ctx.write_output('result.txt', str(owner['alignment_generation']))

    workflow.start('A', job_id=11, value=7)
    instance = storage.read_job_instance_id('A', 11)
    incoming = tmp_path / 'node' / 'A' / 'input' / 'project.txt'
    incoming.write_text('incoming', encoding='utf-8')
    try:
        for round_number in (1, 2):
            obsolete = tmp_path / 'node' / 'A' / 'output' / 'obsolete.txt'
            obsolete.write_text('old result', encoding='utf-8')
            if entry == 'run':
                workflow.run()
            elif entry == 'run_concurrently':
                workflow.run_concurrently(['A'])
            elif entry == 'run_node':
                workflow.run_node('A')
            else:
                workflow.run_component({'A'})
            state = storage.get_component_state(('A',))
            assert state['lifecycle'] == 'done'
            assert state['alignment_generation'] == round_number
            assert storage.read_job_current_owner('A', 11)['alignment_generation'] == round_number
            assert storage.read_job_instance_id('A', 11) == instance
            assert storage.load_job('A', 11).params == {'value': 7}
            assert incoming.read_text(encoding='utf-8') == 'incoming'
            assert not obsolete.exists()
            assert (tmp_path / 'node' / 'A' / 'output' / 'result.txt').read_text() == str(round_number)
            assert observed == [7] * round_number
    finally:
        _close(storage)


@pytest.mark.parametrize('entry', ['selected', 'queued'])
def test_partial_calls_preserve_done_jobs_and_component_generation(tmp_path, entry):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'A')])
    storage = workflow.storage
    calls = []

    @workflow.task('A')
    def work(ctx):
        calls.append(ctx.job_id)

    workflow.start('A', job_id=1)
    workflow.start('A', job_id=2)
    storage.set_job_status('A', 1, 'done')
    old = tmp_path / 'node' / 'A' / 'output' / 'retained.txt'
    old.write_text('retained', encoding='utf-8')
    try:
        if entry == 'selected':
            workflow.run_jobs('A', [2])
        else:
            workflow.run_queued_node_jobs('A')
        assert calls == [2]
        assert storage.get_job_status('A', 1) == 'done'
        assert storage.get_component_state(('A',))['alignment_generation'] == 0
        assert old.read_text(encoding='utf-8') == 'retained'
    finally:
        _close(storage)


def test_failed_independent_preparation_restores_files_and_releases_session(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'A')])
    storage = workflow.storage
    calls = []

    @workflow.task('A')
    def work(ctx):
        calls.append(ctx.job_id)

    workflow.start('A')
    old = tmp_path / 'node' / 'A' / 'output' / 'retained.txt'
    old.write_text('retained', encoding='utf-8')
    primary = RuntimeError('preparation injection')

    def fail_preparation(*args, **kwargs):
        assert storage.get_live_main_session() is not None
        assert storage.get_component_reservation(('A',)) is not None
        raise primary

    monkeypatch.setattr(storage, 'complete_component_fresh_preparation', fail_preparation)
    try:
        with pytest.raises(RuntimeError) as raised:
            workflow.run_node('A')
        assert raised.value is primary
        assert calls == []
        assert old.read_text(encoding='utf-8') == 'retained'
        assert storage.get_component_state(('A',))['alignment_generation'] == 0
        assert storage.get_job_status('A', 1) == 'queued'
        session, = storage.list_execution_sessions()
        assert session['status'] == 'terminal' and session['outcome'] == 'failed'
        assert storage.get_live_main_session() is None
        assert storage.get_component_reservation(('A',)) is None
    finally:
        _close(storage)


@pytest.mark.parametrize('entry', ['run_node', 'run_component', 'run_concurrently'])
@pytest.mark.parametrize('parent_ready', [False, True])
@pytest.mark.parametrize('ignore_readiness', [False, True])
def test_native_parent_preflight_precedes_independent_preparation(tmp_path, entry, parent_ready, ignore_readiness):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('P', 'A')])
    storage = workflow.storage
    calls = []

    @workflow.task('P')
    def parent(ctx):
        return None

    @workflow.task('A')
    def child(ctx):
        calls.append(ctx.job_id)
        ctx.write_output('result.txt', str(len(calls)))

    workflow.start('P')
    workflow.start('A')
    try:
        workflow.run_node('P')
        workflow.run_node('A')
        if not parent_ready:
            prepare_fresh_components(tmp_path, workflow, [{'P'}])
        storage.db_mutation_barrier()
        before, files = _rows(storage), _node_files(tmp_path)

        def run():
            if entry == 'run_node':
                workflow.run_node('A', ignore_readiness=ignore_readiness)
            elif entry == 'run_component':
                workflow.run_component({'A'}, ignore_readiness=ignore_readiness)
            else:
                workflow.run_concurrently(['A'])

        if parent_ready:
            run()
            assert calls == [1, 1]
            assert storage.get_component_state(('A',))['alignment_generation'] == 2
        else:
            with pytest.raises(InvalidGraphError, match='ready'):
                run()
            assert calls == [1]
            assert _rows(storage) == before
            assert _node_files(tmp_path) == files
    finally:
        _close(storage)


def test_changed_component_selection_refuses_before_preparation(tmp_path, monkeypatch):
    from micro_workflow_manager.workflow import execution_scope

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    calls = []

    @workflow.task('A')
    def work(ctx):
        calls.append(ctx.job_id)

    workflow.start('A')
    old = tmp_path / 'node' / 'A' / 'output' / 'retained.txt'
    old.write_text('retained', encoding='utf-8')
    original = execution_scope.execution_session

    @contextmanager
    def changed_admission(*args, **kwargs):
        workflow.graph_obj.add_edge('B', 'A')
        with original(*args, **kwargs) as driver:
            yield driver

    monkeypatch.setattr(execution_scope, 'execution_session', changed_admission)
    try:
        with pytest.raises(RuntimeError, match='preparation.*selection'):
            workflow.run_node('A')
        assert calls == []
        assert old.read_text(encoding='utf-8') == 'retained'
        assert storage.get_job_status('A', 1) == 'queued'
        assert storage.get_component_state(('A', 'B'))['alignment_generation'] == 0
        assert storage.get_live_main_session() is None
        assert storage.get_component_reservation(('A', 'B')) is None
    finally:
        _close(storage)
