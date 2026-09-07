"""An interior prerequisite blocks interval execution after wide preparation."""

import pytest

from micro_workflow_manager import cli
from tests.test_064_read_only_previews import _initialize_native_project, _snapshot
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_117_execution_sampling import _job_snapshot


def _excluded_snapshot(storage, root, nodes):
    return {
        node: {
            'component': storage.get_component_state((node,)),
            'raw_status': storage.get_node_status(node),
            'job': _job_snapshot(storage, node, 1),
            'tree': _snapshot(root / 'node' / node),
        }
        for node in nodes
    }


@pytest.mark.parametrize('runner', ['direct', 'threaded', 'api', 'process'])
@pytest.mark.parametrize('parent_done', [False, True])
def test_runbetween_preserves_unselected_parent_and_leaves_blocked_interior_queued(
    tmp_path, monkeypatch, capsys, runner, parent_done,
):
    edges = [('A', 'C'), ('C', 'B'), ('X', 'C')]
    sources = {node: f'''
from micro_workflow_manager import NodeRouter
router = NodeRouter({node!r}, runner={runner!r})
router.create_job(number=1)
@router.task
def run(ctx):
    ctx.write_output('ran.txt', {node!r})
    return {node!r}
''' for node in ('A', 'C', 'B', 'X')}
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges, task_sources=sources)
    storage = workflow.storage
    try:
        if parent_done:
            assert cli.main(['run', 'X', '--runner', runner]) == 0
        raw_status = 'queued' if parent_done else 'done'
        storage.set_node_status('X', raw_status)
        retained = tmp_path / 'node' / 'C' / 'output' / 'old.txt'
        retained.parent.mkdir(parents=True, exist_ok=True)
        retained.write_text('old selected component output', encoding='utf-8')
        excluded = _excluded_snapshot(storage, tmp_path, ('B', 'X'))
        generation = storage.get_component_state(('C',))['alignment_generation']
        owners = {row['execution_id'] for row in _rows(storage)['job_execution_owners']}
        sessions = {session['session_id'] for session in storage.list_execution_sessions()}
        capsys.readouterr()

        assert cli.main(['runbetween', 'A', 'B', '--runner', runner]) == (0 if parent_done else 1)

        capsys.readouterr()
        assert not retained.exists()
        assert storage.get_component_state(('A',))['lifecycle'] == 'done'
        interior = storage.get_component_state(('C',))
        assert interior['lifecycle'] == ('done' if parent_done else 'queued')
        assert interior['alignment_generation'] == generation + 1
        assert storage.get_job_status('C', 1) == ('done' if parent_done else 'queued')
        assert (tmp_path / 'node' / 'C' / 'output' / 'ran.txt').exists() is parent_done
        assert _excluded_snapshot(storage, tmp_path, excluded) == excluded
        assert storage.get_node_status('X') == raw_status
        assert {row['node_name'] for row in _rows(storage)['job_execution_owners']
                if row['execution_id'] not in owners} == ({'A', 'C'} if parent_done else {'A'})
        current, = [session for session in storage.list_execution_sessions()
                    if session['session_id'] not in sessions]
        assert current['selected_components'] == [('A',), ('C',)]
        assert (current['status'], current['outcome']) == ('terminal', 'done' if parent_done else 'blocked')
        assert all(storage.get_component_reservation((node,)) is None for node in ('A', 'C', 'B', 'X'))
    finally:
        _close(storage)
