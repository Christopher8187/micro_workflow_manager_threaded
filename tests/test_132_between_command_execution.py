"""Public between commands share one selection and preserve excluded execution."""

import pytest

from micro_workflow_manager import cli
from tests.test_064_read_only_previews import _initialize_native_project
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_117_execution_sampling import _job_snapshot


def _between_project(root, monkeypatch):
    edges = [('A', 'L'), ('A', 'R'), ('L', 'B'), ('R', 'B'), ('B', 'Z'), ('A', 'Off')]
    sources = {'A': '''
from micro_workflow_manager import NodeRouter
router = NodeRouter('A', runner='direct')
router.create_job(number=1)
@router.task
def run(ctx):
    for node in ('L', 'R', 'Off'):
        ctx.node(node).add(role='from-A')
    return 'start'
'''}
    for node in ('L', 'R'):
        sources[node] = f'''
from micro_workflow_manager import NodeRouter
router = NodeRouter({node!r}, runner='direct')
@router.task
def run(ctx, role):
    ctx.node('B').add(role={('from-' + node)!r})
    return role
'''
    for node in ('B', 'Off'):
        sources[node] = f'''
from micro_workflow_manager import NodeRouter
router = NodeRouter({node!r}, runner='direct')
router.create_job(number=1, params={{'role': 'operator'}})
@router.task
def run(ctx, role):
    return f'{{ctx.current_node}}/{{ctx.job_id}}/{{role}}'
'''
    workflow = _initialize_native_project(root, monkeypatch, edges=edges, task_sources=sources)
    incoming = root / 'node' / 'A' / 'input' / 'operator.txt'
    incoming.write_text('preserve start input', encoding='utf-8')
    return workflow.storage, incoming


@pytest.mark.parametrize('command', ['runbetween', 'resetbetween'])
def test_fresh_between_prepares_interval_and_preserves_excluded_operator_work(
    tmp_path, monkeypatch, capsys, command,
):
    storage, incoming = _between_project(tmp_path, monkeypatch)
    try:
        capsys.readouterr()
        assert cli.main(['runfrom', 'A', '--runner', 'direct']) == 0
        capsys.readouterr()
        retained = {node: _job_snapshot(storage, node, 1) for node in ('B', 'Off')}
        assert all(storage.get_job_status(node, 1) == 'done' for node in retained)
        original_owners = {row['execution_id'] for row in _rows(storage)['job_execution_owners']}
        generations = {
            (node,): storage.get_component_state((node,))['alignment_generation']
            for node in ('A', 'L', 'R', 'B', 'Off', 'Z')
        }
        arguments = [command, 'A', 'B']
        if command == 'resetbetween':
            arguments.append('--yes')
        else:
            arguments.extend(['--runner', 'direct'])

        assert cli.main(arguments) == 0

        capsys.readouterr()
        assert incoming.read_text() == 'preserve start input'
        assert {node: _job_snapshot(storage, node, 1) for node in retained} == retained
        for node in ('A', 'L', 'R'):
            state = storage.get_component_state((node,))
            assert state['lifecycle'] == ('done' if command == 'runbetween' else 'queued')
            assert state['alignment_generation'] == generations[(node,)] + 1
            assert state['misaligned'] is False
        for node in ('B', 'Off', 'Z'):
            state = storage.get_component_state((node,))
            assert state['lifecycle'] == 'done'
            assert state['alignment_generation'] == generations[(node,)]
        assert storage.get_component_state(('B',))['misaligned'] is True
        assert storage.get_component_state(('Off',))['misaligned'] is True
        new_owners = [row for row in _rows(storage)['job_execution_owners']
                      if row['execution_id'] not in original_owners]
        if command == 'runbetween':
            assert {row['node_name'] for row in new_owners} == {'A', 'L', 'R'}
            assert all(storage.get_job_status(node, job_id) == 'queued'
                       for node in ('B', 'Off') for job_id in storage.list_job_ids(node) if job_id != 1)
        else:
            assert new_owners == []
            assert storage.get_job_status('A', 1) == 'queued'
        assert storage.get_live_main_session() is None
        assert all(storage.get_component_reservation((node,)) is None
                   for node in ('A', 'L', 'R', 'B', 'Off', 'Z'))
    finally:
        _close(storage)


def test_resumebetween_runs_queued_interval_without_running_publication_receivers(
    tmp_path, monkeypatch, capsys,
):
    storage, incoming = _between_project(tmp_path, monkeypatch)
    try:
        retained = {node: _job_snapshot(storage, node, 1) for node in ('B', 'Off')}
        excluded_states = {
            (node,): storage.get_component_state((node,))
            for node in ('B', 'Off', 'Z')
        }
        capsys.readouterr()

        assert cli.main(['resumebetween', 'A', 'B', '--runner', 'direct']) == 0

        capsys.readouterr()
        assert incoming.read_text() == 'preserve start input'
        for node in ('A', 'L', 'R'):
            state = storage.get_component_state((node,))
            assert state['lifecycle'] == 'done'
            assert state['alignment_generation'] == 0
        assert {node: _job_snapshot(storage, node, 1) for node in retained} == retained
        assert {
            (node,): storage.get_component_state((node,))
            for node in ('B', 'Off', 'Z')
        } == excluded_states
        assert {row['node_name'] for row in _rows(storage)['job_execution_owners']} == {'A', 'L', 'R'}
        assert all(storage.get_job_status(node, job_id) == 'queued'
                   for node in ('B', 'Off') for job_id in storage.list_job_ids(node))
        assert storage.get_live_main_session() is None
        assert all(storage.get_component_reservation((node,)) is None
                   for node in ('A', 'L', 'R', 'B', 'Off', 'Z'))
    finally:
        _close(storage)
