"""Shared command previews use native prerequisites and retained publications."""

import pytest

from micro_workflow_manager import cli
from tests.test_076_component_state_transitions import _seed_state
from tests.test_064_read_only_previews import (
    _close_without_sidecars, _initialize_native_project,
    _install_import_sentinels, _snapshot, _wait_restart_listener_retired,
)


@pytest.mark.parametrize('command,arguments,flag', [
    ('run', ['A'], '--plan'),
    ('runfrom', ['A'], '--plan'),
    ('runbetween', ['A', 'C'], '--plan'),
    ('resume', ['A'], '--plan'),
    ('resumefrom', ['A'], '--plan'),
    ('resumebetween', ['A', 'C'], '--plan'),
    ('reset', ['A'], '--dry-run'),
    ('resetfrom', ['A'], '--dry-run'),
    ('resetbetween', ['A', 'C'], '--dry-run'),
])
@pytest.mark.parametrize('native_status,raw_status', [('queued', 'done'), ('done', 'queued')])
def test_graph_command_previews_display_native_external_prerequisites(
    tmp_path, monkeypatch, capsys, command, arguments, flag, native_status, raw_status,
):
    edges = [('P', 'A'), ('A', 'B'), ('B', 'C')]
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    storage = workflow.storage
    if native_status == 'done':
        _seed_state(
            storage, ('P',), lifecycle='done', stability='stable', origin=None, generation=0,
        )
    storage.set_node_status('P', raw_status)
    assert storage.get_component_state(('P',))['lifecycle'] == native_status
    assert storage.get_node_status('P') == raw_status
    _close_without_sidecars(storage, tmp_path)
    sentinel = _install_import_sentinels(tmp_path, edges)
    before = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main([command, *arguments, flag]) == 0

    output = capsys.readouterr().out
    assert f'P: {native_status}' in output
    assert 'user code was not loaded' in output
    assert _snapshot(tmp_path) == before
    assert not sentinel.exists()


@pytest.mark.parametrize('command,flag', [('runbetween', '--plan'), ('resetbetween', '--dry-run')])
def test_between_fresh_preview_includes_stored_job_receiver_outside_crossing_edges(
    tmp_path, monkeypatch, capsys, command, flag,
):
    edges = [('A', 'B'), ('B', 'C'), ('Hidden', 'Other')]
    workflow = _initialize_native_project(
        tmp_path, monkeypatch, edges=edges,
        task_sources={'A': '''
from micro_workflow_manager import NodeRouter
router = NodeRouter('A', runner='direct')
router.create_job(number=1)
@router.task
def run(ctx):
    ctx.system.start('Hidden')
    return 'A'
'''},
    )
    storage = workflow.storage
    capsys.readouterr()
    workflow.run_node('A')
    assert storage.get_job_status('A', 1) == 'done'
    capsys.readouterr()
    assert storage.get_job_status('Hidden', 1) == 'queued'
    assert storage.get_component_state(('Hidden',))['lifecycle'] == 'queued'
    assert ('A', 'Hidden') not in edges
    _wait_restart_listener_retired(storage)
    _close_without_sidecars(storage, tmp_path)
    sentinel = _install_import_sentinels(tmp_path, edges)
    before = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main([command, 'A', 'B', flag]) == 0

    output = capsys.readouterr().out.replace('\\', '/')
    assert 'selected Hoeflein components: {A}' in output
    assert 'excluded end component: {B}' in output
    assert 'leaving edges: A -> B' in output
    assert 'unselected receivers: B, Hidden' in output
    assert 'Hidden/1' in output
    assert 'user code was not loaded' in output
    assert _snapshot(tmp_path) == before
    assert not sentinel.exists()
