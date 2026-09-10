"""Fresh membership repair leaves an unchanged stopped branch intact."""

from __future__ import annotations

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.storage import FileStorage
from tests.test_064_read_only_previews import _close_without_sidecars
from tests.test_117_execution_sampling import _job_snapshot
from tests.test_149_interrupt_declarations import _make_interrupt_project, _router_source


@pytest.mark.parametrize('arguments', [['runfrom', 'S'], ['runbetween', 'S', 'D']])
def test_upstream_membership_repair_preserves_stopped_failed_branch(
    tmp_path, monkeypatch, arguments,
):
    original_edges = [('S', 'I'), ('I', 'D'), ('S', 'R')]
    _make_interrupt_project(
        tmp_path, monkeypatch, edges=original_edges,
        interrupt_nodes={'I'}, jobs={'S', 'R', 'I', 'D'},
    )
    failing = _router_source('I', interrupt='True', jobs=1)
    failing = failing.replace("    return 'I'", "    raise RuntimeError('retained failure')")
    (tmp_path / 'src/node_behavior/I.py').write_text(failing, encoding='utf-8')
    assert cli.main([
        'runfrom', 'S', '--runner', 'direct', '--interrupt-policy', 'run-all',
    ]) == 1
    storage = FileStorage(tmp_path)
    before = {
        node: (_job_snapshot(storage, node, 1), storage.get_component_state((node,)))
        for node in ('I', 'D')
    }
    assert before['I'][1]['lifecycle'] == 'failed'
    _close_without_sidecars(storage, tmp_path)

    (tmp_path / 'src/graph.py').write_text(
        f'EDGES = {original_edges + [("R", "S")]!r}\n', encoding='utf-8',
    )
    assert cli.main(['graph', '--update']) == 0
    assert cli.main([
        *arguments, '--runner', 'direct', '--interrupt-policy', 'stop-all',
    ]) == 0

    storage = FileStorage(tmp_path)
    try:
        for node in ('I', 'D'):
            assert (_job_snapshot(storage, node, 1), storage.get_component_state((node,))) == before[node]
        assert storage.get_component_state(('R', 'S'))['lifecycle'] == 'done'
        assert storage.get_live_main_session() is None
        assert storage.db_connection().execute(
            'SELECT COUNT(*) FROM component_reservations',
        ).fetchone()[0] == 0
        assert sorted(session['outcome'] for session in storage.list_execution_sessions()) == ['failed', 'stopped']
    finally:
        _close_without_sidecars(storage, tmp_path)
