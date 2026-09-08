"""Public regressions for nine-command admission and display boundaries."""

from __future__ import annotations

import socket
from concurrent.futures import Future

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.models import Job
from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_064_read_only_previews import (
    _close_without_sidecars,
    _initialize_native_project,
    _install_import_sentinels,
    _snapshot,
)
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_093_native_cli_readiness import _node_files
from tests.test_117_execution_sampling import _job_snapshot
from tests.test_121_native_preview_recovery import _rows as _tuple_rows
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows


def _prepared_selection_snapshot(storage, root, nodes):
    return {
        'components': {node: storage.get_component_state((node,)) for node in nodes},
        'raw_statuses': {node: storage.get_node_status(node) for node in nodes},
        'jobs': {
            node: {
                job_id: _job_snapshot(storage, node, job_id)
                for job_id in storage.list_job_ids(node)
            }
            for node in nodes
        },
        'causes': {
            node: storage.read_component_misalignment_causes((node,))
            for node in nodes
        },
        'trees': {node: _snapshot(root / 'node' / node) for node in nodes},
        'receipts': [
            tuple(row) for row in storage.db_connection().execute(
                'SELECT * FROM preparation_receipts ORDER BY operation_id'
            )
        ],
    }


@pytest.mark.parametrize('session_kind', ['main', 'interrupt'])
@pytest.mark.parametrize('arguments', [
    ['reset', 'A', '--yes'],
    ['resetfrom', 'A', '--yes'],
    ['resetbetween', 'A', 'B', '--yes'],
])
def test_applied_reset_refuses_abandoned_native_session_before_import_or_mutation(
    tmp_path, monkeypatch, capsys, session_kind, arguments,
):
    edges = [('A', 'B')]
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    storage = workflow.storage
    shape = workflow.topology.snapshot().shape_json
    storage.create_job(Job(node_name='A', job_id=1, params={'kept': 'A'}))
    storage.create_job(Job(node_name='B', job_id=1, params={'kept': 'B'}))
    session_id = f'abandoned-reset-{session_kind}'
    storage.create_execution_session(
        session_id,
        session_kind=session_kind,
        command='run',
        start_component=('B',),
        selected_components=[('B',)],
        started_at='2020-01-01T00:00:00+00:00',
        hostname=socket.gethostname(),
        pid=99999999,
        process_identity='retired-process',
        details={'start_node': 'B'},
        expected_shape=shape,
    )
    storage.reserve_execution_components(session_id, expected_shape=shape)
    before_rows = _tuple_rows(storage)
    _close_without_sidecars(storage, tmp_path)
    sentinel = _install_import_sentinels(tmp_path, edges)
    before_files = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main(arguments) == 1

    captured = capsys.readouterr()
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files
    assert not sentinel.exists()
    assert not (tmp_path / '.mwf' / 'state.sqlite3-wal').exists()
    assert not (tmp_path / '.mwf' / 'state.sqlite3-shm').exists()
    output = (captured.out + captured.err).lower()
    assert session_id in output
    assert 'running' in output or 'recovery' in output


@pytest.mark.parametrize('command,arguments,selected_nodes', [
    ('run', ['run', 'A'], ('A',)),
    ('runfrom', ['runfrom', 'A'], ('A', 'B')),
    ('runbetween', ['runbetween', 'A', 'B'], ('A',)),
])
def test_fresh_cli_parent_change_after_reservation_precedes_preparation(
    tmp_path, monkeypatch, capsys, command, arguments, selected_nodes,
):
    edges = [('P', 'A'), ('A', 'B')]
    sources = {node: f'''
from micro_workflow_manager import NodeRouter
router = NodeRouter({node!r}, runner='direct')
router.create_job(number=1)
@router.task
def run(ctx):
    ctx.write_output('ran.txt', {node!r})
    return {node!r}
''' for node in ('P', 'A', 'B')}
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges, task_sources=sources)
    storage = workflow.storage
    assert cli.main(['run', 'P']) == 0
    assert cli.main(['runfrom', 'A']) == 0
    before = _prepared_selection_snapshot(storage, tmp_path, selected_nodes)
    before_sessions = {row['session_id'] for row in storage.list_execution_sessions()}
    reserve = FileStorage.reserve_execution_components
    changed = []

    def change_parent_after_reservation(self, *args, **kwargs):
        result = reserve(self, *args, **kwargs)
        if isinstance(result, Future):
            result.result()
        if not changed:
            changed.append(True)
            changed_rows = self.submit_db_mutation(lambda connection: connection.execute(
                "UPDATE component_states SET lifecycle='queued', stability=NULL, "
                "instability_origin=NULL, retained_result_shape_id=NULL, "
                "retained_result_alignment_generation=NULL WHERE component_key=? AND lifecycle='done'",
                (encode_component_key(('P',)),),
            ).rowcount)
            assert changed_rows == 1
        return result

    monkeypatch.setattr(FileStorage, 'reserve_execution_components', change_parent_after_reservation)
    capsys.readouterr()
    try:
        assert cli.main(arguments) == 1

        captured = capsys.readouterr()
        assert changed == [True]
        assert storage.get_component_state(('P',))['lifecycle'] == 'queued'
        assert _prepared_selection_snapshot(storage, tmp_path, selected_nodes) == before
        added = [row for row in storage.list_execution_sessions()
                 if row['session_id'] not in before_sessions]
        assert len(added) == 1
        assert (added[0]['status'], added[0]['outcome']) == ('terminal', 'failed')
        assert all(storage.get_component_reservation((node,)) is None for node in selected_nodes)
        message = (captured.out + captured.err).lower()
        assert 'parent' in message
        assert 'changed' in message or 'ready' in message
    finally:
        _close(storage)


def test_runbetween_preserves_lexical_component_order_in_preview_session_and_execution(
    tmp_path, monkeypatch, capsys,
):
    edges = [('A', 'R'), ('A', 'L'), ('R', 'B'), ('L', 'B')]
    sources = {node: f'''
from pathlib import Path
from micro_workflow_manager import NodeRouter
router = NodeRouter({node!r}, runner='direct')
router.create_job(number=1)
@router.task
def run(ctx):
    marker = Path(ctx.system.storage.project_dir) / 'execution-order.txt'
    with marker.open('a', encoding='utf-8') as stream:
        stream.write(ctx.current_node + '\\n')
    return ctx.current_node
''' for node in ('A', 'L', 'R', 'B')}
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges, task_sources=sources)
    storage = workflow.storage
    try:
        sessions = {row['session_id'] for row in storage.list_execution_sessions()}
        capsys.readouterr()

        assert cli.main(['runbetween', 'A', 'B', '--plan']) == 0
        preview = capsys.readouterr().out
        assert 'selected Hoeflein components: {A}, {L}, {R}' in preview
        assert 'selected nodes: A, L, R' in preview

        assert cli.main(['runbetween', 'A', 'B', '--runner', 'direct']) == 0

        output = capsys.readouterr().out
        assert (tmp_path / 'execution-order.txt').read_text(encoding='utf-8') == 'A\nL\nR\n'
        current, = [row for row in storage.list_execution_sessions()
                    if row['session_id'] not in sessions]
        assert current['selected_components'] == [('A',), ('L',), ('R',)]
        assert (current['status'], current['outcome']) == ('terminal', 'done')
        assert 'Ran:\n  A\n  L\n  R' in output
        assert storage.get_job_status('B', 1) == 'queued'
    finally:
        _close(storage)


@pytest.mark.parametrize('planned', [False, True])
def test_runfrom_unknown_refusal_boundary_matches_live_validation(
    tmp_path, monkeypatch, capsys, planned,
):
    edges = [('A', 'B')]
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    storage = workflow.storage
    before_selection = _prepared_selection_snapshot(storage, tmp_path, ('A', 'B'))
    before_sessions = storage.list_execution_sessions()
    before_rows = None
    before_files = None
    sentinel = None
    if planned:
        before_rows = _tuple_rows(storage)
        _close_without_sidecars(storage, tmp_path)
        sentinel = _install_import_sentinels(tmp_path, edges)
        before_files = _snapshot(tmp_path)
    capsys.readouterr()
    arguments = ['runfrom', 'A', 'refuse', 'Missing']
    if planned:
        arguments.append('--plan')

    assert cli.main(arguments) == 1

    error = capsys.readouterr().err
    assert error.strip().endswith('Unknown node: Missing')
    if planned:
        assert _closed_database_rows(tmp_path) == before_rows
        assert _snapshot(tmp_path) == before_files
        assert not sentinel.exists()
        assert not (tmp_path / '.mwf' / 'state.sqlite3-wal').exists()
        assert not (tmp_path / '.mwf' / 'state.sqlite3-shm').exists()
    else:
        try:
            assert _prepared_selection_snapshot(storage, tmp_path, ('A', 'B')) == before_selection
            assert storage.list_execution_sessions() == before_sessions
            assert storage.get_component_reservation(('A',)) is None
            assert storage.get_component_reservation(('B',)) is None
        finally:
            _close(storage)


def test_resumebetween_misalignment_guidance_retains_excluded_end(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('P', 'C'), ('B', 'C'), ('C', 'E')]",
        files={
            'P': '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('P', runner='direct')
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    ctx.node('C').add(value=ctx.job_id)
                    return ctx.job_id
            ''',
            'B': '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('B', runner='direct')
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    return ctx.job_id
            ''',
            'C': '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('C', runner='direct')
                @router.task
                def run(ctx, value):
                    ctx.write_output(f'received-{value}.txt', str(value))
                    return value
            ''',
            'E': '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('E', runner='direct')
                @router.task
                def run(ctx):
                    return ctx.job_id
            ''',
        },
    )
    workflow = load_workflow(tmp_path)
    try:
        workflow.run()
        workflow.start('P', job_id=2)
        assert workflow.run_job('P', 2) == 2
    finally:
        _close(workflow.storage)

    storage = FileStorage(tmp_path)
    try:
        state = storage.get_component_state(('C',))
        assert (state['lifecycle'], state['misaligned']) == ('done', True)
        assert storage.get_component_state(('B',))['lifecycle'] == 'done'
        before_rows = _rows(storage)
        before_files = _node_files(tmp_path)
        capsys.readouterr()

        assert cli.main(['resumebetween', 'B', 'E']) == 1

        text = capsys.readouterr().err
        assert 'mwf resetbetween C E' in text
        assert 'mwf resumebetween B E' in text
        assert 'mwf resetfrom C' not in text
        assert _rows(storage) == before_rows
        assert _node_files(tmp_path) == before_files
        assert storage.get_component_reservation(('B',)) is None
        assert storage.get_component_reservation(('C',)) is None
    finally:
        _close(storage)
