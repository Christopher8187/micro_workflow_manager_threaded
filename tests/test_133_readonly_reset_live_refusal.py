from __future__ import annotations

import os
import socket

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.models import Job, now
from micro_workflow_manager.processes import process_identity
from micro_workflow_manager.storage.sqlite.preview_snapshot import open_preview_snapshot
from tests.test_064_read_only_previews import (
    _close_without_sidecars,
    _initialize_native_project,
    _install_import_sentinels,
    _snapshot,
)
from tests.test_121_native_preview_recovery import _rows


def _closed_database_rows(root):
    connection = open_preview_snapshot(root / '.mwf' / 'state.sqlite3')
    try:
        tables = [row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
        )]
        return {table: sorted((tuple(row) for row in connection.execute(
            'SELECT * FROM "' + table.replace('"', '""') + '"',
        )), key=repr) for table in tables}
    finally:
        connection.close()


@pytest.mark.parametrize('arguments', [
    ['reset', 'A', '--yes'],
    ['resetfrom', 'A', '--yes'],
    ['resetbetween', 'A', 'B', '--yes'],
])
def test_applied_reset_refuses_live_interrupt_before_import_or_mutation(
    tmp_path, monkeypatch, capsys, arguments,
):
    edges = [('A', 'B')]
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    storage = workflow.storage
    shape = workflow.topology.snapshot().shape_json
    storage.create_job(Job(node_name='A', job_id=1, params={'kept': 'A'}))
    storage.create_job(Job(node_name='B', job_id=1, params={'kept': 'B'}))
    session_id = 'live-reset-interrupt'
    storage.create_execution_session(
        session_id,
        session_kind='interrupt',
        command='run',
        start_component=('B',),
        selected_components=[('B',)],
        started_at=now(),
        hostname=socket.gethostname(),
        pid=os.getpid(),
        process_identity=process_identity(os.getpid()),
        details={'start_node': 'B'},
    )
    storage.reserve_execution_components(session_id, expected_shape=shape)
    session = storage.get_execution_session(session_id)
    assert session is not None
    assert session['status'] == 'running'
    assert session['session_kind'] == 'interrupt'
    before_rows = _rows(storage)
    _close_without_sidecars(storage, tmp_path)
    external = _install_import_sentinels(tmp_path, edges)
    before_files = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main(arguments) == 1

    captured = capsys.readouterr()
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files
    assert not external.exists()
    assert not (tmp_path / '.mwf' / 'state.sqlite3-wal').exists()
    assert not (tmp_path / '.mwf' / 'state.sqlite3-shm').exists()
    output = (captured.out + captured.err).lower()
    assert session_id in output
    assert 'live' in output
    assert 'requires no live' in output or 'refus' in output
