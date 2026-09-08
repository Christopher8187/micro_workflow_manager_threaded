from __future__ import annotations

import os
import socket
from hashlib import sha256
from pathlib import Path

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.models import Job
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_086_native_owned_restart import _close
from tests.test_064_read_only_previews import _wait_restart_listener_retired


def _files(root, *, allow_existing_shm=False):
    result = {}
    for path in root.rglob('*'):
        relative = path.relative_to(root)
        if allow_existing_shm and relative.as_posix() == '.mwf/state.sqlite3-shm':
            file_stat = path.stat()
            result[relative] = ('allowed-existing-shm', file_stat.st_dev, file_stat.st_ino)
        else:
            result[relative] = sha256(path.read_bytes()).hexdigest() if path.is_file() else 'directory'
    return result


def _native_project(tmp_path, monkeypatch):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'A')]",
        files={
            'A': '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('A')
                @router.task
                def run(ctx):
                    return 'A result'
            ''',
        },
    )
    workflow = load_workflow(tmp_path, 'direct')
    workflow.storage.register_component_topology(workflow.topology.snapshot())
    if not workflow.storage.job_exists('A', 1):
        workflow.storage.create_job(Job(node_name='A', job_id=1, params={}))
    return workflow



def _complete_native_result(workflow):
    storage = workflow.storage
    assert storage.get_component_state(('A',))['lifecycle'] == 'queued'
    workflow.run_node('A')
    assert storage.get_job_status('A', 1) == 'done'
    state = storage.get_component_state(('A',))
    assert (state['lifecycle'], state['stability'], state['instability_origin']) == ('done', 'stable', None)
    _wait_restart_listener_retired(storage)
    # Keep the obsolete raw-node row different so it cannot satisfy the plan assertion.
    storage.set_node_status('A', 'queued')
    storage.db_mutation_barrier()


def _install_import_sentinels(tmp_path):
    external = tmp_path.parent / f'{tmp_path.name}-preview-imported'
    (tmp_path / 'src' / 'graph.py').write_text(
        f"from pathlib import Path\nPath({str(external)!r}).write_text('graph imported')\n"
        "EDGES = [('A', 'A')]\n",
        encoding='utf-8',
    )
    (tmp_path / 'src' / 'node_behavior' / 'A.py').write_text(
        f"from pathlib import Path\nPath({str(external)!r}).write_text('task imported')\n"
        "from micro_workflow_manager import NodeRouter\nrouter = NodeRouter('A')\n"
        "@router.task\ndef run(ctx):\n    raise AssertionError('preview ran task')\n",
        encoding='utf-8',
    )
    return external


def test_closed_native_wal_preview_creates_no_sidecars_or_project_changes(
    tmp_path, monkeypatch, capsys,
):
    workflow = _native_project(tmp_path, monkeypatch)
    storage = workflow.storage
    _complete_native_result(workflow)
    storage.db_mutation_barrier()
    storage.db_connection().execute('PRAGMA wal_checkpoint(TRUNCATE)')
    _close(storage)
    external = _install_import_sentinels(tmp_path)
    wal = tmp_path / '.mwf' / 'state.sqlite3-wal'
    shm = tmp_path / '.mwf' / 'state.sqlite3-shm'
    assert not wal.exists()
    assert not shm.exists()
    before = _files(tmp_path)

    assert cli.main(['run', 'A', '--plan']) == 0

    assert '{A}: done, stable' in capsys.readouterr().out
    assert _files(tmp_path) == before
    assert not wal.exists()
    assert not shm.exists()
    assert not external.exists()


def test_live_preview_reads_committed_wal_frame_without_changing_main_or_wal(
    tmp_path, monkeypatch, capsys,
):
    workflow = _native_project(tmp_path, monkeypatch)
    storage = workflow.storage
    database = tmp_path / '.mwf' / 'state.sqlite3'
    wal = tmp_path / '.mwf' / 'state.sqlite3-wal'
    shm = tmp_path / '.mwf' / 'state.sqlite3-shm'
    storage.db_mutation_barrier()
    storage.db_connection().execute('PRAGMA wal_checkpoint(TRUNCATE)')
    main_before_update = database.read_bytes()
    _complete_native_result(workflow)
    storage.db_mutation_barrier()
    assert database.read_bytes() == main_before_update
    assert wal.is_file() and wal.stat().st_size > 0
    assert shm.is_file()
    external = _install_import_sentinels(tmp_path)
    before = _files(tmp_path, allow_existing_shm=True)
    main_before = database.read_bytes()
    wal_before = wal.read_bytes()
    try:
        assert cli.main(['run', 'A', '--plan']) == 0
        assert '{A}: done, stable' in capsys.readouterr().out
        assert _files(tmp_path, allow_existing_shm=True) == before
        assert database.read_bytes() == main_before
        assert wal.read_bytes() == wal_before
        assert not external.exists()
    finally:
        _close(storage)


@pytest.mark.parametrize('arguments', [
    ['run', 'A', '--plan'],
    ['recover', '--dry-run'],
])
def test_native_preview_reports_abandoned_session_and_recovery_without_mutation(
    tmp_path, monkeypatch, capsys, arguments,
):
    workflow = _native_project(tmp_path, monkeypatch)
    storage = workflow.storage
    component = ('A',)
    session_id = 'abandoned-preview-session'
    storage.create_execution_session(
        session_id,
        session_kind='main',
        command='run',
        start_component=component,
        selected_components=[component],
        selected_jobs=[('A', 1)],
        started_at='2020-01-01T00:00:00+00:00',
        hostname=socket.gethostname(),
        pid=99999999,
        process_identity='dead-preview-process',
        details={'start_node': 'A'},
        expected_shape=workflow.topology.snapshot().shape_json,
    )
    storage.reserve_execution_components(
        session_id, expected_shape=workflow.topology.snapshot().shape_json,
    )
    context = (session_id, {'A': component}, workflow.topology.snapshot().shape_json)
    roots = tuple(storage._read_session_job_roots(storage.db_connection(), session_id))
    storage.begin_selected_component_execution(
        context, roots, expected_identity=storage._read_component_producing_identity(
            storage.db_connection(), component,
        ), expected_state=storage.get_component_state(component),
        expected_parent_states={}, successful_lineage=('stable', None),
    )
    generation, execution_id = storage.claim_job_execution(
        'A', 1, started_at='2020-01-01T00:00:01+00:00',
        session_id=session_id, component=component,
    )
    assert generation == 0
    assert storage.read_job_control('A', 1)['active_execution_id'] == execution_id
    storage.db_mutation_barrier()
    external = _install_import_sentinels(tmp_path)
    database = tmp_path / '.mwf' / 'state.sqlite3'
    wal = tmp_path / '.mwf' / 'state.sqlite3-wal'
    assert wal.is_file() and wal.stat().st_size > 0
    assert (tmp_path / '.mwf' / 'state.sqlite3-shm').is_file()
    before = _files(tmp_path, allow_existing_shm=True)
    main_before = database.read_bytes()
    wal_before = wal.read_bytes()
    try:
        assert cli.main(arguments) == 0
        output = capsys.readouterr().out.lower()
        assert session_id in output
        assert 'abandoned' in output
        assert 'a/1' in output
        assert 'recover' in output
        assert _files(tmp_path, allow_existing_shm=True) == before
        assert database.read_bytes() == main_before
        assert wal.read_bytes() == wal_before
        assert not external.exists()
    finally:
        _close(storage)


def test_live_preview_pins_existing_sidecars_before_opening_original_sqlite(
    tmp_path, monkeypatch, capsys,
):
    from micro_workflow_manager.storage.sqlite import preview_snapshot

    workflow = _native_project(tmp_path, monkeypatch)
    storage = workflow.storage
    database = tmp_path / '.mwf' / 'state.sqlite3'
    wal = Path(f'{database}-wal')
    shm = Path(f'{database}-shm')
    _complete_native_result(workflow)
    storage.db_mutation_barrier()
    assert wal.is_file() and shm.is_file()
    external = _install_import_sentinels(tmp_path)
    before = _files(tmp_path, allow_existing_shm=True)
    main_before = database.read_bytes()
    wal_before = wal.read_bytes()
    replacement = tmp_path.parent / f'{tmp_path.name}-replacement-wal'
    replacement.write_bytes(wal_before)
    original_connect = preview_snapshot.sqlite3.connect
    attempted = []

    def connect(target, *args, **kwargs):
        text = str(target)
        if not attempted and 'state.sqlite3' in text and 'mode=ro' in text:
            attempted.append(text)
            with pytest.raises(OSError):
                wal.unlink()
            with pytest.raises(OSError):
                os.replace(replacement, wal)
            assert replacement.is_file()
        return original_connect(target, *args, **kwargs)

    try:
        monkeypatch.setattr(preview_snapshot.sqlite3, 'connect', connect)
        assert cli.main(['run', 'A', '--plan']) == 0
        if os.name == 'nt':
            assert attempted, 'The pinned original SQLite open was not reached'
        else:
            assert not attempted, 'The original database must use a private copy on this platform'
        assert '{A}: done, stable' in capsys.readouterr().out
        assert _files(tmp_path, allow_existing_shm=True) == before
        assert database.read_bytes() == main_before
        assert wal.read_bytes() == wal_before
        assert not external.exists()
    finally:
        replacement.unlink(missing_ok=True)
        _close(storage)


def test_live_sidecars_use_private_copy_when_path_deletion_cannot_be_prevented(
    tmp_path, monkeypatch, capsys,
):
    from micro_workflow_manager.storage.sqlite import preview_snapshot

    workflow = _native_project(tmp_path, monkeypatch)
    storage = workflow.storage
    database = tmp_path / '.mwf' / 'state.sqlite3'
    wal = Path(f'{database}-wal')
    shm = Path(f'{database}-shm')
    _complete_native_result(workflow)
    storage.db_mutation_barrier()
    assert wal.is_file() and shm.is_file()
    external = _install_import_sentinels(tmp_path)
    before = _files(tmp_path, allow_existing_shm=True)
    original_connect = preview_snapshot.sqlite3.connect

    def refuse_live(*args, **kwargs):
        raise preview_snapshot.PreviewSnapshotBusyError('Deletion-preventing handles unavailable')

    def connect(target, *args, **kwargs):
        from urllib.parse import unquote
        assert database.resolve().as_posix() not in unquote(str(target)).replace('\\', '/'), (
            'A platform without deletion-preventing handles opened mutable project state'
        )
        return original_connect(target, *args, **kwargs)

    try:
        monkeypatch.setattr(preview_snapshot, '_open_live_snapshot', refuse_live)
        monkeypatch.setattr(preview_snapshot.sqlite3, 'connect', connect)
        assert cli.main(['run', 'A', '--plan']) == 0
        assert '{A}: done, stable' in capsys.readouterr().out
        assert _files(tmp_path, allow_existing_shm=True) == before
        assert not external.exists()
    finally:
        _close(storage)
