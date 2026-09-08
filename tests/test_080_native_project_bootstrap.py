from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import networkx as nx
import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.parser import build_parser
from micro_workflow_manager.cli.files import read_config
from micro_workflow_manager.cli.engine import build_engine_snapshot
from micro_workflow_manager.cli.preview import PreviewStorage, load_preview
from micro_workflow_manager.cli.node_clipboard import (
    copy_node_to_clipboard, paste_node_from_clipboard,
)
from micro_workflow_manager.models import Job
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.topology import ComponentTopology


def _close(storage):
    storage.db_mutation_barrier()
    deadline = time.perf_counter() + 10
    while storage.mutation_writer_diagnostics()['writer_alive']:
        assert time.perf_counter() < deadline, 'Mutation writer did not retire'
        time.sleep(0.01)
    storage.close_database_connections()


def test_embedded_migration_command_is_not_available(capsys):
    with pytest.raises(SystemExit) as error:
        build_parser().parse_args(['migrate'])
    assert error.value.code == 2
    assert "invalid choice: 'migrate'" in capsys.readouterr().err


@pytest.mark.parametrize('layout', ['directory', 'root-file'])
@pytest.mark.parametrize('reader', [build_engine_snapshot, load_preview, PreviewStorage])
def test_direct_readers_refuse_old_project_format_without_writes(tmp_path, layout, reader):
    if layout == 'directory':
        (tmp_path / '.mwf').mkdir()
        configuration = tmp_path / '.mwf' / 'project.json'
    else:
        configuration = tmp_path / '.mwf'
    configuration.write_text(json.dumps({
        'version': 4, 'schema_version': 2, 'edges': [['A', 'B']], 'graph_path': None,
    }), encoding='utf-8')
    before = _filesystem_state(tmp_path)
    with pytest.raises(RuntimeError, match='Unsupported MWF project format'):
        reader(tmp_path)
    assert _filesystem_state(tmp_path) == before


@pytest.mark.parametrize('reader', [load_preview, PreviewStorage])
def test_native_preview_refuses_missing_database_without_fallback(tmp_path, reader):
    storage = FileStorage(tmp_path)
    _close(storage)
    (tmp_path / '.mwf' / 'state.sqlite3').unlink()
    before = _filesystem_state(tmp_path)
    with pytest.raises(RuntimeError, match='Native MWF project state is missing'):
        reader(tmp_path)
    assert _filesystem_state(tmp_path) == before


@pytest.mark.parametrize('reader', [load_preview, PreviewStorage])
@pytest.mark.parametrize('state', ['schema-four', 'missing-trigger', 'noncanonical', 'valid'])
def test_native_preview_validates_the_database_before_returning_observations(tmp_path, reader, state):
    storage = FileStorage(tmp_path)
    storage.create_job(Job(node_name='A', job_id=1, params={'value': 'retained'}))
    _close(storage)
    database = tmp_path / '.mwf' / 'state.sqlite3'
    if state == 'schema-four':
        database.unlink()
        connection = sqlite3.connect(database)
        connection.executescript(
            'CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT);'
            "INSERT INTO metadata VALUES('database_schema_version', '4');"
            'CREATE TABLE nodes(node_name TEXT PRIMARY KEY, status TEXT);'
            'CREATE TABLE jobs(node_name TEXT, job_id INTEGER, status TEXT);'
        )
        connection.close()
    elif state != 'valid':
        connection = sqlite3.connect(database)
        try:
            connection.execute('DROP TRIGGER create_job_instance' if state == 'missing-trigger' else
                               "UPDATE metadata SET value='06' WHERE key='database_schema_version'")
            connection.commit()
        finally:
            connection.close()
    payload = (tmp_path / 'node' / 'A' / 'jobs' / '1' / 'input.json').read_bytes()
    if state == 'valid':
        preview = reader(tmp_path)
        preview_storage = preview.storage if reader is load_preview else preview
        try:
            assert preview_storage.get_job_status('A', 1) == 'queued'
        finally:
            preview_storage.close()
    else:
        with pytest.raises(RuntimeError, match='Unsupported MWF project format|Incomplete SQLite|Invalid SQLite'):
            reader(tmp_path)
    assert (tmp_path / 'node' / 'A' / 'jobs' / '1' / 'input.json').read_bytes() == payload


def test_cli_config_returns_the_same_value_it_validated(tmp_path, monkeypatch):
    storage = FileStorage(tmp_path)
    _close(storage)
    configuration = tmp_path / '.mwf' / 'project.json'
    original_read = Path.read_text
    reads = []

    def replace_after_read(path, *args, **kwargs):
        value = original_read(path, *args, **kwargs)
        if path == configuration:
            reads.append(value)
            if len(reads) == 1:
                path.write_text(json.dumps({'version': 4, 'schema_version': 2}), encoding='utf-8')
        return value

    monkeypatch.setattr(Path, 'read_text', replace_after_read)
    config = read_config(tmp_path)
    assert config['version'] == 6
    assert len(reads) == 1


@pytest.mark.parametrize('replacement', ['empty-database', 'native-database', 'directory', 'configuration'])
def test_fresh_creation_cannot_admit_replaced_owned_paths(tmp_path, monkeypatch, replacement):
    root = tmp_path / 'fresh'
    root.mkdir()
    supplier = FileStorage(tmp_path / 'supplier')
    supplier.create_job(Job(node_name='retained', job_id=7, params={'value': 'supplier'}))
    _close(supplier)
    supplier_metadata = tmp_path / 'supplier' / '.mwf'
    metadata = root / '.mwf'
    database = metadata / 'state.sqlite3'
    configuration = metadata / 'project.json'
    replaced = []
    original_initialize = FileStorage._initialize_storage
    original_schema = FileStorage.initialize_state_database
    original_open = Path.open

    def replace_database():
        donor = root / 'replacement.sqlite3'
        if replacement == 'empty-database':
            donor.write_bytes(b'')
        else:
            donor.write_bytes((supplier_metadata / 'state.sqlite3').read_bytes())
        donor.replace(database)
        replaced.append((database, database.stat().st_ino, database.read_bytes()))

    def initialize(storage, project_dir, **kwargs):
        result = original_initialize(storage, project_dir, **kwargs)
        if Path(project_dir) == root and kwargs.get('create'):
            if replacement == 'native-database':
                replace_database()
            elif replacement == 'directory':
                metadata.rename(root / 'original-claimed-state')
                (supplier_metadata / 'project.json').unlink()
                supplier_metadata.rename(metadata)
                replaced.extend((path, path.stat().st_ino, path.read_bytes()) for path in metadata.iterdir() if path.is_file())
        return result

    def schema(storage, **kwargs):
        if storage.project_dir == root and kwargs.get('create') and replacement == 'empty-database':
            replace_database()
        return original_schema(storage, **kwargs)

    @contextmanager
    def replace_configuration_on_close(handle):
        with handle:
            yield handle
        donor = root / 'replacement.json'
        donor.write_bytes(b'{"owned": "by another writer"}')
        donor.replace(configuration)
        replaced.append((configuration, configuration.stat().st_ino, configuration.read_bytes()))

    def open_file(path, *args, **kwargs):
        handle = original_open(path, *args, **kwargs)
        if replacement == 'configuration' and path == configuration and args and args[0] == 'x':
            return replace_configuration_on_close(handle)
        return handle

    monkeypatch.setattr(FileStorage, '_initialize_storage', initialize)
    monkeypatch.setattr(FileStorage, 'initialize_state_database', schema)
    monkeypatch.setattr(Path, 'open', open_file)
    with pytest.raises(RuntimeError, match='changed during initialization'):
        unexpected = FileStorage(root)
        _close(unexpected)
    assert replaced, 'The controlled replacement was not reached'
    for path, identity, contents in replaced:
        assert path.stat().st_ino == identity
        assert path.read_bytes() == contents
    if replacement in {'empty-database', 'native-database'}:
        assert not configuration.exists()


@pytest.mark.parametrize('fault', ['open', 'write', 'flush', 'fsync', 'close'])
def test_failed_fresh_config_publication_preserves_source_and_allows_retry(tmp_path, monkeypatch, fault):
    retained = tmp_path / 'retained.py'
    retained.write_bytes(b'# source belongs to the user\n')
    configuration = tmp_path / '.mwf' / 'project.json'
    original_open = Path.open
    original_fsync = os.fsync
    fired = []

    def fail(stage):
        if stage == fault and not fired:
            fired.append(stage)
            raise OSError('injected fresh configuration publication failure')

    class InterruptedFile:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.handle.close()
            fail('close')

        def fileno(self):
            return self.handle.fileno()

        def write(self, value):
            result = self.handle.write(value)
            fail('write')
            return result

        def flush(self):
            self.handle.flush()
            fail('flush')

    def open_file(path, *args, **kwargs):
        if path == configuration and args and args[0] == 'x':
            fail('open')
            return InterruptedFile(original_open(path, *args, **kwargs))
        return original_open(path, *args, **kwargs)

    def fsync(descriptor):
        result = original_fsync(descriptor)
        fail('fsync')
        return result

    with monkeypatch.context() as patch:
        patch.setattr(Path, 'open', open_file)
        patch.setattr(os, 'fsync', fsync)
        with pytest.raises(OSError, match='injected fresh configuration publication failure'):
            FileStorage(tmp_path)
    assert fired == [fault]
    assert not (tmp_path / '.mwf').exists()
    assert retained.read_bytes() == b'# source belongs to the user\n'
    storage = FileStorage(tmp_path)
    try:
        assert storage.database_integrity_check() == 'ok'
        assert storage.list_execution_sessions() == []
    finally:
        _close(storage)


@pytest.mark.parametrize('suffix', ['-wal', '-shm'])
def test_failed_creation_preserves_unowned_database_companions(tmp_path, monkeypatch, suffix):
    metadata = tmp_path / '.mwf'
    configuration = metadata / 'project.json'
    companion = metadata / ('state.sqlite3' + suffix)
    original_open = Path.open
    replacement_identity = []

    def open_file(path, *args, **kwargs):
        if path == configuration and args and args[0] == 'x':
            companion.write_bytes(b'belongs to another writer')
            replacement_identity.append(companion.stat().st_ino)
            raise OSError('injected failure after foreign companion appeared')
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, 'open', open_file)
    with pytest.raises(OSError, match='injected failure after foreign companion appeared'):
        FileStorage(tmp_path)
    assert replacement_identity
    assert companion.exists()
    assert companion.stat().st_ino == replacement_identity[0]
    assert companion.read_bytes() == b'belongs to another writer'
    assert not configuration.exists()
    assert not (companion.parent / 'state.sqlite3').exists()
    assert set(companion.parent.iterdir()) == {companion}


def test_a_second_process_cannot_adopt_an_unpublished_fresh_project(tmp_path):
    root = tmp_path / 'fresh'
    ready = tmp_path / 'creator-ready'
    code = '''
import sys
from pathlib import Path
from micro_workflow_manager.storage import FileStorage
original = FileStorage.initialize_state_database
def initialize(storage, **kwargs):
    if kwargs.get('create'):
        Path(sys.argv[2]).write_text('claimed')
        if sys.stdin.readline().strip() != 'continue':
            raise RuntimeError('Creator was not released')
    return original(storage, **kwargs)
FileStorage.initialize_state_database = initialize
storage = FileStorage(Path(sys.argv[1]))
assert storage.database_integrity_check() == 'ok'
storage.close_database_connections()
'''
    creator = subprocess.Popen([sys.executable, '-c', code, str(root), str(ready)],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 15
        while not ready.exists() and creator.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ready.exists(), 'The first creator did not reach its publication gate'
        second = subprocess.run(
            [sys.executable, '-c', 'import sys; from micro_workflow_manager.storage import FileStorage; FileStorage(sys.argv[1])', str(root)],
            capture_output=True, text=True, timeout=15,
        )
        assert second.returncode != 0
        assert 'Unsupported MWF project format' in second.stderr
        assert not (root / '.mwf' / 'project.json').exists()
        output, errors = creator.communicate('continue\n', timeout=15)
        assert creator.returncode == 0, output + errors
        storage = FileStorage(root)
        try:
            assert storage.database_integrity_check() == 'ok'
            assert storage.list_execution_sessions() == []
            assert read_config(root)['version'] == 6
        finally:
            _close(storage)
        assert {path.name for path in (root / '.mwf').iterdir()} == {'project.json', 'state.sqlite3'}
    finally:
        if creator.poll() is None:
            creator.kill()
        creator.communicate()


@pytest.mark.parametrize('command', [copy_node_to_clipboard, paste_node_from_clipboard])
def test_direct_clipboard_refuses_old_project_before_copying_or_replacing_files(tmp_path, command):
    metadata = tmp_path / '.mwf'
    metadata.mkdir()
    (metadata / 'project.json').write_text(json.dumps({
        'version': 4, 'schema_version': 2, 'edges': [['A', 'B']],
    }), encoding='utf-8')
    original = tmp_path / 'node' / 'A' / 'input' / 'retained.txt'
    original.parent.mkdir(parents=True)
    original.write_bytes(b'original project input')
    clipboard = tmp_path / 'clipboard' / 'A' / 'input' / 'retained.txt'
    clipboard.parent.mkdir(parents=True)
    clipboard.write_bytes(b'different clipboard input')
    before = _filesystem_state(tmp_path)
    with pytest.raises(RuntimeError, match='Unsupported MWF project format'):
        command(tmp_path, 'A')
    assert _filesystem_state(tmp_path) == before


@pytest.mark.parametrize('entry', ['storage', 'clipboard'])
def test_missing_clipboard_snapshot_preserves_native_jobs_and_payloads(tmp_path, entry):
    storage = FileStorage(tmp_path)
    try:
        storage.create_job(Job(node_name='A', job_id=1, params={'value': 'original'}))
        identity = storage.read_job_instance_id('A', 1)
        clipboard = tmp_path / 'clipboard' / 'A' / 'jobs' / '1' / 'input.json'
        clipboard.parent.mkdir(parents=True)
        clipboard.write_text('{"value": "replacement"}', encoding='utf-8')
        node_before = _filesystem_state(tmp_path / 'node')
        clipboard_before = _filesystem_state(tmp_path / 'clipboard')
        with pytest.raises(RuntimeError, match='Native clipboard state snapshot is missing'):
            if entry == 'storage':
                storage.import_node_state('A', tmp_path / 'clipboard' / 'A' / '.mwf-node-state.sqlite3')
            else:
                paste_node_from_clipboard(tmp_path, 'A')
        assert storage.read_job_instance_id('A', 1) == identity
        assert storage.load_job('A', 1).params == {'value': 'original'}
        assert _filesystem_state(tmp_path / 'node') == node_before
        assert _filesystem_state(tmp_path / 'clipboard') == clipboard_before
    finally:
        _close(storage)


def test_native_reconciliation_refuses_payload_only_jobs_before_changing_metadata(tmp_path):
    storage = FileStorage(tmp_path)
    try:
        for job_id in (1, 2):
            storage.create_job(Job(node_name='A', job_id=job_id, params={'value': job_id}))
        identities = {job_id: storage.read_job_instance_id('A', job_id) for job_id in (1, 2)}
        storage.set_job_status('A', 1, 'running')
        storage.input_file('A', 2).unlink()
        orphan = tmp_path / 'node' / 'A' / 'jobs' / '99' / 'input.json'
        orphan.parent.mkdir()
        orphan.write_text('{"value": "untracked"}', encoding='utf-8')
        before = _filesystem_state(tmp_path / 'node')
        with pytest.raises(RuntimeError, match='Clipboard payloads have no native job state: A/99'):
            storage.reconcile_pasted_node_state('A')
        assert storage.get_job_status('A', 1) == 'running'
        assert storage.get_job_status('A', 2) == 'queued'
        assert {job_id: storage.read_job_instance_id('A', job_id) for job_id in (1, 2)} == identities
        assert storage.read_job_instance_id('A', 99) is None
        assert _filesystem_state(tmp_path / 'node') == before
    finally:
        _close(storage)


def test_failed_clipboard_import_preserves_destination_job_state(tmp_path):
    source = FileStorage(tmp_path / 'source')
    destination = FileStorage(tmp_path / 'destination')
    try:
        source.create_job(Job(node_name='A', job_id=1, params={'value': 'source'}))
        destination.create_job(Job(node_name='A', job_id=7, params={'value': 'retained'}))
        identity = destination.read_job_instance_id('A', 7)
        snapshot = source.export_node_state('A', tmp_path / 'clipboard.sqlite3')
        connection = sqlite3.connect(snapshot)
        try:
            connection.execute('INSERT INTO jobs SELECT * FROM jobs')
            connection.commit()
        finally:
            connection.close()
        with pytest.raises(sqlite3.IntegrityError):
            destination.import_node_state('A', snapshot)
        assert destination.read_job_instance_id('A', 7) == identity
        assert destination.get_job_status('A', 7) == 'queued'
        assert destination.load_job('A', 7).params == {'value': 'retained'}
        assert destination.read_job_instance_id('A', 1) is None
        assert destination.database_integrity_check() == 'ok'
    finally:
        _close(source)
        _close(destination)


def test_ordinary_creation_and_fresh_process_reopen_use_native_sessions(tmp_path):
    storage = FileStorage(tmp_path)
    try:
        assert storage.list_execution_sessions() == []
        graph = nx.DiGraph()
        graph.add_node('A')
        snapshot = ComponentTopology(graph, []).snapshot()
        storage.register_component_topology(snapshot)
        storage.create_job(Job(node_name='A', job_id=1, params={'value': 'retained'}))
        instance_id = storage.read_job_instance_id('A', 1)
        record = storage.create_execution_session(
            'native-main', session_kind='main', command='run',
            start_component=('A',), selected_components=[('A',)],
            selected_jobs=[('A', 1)], started_at='2026-09-05T12:00:00+00:00',
            hostname='worker.example', pid=123, process_identity='native-process',
            expected_shape=snapshot.shape_json,
        )
        assert record['session_id'] == 'native-main'
        assert storage.finish_execution_session(
            'native-main', outcome='done', finished_at='2026-09-05T12:01:00+00:00',
        )
    finally:
        _close(storage)

    script = '''
import json
import sys
from micro_workflow_manager.storage import FileStorage
storage = FileStorage(sys.argv[1])
try:
    print(json.dumps({
        'sessions': [row['session_id'] for row in storage.list_execution_sessions()],
        'outcome': storage.get_execution_session('native-main')['outcome'],
        'instance_id': storage.read_job_instance_id('A', 1),
        'params': storage.load_job('A', 1).params,
        'integrity': storage.database_integrity_check(),
    }))
finally:
    storage.close_database_connections()
'''
    reopened = subprocess.run(
        [sys.executable, '-c', script, str(tmp_path)], capture_output=True,
        text=True, timeout=30, check=True,
    )
    assert json.loads(reopened.stdout) == {
        'sessions': ['native-main'], 'outcome': 'done',
        'instance_id': instance_id, 'params': {'value': 'retained'}, 'integrity': 'ok',
    }
    assert not (tmp_path / '.mwf' / 'run.json').exists()
    assert not (tmp_path / '.mwf_run.json').exists()


def _filesystem_state(root):
    return {
        path.relative_to(root).as_posix(): (
            path.is_dir(), path.stat().st_ino, path.stat().st_mtime_ns,
            None if path.is_dir() else path.read_bytes(),
        )
        for path in sorted(root.rglob('*'))
    }


def test_old_project_is_refused_before_sqlite_or_filesystem_changes(tmp_path):
    metadata = tmp_path / '.mwf'
    metadata.mkdir()
    (metadata / 'project.json').write_text(json.dumps({
        'version': 4, 'schema_version': 2, 'edges': [['A', 'B']],
    }), encoding='utf-8')
    (metadata / 'run.json').write_bytes(b'{"status":"done","run_id":"old"}')
    database = metadata / 'state.sqlite3'
    with sqlite3.connect(database) as connection:
        connection.execute('CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL)')
        connection.execute("INSERT INTO metadata VALUES('database_schema_version', '4')")
    source = tmp_path / 'src' / 'graph.py'
    source.parent.mkdir()
    source.write_bytes(b'raise RuntimeError("old project code must not run")\n')
    before = _filesystem_state(tmp_path)
    with pytest.raises(RuntimeError, match='Unsupported MWF project format.*migration.md'):
        FileStorage(tmp_path)
    assert _filesystem_state(tmp_path) == before


def test_creation_refuses_an_ordinary_file_at_the_required_node_directory(tmp_path):
    (tmp_path / 'node').write_bytes(b'user file that must survive')
    before = _filesystem_state(tmp_path)
    with pytest.raises(RuntimeError, match='Fresh session storage requires no existing MWF runtime state'):
        FileStorage(tmp_path)
    assert _filesystem_state(tmp_path) == before


def test_same_process_reopen_refuses_damaged_native_schema_without_repair(tmp_path):
    storage = FileStorage(tmp_path)
    _close(storage)
    database = tmp_path / '.mwf' / 'state.sqlite3'
    connection = sqlite3.connect(database)
    try:
        connection.execute('DROP TRIGGER create_job_instance')
        connection.commit()
    finally:
        connection.close()
    before = database.read_bytes()
    with pytest.raises(RuntimeError, match='Incomplete SQLite execution-session schema'):
        FileStorage(tmp_path)
    assert database.read_bytes() == before


@pytest.mark.parametrize('marker', ['06', ' 6 ', b'6'])
def test_native_reopen_refuses_noncanonical_database_version_without_repair(tmp_path, marker):
    storage = FileStorage(tmp_path)
    _close(storage)
    database = tmp_path / '.mwf' / 'state.sqlite3'
    connection = sqlite3.connect(database)
    try:
        connection.execute("UPDATE metadata SET value=? WHERE key='database_schema_version'", (marker,))
        connection.commit()
    finally:
        connection.close()
    before = database.read_bytes()
    with pytest.raises(RuntimeError, match='Invalid SQLite database_schema_version'):
        FileStorage(tmp_path)
    assert database.read_bytes() == before


@pytest.mark.parametrize('damage', [
    'DROP INDEX jobs_status_idx',
    'ALTER TABLE jobs RENAME COLUMN generation TO damaged_generation',
    'ALTER TABLE nodes RENAME COLUMN status TO damaged_status',
    'ALTER TABLE job_events RENAME COLUMN data_json TO damaged_data',
])
def test_reopen_refuses_damaged_core_schema_without_repair(tmp_path, damage):
    storage = FileStorage(tmp_path)
    try:
        storage.create_job(Job(node_name='A', job_id=1, params={'value': 'retained'}))
    finally:
        _close(storage)
    database = tmp_path / '.mwf' / 'state.sqlite3'
    connection = sqlite3.connect(database)
    try:
        connection.execute(damage)
        connection.commit()
    finally:
        connection.close()
    before = database.read_bytes()
    payload = (tmp_path / 'node' / 'A' / 'jobs' / '1' / 'input.json').read_bytes()
    with pytest.raises(RuntimeError, match='Incomplete SQLite execution-session schema'):
        FileStorage(tmp_path)
    assert database.read_bytes() == before
    assert (tmp_path / 'node' / 'A' / 'jobs' / '1' / 'input.json').read_bytes() == payload


def test_refusing_native_schema_does_not_change_existing_journal_settings(tmp_path):
    storage = FileStorage(tmp_path)
    _close(storage)
    connection = sqlite3.connect(tmp_path / '.mwf' / 'state.sqlite3')
    try:
        connection.execute('DROP TRIGGER create_job_instance')
        connection.commit()
        assert connection.execute('PRAGMA journal_mode=DELETE').fetchone() == ('delete',)
    finally:
        connection.close()
    before = _filesystem_state(tmp_path)
    with pytest.raises(RuntimeError, match='Incomplete SQLite execution-session schema'):
        FileStorage(tmp_path)
    assert _filesystem_state(tmp_path) == before


@pytest.mark.parametrize('statement', [
    'PRAGMA foreign_keys', 'PRAGMA busy_timeout', 'PRAGMA synchronous',
    'PRAGMA journal_mode', 'BEGIN IMMEDIATE', 'SELECT name FROM sqlite_master',
])
def test_failed_native_bootstrap_removes_only_its_new_state(tmp_path, monkeypatch, statement):
    source = tmp_path / 'retained.txt'
    source.write_bytes(b'user source')
    original_connect = sqlite3.connect
    fired = False

    class InterruptedConnection(sqlite3.Connection):
        def execute(self, sql, *args, **kwargs):
            nonlocal fired
            if not fired and sql.startswith(statement):
                fired = True
                raise OSError('bootstrap-injected failure')
            return super().execute(sql, *args, **kwargs)

    def connect(*args, **kwargs):
        kwargs['factory'] = InterruptedConnection
        return original_connect(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, 'connect', connect)
        with pytest.raises(OSError, match='bootstrap-injected failure'):
            FileStorage(tmp_path)
    assert fired, 'The requested SQLite boundary was not reached'
    assert not (tmp_path / '.mwf').exists()
    assert source.read_bytes() == b'user source'
    retry = FileStorage(tmp_path)
    try:
        assert retry.list_execution_sessions() == []
        assert retry.database_integrity_check() == 'ok'
    finally:
        _close(retry)


def test_reopen_does_not_recreate_a_database_removed_before_sqlite_open(tmp_path, monkeypatch):
    storage = FileStorage(tmp_path)
    _close(storage)
    database = tmp_path / '.mwf' / 'state.sqlite3'
    configuration = (tmp_path / '.mwf' / 'project.json').read_bytes()
    original_connect = sqlite3.connect
    removed = False

    def connect(path, *args, **kwargs):
        nonlocal removed
        if not removed and 'state.sqlite3' in str(path):
            removed = True
            database.unlink()
        return original_connect(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, 'connect', connect)
        with pytest.raises((RuntimeError, sqlite3.DatabaseError)):
            FileStorage(tmp_path)
    assert removed, 'The SQLite open boundary was not reached'
    assert not database.exists()
    assert not (tmp_path / '.mwf' / 'state.sqlite3-wal').exists()
    assert not (tmp_path / '.mwf' / 'state.sqlite3-shm').exists()
    assert (tmp_path / '.mwf' / 'project.json').read_bytes() == configuration


def test_cli_graph_setup_preserves_native_format_and_existing_job_identity(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert cli.main(['init']) == 0
    storage = FileStorage(tmp_path)
    try:
        storage.create_job(Job(node_name='A', job_id=1, params={'value': 'retained'}))
        instance_id = storage.read_job_instance_id('A', 1)
    finally:
        _close(storage)
    source = tmp_path / 'src' / 'graph.py'
    source.parent.mkdir()
    source.write_text("EDGES = [('A', 'B')]\n", encoding='utf-8')
    (source.parent / 'node_behavior').mkdir()
    assert cli.main(['graph', 'src/graph.py']) == 0
    assert cli.main(['graph', '--update']) == 0
    reopened = FileStorage(tmp_path)
    try:
        assert reopened.list_execution_sessions() == []
        assert reopened.read_job_instance_id('A', 1) == instance_id
        assert reopened.load_job('A', 1).params == {'value': 'retained'}
        config = json.loads((tmp_path / '.mwf' / 'project.json').read_text(encoding='utf-8'))
        assert config['graph_path'] == 'src/graph.py'
        assert config['edges'] == [['A', 'B']]
    finally:
        _close(reopened)


@pytest.mark.parametrize('command', [['graph', 'src/graph.py'], ['run', 'A']])
def test_native_schema_is_validated_before_graph_code_runs(tmp_path, monkeypatch, capsys, command):
    storage = FileStorage(tmp_path)
    _close(storage)
    source = tmp_path / 'src' / 'graph.py'
    source.parent.mkdir()
    source.write_text('raise RuntimeError("user graph was executed before admission")\n', encoding='utf-8')
    configuration = tmp_path / '.mwf' / 'project.json'
    config = json.loads(configuration.read_text(encoding='utf-8'))
    config['graph_path'] = 'src/graph.py'
    configuration.write_text(json.dumps(config), encoding='utf-8')
    connection = sqlite3.connect(tmp_path / '.mwf' / 'state.sqlite3')
    try:
        connection.execute('DROP TRIGGER create_job_instance')
        connection.commit()
    finally:
        connection.close()
    before = _filesystem_state(tmp_path)
    monkeypatch.chdir(tmp_path)
    assert cli.main(command) == 1
    assert 'Incomplete SQLite execution-session schema' in capsys.readouterr().err
    after = _filesystem_state(tmp_path)
    assert after.keys() == before.keys()
    # This command opens writable SQLite state. WAL coordination can change
    # the metadata directory timestamp; refusal must preserve files and data.
    assert {name: entry for name, entry in after.items() if not entry[0]} == {
        name: entry for name, entry in before.items() if not entry[0]
    }


@pytest.mark.parametrize('project_state', ['old', 'damaged-native'])
def test_process_worker_refuses_invalid_state_before_loading_project_code(tmp_path, project_state):
    if project_state == 'old':
        (tmp_path / '.mwf').mkdir()
        (tmp_path / '.mwf' / 'project.json').write_text(
            '{"version":4,"schema_version":2}', encoding='utf-8',
        )
        expected = 'Unsupported MWF project format'
    else:
        storage = FileStorage(tmp_path)
        _close(storage)
        connection = sqlite3.connect(tmp_path / '.mwf' / 'state.sqlite3')
        try:
            connection.execute('DROP TRIGGER create_job_instance')
            connection.commit()
        finally:
            connection.close()
        expected = 'Incomplete SQLite execution-session schema'
    source = tmp_path / 'src' / 'graph.py'
    source.parent.mkdir()
    source.write_text('raise RuntimeError("process imported project code before admission")\n', encoding='utf-8')
    before = _filesystem_state(tmp_path)
    result = subprocess.run([
        sys.executable, '-c',
        'import sys; from micro_workflow_manager.runners.process import _init_process_worker; '
        '_init_process_worker(sys.argv[1], sys.argv[2], None, "immediate")',
        str(tmp_path), str(source),
    ], capture_output=True, text=True, timeout=30)
    assert result.returncode != 0
    assert expected in result.stderr
    assert 'process imported project code before admission' not in result.stderr
    after = _filesystem_state(tmp_path)
    assert after.keys() == before.keys()
    assert {name: entry for name, entry in after.items() if not entry[0]} == {
        name: entry for name, entry in before.items() if not entry[0]
    }


def test_interrupted_graph_config_write_preserves_project_admission(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert cli.main(['init']) == 0
    source = tmp_path / 'src' / 'graph.py'
    source.parent.mkdir()
    source.write_text("EDGES = [('A', 'B')]\n", encoding='utf-8')
    (source.parent / 'node_behavior').mkdir()
    configuration = tmp_path / '.mwf' / 'project.json'
    before = configuration.read_bytes()
    original_write = Path.write_text
    interrupted = False

    def write(path, data, *args, **kwargs):
        nonlocal interrupted
        if path.parent == configuration.parent and 'project.json' in path.name:
            interrupted = True
            original_write(path, data[:8], *args, **kwargs)
            raise OSError('injected configuration write failure')
        return original_write(path, data, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Path, 'write_text', write)
        assert cli.main(['graph', 'src/graph.py']) == 1
    assert interrupted, 'The configuration write boundary was not reached'
    assert 'injected configuration write failure' in capsys.readouterr().err
    assert configuration.read_bytes() == before
    assert not list(configuration.parent.glob('*.tmp'))
    reopened = FileStorage(tmp_path)
    try:
        assert reopened.list_execution_sessions() == []
    finally:
        _close(reopened)
    assert cli.main(['graph', 'src/graph.py']) == 0


def test_cli_init_creates_native_state_and_repeated_init_preserves_it(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert cli.main(['init']) == 0
    storage = FileStorage(tmp_path)
    try:
        assert storage.list_execution_sessions() == []
        storage.create_job(Job(node_name='A', job_id=1, params={'value': 'retained'}))
        instance_id = storage.read_job_instance_id('A', 1)
    finally:
        _close(storage)
    configuration = (tmp_path / '.mwf' / 'project.json').read_bytes()
    assert cli.main(['init']) == 0
    reopened = FileStorage(tmp_path)
    try:
        assert reopened.read_job_instance_id('A', 1) == instance_id
        assert reopened.load_job('A', 1).params == {'value': 'retained'}
        assert (tmp_path / '.mwf' / 'project.json').read_bytes() == configuration
    finally:
        _close(reopened)


@pytest.mark.parametrize('layout', ['directory', 'root-file'])
@pytest.mark.parametrize('command', [
    ['init'], ['graph', 'src/graph.py', '--dry-run'], ['run', 'A', '--plan'],
    ['resume', 'A', '--plan'], ['reset', 'A', '--dry-run'],
    ['doctor'], ['copy', 'A'], ['threads'],
])
def test_cli_refuses_old_projects_before_loading_code_or_changing_files(
    tmp_path, monkeypatch, capsys, layout, command,
):
    config = {'version': 4, 'schema_version': 2, 'graph_path': 'src/graph.py',
              'runner': 'threaded', 'edges': [['A', 'B']]}
    if layout == 'directory':
        metadata = tmp_path / '.mwf'
        metadata.mkdir()
        configuration = metadata / 'project.json'
        run_path = metadata / 'run.json'
    else:
        configuration = tmp_path / '.mwf'
        run_path = tmp_path / '.mwf_run.json'
    configuration.write_text(json.dumps(config), encoding='utf-8')
    run_path.write_bytes(b'{"status":"done","run_id":"old"}')
    source = tmp_path / 'src' / 'graph.py'
    source.parent.mkdir()
    source.write_bytes(b'raise RuntimeError("old task code was loaded")\n')
    payload = tmp_path / 'node' / 'A' / 'input' / 'retained.txt'
    payload.parent.mkdir(parents=True)
    payload.write_bytes(b'original input')
    before = _filesystem_state(tmp_path)
    monkeypatch.chdir(tmp_path)
    assert cli.main(command) == 1
    error = capsys.readouterr().err
    assert 'Unsupported MWF project format' in error
    assert 'migration.md' in error
    assert _filesystem_state(tmp_path) == before
