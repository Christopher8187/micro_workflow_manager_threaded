"""Native previews preserve project state and do not import user code."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path
import socket
import textwrap
import time

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.models import Job


def _wait_writer(storage):
    deadline = time.monotonic() + 10
    while storage.mutation_writer_diagnostics()['writer_alive']:
        assert time.monotonic() < deadline
        time.sleep(0.01)


def _wait_restart_listener_retired(storage):
    deadline = time.monotonic() + 10
    while True:
        with storage._state_cross_guard:
            if storage._state_listener_record is None:
                assert not storage._state_cross_callbacks
                return
        assert time.monotonic() < deadline, 'Completed workflow retained its listener'
        time.sleep(0.01)


def _close(storage):
    storage.db_mutation_barrier()
    _wait_writer(storage)
    storage.close_database_connections()


def _snapshot(root, *, mutable_existing_shm=False):
    result = {}
    for path in root.rglob('*'):
        relative = path.relative_to(root)
        if not path.is_file():
            result[relative] = ('directory',)
        elif (
            mutable_existing_shm
            and relative.as_posix() == '.mwf/state.sqlite3-shm'
        ):
            observed = path.stat()
            result[relative] = ('existing-shm', observed.st_dev, observed.st_ino)
        else:
            result[relative] = ('file', sha256(path.read_bytes()).hexdigest())
    return result


def _task_source(node, body="return 'finished'"):
    return textwrap.dedent(
        f'''\
        from micro_workflow_manager import NodeRouter
        router = NodeRouter({node!r}, runner='direct')
        @router.task
        def run(ctx):
            {body}
        '''
    )


def _initialize_native_project(
    root,
    monkeypatch,
    *,
    edges,
    task_sources=None,
):
    monkeypatch.chdir(root)
    source = root / 'src'
    behaviors = source / 'node_behavior'
    behaviors.mkdir(parents=True)
    (source / 'graph.py').write_text(f'EDGES = {edges!r}\n', encoding='utf-8')
    nodes = {node for edge in edges for node in edge}
    for node in sorted(nodes):
        content = (task_sources or {}).get(node, _task_source(node))
        (behaviors / f'{node}.py').write_text(content, encoding='utf-8')
    assert cli.main(['init']) == 0
    assert cli.main(['graph', 'src/graph.py', '--runner', 'direct']) == 0
    workflow = load_workflow(root, 'direct')
    workflow.storage.register_component_topology(workflow.topology.snapshot())
    return workflow


def _close_without_sidecars(storage, root):
    storage.db_mutation_barrier()
    storage.db_connection().execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchall()
    _close(storage)
    assert not (root / '.mwf' / 'state.sqlite3-wal').exists()
    assert not (root / '.mwf' / 'state.sqlite3-shm').exists()


def _install_import_sentinels(root, edges, *, autostart=None):
    external = root.parent / f'{root.name}-preview-imported'
    (root / 'src' / 'graph.py').write_text(
        "from pathlib import Path\n"
        f"Path({str(external)!r}).write_text('graph imported')\n"
        f"EDGES = {edges!r}\n",
        encoding='utf-8',
    )
    nodes = {node for edge in edges for node in edge}
    for node in sorted(nodes):
        action = (
            f"    ctx.node({autostart[1]!r}).add(autostart=True)\n"
            if autostart is not None and autostart[0] == node
            else ''
        )
        (root / 'src' / 'node_behavior' / f'{node}.py').write_text(
            "from pathlib import Path\n"
            f"Path({str(external)!r}).write_text('task imported')\n"
            "from micro_workflow_manager import NodeRouter\n"
            f"router = NodeRouter({node!r}, runner='direct')\n"
            "@router.task\n"
            "def run(ctx):\n"
            f"{action}"
            "    raise AssertionError('A preview must never run task code')\n",
            encoding='utf-8',
        )
    return external


@pytest.mark.parametrize('arguments', [
    ['run', 'A', '--plan'],
    ['runfrom', 'A', '--plan'],
    ['resume', 'A', '--plan'],
    ['resumefrom', 'A', '--plan'],
    ['reset', 'A', '--dry-run'],
    ['resetfrom', 'A', '--dry-run'],
    ['run', 'A', 'job', '1', '--plan'],
    ['run', 'A', 'sample', '100%', '--plan'],
    ['run', 'A', 'sample', '0%'],
    ['reset', 'A', 'job', '1', '--dry-run'],
    ['reset', '*', '--dry-run'],
])
def test_native_previews_use_persisted_state_without_imports_or_sidecars(
    tmp_path,
    monkeypatch,
    capsys,
    arguments,
):
    edges = [('A', 'B')]
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    workflow.storage.create_job(Job(node_name='A', job_id=1, params={'kept': True}))
    state = workflow.storage.get_component_state(('A',))
    assert state is not None
    assert state['lifecycle'] == 'queued'
    assert state['misaligned'] is False
    _close_without_sidecars(workflow.storage, tmp_path)
    external = _install_import_sentinels(tmp_path, edges)
    before = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main(arguments) == 0

    output = capsys.readouterr().out
    assert 'A' in output
    sampling = len(arguments) > 2 and arguments[2] == 'sample'
    if sampling:
        assert 'A: selected' in output
        assert 'eligible jobs' in output
    elif arguments[0] in {'reset', 'resetfrom'}:
        assert 'A: would preserve jobs/input; queued=1;' in output
    else:
        assert 'A: node_status=queued, queued=1' in output
    if not sampling:
        assert 'synchronized raw edges' in output
    assert 'user code was not loaded' in output
    assert _snapshot(tmp_path) == before
    assert not (tmp_path / '.mwf' / 'state.sqlite3-wal').exists()
    assert not (tmp_path / '.mwf' / 'state.sqlite3-shm').exists()
    assert not external.exists()


def test_live_native_preview_reads_committed_wal_state_and_preserves_project(
    tmp_path,
    monkeypatch,
    capsys,
):
    edges = [('A', 'A')]
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    storage = workflow.storage
    workflow.add_job(None, 'A', job_id=1)
    storage.db_mutation_barrier()
    database = tmp_path / '.mwf' / 'state.sqlite3'
    wal = Path(f'{database}-wal')
    shm = Path(f'{database}-shm')
    storage.db_connection().execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchall()
    main_before_execution = database.read_bytes()

    workflow.run_node('A')
    storage.db_mutation_barrier()
    _wait_writer(storage)
    _wait_restart_listener_retired(storage)

    assert database.read_bytes() == main_before_execution
    assert wal.is_file() and wal.stat().st_size > 0
    assert shm.is_file()
    state = storage.get_component_state(('A',))
    assert state is not None
    assert state['lifecycle'] == 'done'
    assert state['misaligned'] is False
    external = _install_import_sentinels(tmp_path, edges)
    before = _snapshot(tmp_path, mutable_existing_shm=True)
    main_before = database.read_bytes()
    wal_before = wal.read_bytes()
    capsys.readouterr()
    try:
        assert cli.main(['run', 'A', '--plan']) == 0
        output = capsys.readouterr().out
        assert 'A: node_status=done, done=1' in output
        assert 'user code was not loaded' in output
        assert _snapshot(tmp_path, mutable_existing_shm=True) == before
        assert database.read_bytes() == main_before
        assert wal.read_bytes() == wal_before
        assert not external.exists()
    finally:
        _close(storage)


def test_preview_uses_synchronized_edges_and_static_autostart_without_imports(
    tmp_path,
    monkeypatch,
    capsys,
):
    edges = [('Before', 'A'), ('A', 'B'), ('B', 'After')]
    tasks = {
        'A': _task_source('A', "ctx.node('B').add(autostart=True)"),
    }
    workflow = _initialize_native_project(
        tmp_path,
        monkeypatch,
        edges=edges,
        task_sources=tasks,
    )
    assert workflow.component_for('B') == {'A', 'B'}
    _close_without_sidecars(workflow.storage, tmp_path)
    external = _install_import_sentinels(
        tmp_path,
        edges,
        autostart=('A', 'B'),
    )
    before = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main(['run', 'B', '--plan']) == 0

    output = capsys.readouterr().out
    assert 'A: node_status=queued, no jobs' in output
    assert 'B: node_status=queued, no jobs' in output
    assert 'After: node_status=' not in output
    assert 'same Hoeflein component: A' in output
    assert 'incomplete start-component inputs: Before' in output
    assert 'synchronized raw edges' in output
    assert 'AST-read autostart declarations' in output
    assert _snapshot(tmp_path) == before
    assert not external.exists()


@pytest.mark.parametrize('arguments', [
    ['run', 'A', '--plan'],
    ['recover', '--dry-run'],
])
def test_native_preview_reports_abandoned_session_and_owned_job_without_mutation(
    tmp_path,
    monkeypatch,
    capsys,
    arguments,
):
    edges = [('A', 'A')]
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    storage = workflow.storage
    storage.create_job(Job(node_name='A', job_id=1, params={'retained': True}))
    snapshot = workflow.topology.snapshot()
    component = ('A',)
    session_id = 'abandoned-preview-session'
    storage.create_execution_session(
        session_id,
        session_kind='main',
        command='run',
        start_component=component,
        selected_components=[component],
        started_at='2020-01-01T00:00:00+00:00',
        hostname=socket.gethostname(),
        pid=99999999,
        process_identity='retired-preview-process',
        details={'start_node': 'A'},
    )
    storage.reserve_execution_components(
        session_id,
        expected_shape=snapshot.shape_json,
    )
    state = storage.get_component_state(component)
    assert state['lifecycle'] == 'queued'
    storage.begin_queued_component_execution(
        session_id,
        component,
        expected_shape=snapshot.shape_json,
        expected_alignment_generation=state['alignment_generation'],
        successful_lineage=('stable', None),
    )
    state = storage.get_component_state(component)
    assert state is not None
    assert state['lifecycle'] == 'running'
    assert state['misaligned'] is False
    generation, execution_id = storage.claim_job_execution(
        'A',
        1,
        started_at='2020-01-01T00:00:01+00:00',
        session_id=session_id,
        component=component,
    )
    assert generation == 0
    assert storage.read_job_control('A', 1)['active_execution_id'] == execution_id
    storage.db_mutation_barrier()
    _wait_writer(storage)
    database = tmp_path / '.mwf' / 'state.sqlite3'
    wal = Path(f'{database}-wal')
    shm = Path(f'{database}-shm')
    assert wal.is_file() and shm.is_file()
    external = _install_import_sentinels(tmp_path, edges)
    before = _snapshot(tmp_path, mutable_existing_shm=True)
    main_before = database.read_bytes()
    wal_before = wal.read_bytes()
    capsys.readouterr()
    try:
        assert cli.main(arguments) == 0
        output = capsys.readouterr().out.lower()
        assert session_id in output
        assert 'abandoned' in output
        assert 'a/1' in output
        assert 'recover' in output
        if arguments[0] == 'run':
            assert 'a: node_status=queued, running=1' in output
        assert _snapshot(tmp_path, mutable_existing_shm=True) == before
        assert database.read_bytes() == main_before
        assert wal.read_bytes() == wal_before
        assert not external.exists()
    finally:
        _close(storage)
