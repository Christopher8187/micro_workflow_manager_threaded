from __future__ import annotations

import json
import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.models import Job, now
from micro_workflow_manager.storage.sqlite.schema import DATABASE_SCHEMA_VERSION
from tests.test_064_read_only_previews import (
    _close,
    _initialize_native_project,
    _install_import_sentinels,
    _live_execution_session_identity,
    _mark_execution_session_stale,
    _snapshot,
    _wait_writer,
)
from tests.test_121_native_preview_recovery import _rows


def _assert_current_format(storage):
    row = storage.db_connection().execute(
        "SELECT value FROM metadata WHERE key='database_schema_version'",
    ).fetchone()
    assert row is not None
    assert row[0] == str(DATABASE_SCHEMA_VERSION)


def _create_active_scope(storage, shape, node, session_id, *, live=False):
    storage.create_job(Job(node_name=node, job_id=1, params={'scope': session_id}))
    storage.create_execution_session(
        session_id,
        session_kind='main' if node == 'A' else 'interrupt',
        command='run',
        start_component=(node,),
        selected_components=[(node,)],
        **_live_execution_session_identity(),
        expected_shape=shape,
    )
    storage.reserve_execution_components(session_id, expected_shape=shape)
    storage.begin_queued_component_execution(
        session_id,
        (node,),
        expected_shape=shape,
        expected_alignment_generation=0,
        successful_lineage=('stable', None),
    )
    generation, execution_id = storage.claim_job_execution(
        node,
        1,
        started_at=now(),
        session_id=session_id,
        component=(node,),
    )
    if not live:
        _mark_execution_session_stale(storage, session_id)
    return {
        'session_id': session_id,
        'generation': generation,
        'execution_id': execution_id,
        'instance_id': storage.read_job_instance_id(node, 1),
        'owner': storage.get_job_execution_owner(execution_id),
    }


def _scope_rows(storage, node, session_id):
    """Capture rows owned by one node/session, excluding incoming holds."""
    connection = storage.db_connection()
    component_key = encode_component_key((node,))
    result = {}
    tables = [row[0] for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
    )]
    for table in tables:
        quoted = '"' + table.replace('"', '""') + '"'
        columns = [str(row[1]) for row in connection.execute(f'PRAGMA table_info({quoted})')]
        session_columns = [column for column in columns if column.endswith('session_id')]
        node_columns = [
            column for column in columns
            if column in {
                'node_name', 'producer_node', 'receiver_node', 'source_node',
                'target_node', 'start_node',
            }
        ]
        component_columns = [] if table == 'component_holds' else [
            column for column in columns
            if column in {'component_key', 'start_component'}
        ]
        selected = []
        for row in connection.execute(f'SELECT * FROM {quoted}'):
            values = dict(zip(columns, tuple(row), strict=True))
            if (
                any(values[column] == session_id for column in session_columns)
                or any(values[column] == node for column in node_columns)
                or any(values[column] == component_key for column in component_columns)
            ):
                selected.append(tuple(row))
        if selected:
            result[table] = sorted(selected, key=repr)
    return result


def _node_tree(root, node):
    path = root / 'node' / node
    return _snapshot(path) if path.exists() else {}


def _job_row(storage, node):
    return storage.db_connection().execute(
        'SELECT status, generation, active_execution_id, active_pid, '
        'active_thread_id, active_started_at, restart_requested_at, '
        'restart_requested_by_pid, restart_reason '
        'FROM jobs WHERE node_name=? AND job_id=1',
        (node,),
    ).fetchone()


def _session_row(storage, session_id):
    return storage.db_connection().execute(
        'SELECT status, outcome, finished_at, failures_json '
        'FROM execution_sessions WHERE session_id=?',
        (session_id,),
    ).fetchone()


def _assert_recovered_failed_scope(storage, node, owner, *, terminal):
    session = _session_row(storage, owner['session_id'])
    assert session[0] == 'terminal'
    assert session[1] == 'failed'
    assert isinstance(session[2], str) and session[2]
    failures = json.loads(session[3])
    assert isinstance(failures, list) and failures

    component_key = encode_component_key((node,))
    component = storage.db_connection().execute(
        'SELECT lifecycle FROM component_states WHERE component_key=?',
        (component_key,),
    ).fetchone()
    assert component is not None and component[0] == 'failed'
    assert storage.db_connection().execute(
        'SELECT 1 FROM pending_component_executions WHERE session_id=?',
        (owner['session_id'],),
    ).fetchone() is None
    assert storage.db_connection().execute(
        'SELECT 1 FROM component_reservations WHERE session_id=?',
        (owner['session_id'],),
    ).fetchone() is None

    row = _job_row(storage, node)
    expected_generation = owner['generation'] if terminal else owner['generation'] + 1
    assert row[0] == ('done' if terminal else 'queued')
    assert row[1] == expected_generation
    assert tuple(row[2:]) == (None, None, None, None, None, None, None)
    instance = storage.db_connection().execute(
        'SELECT instance_id, last_execution_id FROM job_instances '
        'WHERE node_name=? AND job_id=1',
        (node,),
    ).fetchone()
    assert tuple(instance) == (owner['instance_id'], owner['execution_id'])
    assert storage.get_job_execution_owner(owner['execution_id']) == owner['owner']
    assert storage.read_job_current_owner(node, 1) == owner['owner']


@pytest.mark.parametrize('invalid_output', ['missing', 'malformed'])
def test_applied_recover_processes_every_safe_stale_session_and_is_idempotent(
    tmp_path,
    monkeypatch,
    capsys,
    invalid_output,
):
    edges = [(node, node) for node in ('A', 'B', 'C', 'D')]
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    storage = workflow.storage
    _assert_current_format(storage)
    shape = workflow.topology.snapshot().shape_json
    owners = {
        'A': _create_active_scope(storage, shape, 'A', 'stale-A'),
        'B': _create_active_scope(storage, shape, 'B', 'stale-B'),
        'C': _create_active_scope(storage, shape, 'C', 'live-C', live=True),
        'D': _create_active_scope(storage, shape, 'D', 'damaged-D'),
    }
    storage.acquire_component_holds('stale-B', [('C',)])
    storage.acquire_component_holds('stale-B', [('C',)])

    b_payload = {
        'status': 'done',
        'generation': owners['B']['generation'],
        'execution_id': owners['B']['execution_id'],
        'result_type': 'str',
        'result_repr': "'retained terminal result'",
    }
    storage.write_output('B', 1, b_payload)
    b_output = storage.output_file('B', 1)
    b_output_bytes = b_output.read_bytes()
    a_output = storage.output_file('A', 1)
    if invalid_output == 'malformed':
        a_output.parent.mkdir(parents=True, exist_ok=True)
        a_output.write_bytes(b'{not valid json')
    else:
        assert not a_output.exists()

    connection = storage.db_connection()
    assert connection.execute(
        "UPDATE job_instances SET last_execution_id=NULL "
        "WHERE node_name='D' AND job_id=1",
    ).rowcount == 1
    connection.commit()
    storage.db_mutation_barrier()
    _wait_writer(storage)

    before_live_rows = _scope_rows(storage, 'C', 'live-C')
    before_live_files = _node_tree(tmp_path, 'C')
    before_damaged_rows = _scope_rows(storage, 'D', 'damaged-D')
    before_damaged_files = _node_tree(tmp_path, 'D')
    a_events_before = tuple(storage.read_job_events('A', 1))
    external = _install_import_sentinels(tmp_path, edges)
    capsys.readouterr()
    try:
        assert cli.main(['recover']) == 1
        captured = capsys.readouterr()
        output = captured.out + captured.err
        assert 'stale-A' in output
        assert 'stale-B' in output
        assert 'damaged-D' in output
        assert 'live-C' in output
        assert not external.exists()

        storage.db_mutation_barrier()
        _assert_recovered_failed_scope(storage, 'A', owners['A'], terminal=False)
        _assert_recovered_failed_scope(storage, 'B', owners['B'], terminal=True)
        assert tuple(storage.read_job_events('A', 1)) != a_events_before
        assert not a_output.exists()
        assert b_output.read_bytes() == b_output_bytes
        assert storage.read_json(b_output) == b_payload
        assert connection.execute(
            "SELECT COUNT(*) FROM component_holds WHERE session_id='stale-B'",
        ).fetchone()[0] == 0

        assert _scope_rows(storage, 'C', 'live-C') == before_live_rows
        assert _node_tree(tmp_path, 'C') == before_live_files
        assert _scope_rows(storage, 'D', 'damaged-D') == before_damaged_rows
        assert _node_tree(tmp_path, 'D') == before_damaged_files

        after_first_rows = _rows(storage)
        after_first_files = _snapshot(tmp_path, mutable_existing_shm=True)
        capsys.readouterr()
        assert cli.main(['recover']) == 1
        repeated = capsys.readouterr()
        repeated_output = repeated.out + repeated.err
        assert 'damaged-D' in repeated_output
        assert 'stale-A' not in repeated_output
        assert 'stale-B' not in repeated_output
        storage.db_mutation_barrier()
        assert _rows(storage) == after_first_rows
        assert _snapshot(tmp_path, mutable_existing_shm=True) == after_first_files
        assert not external.exists()
    finally:
        _close(storage)


def test_mutating_startup_refuses_damaged_stale_session_before_user_imports(
    tmp_path,
    monkeypatch,
    capsys,
):
    edges = [('A', 'A'), ('D', 'D')]
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    storage = workflow.storage
    _assert_current_format(storage)
    shape = workflow.topology.snapshot().shape_json
    owner = _create_active_scope(storage, shape, 'D', 'damaged-startup-D')
    damaged_output = storage.output_file('D', 1)
    damaged_output.parent.mkdir(parents=True, exist_ok=True)
    damaged_output.write_bytes(b'preserve damaged stale output')
    connection = storage.db_connection()
    assert connection.execute(
        "UPDATE job_instances SET last_execution_id=NULL "
        "WHERE node_name='D' AND job_id=1",
    ).rowcount == 1
    connection.commit()
    storage.db_mutation_barrier()
    _wait_writer(storage)

    external = _install_import_sentinels(tmp_path, edges)
    before_rows = _rows(storage)
    before_files = _snapshot(tmp_path, mutable_existing_shm=True)
    before_scope = _scope_rows(storage, 'D', owner['session_id'])
    capsys.readouterr()
    try:
        assert cli.main(['run', 'A']) == 1
        captured = capsys.readouterr()
        output = captured.out + captured.err
        assert 'damaged-startup-D' in output
        assert 'recover' in output.lower() or 'stale' in output.lower()
        storage.db_mutation_barrier()
        assert _rows(storage) == before_rows
        assert _scope_rows(storage, 'D', owner['session_id']) == before_scope
        assert _snapshot(tmp_path, mutable_existing_shm=True) == before_files
        assert damaged_output.read_bytes() == b'preserve damaged stale output'
        assert not external.exists()
    finally:
        _close(storage)
