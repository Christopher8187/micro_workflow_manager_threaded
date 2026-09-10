from __future__ import annotations

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.models import Job, now
from tests.test_064_read_only_previews import (
    _close, _initialize_native_project, _install_import_sentinels,
    _live_execution_session_identity, _mark_execution_session_stale,
    _snapshot, _wait_writer,
)


def _rows(storage):
    connection = storage.db_connection()
    tables = [row[0] for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
    )]
    return {table: sorted((tuple(row) for row in connection.execute(
        'SELECT * FROM "' + table.replace('"', '""') + '"',
    )), key=repr) for table in tables}


@pytest.mark.parametrize('damage', [
    None, 'last-owner', 'active-owner', 'pending-missing', 'pending-kind',
    'pending-generation', 'component-queued', 'component-done', 'owner-terminal',
    'pid-missing', 'thread-missing', 'start-missing', 'pid-invalid', 'thread-invalid', 'start-invalid',
])
def test_recovery_preview_lists_all_abandoned_sessions_and_preserves_live_owner(
    tmp_path, monkeypatch, capsys, damage,
):
    edges = [(node, node) for node in ('A', 'B', 'C')]
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    storage = workflow.storage
    shape = workflow.topology.snapshot().shape_json
    owners = {}
    for node in ('A', 'B', 'C'):
        live = node == 'C'
        storage.create_job(Job(node_name=node, job_id=1, params={'node': node}))
        session_id = ('live-' if live else 'abandoned-') + node
        storage.create_execution_session(
            session_id, session_kind='main' if node == 'A' else 'interrupt', command='run',
            start_component=(node,), selected_components=[(node,)],
            **_live_execution_session_identity(),
            expected_shape=shape,
        )
        storage.reserve_execution_components(session_id, expected_shape=shape)
        storage.begin_queued_component_execution(
            session_id, (node,), expected_shape=shape, expected_alignment_generation=0,
            successful_lineage=('stable', None),
        )
        owners[node] = storage.claim_job_execution(
            node, 1, started_at=now(), session_id=session_id, component=(node,),
        )
    storage.acquire_component_holds('abandoned-B', [('C',)])
    storage.acquire_component_holds('abandoned-B', [('C',)])
    generation, execution_id = owners['B']
    storage.write_output('B', 1, {
        'status': 'done', 'generation': generation, 'execution_id': execution_id,
        'result_type': 'str', 'result_repr': "'retained terminal result'",
    })
    _mark_execution_session_stale(storage, 'abandoned-A')
    _mark_execution_session_stale(storage, 'abandoned-B')
    storage.db_mutation_barrier()
    _wait_writer(storage)
    if damage is not None:
        connection = storage.db_connection()
        statements = {
            'last-owner': "UPDATE job_instances SET last_execution_id=NULL WHERE node_name='A' AND job_id=1",
            'active-owner': "UPDATE jobs SET active_execution_id=NULL WHERE node_name='A' AND job_id=1",
            'pending-missing': "DELETE FROM pending_component_executions WHERE session_id='abandoned-A'",
            'pending-kind': "UPDATE pending_component_executions SET execution_kind='jobs' WHERE session_id='abandoned-A'",
            'pending-generation': "UPDATE pending_component_executions SET alignment_generation=1 WHERE session_id='abandoned-A'",
            'component-queued': "UPDATE component_states SET lifecycle='queued' WHERE component_key='[\"A\"]'",
            'component-done': "UPDATE component_states SET lifecycle='done', stability='stable' WHERE component_key='[\"A\"]'",
            'owner-terminal': "UPDATE execution_sessions SET status='terminal', outcome='failed' WHERE session_id='abandoned-A'",
            'pid-missing': "UPDATE jobs SET active_pid=NULL WHERE node_name='A' AND job_id=1",
            'thread-missing': "UPDATE jobs SET active_thread_id=NULL WHERE node_name='A' AND job_id=1",
            'start-missing': "UPDATE jobs SET active_started_at=NULL WHERE node_name='A' AND job_id=1",
            'pid-invalid': "UPDATE jobs SET active_pid=0 WHERE node_name='A' AND job_id=1",
            'thread-invalid': "UPDATE jobs SET active_thread_id=0 WHERE node_name='A' AND job_id=1",
            'start-invalid': "UPDATE jobs SET active_started_at='invalid' WHERE node_name='A' AND job_id=1",
        }
        statement = statements[damage]
        connection.execute(statement)
        connection.commit()
    external = _install_import_sentinels(tmp_path, edges)
    before_rows = _rows(storage)
    before_files = _snapshot(tmp_path, mutable_existing_shm=True)
    capsys.readouterr()
    try:
        assert cli.main(['recover', '--dry-run']) == (1 if damage else 0)
        output = capsys.readouterr().out
        if damage != 'owner-terminal':
            assert 'abandoned session abandoned-A' in output
        assert 'abandoned session abandoned-B' in output
        assert 'live session live-C: recovery leaves it active' in output
        assert 'would reconcile terminal output as done: B/1 at generation 0' in output
        assert 'would release reservation: {B}' in output
        assert 'would release hold: {C}, count=2' in output
        assert 'requeue abandoned execution: C/1' not in output
        if damage:
            assert 'recovery observation failed:' in output
            assert 'A/1' in output
            assert 'would release reservation: {A}' not in output
            assert 'would requeue abandoned execution: A/1' not in output
        else:
            assert 'would requeue abandoned execution: A/1 at generation 1' in output
        assert _rows(storage) == before_rows
        assert _snapshot(tmp_path, mutable_existing_shm=True) == before_files
        assert not external.exists()
    finally:
        _close(storage)
