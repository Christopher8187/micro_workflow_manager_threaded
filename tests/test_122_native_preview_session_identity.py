from __future__ import annotations

import os
import socket

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.models import Job, now
from micro_workflow_manager.processes import process_identity
from tests.test_064_read_only_previews import (
    _close, _initialize_native_project, _install_import_sentinels, _snapshot, _wait_writer,
)
from tests.test_121_native_preview_recovery import _rows


@pytest.mark.parametrize(('field', 'value'), [
    ('pid', 0), ('pid', 'invalid'), ('hostname', ''), ('started_at', 'invalid'),
    ('heartbeat_at', 'invalid'), ('heartbeat_at', ''), ('process_identity', ''),
])
def test_damaged_session_identity_is_refused_before_liveness_classification(
    tmp_path, monkeypatch, capsys, field, value,
):
    edges = [(node, node) for node in ('A', 'B', 'C')]
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    storage = workflow.storage
    shape = workflow.topology.snapshot().shape_json
    session_ids = {'A': 'damaged-A', 'B': 'abandoned-B', 'C': 'live-C'}
    for node in ('A', 'B', 'C'):
        live = node != 'B'
        session_id = session_ids[node]
        storage.create_job(Job(node_name=node, job_id=1, params={}))
        storage.create_execution_session(
            session_id, session_kind='main' if node == 'A' else 'interrupt', command='run',
            start_component=(node,), selected_components=[(node,)],
            started_at=now() if live else '2020-01-01T00:00:00+00:00',
            hostname=socket.gethostname(), pid=os.getpid() if live else 99999999,
            process_identity=process_identity(os.getpid()) if live else 'retired-process',
        )
        storage.reserve_execution_components(session_id, expected_shape=shape)
        storage.begin_queued_component_execution(
            session_id, (node,), expected_shape=shape, expected_alignment_generation=0,
            successful_lineage=('stable', None),
        )
        storage.claim_job_execution(node, 1, started_at=now(), session_id=session_id, component=(node,))
    storage.submit_db_mutation(lambda connection: connection.execute(
        f"UPDATE execution_sessions SET {field}=? WHERE session_id='damaged-A'", (value,),
    ))
    storage.db_mutation_barrier()
    _wait_writer(storage)
    external = _install_import_sentinels(tmp_path, edges)
    before_rows = _rows(storage)
    before_files = _snapshot(tmp_path, mutable_existing_shm=True)
    capsys.readouterr()
    try:
        assert cli.main(['recover', '--dry-run']) == 1
        output = capsys.readouterr().out
        assert 'damaged running session damaged-A' in output
        assert 'recovery refused for this session' in output
        assert 'would release reservation: {A}' not in output
        assert 'would requeue abandoned execution: A/1' not in output
        assert 'abandoned session abandoned-B' in output
        assert 'would requeue abandoned execution: B/1 at generation 1' in output
        assert 'would release reservation: {B}' in output
        assert 'live session live-C: recovery leaves it active' in output
        assert _rows(storage) == before_rows
        assert _snapshot(tmp_path, mutable_existing_shm=True) == before_files
        assert not external.exists()
    finally:
        _close(storage)
