from __future__ import annotations

import json
import pytest

from micro_workflow_manager import cli
from tests.test_064_read_only_previews import (
    _close, _initialize_native_project, _install_import_sentinels,
    _live_execution_session_identity, _mark_execution_session_stale,
    _snapshot, _wait_writer,
)
from tests.test_076_component_state_transitions import _seed_state
from tests.test_121_native_preview_recovery import _rows


def _session_dict(storage, session_id):
    row = storage.db_connection().execute(
        'SELECT * FROM execution_sessions WHERE session_id=?', (session_id,),
    ).fetchone()
    assert row is not None
    return dict(row)


@pytest.mark.parametrize('case', [
    'queued', 'done', 'sampled', 'failed', 'failed-with-history',
    'running-without-pending',
])
def test_recovery_preserves_unstarted_or_settled_component_and_refuses_missing_pending(
    tmp_path, monkeypatch, capsys, case,
):
    edges = [('A', 'A')]
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    storage = workflow.storage
    shape = workflow.topology.snapshot().shape_json
    session_id = 'abandoned-reservation-A'
    storage.create_execution_session(
        session_id, session_kind='main', command='run',
        start_component=('A',), selected_components=[('A',)],
        **_live_execution_session_identity(), expected_shape=shape,
    )
    storage.reserve_execution_components(session_id, expected_shape=shape)
    lifecycle = {'failed-with-history': 'sampled', 'running-without-pending': 'running'}.get(case, case)
    _seed_state(
        storage, ('A',), lifecycle=lifecycle, generation=7,
        stability='stable' if lifecycle in ('done', 'sampled') else None,
        origin=None,
    )
    if case == 'failed-with-history':
        storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE component_states SET lifecycle='failed', stability=NULL, instability_origin=NULL "
            "WHERE component_key='[\"A\"]'",
        ))
    _mark_execution_session_stale(storage, session_id)
    storage.db_mutation_barrier()
    _wait_writer(storage)
    assert storage.db_connection().execute(
        'SELECT 1 FROM pending_component_executions WHERE session_id=?', (session_id,),
    ).fetchone() is None
    assert storage.list_job_ids('A') == []
    external = _install_import_sentinels(tmp_path, edges)
    before_rows = _rows(storage)
    before_session = _session_dict(storage, session_id)
    before_files = _snapshot(tmp_path, mutable_existing_shm=True)
    before_node = _snapshot(tmp_path / 'node' / 'A')
    capsys.readouterr()
    try:
        damaged = case == 'running-without-pending'
        assert cli.main(['recover']) == (1 if damaged else 0)
        captured = capsys.readouterr()
        assert session_id in captured.out + captured.err
        assert not external.exists()
        storage.db_mutation_barrier()
        after_rows = _rows(storage)
        if damaged:
            assert 'pending' in (captured.out + captured.err).lower()
            assert after_rows == before_rows
            assert _snapshot(tmp_path, mutable_existing_shm=True) == before_files
        else:
            session = _session_dict(storage, session_id)
            changed_fields = {
                key for key in session if session[key] != before_session[key]
            }
            assert changed_fields == {'status', 'outcome', 'finished_at', 'failures_json'}
            assert session['status'] == 'terminal' and session['outcome'] == 'failed'
            assert isinstance(session['finished_at'], str) and session['finished_at']
            assert isinstance(json.loads(session['failures_json']), list)
            assert json.loads(session['failures_json'])
            assert after_rows['component_reservations'] == []
            assert before_rows['component_reservations'] == [('["A"]', session_id)]
            for table, rows in before_rows.items():
                if table not in ('execution_sessions', 'component_reservations', 'recovery_receipts'):
                    assert after_rows[table] == rows, table
            receipts = storage.db_connection().execute(
                'SELECT session_id, state FROM recovery_receipts ORDER BY operation_id',
            ).fetchall()
            assert [tuple(row) for row in receipts] == [(session_id, 'committed')]
            assert _snapshot(tmp_path / 'node' / 'A') == before_node
        after_files = _snapshot(tmp_path, mutable_existing_shm=True)
        assert cli.main(['recover']) == (1 if damaged else 0)
        storage.db_mutation_barrier()
        assert _rows(storage) == after_rows
        assert _snapshot(tmp_path, mutable_existing_shm=True) == after_files
        assert not external.exists()
    finally:
        _close(storage)
