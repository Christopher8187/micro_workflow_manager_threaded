"""Atomic sampled history checks at native start and session settlement."""

import sqlite3

import networkx as nx
import pytest

from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.models import Job
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.component_states import ComponentTerminalOutcome
from micro_workflow_manager.storage import resume_preparation
from micro_workflow_manager.topology import ComponentTopology
from tests.test_076_component_state_transitions import _seed_state, _session
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_093_native_cli_readiness import _node_files


def _sampled_owner(storage, *, repair_job=False, retained_history=True):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage.register_component_topology(snapshot)
    if repair_job:
        storage.create_job(Job(node_name='A', job_id=1, params={'retained': True}))
        storage.set_job_status('A', 1, 'cancelled')
    _session(storage, 'sample-origin', 'interrupt', ('A',), snapshot)
    assert storage.finish_execution_session(
        'sample-origin', outcome='done', finished_at='2026-09-07T10:00:00+00:00',
    ) is True
    if retained_history:
        _seed_state(
            storage, ('A',), lifecycle='sampled', stability='unstable',
            origin='sample-origin', generation=0,
        )
    else:
        assert storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE component_states SET lifecycle='sampled', stability='unstable', "
            "instability_origin='sample-origin' WHERE component_key=?",
            (encode_component_key(('A',)),),
        ).rowcount) == 1
    _session(storage, 'resume-owner', 'main', ('A',), snapshot)
    assert storage.reserve_execution_components(
        'resume-owner', expected_shape=snapshot.shape_json,
    ) is True
    return snapshot


def _begin(storage, snapshot):
    return storage.begin_sampled_component_execution(
        'resume-owner', ('A',), expected_shape=snapshot.shape_json,
        expected_alignment_generation=0, successful_lineage=('unstable', 'sample-origin'),
    )


def _seed_history(storage, lifecycle):
    assert storage.submit_db_mutation(lambda connection: connection.execute(
        'INSERT OR REPLACE INTO component_successful_results '
        '(component_key, shape_id, alignment_generation, lifecycle, stability, instability_origin) '
        "SELECT component_key, shape_id, 0, ?, 'unstable', 'sample-origin' "
        'FROM component_definitions WHERE component_key=?',
        (lifecycle, encode_component_key(('A',))),
    ).rowcount) == 1


def _settle(storage, snapshot, outcome):
    successful = outcome == 'done'
    return storage.decide_execution_session_exit(
        'resume-owner', outcome=outcome, finished_at='2026-09-07T10:01:00+00:00',
        component_outcomes=[ComponentTerminalOutcome(
            ('A',), snapshot.shape_json, 0, outcome,
            'unstable' if successful else None, 'sample-origin' if successful else None,
        )],
    )


def test_sampled_start_refuses_absent_history_and_failure_preserves_valid_history(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = _sampled_owner(storage, retained_history=False)
        assert _rows(storage)['component_successful_results'] == []
        missing = _rows(storage)

        with pytest.raises(RuntimeError, match='retained successful'):
            _begin(storage, snapshot)
        assert _rows(storage) == missing

        _seed_state(
            storage, ('A',), lifecycle='sampled', stability='unstable',
            origin='sample-origin', generation=0,
        )
        retained = _rows(storage)['component_successful_results']
        retained_state = next(
            row for row in _rows(storage)['component_states']
            if row['component_key'] == encode_component_key(('A',))
        )
        assert (
            retained_state['retained_result_shape_id'],
            retained_state['retained_result_alignment_generation'],
        ) == (retained[0]['shape_id'], retained[0]['alignment_generation'])
        assert _begin(storage, snapshot) is True

        assert len(retained) == 1
        assert tuple(retained[0][name] for name in (
            'lifecycle', 'stability', 'instability_origin', 'alignment_generation',
        )) == ('sampled', 'unstable', 'sample-origin', 0)
        assert _settle(storage, snapshot, 'failed') == {'restarts': {}, 'released': 1}
        assert _rows(storage)['component_successful_results'] == retained
        state = storage.get_component_state(('A',))
        assert tuple(state[name] for name in ('lifecycle', 'stability', 'instability_origin')) == (
            'failed', None, None,
        )
        assert storage.get_component_reservation(('A',)) is None
        assert _rows(storage)['pending_component_executions'] == []
        failed_state = next(
            row for row in _rows(storage)['component_states']
            if row['component_key'] == encode_component_key(('A',))
        )
        assert (
            failed_state['retained_result_shape_id'],
            failed_state['retained_result_alignment_generation'],
        ) == (retained[0]['shape_id'], retained[0]['alignment_generation'])
    finally:
        _close(storage)

    reopened = FileStorage(tmp_path)
    try:
        assert _rows(reopened)['component_successful_results'] == retained
        reopened_state = next(
            row for row in _rows(reopened)['component_states']
            if row['component_key'] == encode_component_key(('A',))
        )
        assert (
            reopened_state['retained_result_shape_id'],
            reopened_state['retained_result_alignment_generation'],
        ) == (retained[0]['shape_id'], retained[0]['alignment_generation'])
        assert reopened.get_component_state(('A',)) == state
    finally:
        _close(reopened)


def test_sampled_start_refuses_conflicting_history_without_changing_rows(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = _sampled_owner(storage)
        _seed_history(storage, 'done')
        before = _rows(storage)

        with pytest.raises(RuntimeError, match='retained successful'):
            _begin(storage, snapshot)

        assert _rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('outcome', ['done', 'failed'])
def test_sampled_settlement_refuses_history_changed_after_start(tmp_path, outcome):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = _sampled_owner(storage)
        _seed_history(storage, 'sampled')
        assert _begin(storage, snapshot) is True
        _seed_history(storage, 'done')
        before = _rows(storage)

        with pytest.raises(RuntimeError, match='retained successful'):
            _settle(storage, snapshot, outcome)

        assert _rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('suppression', ['ABORT', 'IGNORE'])
def test_sampled_pending_write_rolls_back_with_failed_start(tmp_path, suppression):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = _sampled_owner(storage)
        raising = "RAISE(ABORT, 'pending rejected')" if suppression == 'ABORT' else 'RAISE(IGNORE)'
        storage.submit_db_mutation(lambda connection: connection.execute(
            'CREATE TRIGGER reject_resume_pending BEFORE INSERT ON pending_component_executions '
            f'BEGIN SELECT {raising}; END',
        ))
        before = _rows(storage)
        error = sqlite3.IntegrityError if suppression == 'ABORT' else RuntimeError
        message = 'pending rejected' if suppression == 'ABORT' else 'Component start was not recorded'

        with pytest.raises(error, match=message):
            _begin(storage, snapshot)

        assert _rows(storage) == before
        assert _rows(storage)['component_successful_results'] == before['component_successful_results']
    finally:
        _close(storage)


def test_resume_preparation_rechecks_history_after_staging_and_restores_files(tmp_path, monkeypatch):
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        snapshot = _sampled_owner(storage, repair_job=True)
        _seed_history(storage, 'sampled')
        output = storage.output_file('A', 1)
        output.write_bytes(b'{"status":"cancelled","retained":"previous result"}')
        before_files = _node_files(tmp_path)
        before_locks = _rows(storage)['advisory_locks']
        expected_states = {('A',): storage.get_component_state(('A',))}
        original = resume_preparation.submit_preparation_decision
        observed = {}

        def change_after_staging(current_storage, operation):
            assert current_storage is storage
            assert not output.exists()
            _seed_history(storage, 'done')
            observed['rows'] = _rows(storage)
            return original(storage, operation)

        monkeypatch.setattr(resume_preparation, 'submit_preparation_decision', change_after_staging)

        with pytest.raises(RuntimeError, match='retained successful'):
            resume_preparation.prepare_resume_components(
                storage, 'resume-owner', expected_states, clear_trace_nodes=('A',),
                expected_admitted_shape=snapshot.shape_json,
            )

        expected = observed['rows']
        assert {row['name'] for row in expected['advisory_locks']} == {
            'node-A-input', 'node-A-jobs',
        }
        assert before_locks == []
        expected['advisory_locks'] = before_locks
        assert len(expected['preparation_receipts']) == 1
        assert expected['preparation_receipts'][0]['state'] == 'prepared'
        expected['preparation_receipts'][0]['state'] = 'aborted'
        assert _rows(storage) == expected
        assert _node_files(tmp_path) == before_files
        assert not (tmp_path / '.mwf' / 'preparation-trash').exists()
    finally:
        _close(storage)
