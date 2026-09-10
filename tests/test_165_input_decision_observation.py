"""A receipt-read failure must preserve the original writer notification error."""

import pytest

from micro_workflow_manager import MicroWorkflow
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_148_native_cleanup_recovery import _tree_identity


@pytest.mark.parametrize('decision', ['prepared', 'committed'])
def test_failed_receipt_observation_preserves_notification_error_and_exact_durable_result(tmp_path, monkeypatch, decision):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    notification_error = OSError('original publication notification failure')
    observation_error = OSError('publication decision read unavailable')
    captured = {}
    reads = []

    @workflow.task('A')
    def produce(ctx):
        notify = storage.notify_state_change
        read_state = storage._input_publication_state
        before = _rows(storage)

        def fail_notification():
            receipts = storage.db_connection().execute('SELECT * FROM input_publications').fetchall()
            if not captured and len(receipts) == 1 and receipts[0]['state'] == decision:
                captured['receipt'] = dict(receipts[0])
                captured['rows'] = _rows(storage)
                captured['node'] = _tree_identity(tmp_path / 'node')
                captured['staging'] = _tree_identity(tmp_path / '.mwf' / 'input-publications')
                raise notification_error
            return notify()

        def fail_observation(operation_id):
            if captured:
                reads.append(operation_id)
                raise observation_error
            return read_state(operation_id)

        with monkeypatch.context() as patch:
            patch.setattr(storage, 'notify_state_change', fail_notification)
            patch.setattr(storage, '_input_publication_state', fail_observation)
            with pytest.raises(OSError) as caught:
                ctx.node('B').write_input('value.txt', 'durable publication')
        assert caught.value is notification_error
        assert len(reads) == 2 and set(reads) == {captured['receipt']['operation_id']}
        assert any(str(observation_error) in note for note in notification_error.__notes__)
        expected_rows = dict(captured['rows'], advisory_locks=before['advisory_locks'])
        assert _rows(storage) == expected_rows
        assert _tree_identity(tmp_path / 'node') == captured['node']
        assert _tree_identity(tmp_path / '.mwf' / 'input-publications') == captured['staging']
        target = tmp_path / 'node' / 'B' / 'input' / 'A' / 'value.txt'
        if decision == 'prepared':
            assert not target.exists()
            with pytest.raises(RuntimeError, match='unfinished publication'):
                storage.read_node_input_owner('B', 'A/value.txt')
        else:
            assert target.read_bytes() == b'durable publication'
            assert storage.read_node_input_owner('B', 'A/value.txt') == storage.read_job_current_owner('A', 1)

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
        assert captured['receipt']['state'] == decision
    finally:
        _close(storage)
