"""Preparation admission and release retain their exact durable owner."""

import pytest

from micro_workflow_manager.storage.preparation_receipts import PreparationReceipt
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_105_preparation_receiver_guards import _established_workflow, _run_preparation, _files


def test_suppressed_receiver_guard_insert_rolls_back_complete_admission(tmp_path):
    workflow = _established_workflow(tmp_path)
    storage = workflow.storage
    try:
        storage.submit_db_mutation(lambda connection: connection.execute(
            'CREATE TRIGGER suppress_preparation_guard BEFORE INSERT ON receiver_mutation_guards '
            'BEGIN SELECT RAISE(IGNORE); END',
        ))
        before = _rows(storage), _files(tmp_path / 'node')
        with pytest.raises(RuntimeError):
            _run_preparation(tmp_path, workflow, 'reset')
        assert (_rows(storage), _files(tmp_path / 'node')) == before
        trash = tmp_path / '.mwf' / 'preparation-trash'
        assert not trash.exists() or not any(trash.iterdir())
    finally:
        _close(storage)


@pytest.mark.parametrize('changed_table', ['preparation_attempts', 'receiver_mutation_guards'])
def test_preparation_refuses_changed_owner_without_releasing_its_guards(tmp_path, monkeypatch, changed_table):
    workflow = _established_workflow(tmp_path)
    storage = workflow.storage
    original_commit = PreparationReceipt.commit
    injected = []
    retained = {}
    before = _rows(storage)
    old_attempts = {row['operation_id'] for row in before['preparation_attempts']}
    old_receipts = {row['operation_id'] for row in before['preparation_receipts']}

    def change_owner_after_committed_decision(receipt, connection):
        result = original_commit(receipt, connection)
        if receipt.operation == 'reset' and not injected:
            changed = connection.execute(
                'UPDATE ' + changed_table + ' SET hostname=? WHERE operation_id=?',
                ('independent-preparation-owner', receipt.guard_id),
            ).rowcount
            assert changed == 1
            injected.append(receipt.guard_id)
            retained['attempt'] = dict(connection.execute(
                'SELECT * FROM preparation_attempts WHERE operation_id=?', (receipt.guard_id,),
            ).fetchone())
            retained['guards'] = [dict(row) for row in connection.execute(
                'SELECT * FROM receiver_mutation_guards WHERE operation_id=? ORDER BY receiver_node',
                (receipt.guard_id,),
            )]
            retained['receipt'] = dict(connection.execute(
                'SELECT * FROM preparation_receipts WHERE operation_id=?', (receipt.operation_id,),
            ).fetchone())
        return result

    monkeypatch.setattr(PreparationReceipt, 'commit', change_owner_after_committed_decision)
    try:
        with pytest.raises(RuntimeError):
            _run_preparation(tmp_path, workflow, 'reset')
        assert len(injected) == 1
        after = _rows(storage)
        assert [row for row in after['preparation_attempts'] if row['operation_id'] in old_attempts] == before['preparation_attempts']
        assert [row for row in after['preparation_receipts'] if row['operation_id'] in old_receipts] == before['preparation_receipts']
        assert [row for row in after['preparation_attempts'] if row['operation_id'] not in old_attempts] == [retained['attempt']]
        assert [row for row in after['preparation_receipts'] if row['operation_id'] not in old_receipts] == [retained['receipt']]
        assert after['receiver_mutation_guards'] == before['receiver_mutation_guards'] + retained['guards']
        assert retained['attempt']['state'] == 'preparing'
        assert retained['receipt']['state'] == 'committed'
        assert retained['guards'] and retained['guards'][0]['receiver_node'] == 'B'
        assert storage.get_component_state(('A',))['lifecycle'] == 'queued'
        assert not (tmp_path / 'node' / 'B' / 'input' / 'A' / 'owned.txt').exists()
        assert (tmp_path / 'node' / 'B' / 'input' / 'X' / 'owned.txt').read_text() == 'from X'
    finally:
        _close(storage)
