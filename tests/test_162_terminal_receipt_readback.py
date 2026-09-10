"""Terminal receipt writers must observe the exact row they wrote."""

from __future__ import annotations

import sqlite3

import pytest

from micro_workflow_manager import MicroWorkflow, cli
from micro_workflow_manager.storage import FileStorage, input_publication_files, native_recovery
from micro_workflow_manager.storage.native_recovery_files import StagedRecoveryFiles
from micro_workflow_manager.storage.native_recovery_receipts import RecoveryReceipt
from tests.test_064_read_only_previews import _initialize_native_project
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_105_preparation_receiver_guards import _files, _run_preparation
from tests.test_146_native_applied_recovery import _create_active_scope
from tests.test_147_native_recovery_atomicity import (
    TARGET_SESSION,
    _recovery_receipts,
    _scope_business_rows,
)


def _without_new_rows(rows, table, old_ids):
    result = {name: list(values) for name, values in rows.items()}
    result[table] = [row for row in result[table] if row['operation_id'] in old_ids]
    return result


def test_preparation_terminal_receipt_reversion_rolls_back_business_and_restores_files(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'A')])
    storage = workflow.storage

    @workflow.task('A')
    def produce(ctx):
        ctx.write_output('ran.txt', 'original output')

    try:
        workflow.start('A')
        workflow.run_node('A')
        assert (tmp_path / 'node' / 'A' / 'output' / 'ran.txt').read_bytes() == b'original output'
        storage.submit_db_mutation(lambda connection: connection.execute(
            "CREATE TRIGGER revert_preparation_receipt AFTER UPDATE OF state ON preparation_receipts "
            "WHEN NEW.state='committed' BEGIN UPDATE preparation_receipts SET state='prepared', "
            "decision_json=NULL, decision_digest=NULL WHERE operation_id=NEW.operation_id; END",
        ))
        before_rows = _rows(storage)
        before_files = _files(tmp_path / 'node')
        old_attempts = {row['operation_id'] for row in before_rows['preparation_attempts']}
        old_receipts = {row['operation_id'] for row in before_rows['preparation_receipts']}

        with pytest.raises(RuntimeError):
            _run_preparation(tmp_path, workflow, 'reset')

        after_rows = _rows(storage)
        attempts = [row for row in after_rows['preparation_attempts'] if row['operation_id'] not in old_attempts]
        receipts = [row for row in after_rows['preparation_receipts'] if row['operation_id'] not in old_receipts]
        assert len(attempts) == len(receipts) == 1
        assert attempts[0]['state'] == receipts[0]['state'] == 'aborted'
        assert receipts[0]['guard_id'] == attempts[0]['operation_id']
        assert receipts[0]['decision_json'] is not None and receipts[0]['decision_digest'] is not None
        expected = _without_new_rows(after_rows, 'preparation_attempts', old_attempts)
        expected = _without_new_rows(expected, 'preparation_receipts', old_receipts)
        assert expected == before_rows
        assert after_rows['receiver_mutation_guards'] == before_rows['receiver_mutation_guards']
        assert _files(tmp_path / 'node') == before_files
        trash = tmp_path / '.mwf' / 'preparation-trash'
        assert not trash.exists() or not any(trash.iterdir())
    finally:
        _close(storage)


def test_input_terminal_receipt_reversion_rolls_back_business_and_restores_original(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage

    @workflow.task('A')
    def produce(ctx):
        handle = ctx.node('B')
        path = handle.write_input('value.txt', 'original')
        owner = storage.read_node_input_owner('B', 'A/value.txt')
        storage.submit_db_mutation(lambda connection: connection.execute(
            "CREATE TRIGGER revert_input_receipt AFTER UPDATE OF state ON input_publications "
            "WHEN NEW.state='committed' BEGIN UPDATE input_publications SET state='prepared', "
            "decision_json=NULL, decision_digest=NULL WHERE operation_id=NEW.operation_id; END",
        ))
        before_rows = _rows(storage)
        before_files = _files(tmp_path / 'node')
        old_receipts = {row['operation_id'] for row in before_rows['input_publications']}

        with pytest.raises(RuntimeError):
            handle.write_input('value.txt', 'replacement', overwrite=True)

        after_rows = _rows(storage)
        receipts = [row for row in after_rows['input_publications'] if row['operation_id'] not in old_receipts]
        assert len(receipts) == 1 and receipts[0]['state'] == 'aborted'
        assert receipts[0]['decision_json'] is not None and receipts[0]['decision_digest'] is not None
        assert _without_new_rows(after_rows, 'input_publications', old_receipts) == before_rows
        assert _files(tmp_path / 'node') == before_files
        assert path.read_bytes() == b'original'
        assert storage.read_node_input_owner('B', 'A/value.txt') == owner
        staging = tmp_path / '.mwf' / 'input-publications'
        assert not staging.exists() or not any(staging.iterdir())

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
    finally:
        _close(storage)


def test_native_recovery_terminal_receipt_reversion_rolls_back_session_recovery(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=[('A', 'A')])
    storage = workflow.storage
    shape = workflow.topology.snapshot().shape_json
    owner = _create_active_scope(storage, shape, 'A', TARGET_SESSION)
    output = storage.output_file('A', 1)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(b'invalid abandoned output')
    before_business, before_receipts = _scope_business_rows(storage, 'A', TARGET_SESSION)
    assert before_receipts == []
    before_output = output.read_bytes()
    _close(storage)

    commit = native_recovery._commit_session_recovery
    injected = []

    def revert_terminal_receipt(active_storage, plan, receipt):
        if not injected:
            active_storage.submit_db_mutation(lambda connection: connection.execute(
                "CREATE TRIGGER revert_recovery_receipt AFTER UPDATE OF state ON recovery_receipts "
                "WHEN NEW.state='committed' BEGIN UPDATE recovery_receipts SET state='prepared', "
                "decision_json=NULL, decision_digest=NULL WHERE operation_id=NEW.operation_id; END",
            ))
            injected.append(receipt.operation_id)
        return commit(active_storage, plan, receipt)

    monkeypatch.setattr(native_recovery, '_commit_session_recovery', revert_terminal_receipt)
    capsys.readouterr()
    assert cli.main(['recover']) == 1
    rendered = capsys.readouterr()
    assert injected and injected[0] in rendered.out + rendered.err
    with sqlite3.connect(tmp_path / '.mwf' / 'state.sqlite3') as connection:
        connection.execute('DROP TRIGGER revert_recovery_receipt')

    reopened = FileStorage(tmp_path)
    try:
        after_business, _ = _scope_business_rows(reopened, 'A', TARGET_SESSION)
        assert after_business == before_business
        receipts = _recovery_receipts(reopened, TARGET_SESSION)
        assert len(receipts) == 1
        assert receipts[0]['operation_id'] == injected[0] and receipts[0]['state'] == 'aborted'
        assert output.read_bytes() == before_output
        assert reopened.get_execution_session(TARGET_SESSION)['status'] == 'running'
        assert reopened.read_job_current_owner('A', 1) == owner['owner']
    finally:
        _close(reopened)


def test_native_recovery_does_not_stage_outputs_when_prepared_receipt_insert_is_suppressed(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=[('A', 'A')])
    storage = workflow.storage
    shape = workflow.topology.snapshot().shape_json
    owner = _create_active_scope(storage, shape, 'A', TARGET_SESSION)
    output = storage.output_file('A', 1)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(b'invalid abandoned output')
    before_business, before_receipts = _scope_business_rows(storage, 'A', TARGET_SESSION)
    assert before_receipts == []
    before_output = output.read_bytes()
    _close(storage)

    prepare = RecoveryReceipt.prepare
    stage = StagedRecoveryFiles.stage
    injected = []
    staged = []

    def suppress_prepared_insert(receipt, manifest):
        if not injected:
            receipt.storage.submit_db_mutation(lambda connection: connection.execute(
                'CREATE TRIGGER suppress_recovery_receipt BEFORE INSERT ON recovery_receipts '
                'BEGIN SELECT RAISE(IGNORE); END',
            ))
            injected.append(receipt.operation_id)
        return prepare(receipt, manifest)

    def observe_stage(files):
        staged.append(files.directory.name)
        return stage(files)

    monkeypatch.setattr(RecoveryReceipt, 'prepare', suppress_prepared_insert)
    monkeypatch.setattr(StagedRecoveryFiles, 'stage', observe_stage)
    capsys.readouterr()
    assert cli.main(['recover']) == 1
    rendered = capsys.readouterr()
    assert len(injected) == 1 and injected[0] in rendered.out + rendered.err
    assert staged == []
    with sqlite3.connect(tmp_path / '.mwf' / 'state.sqlite3') as connection:
        connection.execute('DROP TRIGGER suppress_recovery_receipt')

    reopened = FileStorage(tmp_path)
    try:
        after_business, receipts = _scope_business_rows(reopened, 'A', TARGET_SESSION)
        assert after_business == before_business
        assert receipts == []
        assert output.read_bytes() == before_output
        assert reopened.get_execution_session(TARGET_SESSION)['status'] == 'running'
        assert reopened.read_job_current_owner('A', 1) == owner['owner']
        staging = tmp_path / '.mwf' / 'recovery-trash'
        assert not staging.exists() or not any(staging.iterdir())
    finally:
        _close(reopened)
