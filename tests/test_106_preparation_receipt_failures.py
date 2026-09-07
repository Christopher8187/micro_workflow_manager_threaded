from __future__ import annotations

import sqlite3
from threading import Event

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.file_systems import NodeInputFileSystem
from micro_workflow_manager.storage import preparation_files
from micro_workflow_manager.storage.preparation_receipts import PreparationReceipt
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_105_preparation_receiver_guards import _established_workflow, _files, _run_preparation


def _business_rows(storage):
    rows = _rows(storage)
    for table in ('preparation_receipts', 'receiver_mutation_guards'):
        rows.pop(table)
    return rows


def _refuse_deletion(storage, artifact):
    table = 'jobs' if artifact == 'job' else 'managed_input_files'
    column = 'node_name' if artifact == 'job' else 'receiver_node'
    storage.submit_db_mutation(lambda connection: connection.execute(
        f'CREATE TRIGGER refuse_prepared_deletion BEFORE DELETE ON {table} '
        f"WHEN OLD.{column}='B' BEGIN SELECT RAISE(ABORT, 'injected preparation deletion failure'); END",
    ))


@pytest.mark.parametrize('publication', ['overwrite', 'append'])
def test_full_preparation_refuses_input_with_an_unowned_predecessor_before_mutation(tmp_path, publication):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    path = tmp_path / 'node' / 'B' / 'input' / 'A' / 'existing.txt'
    path.parent.mkdir()
    path.write_bytes(b'project input')

    @workflow.task('A')
    def produce(ctx):
        entry = NodeInputFileSystem('B').file(ctx, 'existing.txt')
        if publication == 'overwrite':
            entry.write_text('managed replacement')
        else:
            entry.append_text(' plus managed input')

    workflow.start('A')
    try:
        workflow.run_node('A')
        before = _rows(storage), _files(tmp_path / 'node')
        with pytest.raises(RuntimeError, match='ambiguous managed input'):
            _run_preparation(tmp_path, workflow, 'reset')
        assert (_rows(storage), _files(tmp_path / 'node')) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('artifact', ['input', 'job'])
def test_sql_failure_restores_owned_inputs_jobs_and_selected_component(tmp_path, artifact):
    workflow = _established_workflow(tmp_path)
    storage = workflow.storage
    try:
        _refuse_deletion(storage, artifact)
        before = _business_rows(storage), _files(tmp_path / 'node')
        receipts = {row['operation_id'] for row in _rows(storage)['preparation_receipts']}
        with pytest.raises(sqlite3.IntegrityError, match='injected preparation deletion failure'):
            _run_preparation(tmp_path, workflow, 'reset')
        assert (_business_rows(storage), _files(tmp_path / 'node')) == before
        new_receipt, = [row for row in _rows(storage)['preparation_receipts'] if row['operation_id'] not in receipts]
        assert new_receipt['state'] == 'aborted'
        assert _rows(storage)['receiver_mutation_guards'] == []
        assert not (tmp_path / '.mwf' / 'preparation-trash').exists()
    finally:
        _close(storage)


def test_unknown_preparation_outcome_retains_files_receipt_and_guard_for_recovery(tmp_path, monkeypatch):
    workflow = _established_workflow(tmp_path)
    storage = workflow.storage
    try:
        _refuse_deletion(storage, 'input')
        before = _business_rows(storage)
        receipts = {row['operation_id'] for row in _rows(storage)['preparation_receipts']}

        def unavailable(receipt):
            raise OSError('receipt outcome temporarily unreadable')

        monkeypatch.setattr(PreparationReceipt, 'state', unavailable)
        with pytest.raises(sqlite3.IntegrityError, match='injected preparation deletion failure') as caught:
            _run_preparation(tmp_path, workflow, 'reset')
        assert _business_rows(storage) == before
        receipt, = [row for row in _rows(storage)['preparation_receipts'] if row['operation_id'] not in receipts]
        guard, = _rows(storage)['receiver_mutation_guards']
        assert receipt['state'] == 'prepared'
        assert guard['receiver_node'] == 'B' and guard['operation_id'] == receipt['guard_id']
        saved = tmp_path / '.mwf' / 'preparation-trash' / receipt['operation_id']
        assert any(path.read_bytes() == b'from A' for path in saved.rglob('*') if path.is_file())
        assert not (tmp_path / 'node' / 'B' / 'input' / 'A' / 'owned.txt').exists()
        assert any('receipt outcome temporarily unreadable' in note for note in caught.value.__notes__)
        with pytest.raises(RuntimeError, match='unfinished preparation'):
            storage.read_node_input_owner('B', 'A/owned.txt')
        with pytest.raises(RuntimeError, match='unfinished preparation'):
            _run_preparation(tmp_path, workflow, 'reset')
        assert _business_rows(storage) == before
    finally:
        _close(storage)


def test_postcommit_cleanup_interrupt_preserves_original_notification_error(tmp_path, monkeypatch):
    workflow = _established_workflow(tmp_path)
    storage = workflow.storage
    original = OSError('original notification failure after preparation commit')
    cleanup_error = KeyboardInterrupt('interrupted retired preparation files cleanup')
    before_a = storage.get_component_state(('A',))
    notify = storage.notify_state_change
    injected = []

    def fail_notification():
        if not injected and storage.get_component_state(('A',))['alignment_generation'] > before_a['alignment_generation']:
            injected.append(True)
            raise original
        return notify()

    def fail_cleanup(path, *args, **kwargs):
        raise cleanup_error

    monkeypatch.setattr(storage, 'notify_state_change', fail_notification)
    monkeypatch.setattr(preparation_files.shutil, 'rmtree', fail_cleanup)
    try:
        with pytest.raises((OSError, KeyboardInterrupt)) as caught:
            _run_preparation(tmp_path, workflow, 'reset')
        assert caught.value is original and injected == [True]
        assert any(str(cleanup_error) in note for note in original.__notes__)
        assert storage.get_component_state(('A',)) == dict(
            before_a, lifecycle='queued', stability=None, instability_origin=None,
            misaligned=False, alignment_generation=before_a['alignment_generation'] + 1,
        )
        assert not (tmp_path / 'node' / 'B' / 'input' / 'A' / 'owned.txt').exists()
        assert _rows(storage)['receiver_mutation_guards'] == []
        assert list((tmp_path / '.mwf' / 'preparation-trash').iterdir())
    finally:
        _close(storage)


@pytest.mark.parametrize('outcome', ['commit', 'rollback'])
def test_repeated_interrupts_drain_exact_preparation_decision_before_file_resolution(tmp_path, monkeypatch, outcome):
    workflow = _established_workflow(tmp_path)
    storage = workflow.storage
    original = KeyboardInterrupt('original interrupted preparation waiter')
    repeated = KeyboardInterrupt('second interrupted preparation waiter')
    entered, release = Event(), Event()
    submit = storage.submit_db_mutation
    waits = []
    before = _business_rows(storage), _files(tmp_path / 'node')
    before_a = storage.get_component_state(('A',))

    class InterruptedWait:
        def __init__(self, future):
            self.future = future

        def done(self):
            return self.future.done()

        def result(self):
            waits.append(len(waits) + 1)
            if len(waits) == 1:
                assert entered.wait(10)
                raise original
            if len(waits) == 2:
                release.set()
                raise repeated
            return self.future.result()

    def delayed_commit(operation, **kwargs):
        if operation.__name__ != 'commit' or not operation.__code__.co_filename.endswith('preparation_execution.py'):
            return submit(operation, **kwargs)

        def decide(connection):
            entered.set()
            assert release.wait(10)
            if outcome == 'rollback':
                raise OSError('queued preparation decision failed')
            return operation(connection)

        assert kwargs['wait'] is False
        return InterruptedWait(submit(decide, **kwargs))

    monkeypatch.setattr(storage, 'submit_db_mutation', delayed_commit)
    try:
        with pytest.raises(KeyboardInterrupt) as caught:
            _run_preparation(tmp_path, workflow, 'reset')
        assert caught.value is original and len(waits) >= 2
        assert _rows(storage)['receiver_mutation_guards'] == []
        if outcome == 'rollback':
            assert (_business_rows(storage), _files(tmp_path / 'node')) == before
        else:
            assert storage.get_component_state(('A',)) == dict(
                before_a, lifecycle='queued', stability=None, instability_origin=None,
                misaligned=False, alignment_generation=before_a['alignment_generation'] + 1,
            )
            assert not (tmp_path / 'node' / 'B' / 'input' / 'A' / 'owned.txt').exists()
            assert storage.get_component_state(('B',))['misaligned'] is True
        assert not (tmp_path / '.mwf' / 'preparation-trash').exists()
    finally:
        release.set()
        _close(storage)
