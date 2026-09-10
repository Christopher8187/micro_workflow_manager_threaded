"""A reversible file change cannot start until its exact receipt exists."""

from __future__ import annotations

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.storage import input_publication_files
from micro_workflow_manager.storage.preparation_staging import PreparationStaging
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_105_preparation_receiver_guards import _established_workflow, _files, _run_preparation


def _without_new_attempt(rows, old_attempts):
    result = {table: list(values) for table, values in rows.items()}
    result['preparation_attempts'] = [
        row for row in result['preparation_attempts'] if row['operation_id'] in old_attempts
    ]
    return result


def test_preparation_does_not_stage_files_when_prepared_receipt_insert_is_suppressed(tmp_path, monkeypatch):
    workflow = _established_workflow(tmp_path)
    storage = workflow.storage
    staged = []
    stage = PreparationStaging.stage
    try:
        storage.submit_db_mutation(lambda connection: connection.execute(
            'CREATE TRIGGER suppress_preparation_receipt BEFORE INSERT ON preparation_receipts '
            'BEGIN SELECT RAISE(IGNORE); END',
        ))
        before_rows = _rows(storage)
        before_files = _files(tmp_path / 'node')
        old_attempts = {row['operation_id'] for row in before_rows['preparation_attempts']}
        old_receipts = {row['operation_id'] for row in before_rows['preparation_receipts']}

        def observe_stage(files):
            staged.append(files.directory.name)
            return stage(files)

        monkeypatch.setattr(PreparationStaging, 'stage', observe_stage)
        with pytest.raises(RuntimeError, match='receipt'):
            _run_preparation(tmp_path, workflow, 'reset')

        after_rows = _rows(storage)
        new_attempts = [
            row for row in after_rows['preparation_attempts']
            if row['operation_id'] not in old_attempts
        ]
        assert len(new_attempts) == 1
        assert new_attempts[0]['state'] == 'aborted'
        assert new_attempts[0]['finished_at'] is not None
        assert _without_new_attempt(after_rows, old_attempts) == before_rows
        assert {row['operation_id'] for row in after_rows['preparation_receipts']} == old_receipts
        assert not [
            row for row in after_rows['receiver_mutation_guards']
            if row['operation_id'] == new_attempts[0]['operation_id']
        ]
        assert staged == []
        assert _files(tmp_path / 'node') == before_files
        trash = tmp_path / '.mwf' / 'preparation-trash'
        assert not trash.exists() or not any(trash.iterdir())
    finally:
        _close(storage)


def test_input_publication_does_not_publish_when_prepared_receipt_insert_is_suppressed(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    published = []
    publish = input_publication_files.StagedInputFiles.publish

    @workflow.task('A')
    def produce(ctx):
        handle = ctx.node('B')
        path = handle.write_input('value.txt', 'original')
        owner = storage.read_node_input_owner('B', 'A/value.txt')
        storage.submit_db_mutation(lambda connection: connection.execute(
            'CREATE TRIGGER suppress_input_receipt BEFORE INSERT ON input_publications '
            'BEGIN SELECT RAISE(IGNORE); END',
        ))
        before_rows = _rows(storage)
        before_files = _files(tmp_path / 'node')
        old_receipts = {row['operation_id'] for row in before_rows['input_publications']}

        def observe_publish(files):
            published.append(files.operation_id)
            return publish(files)

        with monkeypatch.context() as patch:
            patch.setattr(input_publication_files.StagedInputFiles, 'publish', observe_publish)
            with pytest.raises(RuntimeError, match='receipt'):
                handle.write_input('value.txt', 'replacement', overwrite=True)

        assert published == []
        assert _rows(storage) == before_rows
        assert {row['operation_id'] for row in _rows(storage)['input_publications']} == old_receipts
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
