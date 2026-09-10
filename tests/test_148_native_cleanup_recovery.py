from __future__ import annotations

import os
import sqlite3
import stat
from hashlib import sha256
from pathlib import Path

import pytest

from micro_workflow_manager import MicroWorkflow, cli
from micro_workflow_manager.storage import FileStorage, input_publication_files
from micro_workflow_manager.storage.input_publication_files import StagedInputFiles
from micro_workflow_manager.storage.preparation_receipts import PreparationReceipt
from micro_workflow_manager.storage.preparation_staging import PreparationStaging
from tests.test_064_read_only_previews import _close, _snapshot
from tests.test_090_component_session_settlement import _rows
from tests.test_105_preparation_receiver_guards import _established_workflow, _run_preparation
from tests.test_106_preparation_receipt_failures import _refuse_deletion
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows


PREPARATION_ATTEMPTS = 'preparation_attempts'


@pytest.fixture(autouse=True)
def _run_public_recovery_from_project(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


def _tree_identity(root):
    result = {}
    for path in sorted(root.rglob('*')):
        relative = path.relative_to(root).as_posix()
        observed = path.stat()
        mode = stat.S_IMODE(observed.st_mode)
        if path.is_dir():
            result[relative] = ('directory', mode, observed.st_dev, observed.st_ino)
        else:
            content = path.read_bytes()
            result[relative] = (
                'file', sha256(content).hexdigest(), mode,
                observed.st_dev, observed.st_ino, observed.st_size,
                observed.st_mtime_ns,
            )
    return result


def _storage_database_rows(storage):
    connection = storage.db_connection()
    tables = [row[0] for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
    )]
    return {table: sorted((tuple(row) for row in connection.execute(
        'SELECT * FROM "' + table.replace('"', '""') + '"',
    )), key=repr) for table in tables}


def _closed_rows_without(root, *metadata_tables):
    rows = _closed_database_rows(root)
    for table in metadata_tables:
        rows.pop(table, None)
    return rows


def _preparation_receipt(root, operation_id):
    rows = _closed_database_rows(root)['preparation_receipts']
    matches = [row for row in rows if row[0] == operation_id]
    assert len(matches) == 1
    return matches[0]


def _input_receipt(root, operation_id):
    rows = _closed_database_rows(root)['input_publications']
    matches = [row for row in rows if row[0] == operation_id]
    assert len(matches) == 1
    return matches[0]


def _retire_preparation_attempt(storage, guard_id):
    def retire(connection):
        changed = connection.execute(
            f'UPDATE {PREPARATION_ATTEMPTS} '
            "SET owner_pid=99999999, process_identity='retired-preparation', "
            "heartbeat_at='2020-01-01T00:00:00+00:00' WHERE operation_id=?",
            (guard_id,),
        ).rowcount
        if changed != 1:
            raise RuntimeError('Prepared operation lost its native attempt owner')
        connection.execute(
            "UPDATE receiver_mutation_guards SET owner_pid=99999999, "
            "process_identity='retired-preparation' WHERE operation_id=?",
            (guard_id,),
        )

    storage.submit_db_mutation(retire)


def _leave_prepared_component_cleanup(tmp_path, monkeypatch):
    workflow = _established_workflow(tmp_path)
    storage = workflow.storage
    before_business = {
        table: rows for table, rows in _storage_database_rows(storage).items()
        if table not in {
            'preparation_receipts',
            'receiver_mutation_guards',
            PREPARATION_ATTEMPTS,
        }
    }
    before_tree = _tree_identity(tmp_path / 'node')
    known = {row['operation_id'] for row in _rows(storage)['preparation_receipts']}
    _refuse_deletion(storage, 'input')

    with monkeypatch.context() as patch:
        patch.setattr(
            PreparationReceipt,
            'state',
            lambda receipt: (_ for _ in ()).throw(OSError('simulated preparation process loss')),
        )
        with pytest.raises(sqlite3.IntegrityError, match='injected preparation deletion failure'):
            _run_preparation(tmp_path, workflow, 'reset')

    receipts = [
        row for row in _rows(storage)['preparation_receipts']
        if row['operation_id'] not in known
    ]
    assert len(receipts) == 1
    receipt = receipts[0]
    assert receipt['state'] == 'prepared'
    storage.submit_db_mutation(
        lambda connection: connection.execute('DROP TRIGGER refuse_prepared_deletion'),
    )
    _retire_preparation_attempt(storage, receipt['guard_id'])
    storage.db_mutation_barrier()
    staging = tmp_path / '.mwf' / 'preparation-trash' / receipt['operation_id']
    assert staging.is_dir() and any(staging.iterdir())
    assert (tmp_path / 'node' / 'A' / 'output').is_dir()
    _close(storage)
    return {
        'operation_id': receipt['operation_id'],
        'guard_id': receipt['guard_id'],
        'staging': staging,
        'before_business': before_business,
        'before_tree': before_tree,
    }


def _assert_preparation_restored(tmp_path, prepared):
    assert _closed_rows_without(
        tmp_path,
        'preparation_receipts',
        'receiver_mutation_guards',
        PREPARATION_ATTEMPTS,
    ) == prepared['before_business']
    assert _tree_identity(tmp_path / 'node') == prepared['before_tree']
    receipt = _preparation_receipt(tmp_path, prepared['operation_id'])
    assert receipt[5] == 'aborted'
    rows = _closed_database_rows(tmp_path)
    assert not [row for row in rows['receiver_mutation_guards'] if row[1] == prepared['guard_id']]
    attempts = [row for row in rows[PREPARATION_ATTEMPTS] if row[0] == prepared['guard_id']]
    assert len(attempts) == 1 and 'aborted' in attempts[0]
    assert not prepared['staging'].exists()


@pytest.mark.parametrize(
    'interruption',
    ['before-first-move', 'after-first-move', 'after-second-move',
     'after-third-move', 'after-replacement-directory'],
)
def test_prepared_component_cleanup_recovers_each_forward_file_boundary(
    tmp_path,
    monkeypatch,
    interruption,
):
    workflow = _established_workflow(tmp_path)
    storage = workflow.storage
    before_business = {
        table: rows for table, rows in _storage_database_rows(storage).items()
        if table not in {
            'preparation_receipts',
            'receiver_mutation_guards',
            PREPARATION_ATTEMPTS,
        }
    }
    before_tree = _tree_identity(tmp_path / 'node')
    known = {row['operation_id'] for row in _rows(storage)['preparation_receipts']}
    rename, mkdir = Path.rename, Path.mkdir
    observed_moves = []
    replacement_target = tmp_path / 'node' / 'A' / 'output'
    move_boundaries = {
        1: 'after-first-move',
        2: 'after-second-move',
        3: 'after-third-move',
    }

    def interrupt_move(path, target):
        target = Path(target)
        forward = (
            path.is_relative_to(tmp_path / 'node')
            and target.is_relative_to(tmp_path / '.mwf' / 'preparation-trash')
        )
        publishes_replacement = (
            interruption == 'after-replacement-directory'
            and path.is_relative_to(tmp_path / '.mwf')
            and target == replacement_target
        )
        if forward and interruption == 'before-first-move' and not observed_moves:
            raise OSError('interrupted before first preparation move')
        result = rename(path, target)
        if publishes_replacement:
            raise OSError('interrupted after replacement directory publication')
        if forward:
            observed_moves.append(Path(target))
            if interruption == move_boundaries.get(len(observed_moves)):
                raise OSError(f'interrupted after preparation move {len(observed_moves)}')
        return result

    def interrupt_directory(path, *args, **kwargs):
        result = mkdir(path, *args, **kwargs)
        replacement = path == replacement_target
        if replacement and interruption == 'after-replacement-directory':
            raise OSError('interrupted after replacement directory publication')
        return result

    with monkeypatch.context() as patch:
        patch.setattr(Path, 'rename', interrupt_move)
        patch.setattr(Path, 'mkdir', interrupt_directory)
        patch.setattr(
            PreparationReceipt,
            'state',
            lambda receipt: (_ for _ in ()).throw(
                OSError('simulated preparation process loss'),
            ),
        )
        with pytest.raises(OSError, match='interrupted'):
            _run_preparation(tmp_path, workflow, 'reset')

    receipts = [
        row for row in _rows(storage)['preparation_receipts']
        if row['operation_id'] not in known
    ]
    assert len(receipts) == 1 and receipts[0]['state'] == 'prepared'
    receipt = receipts[0]
    _retire_preparation_attempt(storage, receipt['guard_id'])
    storage.db_mutation_barrier()
    staging = tmp_path / '.mwf' / 'preparation-trash' / receipt['operation_id']
    assert staging.is_dir()
    _close(storage)
    prepared = {
        'operation_id': receipt['operation_id'],
        'guard_id': receipt['guard_id'],
        'staging': staging,
        'before_business': before_business,
        'before_tree': before_tree,
    }
    assert cli.main(['recover']) == 0
    _assert_preparation_restored(tmp_path, prepared)


@pytest.mark.parametrize('interruption', ['mid-restoration', 'after-restoration'])
def test_prepared_component_cleanup_retries_from_exact_file_identities(
    tmp_path,
    monkeypatch,
    capsys,
    interruption,
):
    prepared = _leave_prepared_component_cleanup(tmp_path, monkeypatch)
    injected = []
    with monkeypatch.context() as patch:
        if interruption == 'mid-restoration':
            rename = Path.rename
            restored = []

            def interrupt_second_restore(path, target):
                saved_original = path.is_relative_to(prepared['staging'])
                visible_target = Path(target).is_relative_to(tmp_path / 'node')
                if saved_original and visible_target:
                    if restored:
                        injected.append(True)
                        raise OSError('interrupted midway through preparation restoration')
                    result = rename(path, target)
                    restored.append(Path(target))
                    return result
                return rename(path, target)

            patch.setattr(Path, 'rename', interrupt_second_restore)
        else:
            from micro_workflow_manager.storage import native_recovery

            finish = native_recovery._finish_cleanup_receipt

            def interrupt_after_restore(storage, plan, state):
                if plan.operation_id == prepared['operation_id'] and state == 'aborted':
                    assert _tree_identity(tmp_path / 'node') == prepared['before_tree']
                    injected.append(True)
                    raise OSError('interrupted after preparation restoration')
                return finish(storage, plan, state)

            patch.setattr(native_recovery, '_finish_cleanup_receipt', interrupt_after_restore)
        assert cli.main(['recover']) == 1
        assert injected == [True]
        assert _preparation_receipt(tmp_path, prepared['operation_id'])[5] == 'prepared'

    capsys.readouterr()
    assert cli.main(['recover']) == 0
    _assert_preparation_restored(tmp_path, prepared)
    after_rows = _closed_database_rows(tmp_path)
    after_files = _snapshot(tmp_path, mutable_existing_shm=True)
    assert cli.main(['recover']) == 0
    assert _closed_database_rows(tmp_path) == after_rows
    assert _snapshot(tmp_path, mutable_existing_shm=True) == after_files


@pytest.mark.parametrize('damage', ['saved-original', 'visible-replacement', 'unexpected-private'])
def test_prepared_component_cleanup_refuses_changed_recovery_material(
    tmp_path,
    monkeypatch,
    capsys,
    damage,
):
    prepared = _leave_prepared_component_cleanup(tmp_path, monkeypatch)
    if damage == 'saved-original':
        saved = next(path for path in prepared['staging'].rglob('*') if path.is_file())
        saved.write_bytes(b'changed saved preparation file')
    elif damage == 'visible-replacement':
        replacement = tmp_path / 'node' / 'A' / 'output'
        replacement.rmdir()
        replacement.mkdir()
    else:
        (prepared['staging'] / 'unexpected-private').write_bytes(b'not in manifest')
    before_rows = _closed_database_rows(tmp_path)
    before_node = _tree_identity(tmp_path / 'node')
    before_staging = _tree_identity(prepared['staging'])

    assert cli.main(['recover']) == 1
    output = capsys.readouterr()
    assert prepared['operation_id'] in output.out + output.err
    assert _closed_database_rows(tmp_path) == before_rows
    assert _tree_identity(tmp_path / 'node') == before_node
    assert _tree_identity(prepared['staging']) == before_staging
    assert _preparation_receipt(tmp_path, prepared['operation_id'])[5] == 'prepared'


@pytest.mark.parametrize('unexpected_private', [False, True])
def test_committed_component_cleanup_keeps_visible_result_and_discards_only_known_trash(
    tmp_path,
    monkeypatch,
    capsys,
    unexpected_private,
):
    workflow = _established_workflow(tmp_path)
    storage = workflow.storage
    known = {row['operation_id'] for row in _rows(storage)['preparation_receipts']}
    cleanup_calls = []
    remove_recorded = PreparationStaging._remove_recorded_tree

    def interrupt_cleanup(files, relative, expected):
        if not cleanup_calls and (files.root / relative).exists():
            assert relative.startswith(files.relative + '/')
            cleanup_calls.append(relative)
            raise OSError('simulated committed cleanup crash')
        return remove_recorded(files, relative, expected)

    with monkeypatch.context() as patch:
        patch.setattr(PreparationStaging, '_remove_recorded_tree', interrupt_cleanup)
        _run_preparation(tmp_path, workflow, 'reset')
    assert len(cleanup_calls) == 1
    receipt, = [
        row for row in _rows(storage)['preparation_receipts']
        if row['operation_id'] not in known
    ]
    assert receipt['state'] == 'committed'
    staging = tmp_path / '.mwf' / 'preparation-trash' / receipt['operation_id']
    assert staging.is_dir()
    _close(storage)
    if unexpected_private:
        (staging / 'unexpected-private').write_bytes(b'not in manifest')
    before_rows = _closed_database_rows(tmp_path)
    before_node = _tree_identity(tmp_path / 'node')
    before_staging = _tree_identity(staging)

    assert cli.main(['recover']) == (1 if unexpected_private else 0)
    output = capsys.readouterr()
    if unexpected_private:
        assert receipt['operation_id'] in output.out + output.err
    assert _closed_database_rows(tmp_path) == before_rows
    assert _tree_identity(tmp_path / 'node') == before_node
    if unexpected_private:
        assert _tree_identity(staging) == before_staging
    else:
        assert not staging.exists()


def _leave_prepared_input_publication(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    remembered = {}

    @workflow.task('A')
    def publish(ctx):
        receiver = ctx.node('B')
        original = receiver.write_input('keep.txt', 'original')
        original.chmod(stat.S_IREAD)
        remembered['original_mode'] = stat.S_IMODE(original.stat().st_mode)
        replace = os.replace
        exclusive_move = input_publication_files._move
        keep_target = tmp_path / 'node' / 'B' / 'input' / 'A' / 'keep.txt'
        later_target = tmp_path / 'node' / 'B' / 'input' / 'A' / 'later.txt'
        forward_moves = []
        with monkeypatch.context() as patch:
            def observe_overwrite(source, target):
                result = replace(source, target)
                if Path(target) == keep_target:
                    forward_moves.append(('replace', Path(target)))
                return result

            def fail_second_exclusive_move(source, target):
                result = exclusive_move(source, target)
                if Path(target) == later_target:
                    forward_moves.append(('exclusive', Path(target)))
                    assert forward_moves == [
                        ('replace', keep_target),
                        ('exclusive', later_target),
                    ]
                    assert keep_target.read_text() == 'replacement'
                    assert later_target.read_text() == 'later'
                    raise OSError('simulated input publication process loss')
                return result

            patch.setattr(os, 'replace', observe_overwrite)
            patch.setattr(input_publication_files, '_move', fail_second_exclusive_move)
            patch.setattr(
                storage,
                '_input_publication_state',
                lambda operation_id: (_ for _ in ()).throw(
                    OSError('publication outcome unavailable'),
                ),
            )
            with pytest.raises(OSError, match='simulated input publication process loss'):
                receiver.write_inputs(
                    [('keep.txt', 'replacement'), ('later.txt', 'later')],
                    overwrite=True,
                )
            assert forward_moves == [
                ('replace', keep_target),
                ('exclusive', later_target),
            ]

    workflow.start('A')
    workflow.run_job('A', 1, ignore_readiness=True)
    prepared, = [
        row for row in _rows(storage)['input_publications'] if row['state'] == 'prepared'
    ]
    staging = tmp_path / '.mwf' / 'input-publications' / prepared['operation_id']
    assert staging.is_dir()
    business = {
        table: rows
        for table, rows in _storage_database_rows(storage).items()
        if table != 'input_publications'
    }
    _close(storage)
    return {
        'operation_id': prepared['operation_id'],
        'staging': staging,
        'original_mode': remembered['original_mode'],
        'owner_rows': business['job_execution_owners'],
        'business': business,
    }


def test_prepared_input_copy_recovery_recognizes_completed_copy_before_receipt_abort(
    tmp_path,
    monkeypatch,
):
    prepared = _leave_prepared_input_publication(tmp_path, monkeypatch)
    target = tmp_path / 'node' / 'B' / 'input' / 'A' / 'keep.txt'
    copied = []
    copy_file = FileStorage.atomic_copy_file
    with monkeypatch.context() as patch:
        def restore_then_interrupt(storage, source, destination):
            result = copy_file(storage, source, destination)
            copied_backup = Path(source).is_relative_to(prepared['staging'])
            if Path(destination) == target and copied_backup:
                copied.append(True)
                raise OSError('interrupted after copied input restoration')
            return result

        patch.setattr(FileStorage, 'atomic_copy_file', restore_then_interrupt)
        assert cli.main(['recover']) == 1
    assert copied == [True]
    assert target.read_bytes() == b'original'
    assert stat.S_IMODE(target.stat().st_mode) == prepared['original_mode']
    assert _input_receipt(tmp_path, prepared['operation_id'])[3] == 'prepared'

    assert cli.main(['recover']) == 0
    assert target.read_bytes() == b'original'
    assert stat.S_IMODE(target.stat().st_mode) == prepared['original_mode']
    assert not (target.parent / 'later.txt').exists()
    assert _input_receipt(tmp_path, prepared['operation_id'])[3] == 'aborted'
    assert not prepared['staging'].exists()
    rows = _closed_database_rows(tmp_path)
    assert rows['job_execution_owners'] == prepared['owner_rows']
    rows.pop('input_publications')
    assert rows == prepared['business']


@pytest.mark.parametrize('damage', ['backup', 'visible', 'unexpected-private'])
def test_prepared_input_recovery_refuses_changed_copy_material(
    tmp_path,
    monkeypatch,
    capsys,
    damage,
):
    prepared = _leave_prepared_input_publication(tmp_path, monkeypatch)
    target = tmp_path / 'node' / 'B' / 'input' / 'A' / 'keep.txt'
    if damage == 'backup':
        backup = next(prepared['staging'].glob('*.old'))
        backup.chmod(stat.S_IMODE(backup.stat().st_mode) | stat.S_IWRITE)
        backup.write_bytes(b'changed backup')
    elif damage == 'visible':
        target.write_bytes(b'independent visible replacement')
    else:
        (prepared['staging'] / 'unexpected-private').write_bytes(b'not in manifest')
    before_rows = _closed_database_rows(tmp_path)
    before_node = _tree_identity(tmp_path / 'node')
    before_staging = _tree_identity(prepared['staging'])

    assert cli.main(['recover']) == 1
    output = capsys.readouterr()
    assert prepared['operation_id'] in output.out + output.err
    assert _closed_database_rows(tmp_path) == before_rows
    assert _tree_identity(tmp_path / 'node') == before_node
    assert _tree_identity(prepared['staging']) == before_staging
    assert _input_receipt(tmp_path, prepared['operation_id'])[3] == 'prepared'


@pytest.mark.parametrize('unexpected_private', [False, True])
def test_committed_input_recovery_preserves_publication_and_discards_only_known_staging(
    tmp_path,
    monkeypatch,
    capsys,
    unexpected_private,
):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    retained = {}

    @workflow.task('A')
    def publish(ctx):
        ctx.node('B').write_input('keep.txt', 'original')
        with monkeypatch.context() as patch:
            patch.setattr(
                StagedInputFiles,
                'discard',
                lambda files: (_ for _ in ()).throw(
                    OSError('simulated committed publication crash'),
                ),
            )
            with pytest.warns(RuntimeWarning, match='retained staging files'):
                retained['path'] = ctx.node('B').write_input(
                    'keep.txt', 'committed replacement', overwrite=True,
                )

    workflow.start('A')
    workflow.run_job('A', 1, ignore_readiness=True)
    receipt, = [
        row for row in _rows(storage)['input_publications']
        if row['state'] == 'committed'
        and (tmp_path / '.mwf' / 'input-publications' / row['operation_id']).exists()
    ]
    staging = tmp_path / '.mwf' / 'input-publications' / receipt['operation_id']
    owner = storage.read_node_input_owner('B', 'A/keep.txt')
    _close(storage)
    if unexpected_private:
        (staging / 'unexpected-private').write_bytes(b'not in manifest')
    before_rows = _closed_database_rows(tmp_path)
    before_node = _tree_identity(tmp_path / 'node')
    before_staging = _tree_identity(staging)

    assert cli.main(['recover']) == (1 if unexpected_private else 0)
    output = capsys.readouterr()
    if unexpected_private:
        assert receipt['operation_id'] in output.out + output.err
    assert _closed_database_rows(tmp_path) == before_rows
    assert _tree_identity(tmp_path / 'node') == before_node
    assert retained['path'].read_bytes() == b'committed replacement'
    reopened = FileStorage(tmp_path)
    try:
        assert reopened.read_node_input_owner('B', 'A/keep.txt') == owner
    finally:
        _close(reopened)
    if unexpected_private:
        assert _tree_identity(staging) == before_staging
    else:
        assert not staging.exists()
