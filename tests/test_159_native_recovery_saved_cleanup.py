"""Identity-sensitive cleanup of committed native recovery staging."""

from __future__ import annotations

import json

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.native_recovery_files import StagedRecoveryFiles
from tests.test_064_read_only_previews import _close
from tests.test_147_native_recovery_atomicity import _scope_business_rows
from tests.test_156_native_session_recovery_receipts import (
    SimulatedRecoveryProcessLoss,
    _leave_committed_native_recovery,
    _target_receipt,
)


@pytest.fixture(autouse=True)
def _project_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


def _receipt_manifest(tmp_path, committed):
    storage = FileStorage(tmp_path)
    try:
        receipt = _target_receipt(storage, committed["operation_id"])
        business = _scope_business_rows(storage, "A", "failed-A")[0]
        return receipt, json.loads(receipt["manifest_json"]), business
    finally:
        _close(storage)


def test_committed_cleanup_refuses_replacement_after_final_saved_identity_read(
    tmp_path,
    monkeypatch,
    capsys,
):
    committed = _leave_committed_native_recovery(tmp_path, monkeypatch)
    receipt, _, before_business = _receipt_manifest(tmp_path, committed)
    saved = committed["entries"][0][1]
    original_bytes = saved.read_bytes()
    displaced = tmp_path / "independently-retained-original-output"
    replacement = b"independent replacement after final saved identity read"
    injected = []

    def replace_saved(files, original, saved_name):
        saved_path = files.directory / saved_name
        if injected or saved_path != saved:
            return
        saved_path.rename(displaced)
        saved_path.write_bytes(replacement)
        injected.append((original.relative_path, saved_name))

    with monkeypatch.context() as patch:
        patch.setattr(
            StagedRecoveryFiles,
            "_after_discard_identity_read",
            replace_saved,
        )
        capsys.readouterr()
        assert cli.main(["recover"]) == 1
    captured = capsys.readouterr()
    assert committed["operation_id"] in captured.out + captured.err
    assert len(injected) == 1
    assert saved.read_bytes() == replacement
    assert displaced.read_bytes() == original_bytes

    storage = FileStorage(tmp_path)
    try:
        assert _target_receipt(storage, committed["operation_id"]) == receipt
        assert _scope_business_rows(storage, "A", "failed-A")[0] == before_business
        assert committed["staging"].is_dir()
    finally:
        _close(storage)


def test_committed_cleanup_replays_process_loss_after_saved_file_detach(
    tmp_path,
    monkeypatch,
):
    committed = _leave_committed_native_recovery(tmp_path, monkeypatch)
    receipt, manifest, before_business = _receipt_manifest(tmp_path, committed)
    interrupted = []
    process_loss = SimulatedRecoveryProcessLoss(
        "lost process after verified saved-file detachment"
    )

    def lose_after_detach(files, original, saved_name):
        if interrupted:
            return
        interrupted.append((original.relative_path, saved_name))
        raise process_loss

    with monkeypatch.context() as patch:
        patch.setattr(
            StagedRecoveryFiles,
            "_after_discard_detach",
            lose_after_detach,
            raising=False,
        )
        with pytest.raises(SimulatedRecoveryProcessLoss) as caught:
            cli.main(["recover"])
    assert caught.value is process_loss
    assert len(interrupted) == 1
    discard = manifest["discard"]
    discard_directory = tmp_path.joinpath(*discard["directory"].split("/"))
    assert discard_directory.is_dir()
    assert any(discard_directory.iterdir())

    storage = FileStorage(tmp_path)
    try:
        assert _target_receipt(storage, committed["operation_id"]) == receipt
        assert _scope_business_rows(storage, "A", "failed-A")[0] == before_business
    finally:
        _close(storage)

    assert cli.main(["recover"]) == 0
    assert not committed["staging"].exists()
    reopened = FileStorage(tmp_path)
    try:
        assert _target_receipt(reopened, committed["operation_id"]) == receipt
        assert _scope_business_rows(reopened, "A", "failed-A")[0] == before_business
    finally:
        _close(reopened)


def test_committed_cleanup_replays_loss_after_discard_directory_removal(
    tmp_path,
    monkeypatch,
):
    committed = _leave_committed_native_recovery(tmp_path, monkeypatch)
    receipt, manifest, before_business = _receipt_manifest(tmp_path, committed)
    interrupted = []
    process_loss = SimulatedRecoveryProcessLoss(
        "lost process after recovery discard directory removal"
    )

    def lose_after_discard_directory_remove(files):
        interrupted.append(files.operation_id)
        raise process_loss

    with monkeypatch.context() as patch:
        patch.setattr(
            StagedRecoveryFiles,
            "_after_discard_directory_remove",
            lose_after_discard_directory_remove,
            raising=False,
        )
        with pytest.raises(SimulatedRecoveryProcessLoss) as caught:
            cli.main(["recover"])
    assert caught.value is process_loss
    assert interrupted == [committed["operation_id"]]
    discard = manifest["discard"]
    discard_directory = tmp_path.joinpath(*discard["directory"].split("/"))
    assert committed["staging"].is_dir()
    assert list(committed["staging"].iterdir()) == []
    assert not discard_directory.exists()

    storage = FileStorage(tmp_path)
    try:
        assert _target_receipt(storage, committed["operation_id"]) == receipt
        assert _scope_business_rows(storage, "A", "failed-A")[0] == before_business
    finally:
        _close(storage)

    assert cli.main(["recover"]) == 0
    assert not committed["staging"].exists()
    reopened = FileStorage(tmp_path)
    try:
        assert _target_receipt(reopened, committed["operation_id"]) == receipt
        assert _scope_business_rows(reopened, "A", "failed-A")[0] == before_business
    finally:
        _close(reopened)
