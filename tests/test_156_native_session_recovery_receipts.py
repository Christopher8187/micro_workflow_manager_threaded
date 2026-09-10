"""RED cases for reopening interrupted native session recovery operations."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.native_recovery_files import StagedRecoveryFiles
from micro_workflow_manager.storage.native_recovery_receipts import RecoveryReceipt
from micro_workflow_manager.storage.operation_lock import operation_lock
from micro_workflow_manager.storage.recovery_output import recovery_path
from tests.test_064_read_only_previews import (
    _initialize_native_project,
    _mark_execution_session_stale,
    _snapshot,
)
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows
from tests.test_146_native_applied_recovery import (
    _assert_recovered_failed_scope,
    _create_active_scope,
)
from tests.test_147_native_recovery_atomicity import (
    SAFE_SESSION,
    TARGET_SESSION,
    _claim_second_job,
    _job_row,
    _recovery_receipts,
    _scope_business_rows,
)


class SimulatedRecoveryProcessLoss(BaseException):
    pass


@pytest.fixture(autouse=True)
def _project_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


def _close(storage):
    storage.db_mutation_barrier()
    deadline = time.monotonic() + 10
    while storage.mutation_writer_diagnostics()["writer_alive"]:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    storage.close_database_connections()


def _target_receipt(storage, operation_id):
    matches = [
        receipt
        for receipt in _recovery_receipts(storage, TARGET_SESSION)
        if receipt["operation_id"] == operation_id
    ]
    assert len(matches) == 1
    return matches[0]


def _manifest_entries(root, receipt):
    manifest = json.loads(receipt["manifest_json"])
    directory = recovery_path(root, manifest["directory"])
    return directory, [
        (
            recovery_path(root, entry["original"]["path"]),
            directory / entry["saved"],
        )
        for entry in manifest["paths"]
    ]


def _leave_prepared_native_recovery(tmp_path, monkeypatch, *, moves_before_loss):
    workflow = _initialize_native_project(
        tmp_path,
        monkeypatch,
        edges=[("A", "A"), ("Z", "Z")],
    )
    storage = workflow.storage
    shape = workflow.topology.snapshot().shape_json
    first = _create_active_scope(storage, shape, "A", TARGET_SESSION, live=True)
    second = _claim_second_job(storage)
    _mark_execution_session_stale(storage, TARGET_SESSION)
    safe = _create_active_scope(storage, shape, "Z", SAFE_SESSION)
    outputs = []
    for job_id, content in ((1, b"invalid abandoned output one"), (2, b"{invalid output two")):
        output = storage.output_file("A", job_id)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(content)
        outputs.append(output)
    before_business, before_receipts = _scope_business_rows(storage, "A", TARGET_SESSION)
    assert before_receipts == []
    before_owners = {
        1: storage.get_job_execution_owner(first["execution_id"]),
        2: storage.get_job_execution_owner(second["execution_id"]),
    }

    from micro_workflow_manager.storage import native_recovery_files

    replace = native_recovery_files.os.replace
    moved = []
    process_loss = SimulatedRecoveryProcessLoss(
        f"lost process after native recovery move {moves_before_loss}"
    )

    def lose_after_forward_move(source, destination):
        result = replace(source, destination)
        source, destination = Path(source), Path(destination)
        if (
            source.is_relative_to(tmp_path / "node")
            and destination.is_relative_to(tmp_path / ".mwf" / "recovery-trash")
        ):
            moved.append((source, destination))
            if len(moved) == moves_before_loss:
                raise process_loss
        return result

    def unavailable_receipt_state(receipt):
        raise OSError(f"receipt state unavailable after process loss: {receipt.operation_id}")

    with monkeypatch.context() as patch:
        patch.setattr(native_recovery_files.os, "replace", lose_after_forward_move)
        patch.setattr(RecoveryReceipt, "state", unavailable_receipt_state)
        with pytest.raises(SimulatedRecoveryProcessLoss) as caught:
            cli.main(["recover"])
    assert caught.value is process_loss
    assert any(
        "receipt state unavailable after process loss" in note
        for note in getattr(process_loss, "__notes__", ())
    )
    assert len(moved) == moves_before_loss
    storage.db_mutation_barrier()
    receipts = _recovery_receipts(storage, TARGET_SESSION)
    assert len(receipts) == 1 and receipts[0]["state"] == "prepared"
    receipt = receipts[0]
    staging, entries = _manifest_entries(tmp_path, receipt)
    assert staging.is_dir()
    assert len(entries) == 2
    assert sum(visible.exists() for visible, _ in entries) == 2 - moves_before_loss
    assert sum(saved.exists() for _, saved in entries) == moves_before_loss
    assert _scope_business_rows(storage, "A", TARGET_SESSION)[0] == before_business
    assert storage.get_execution_session(SAFE_SESSION)["status"] == "running"
    _close(storage)
    return {
        "operation_id": receipt["operation_id"],
        "staging": staging,
        "entries": entries,
        "first": first,
        "second": second,
        "safe": safe,
        "before_business": before_business,
        "before_owners": before_owners,
        "outputs": outputs,
    }


def _assert_final_native_recovery(tmp_path, prepared):
    storage = FileStorage(tmp_path)
    try:
        target_receipts = _recovery_receipts(storage, TARGET_SESSION)
        assert len(target_receipts) == 2
        states = {row["operation_id"]: row["state"] for row in target_receipts}
        assert states[prepared["operation_id"]] == "aborted"
        committed = [
            operation_id
            for operation_id, state in states.items()
            if state == "committed"
        ]
        assert len(committed) == 1 and committed[0] != prepared["operation_id"]
        assert not prepared["staging"].exists()
        assert not (tmp_path / ".mwf" / "recovery-trash" / committed[0]).exists()
        _assert_recovered_failed_scope(storage, "A", prepared["first"], terminal=False)
        assert _job_row(storage, "A", 2)[0:3] == (
            "queued",
            prepared["second"]["generation"] + 1,
            None,
        )
        assert storage.get_job_execution_owner(
            prepared["second"]["execution_id"]
        ) == prepared["before_owners"][2]
        assert storage.read_job_current_owner("A", 2) == prepared["before_owners"][2]
        assert all(not output.exists() for output in prepared["outputs"])
        _assert_recovered_failed_scope(storage, "Z", prepared["safe"], terminal=False)
        assert [row["state"] for row in _recovery_receipts(storage, SAFE_SESSION)] == ["committed"]
    finally:
        _close(storage)


@pytest.mark.parametrize("moves_before_loss", [1, 2])
def test_reopened_recovery_restores_prepared_files_then_uses_a_fresh_operation(
    tmp_path,
    monkeypatch,
    moves_before_loss,
):
    prepared = _leave_prepared_native_recovery(
        tmp_path,
        monkeypatch,
        moves_before_loss=moves_before_loss,
    )

    assert cli.main(["recover"]) == 0
    _assert_final_native_recovery(tmp_path, prepared)

    after_rows = _closed_database_rows(tmp_path)
    after_files = _snapshot(tmp_path, mutable_existing_shm=True)
    assert cli.main(["recover"]) == 0
    assert _closed_database_rows(tmp_path) == after_rows
    assert _snapshot(tmp_path, mutable_existing_shm=True) == after_files


def test_reopened_recovery_retries_after_process_loss_midway_through_restoration(
    tmp_path,
    monkeypatch,
):
    prepared = _leave_prepared_native_recovery(tmp_path, monkeypatch, moves_before_loss=2)
    calls = []

    def interrupt_second_restore(files, original, saved):
        calls.append((original.relative_path, saved))
        if len(calls) == 2:
            raise SimulatedRecoveryProcessLoss("lost during restoration")

    with monkeypatch.context() as patch:
        patch.setattr(
            StagedRecoveryFiles,
            "_before_restore_entry",
            interrupt_second_restore,
            raising=False,
        )
        with pytest.raises(SimulatedRecoveryProcessLoss, match="restoration"):
            cli.main(["recover"])
    assert len(calls) == 2
    storage = FileStorage(tmp_path)
    try:
        assert _target_receipt(storage, prepared["operation_id"])["state"] == "prepared"
        assert _scope_business_rows(storage, "A", TARGET_SESSION)[0] == prepared["before_business"]
        assert sum(visible.exists() for visible, _ in prepared["entries"]) == 1
        assert sum(saved.exists() for _, saved in prepared["entries"]) == 1
        assert storage.get_execution_session(SAFE_SESSION)["status"] == "running"
    finally:
        _close(storage)

    assert cli.main(["recover"]) == 0
    _assert_final_native_recovery(tmp_path, prepared)


def _leave_committed_native_recovery(tmp_path, monkeypatch):
    workflow = _initialize_native_project(
        tmp_path,
        monkeypatch,
        edges=[("A", "A"), ("Z", "Z")],
    )
    storage = workflow.storage
    shape = workflow.topology.snapshot().shape_json
    first = _create_active_scope(storage, shape, "A", TARGET_SESSION, live=True)
    second = _claim_second_job(storage)
    _mark_execution_session_stale(storage, TARGET_SESSION)
    safe = _create_active_scope(storage, shape, "Z", SAFE_SESSION)
    for job_id in (1, 2):
        path = storage.output_file("A", job_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"invalid output {job_id}".encode())
    discard = StagedRecoveryFiles.discard
    interrupted = []

    def retain_committed_target(files):
        if files.plan.session_id == TARGET_SESSION:
            interrupted.append(files.operation_id if hasattr(files, "operation_id") else files.relative)
            raise OSError("lost process during committed recovery cleanup")
        return discard(files)

    with monkeypatch.context() as patch:
        patch.setattr(StagedRecoveryFiles, "discard", retain_committed_target)
        assert cli.main(["recover"]) == 0
    assert len(interrupted) == 1
    target_receipts = _recovery_receipts(storage, TARGET_SESSION)
    assert len(target_receipts) == 1 and target_receipts[0]["state"] == "committed"
    receipt = target_receipts[0]
    staging, entries = _manifest_entries(tmp_path, receipt)
    assert staging.is_dir() and all(saved.exists() for _, saved in entries)
    before_business, ignored_receipts = _scope_business_rows(storage, "A", TARGET_SESSION)
    assert ignored_receipts
    before_owner = {
        1: storage.get_job_execution_owner(first["execution_id"]),
        2: storage.get_job_execution_owner(second["execution_id"]),
    }
    _assert_recovered_failed_scope(storage, "A", first, terminal=False)
    _assert_recovered_failed_scope(storage, "Z", safe, terminal=False)
    _close(storage)
    return {
        "operation_id": receipt["operation_id"],
        "staging": staging,
        "entries": entries,
        "first": first,
        "second": second,
        "safe": safe,
        "before_business": before_business,
        "before_owner": before_owner,
    }


@pytest.mark.parametrize("unexpected_private", [False, True])
def test_reopened_recovery_cleans_only_valid_committed_native_staging(
    tmp_path,
    monkeypatch,
    capsys,
    unexpected_private,
):
    committed = _leave_committed_native_recovery(tmp_path, monkeypatch)
    extra = committed["staging"] / "unexpected-private"
    if unexpected_private:
        extra.write_bytes(b"unrelated private data")
    before_staging = _snapshot(committed["staging"])

    capsys.readouterr()
    assert cli.main(["recover"]) == (1 if unexpected_private else 0)
    captured = capsys.readouterr()
    output = captured.out + captured.err
    storage = FileStorage(tmp_path)
    try:
        assert _scope_business_rows(storage, "A", TARGET_SESSION)[0] == committed["before_business"]
        assert _target_receipt(storage, committed["operation_id"])["state"] == "committed"
        assert storage.get_job_execution_owner(
            committed["first"]["execution_id"]
        ) == committed["before_owner"][1]
        assert storage.get_job_execution_owner(
            committed["second"]["execution_id"]
        ) == committed["before_owner"][2]
        if unexpected_private:
            assert committed["operation_id"] in output
            assert _snapshot(committed["staging"]) == before_staging
            assert extra.read_bytes() == b"unrelated private data"
        else:
            assert not committed["staging"].exists()
    finally:
        _close(storage)


@pytest.mark.parametrize(
    "damage",
    [
        "saved-output",
        "visible-output",
        "unexpected-private",
        "receipt-owner",
        "receipt-observation",
        "receipt-manifest",
        "forged-committed-state",
    ],
)
def test_prepared_native_recovery_refuses_changed_material_but_recovers_safe_session(
    tmp_path,
    monkeypatch,
    capsys,
    damage,
):
    prepared = _leave_prepared_native_recovery(tmp_path, monkeypatch, moves_before_loss=2)
    storage = FileStorage(tmp_path)
    _target_receipt(storage, prepared["operation_id"])
    visible, saved = prepared["entries"][0]
    if damage == "saved-output":
        saved.write_bytes(b"changed staged output")
    elif damage == "visible-output":
        visible.parent.mkdir(parents=True, exist_ok=True)
        visible.write_bytes(b"independent visible output")
    elif damage == "unexpected-private":
        (prepared["staging"] / "unexpected-private").write_bytes(b"unrelated")
    else:
        column, value = {
            "receipt-owner": ("owner_identity", "altered-recovery-owner"),
            "receipt-observation": ("observation_json", json.dumps({"altered": True})),
            "receipt-manifest": ("manifest_json", json.dumps({"altered": True})),
            "forged-committed-state": ("state", "committed"),
        }[damage]
        assert storage.db_connection().execute(
            f"UPDATE recovery_receipts SET {column}=? WHERE operation_id=?",
            (value, prepared["operation_id"]),
        ).rowcount == 1
        storage.db_connection().commit()
    storage.db_mutation_barrier()
    before_business = _scope_business_rows(storage, "A", TARGET_SESSION)[0]
    before_receipt = _target_receipt(storage, prepared["operation_id"])
    before_node = _snapshot(tmp_path / "node" / "A")
    before_staging = _snapshot(prepared["staging"])
    _close(storage)

    capsys.readouterr()
    assert cli.main(["recover"]) == 1
    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert prepared["operation_id"] in output or TARGET_SESSION in output
    reopened = FileStorage(tmp_path)
    try:
        assert _scope_business_rows(reopened, "A", TARGET_SESSION)[0] == before_business
        assert _target_receipt(reopened, prepared["operation_id"]) == before_receipt
        assert _snapshot(tmp_path / "node" / "A") == before_node
        assert _snapshot(prepared["staging"]) == before_staging
        assert reopened.get_execution_session(TARGET_SESSION)["status"] == "running"
        assert reopened.get_job_execution_owner(
            prepared["first"]["execution_id"]
        ) == prepared["before_owners"][1]
        assert reopened.get_job_execution_owner(
            prepared["second"]["execution_id"]
        ) == prepared["before_owners"][2]
        _assert_recovered_failed_scope(reopened, "Z", prepared["safe"], terminal=False)
    finally:
        _close(reopened)


def test_prepared_recovery_refuses_an_actively_locked_operation_and_continues_safe_session(
    tmp_path,
    monkeypatch,
    capsys,
):
    prepared = _leave_prepared_native_recovery(tmp_path, monkeypatch, moves_before_loss=2)
    ready, release = threading.Event(), threading.Event()
    holder_error = []

    def hold_operation():
        holder = FileStorage(tmp_path)
        try:
            with operation_lock(holder, "recovery-operations", prepared["operation_id"]):
                ready.set()
                assert release.wait(30)
        except BaseException as error:
            holder_error.append(error)
            ready.set()
        finally:
            _close(holder)

    thread = threading.Thread(target=hold_operation, daemon=True)
    thread.start()
    assert ready.wait(10)
    assert holder_error == []
    storage = FileStorage(tmp_path)
    before_business = _scope_business_rows(storage, "A", TARGET_SESSION)[0]
    before_receipt = _target_receipt(storage, prepared["operation_id"])
    before_node = _snapshot(tmp_path / "node" / "A")
    before_staging = _snapshot(prepared["staging"])
    _close(storage)
    try:
        capsys.readouterr()
        assert cli.main(["recover"]) == 1
        captured = capsys.readouterr()
        assert prepared["operation_id"] in captured.out + captured.err
        reopened = FileStorage(tmp_path)
        try:
            assert _scope_business_rows(reopened, "A", TARGET_SESSION)[0] == before_business
            assert _target_receipt(reopened, prepared["operation_id"]) == before_receipt
            assert _snapshot(tmp_path / "node" / "A") == before_node
            assert _snapshot(prepared["staging"]) == before_staging
            assert reopened.get_execution_session(TARGET_SESSION)["status"] == "running"
            _assert_recovered_failed_scope(reopened, "Z", prepared["safe"], terminal=False)
        finally:
            _close(reopened)
    finally:
        release.set()
        thread.join(15)
        assert not thread.is_alive()
        assert holder_error == []

    assert cli.main(["recover"]) == 0
    _assert_final_native_recovery(tmp_path, prepared)


def test_restore_refuses_visible_destination_created_after_initial_validation(
    tmp_path,
    monkeypatch,
    capsys,
):
    prepared = _leave_prepared_native_recovery(tmp_path, monkeypatch, moves_before_loss=2)
    injected = []
    replacement = b"independent visible replacement at restore boundary"

    def publish_destination(files, original, saved):
        if injected:
            return
        visible = recovery_path(files.root, original.relative_path)
        visible.parent.mkdir(parents=True, exist_ok=True)
        visible.write_bytes(replacement)
        injected.append((visible, files.directory / saved))

    with monkeypatch.context() as patch:
        patch.setattr(
            StagedRecoveryFiles,
            "_before_restore_entry",
            publish_destination,
            raising=False,
        )
        capsys.readouterr()
        assert cli.main(["recover"]) == 1
    assert len(injected) == 1
    visible, saved = injected[0]
    assert visible.read_bytes() == replacement
    assert saved.exists()
    storage = FileStorage(tmp_path)
    try:
        assert _target_receipt(storage, prepared["operation_id"])["state"] == "prepared"
        assert _scope_business_rows(storage, "A", TARGET_SESSION)[0] == prepared["before_business"]
        assert storage.get_execution_session(TARGET_SESSION)["status"] == "running"
        _assert_recovered_failed_scope(storage, "Z", prepared["safe"], terminal=False)
    finally:
        _close(storage)


def test_committed_cleanup_refuses_saved_path_replaced_after_initial_validation(
    tmp_path,
    monkeypatch,
    capsys,
):
    committed = _leave_committed_native_recovery(tmp_path, monkeypatch)
    injected = []
    replacement = b"independent saved-path replacement"

    def replace_saved(files, original, saved):
        if injected:
            return
        saved_path = files.directory / saved
        saved_path.write_bytes(replacement)
        injected.append(saved_path)

    with monkeypatch.context() as patch:
        patch.setattr(
            StagedRecoveryFiles,
            "_before_discard_entry",
            replace_saved,
            raising=False,
        )
        capsys.readouterr()
        assert cli.main(["recover"]) == 1
    assert len(injected) == 1
    saved = injected[0]
    assert saved.read_bytes() == replacement
    storage = FileStorage(tmp_path)
    try:
        assert _target_receipt(storage, committed["operation_id"])["state"] == "committed"
        assert _scope_business_rows(storage, "A", TARGET_SESSION)[0] == committed["before_business"]
        assert storage.get_job_execution_owner(
            committed["first"]["execution_id"]
        ) == committed["before_owner"][1]
        assert storage.get_job_execution_owner(
            committed["second"]["execution_id"]
        ) == committed["before_owner"][2]
    finally:
        _close(storage)
