"""Crash-boundary and terminal-decision checks for the native clipboard."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.storage import clipboard_files
from micro_workflow_manager.storage.clipboard_files import (
    ClipboardFiles,
    PreparedClipboardAllocation,
    prepare_clipboard_allocation,
)
from micro_workflow_manager.storage.clipboard_snapshot import observe_clipboard_node
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_117_execution_sampling import _files
from tests.test_183_native_clipboard_scope import _component_project, _node_rows, _settle_writer


class SimulatedClipboardProcessLoss(BaseException):
    pass


def _prepared_files(tmp_path, *, operation_id="1" * 32, extra_previous=False):
    root = tmp_path / "project"
    source = root / "node" / "A"
    destination = root / "clipboard" / "A"
    source.mkdir(parents=True)
    destination.mkdir(parents=True)
    (source / "incoming.txt").write_bytes(b"incoming")
    (destination / "previous.txt").write_bytes(b"previous")
    if extra_previous:
        (destination / "second.txt").write_bytes(b"second previous")
    temporary = tmp_path / "private"
    temporary.mkdir()
    allocation = prepare_clipboard_allocation(
        root, temporary, operation_id,
        source_relative="node/A", destination_relative="clipboard/A",
        include_snapshot=False,
    )
    return allocation.install(), root


def test_clipboard_restore_retries_a_crash_after_destination_detachment(
    tmp_path, monkeypatch,
):
    files, root = _prepared_files(tmp_path)
    actual_move = clipboard_files.move_without_replacement
    loss = SimulatedClipboardProcessLoss("lost after clipboard destination detachment")
    calls = 0

    def detach_then_lose(source, destination):
        nonlocal calls
        calls += 1
        if calls == 1:
            actual_move(source, destination)
            raise loss
        if calls == 2:
            raise loss
        return actual_move(source, destination)

    with monkeypatch.context() as faults:
        faults.setattr(clipboard_files, "move_without_replacement", detach_then_lose)
        with pytest.raises(SimulatedClipboardProcessLoss) as raised:
            files.publish()
        assert raised.value is loss
    assert files.restoration_steps() == "detached"

    files.restore()
    files.require_restored()
    assert (root / "clipboard" / "A" / "previous.txt").read_bytes() == b"previous"
    assert (files.incoming / "incoming.txt").read_bytes() == b"incoming"


def test_clipboard_restore_retries_a_crash_during_inverse_publication(
    tmp_path, monkeypatch,
):
    files, root = _prepared_files(tmp_path)
    files.publish()
    actual_move = clipboard_files.move_without_replacement
    loss = SimulatedClipboardProcessLoss("lost while making replacement private")
    reached = False

    def move_then_lose(source, destination):
        nonlocal reached
        actual_move(source, destination)
        reached = True
        raise loss

    with monkeypatch.context() as faults:
        faults.setattr(clipboard_files, "move_without_replacement", move_then_lose)
        with pytest.raises(SimulatedClipboardProcessLoss) as raised:
            files.restore()
        assert raised.value is loss
    assert reached
    assert files.restoration_steps() == "detached"

    files.restore()
    files.require_restored()
    assert (root / "clipboard" / "A" / "previous.txt").read_bytes() == b"previous"


def test_committed_clipboard_cleanup_retries_after_one_known_file_was_removed(
    tmp_path, monkeypatch,
):
    files, root = _prepared_files(
        tmp_path, operation_id="2" * 32, extra_previous=True,
    )
    files.publish()
    target = files.previous / "previous.txt"
    actual_unlink = Path.unlink
    loss = SimulatedClipboardProcessLoss("lost during committed clipboard cleanup")
    reached = False

    def unlink_then_lose(path, *args, **kwargs):
        nonlocal reached
        result = actual_unlink(path, *args, **kwargs)
        if path == target:
            reached = True
            raise loss
        return result

    with monkeypatch.context() as faults:
        faults.setattr(Path, "unlink", unlink_then_lose)
        with pytest.raises(SimulatedClipboardProcessLoss) as raised:
            files.discard(committed=True)
        assert raised.value is loss
    assert reached and not target.exists()

    files.discard(committed=True)
    assert not files.directory.exists()
    assert (root / "clipboard" / "A" / "incoming.txt").read_bytes() == b"incoming"


def test_committed_receipt_tamper_after_publish_retains_recovery_material(
    tmp_path, monkeypatch, capsys,
):
    storage = _component_project(tmp_path, monkeypatch)
    try:
        capsys.readouterr()
        assert cli.main(["run", "A", "--runner", "direct"]) == 0
        capsys.readouterr()
        assert cli.main(["copy", "A"]) == 0
        capsys.readouterr()
        (tmp_path / "node" / "A" / "output" / "changed.txt").write_bytes(b"changed")
        original_commit = storage._commit_clipboard
        loss = SimulatedClipboardProcessLoss("lost after the clipboard writer committed")
        operation_id = None

        def commit_then_tamper(expected, guards, receipt, files, observation, saved):
            nonlocal operation_id
            result = original_commit(expected, guards, receipt, files, observation, saved)
            operation_id = expected["operation_id"]
            connection = storage._new_db_connection()
            try:
                connection.execute(
                    "UPDATE clipboard_receipts SET decision_json='{}' WHERE operation_id=?",
                    (operation_id,),
                )
                connection.commit()
            finally:
                connection.close()
            raise loss

        with monkeypatch.context() as faults:
            faults.setattr(storage, "_commit_clipboard", commit_then_tamper)
            with pytest.raises(SimulatedClipboardProcessLoss) as raised:
                storage.restore_node_clipboard("A", tmp_path / "clipboard" / "A")
            assert raised.value is loss
        assert operation_id is not None
        _settle_writer(storage)
        staging = tmp_path / ".mwf" / "clipboard-operations" / operation_id
        assert staging.is_dir()
        before_rows = _rows(storage)
        before_files = _files(tmp_path)

        assert cli.main(["recover", "--dry-run"]) == 1
        captured = capsys.readouterr()
        assert operation_id in captured.out + captured.err
        assert "clipboard recovery observation failed" in captured.out
        assert _rows(storage) == before_rows
        assert _files(tmp_path) == before_files
    finally:
        _close(storage)


def test_copy_accepts_a_committed_unit_from_an_aborted_preparation_attempt(
    tmp_path, monkeypatch, capsys,
):
    storage = _component_project(tmp_path, monkeypatch)
    try:
        capsys.readouterr()
        assert cli.main(["run", "A", "--runner", "direct"]) == 0
        capsys.readouterr()
        component_key = encode_component_key(("A", "B"))
        state = storage.db_connection().execute(
            "SELECT component_key, alignment_generation FROM component_states "
            "WHERE component_key=?", (component_key,),
        ).fetchone()
        receipt = storage.db_connection().execute(
            "SELECT guard_id FROM preparation_receipts WHERE component_key=? "
            "AND state='committed' ORDER BY rowid DESC LIMIT 1",
            (state["component_key"],),
        ).fetchone()
        assert receipt is not None

        def retain_committed_unit_in_aborted_attempt(connection):
            attempt = connection.execute(
                "SELECT expected_receipts_json FROM preparation_attempts WHERE operation_id=?",
                (receipt["guard_id"],),
            ).fetchone()
            expected = json.loads(attempt["expected_receipts_json"])
            expected.append({
                "operation": "reset",
                "component_key": encode_component_key(("C",)),
            })
            changed = connection.execute(
                "UPDATE preparation_attempts SET state='aborted', "
                "expected_receipts_json=?, completed_membership_revision=NULL "
                "WHERE operation_id=? AND state='committed'",
                (json.dumps(expected, sort_keys=True, separators=(",", ":")),
                 receipt["guard_id"]),
            ).rowcount
            if changed != 1:
                raise RuntimeError("fixture did not find a committed preparation attempt")

        storage.submit_db_mutation(retain_committed_unit_in_aborted_attempt)
        _settle_writer(storage)
        assert cli.main(["copy", "A"]) == 0
        assert "Saved clipboard node" in capsys.readouterr().out
    finally:
        _close(storage)


@pytest.mark.parametrize(
    "later_command",
    (["run", "A", "--runner", "direct"], ["reset", "A"]),
    ids=("later-run", "later-reset"),
)
def test_fully_cleaned_clipboard_history_does_not_bind_later_component_state(
    tmp_path, monkeypatch, capsys, later_command,
):
    storage = _component_project(tmp_path, monkeypatch)
    try:
        capsys.readouterr()
        assert cli.main(["run", "A", "--runner", "direct"]) == 0
        capsys.readouterr()
        assert cli.main(["copy", "A"]) == 0
        capsys.readouterr()
        terminal = storage.db_connection().execute(
            "SELECT attempt.operation_id, receipt.decision_json "
            "FROM clipboard_attempts AS attempt JOIN clipboard_receipts AS receipt "
            "ON receipt.operation_id=attempt.operation_id "
            "WHERE attempt.operation='copy' AND attempt.state='committed' "
            "AND receipt.state='committed' ORDER BY attempt.started_at DESC LIMIT 1"
        ).fetchone()
        assert terminal is not None
        assert not (
            tmp_path / ".mwf" / "clipboard-operations" / terminal["operation_id"]
        ).exists()

        if later_command[0] == "reset":
            monkeypatch.setattr("builtins.input", lambda _prompt: "reset")
        assert cli.main(later_command) == 0
        capsys.readouterr()
        old_digest = json.loads(terminal["decision_json"])["details"]["observation_digest"]
        assert observe_clipboard_node(
            storage.db_connection(), "A",
        ).digest != old_digest

        assert cli.main(["recover", "--dry-run"]) == 0
        preview = capsys.readouterr()
        assert "clipboard recovery observation failed" not in preview.out + preview.err
        assert cli.main(["recover"]) == 0
        recovered = capsys.readouterr()
        assert "Recovery refused" not in recovered.out + recovered.err
        assert cli.main(["doctor"]) == 0
        doctor = capsys.readouterr()
        assert "invalid native clipboard" not in doctor.out + doctor.err
        assert cli.main(["copy", "A"]) == 0
        assert "Saved clipboard node" in capsys.readouterr().out
    finally:
        _close(storage)


def test_terminal_private_cleanup_ignores_later_valid_state_and_visible_files(
    tmp_path, monkeypatch, capsys,
):
    storage = _component_project(tmp_path, monkeypatch)
    workflow = None
    try:
        capsys.readouterr()
        assert cli.main(["run", "A", "--runner", "direct"]) == 0
        capsys.readouterr()
        prior = tmp_path / "clipboard" / "A" / "prior.txt"
        prior.parent.mkdir(parents=True)
        prior.write_bytes(b"prior clipboard material")
        actual_discard = ClipboardFiles.discard
        loss = SimulatedClipboardProcessLoss("lost before terminal clipboard cleanup")

        def retain_terminal_material(files, *, committed):
            if committed:
                raise loss
            return actual_discard(files, committed=committed)

        with monkeypatch.context() as faults:
            faults.setattr(ClipboardFiles, "discard", retain_terminal_material)
            with pytest.raises(SimulatedClipboardProcessLoss) as raised:
                cli.main(["copy", "A"])
            assert raised.value is loss
        terminal = storage.db_connection().execute(
            "SELECT attempt.operation_id, receipt.decision_json "
            "FROM clipboard_attempts AS attempt JOIN clipboard_receipts AS receipt "
            "ON receipt.operation_id=attempt.operation_id "
            "WHERE attempt.operation='copy' AND attempt.state='committed' "
            "ORDER BY attempt.started_at DESC LIMIT 1"
        ).fetchone()
        staging = tmp_path / ".mwf" / "clipboard-operations" / terminal["operation_id"]
        assert staging.is_dir() and (staging / "previous" / "prior.txt").is_file()
        old_digest = json.loads(terminal["decision_json"])["details"]["observation_digest"]

        workflow = load_workflow(tmp_path, "direct")
        workflow.run_node("A")
        later = tmp_path / "clipboard" / "A" / "later.txt"
        later.write_bytes(b"later visible clipboard material")
        assert observe_clipboard_node(workflow.storage.db_connection(), "A").digest != old_digest

        assert cli.main(["recover", "--dry-run"]) == 0
        preview = capsys.readouterr()
        assert terminal["operation_id"] in preview.out
        assert cli.main(["recover"]) == 0
        capsys.readouterr()
        assert not staging.exists()
        assert later.read_bytes() == b"later visible clipboard material"
    finally:
        if workflow is not None:
            _close(workflow.storage)
        _close(storage)


def test_prepared_attempt_update_requires_exact_writer_readback(
    tmp_path, monkeypatch, capsys,
):
    storage = _component_project(tmp_path, monkeypatch)
    try:
        capsys.readouterr()
        assert cli.main(["run", "A", "--runner", "direct"]) == 0
        capsys.readouterr()
        before = observe_clipboard_node(storage.db_connection(), "A").digest
        storage.submit_db_mutation(lambda connection: connection.execute(
            "CREATE TRIGGER revert_clipboard_prepare AFTER UPDATE OF state "
            "ON clipboard_attempts WHEN NEW.state='prepared' BEGIN "
            "UPDATE clipboard_attempts SET state='allocating' "
            "WHERE operation_id=NEW.operation_id; END"
        ))

        with pytest.raises(
            RuntimeError, match="Clipboard attempt or receiver guards changed",
        ):
            storage.capture_node_clipboard("A", tmp_path / "clipboard" / "A")
        row = storage.db_connection().execute(
            "SELECT attempt.*, receipt.state AS receipt_state, receipt.decision_json "
            "FROM clipboard_attempts AS attempt JOIN clipboard_receipts AS receipt "
            "ON receipt.operation_id=attempt.operation_id ORDER BY attempt.started_at DESC LIMIT 1"
        ).fetchone()
        assert row is not None and row["state"] == row["receipt_state"] == "aborted"
        assert json.loads(row["decision_json"])["details"] == {
            "abort_from": "allocating", "observation_digest": before, "restored": True,
        }
        assert observe_clipboard_node(storage.db_connection(), "A").digest == before
        assert not (tmp_path / ".mwf" / "clipboard-operations" / row["operation_id"]).exists()
    finally:
        _close(storage)


def test_abort_requires_exact_terminal_attempt_and_receipt_readback(
    tmp_path, monkeypatch, capsys,
):
    storage = _component_project(tmp_path, monkeypatch)
    try:
        capsys.readouterr()
        assert cli.main(["run", "A", "--runner", "direct"]) == 0
        capsys.readouterr()
        before = observe_clipboard_node(storage.db_connection(), "A").digest
        storage.submit_db_mutation(lambda connection: connection.execute(
            "CREATE TRIGGER revert_clipboard_abort AFTER UPDATE OF state "
            "ON clipboard_attempts WHEN NEW.state='aborted' BEGIN "
            "UPDATE clipboard_attempts SET state='prepared', finished_at=NULL "
            "WHERE operation_id=NEW.operation_id; END"
        ))
        original = RuntimeError("injected clipboard publication failure")

        def fail_publication(files):
            raise original

        with monkeypatch.context() as faults:
            faults.setattr(ClipboardFiles, "publish", fail_publication)
            with pytest.raises(RuntimeError) as raised:
                storage.capture_node_clipboard("A", tmp_path / "clipboard" / "A")
        assert raised.value is original
        row = storage.db_connection().execute(
            "SELECT attempt.operation_id, attempt.state, receipt.state AS receipt_state "
            "FROM clipboard_attempts AS attempt JOIN clipboard_receipts AS receipt "
            "ON receipt.operation_id=attempt.operation_id ORDER BY attempt.started_at DESC LIMIT 1"
        ).fetchone()
        assert row is not None and row["state"] == row["receipt_state"] == "prepared"
        assert storage.db_connection().execute(
            "SELECT COUNT(*) FROM receiver_mutation_guards WHERE operation_id=?",
            (row["operation_id"],),
        ).fetchone()[0] == 2
        staging = tmp_path / ".mwf" / "clipboard-operations" / row["operation_id"]
        assert staging.is_dir()
        assert observe_clipboard_node(storage.db_connection(), "A").digest == before

        storage.submit_db_mutation(
            lambda connection: connection.execute("DROP TRIGGER revert_clipboard_abort")
        )
        assert cli.main(["recover"]) == 0
        capsys.readouterr()
        terminal = storage.db_connection().execute(
            "SELECT attempt.state, receipt.state AS receipt_state "
            "FROM clipboard_attempts AS attempt JOIN clipboard_receipts AS receipt "
            "ON receipt.operation_id=attempt.operation_id WHERE attempt.operation_id=?",
            (row["operation_id"],),
        ).fetchone()
        assert terminal["state"] == terminal["receipt_state"] == "aborted"
        assert not staging.exists()
    finally:
        _close(storage)


def test_failed_paste_records_and_cleans_an_exact_aborted_decision(
    tmp_path, monkeypatch, capsys,
):
    storage = _component_project(tmp_path, monkeypatch)
    try:
        capsys.readouterr()
        assert cli.main(["run", "A", "--runner", "direct"]) == 0
        capsys.readouterr()
        assert cli.main(["copy", "A"]) == 0
        capsys.readouterr()
        changed = tmp_path / "node" / "A" / "output" / "after-copy.txt"
        changed.write_bytes(b"preserve current node")
        before_rows = _node_rows(storage.db_connection(), "A")
        before_files = _files(tmp_path / "node" / "A")
        failure = RuntimeError("injected paste publication failure")

        def fail_publication(_files):
            raise failure

        with monkeypatch.context() as faults:
            faults.setattr(ClipboardFiles, "publish", fail_publication)
            with pytest.raises(RuntimeError) as raised:
                storage.restore_node_clipboard("A", tmp_path / "clipboard" / "A")
            assert raised.value is failure
        assert not getattr(failure, "__notes__", ())
        terminal = storage.db_connection().execute(
            "SELECT attempt.operation_id, attempt.state, receipt.state AS receipt_state, "
            "receipt.decision_json FROM clipboard_attempts AS attempt "
            "JOIN clipboard_receipts AS receipt ON receipt.operation_id=attempt.operation_id "
            "WHERE attempt.operation='paste' ORDER BY attempt.started_at DESC LIMIT 1"
        ).fetchone()
        assert terminal["state"] == terminal["receipt_state"] == "aborted"
        details = json.loads(terminal["decision_json"])["details"]
        assert details["abort_from"] == "prepared" and details["restored"] is True
        assert not (
            tmp_path / ".mwf" / "clipboard-operations" / terminal["operation_id"]
        ).exists()
        assert _node_rows(storage.db_connection(), "A") == before_rows
        assert _files(tmp_path / "node" / "A") == before_files
    finally:
        _close(storage)


def test_allocation_failure_records_an_exact_unrestored_abort(
    tmp_path, monkeypatch, capsys,
):
    storage = _component_project(tmp_path, monkeypatch)
    try:
        capsys.readouterr()
        assert cli.main(["run", "A", "--runner", "direct"]) == 0
        capsys.readouterr()
        failure = RuntimeError("injected clipboard allocation installation failure")

        def fail_before_install(allocation):
            raise failure

        with monkeypatch.context() as faults:
            faults.setattr(PreparedClipboardAllocation, "install", fail_before_install)
            assert cli.main(["copy", "A"]) == 1
        assert str(failure) in capsys.readouterr().err
        row = storage.db_connection().execute(
            "SELECT attempt.operation_id, attempt.state, receipt.state AS receipt_state, "
            "receipt.decision_json FROM clipboard_attempts AS attempt "
            "JOIN clipboard_receipts AS receipt ON receipt.operation_id=attempt.operation_id "
            "ORDER BY attempt.started_at DESC LIMIT 1"
        ).fetchone()
        assert row is not None and row["state"] == row["receipt_state"] == "aborted"
        details = json.loads(row["decision_json"])["details"]
        assert details["abort_from"] == "allocating" and details["restored"] is False
        assert not (tmp_path / ".mwf" / "clipboard-operations" / row["operation_id"]).exists()
        assert cli.main(["recover", "--dry-run"]) == 0
        preview = capsys.readouterr()
        assert "clipboard recovery observation failed" not in preview.out + preview.err
        assert cli.main(["copy", "A"]) == 0
    finally:
        _close(storage)
