from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.storage import preparation_staging
from micro_workflow_manager.storage.preparation_files import stage_preparation_files
from micro_workflow_manager.storage.preparation_staging import PreparationStaging
from micro_workflow_manager.storage.recovery_tree import directory_marker
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_105_preparation_receiver_guards import _established_workflow, _run_preparation
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows
from tests.test_148_native_cleanup_recovery import (
    _leave_prepared_component_cleanup,
    _preparation_receipt,
    _storage_database_rows,
    _tree_identity,
)


PREPARATION_METADATA = {
    "preparation_attempts",
    "preparation_receipts",
    "receiver_mutation_guards",
}


@pytest.fixture(autouse=True)
def _run_public_recovery_from_project(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


def _without_preparation_metadata(rows):
    return {table: values for table, values in rows.items() if table not in PREPARATION_METADATA}


def test_postcommit_guard_acquisition_failure_becomes_recoverable_without_manual_retirement(
    tmp_path,
    monkeypatch,
    capsys,
):
    workflow = _established_workflow(tmp_path)
    storage = workflow.storage
    original = OSError("notification failed after receiver guards committed")
    before_attempts = {row["operation_id"] for row in _rows(storage)["preparation_attempts"]}
    before_business = _without_preparation_metadata(_storage_database_rows(storage))
    before_node = _tree_identity(tmp_path / "node")
    observed = []
    notify = storage.notify_state_change

    def fail_after_acquisition_commit():
        attempts = storage.db_connection().execute(
            "SELECT operation_id FROM preparation_attempts WHERE state='preparing' ORDER BY operation_id"
        ).fetchall()
        created = [row["operation_id"] for row in attempts if row["operation_id"] not in before_attempts]
        if created and not observed:
            observed.append(created[0])
            raise original
        return notify()

    with monkeypatch.context() as patch:
        patch.setattr(storage, "notify_state_change", fail_after_acquisition_commit)
        with pytest.raises(OSError) as caught:
            _run_preparation(tmp_path, workflow, "reset")
    assert caught.value is original
    assert len(observed) == 1
    operation_id = observed[0]
    rows = _rows(storage)
    attempt, = [row for row in rows["preparation_attempts"] if row["operation_id"] == operation_id]
    guards = [row for row in rows["receiver_mutation_guards"] if row["operation_id"] == operation_id]
    assert attempt["state"] == "interrupted"
    assert attempt["session_id"] is None
    assert attempt["owner_pid"] == os.getpid()
    assert {guard["receiver_node"] for guard in guards} == set(json.loads(attempt["receivers_json"]))
    assert all(
        (
            guard["session_id"],
            guard["owner_pid"],
            guard["process_identity"],
            guard["hostname"],
        )
        == (
            attempt["session_id"],
            attempt["owner_pid"],
            attempt["process_identity"],
            attempt["hostname"],
        )
        for guard in guards
    )
    assert _without_preparation_metadata(_storage_database_rows(storage)) == before_business
    assert _tree_identity(tmp_path / "node") == before_node
    _close(storage)

    capsys.readouterr()
    assert cli.main(["recover"]) == 0
    output = capsys.readouterr()
    assert operation_id in output.out + output.err
    rows = _closed_database_rows(tmp_path)
    attempt, = [row for row in rows["preparation_attempts"] if row[0] == operation_id]
    assert attempt[1] == "aborted"
    assert not [row for row in rows["receiver_mutation_guards"] if row[1] == operation_id]
    assert _without_preparation_metadata(rows) == before_business
    assert _tree_identity(tmp_path / "node") == before_node


def test_partial_private_allocation_failure_removes_only_its_new_operation_tree(
    tmp_path,
    monkeypatch,
):
    (tmp_path / "node" / "A").mkdir(parents=True)
    (tmp_path / "node" / "B").mkdir(parents=True)
    trash = tmp_path / ".mwf" / "preparation-trash"
    preserved = trash / "independent-material"
    preserved.mkdir(parents=True)
    (preserved / "keep.txt").write_bytes(b"independent private material")
    before_node = _tree_identity(tmp_path / "node")
    original = PermissionError("second replacement allocation refused")
    mkdir = Path.mkdir
    observed = []

    def fail_second_replacement(path, *args, **kwargs):
        if path.name == "created_1" and path.parent.parent == trash and not observed:
            assert (path.parent / "created_0").is_dir()
            observed.append(path.parent)
            raise original
        return mkdir(path, *args, **kwargs)

    plans = tuple(
        SimpleNamespace(node=node, clear_output=True, delete_ids=(), reset_ids=())
        for node in ("A", "B")
    )
    with monkeypatch.context() as patch:
        patch.setattr(Path, "mkdir", fail_second_replacement)
        with pytest.raises(PermissionError) as caught:
            with stage_preparation_files(tmp_path, plans):
                pytest.fail("allocation fault did not stop preparation")
    assert caught.value is original
    assert len(observed) == 1
    assert not observed[0].exists()
    assert {path.name for path in trash.iterdir()} == {preserved.name}
    assert (preserved / "keep.txt").read_bytes() == b"independent private material"
    assert _tree_identity(tmp_path / "node") == before_node


def test_restore_refuses_empty_visible_directory_replaced_after_its_identity_check(
    tmp_path,
    monkeypatch,
    capsys,
):
    prepared = _leave_prepared_component_cleanup(tmp_path, monkeypatch)
    target = tmp_path / "node" / "A" / "output"
    assert target.is_dir() and not any(target.iterdir())
    before_rows = _closed_database_rows(tmp_path)
    before_staging = _tree_identity(prepared["staging"])
    before_node = _tree_identity(tmp_path / "node")
    before_node.pop("A/output")
    restoration_steps = PreparationStaging.restoration_steps
    capture_tree = preparation_staging.capture_recovery_tree
    preflights = []
    armed = []
    injected = []

    def arm_after_restore_preflight(files):
        steps = restoration_steps(files)
        if files.relative.endswith(prepared["operation_id"]):
            preflights.append(True)
            if len(preflights) == 2:
                armed.append(True)
        return steps

    def replace_after_identity_check(root, relative):
        observed = capture_tree(root, relative)
        if armed and relative == "node/A/output" and not injected:
            independent = tmp_path / "independent-empty-directory"
            independent.mkdir()
            marker = directory_marker(independent.lstat())
            target.rmdir()
            independent.rename(target)
            assert directory_marker(target.lstat()) == marker
            injected.append(marker)
        return observed

    with monkeypatch.context() as patch:
        patch.setattr(PreparationStaging, "restoration_steps", arm_after_restore_preflight)
        patch.setattr(preparation_staging, "capture_recovery_tree", replace_after_identity_check)
        capsys.readouterr()
        assert cli.main(["recover"]) == 1
    output = capsys.readouterr()
    assert prepared["operation_id"] in output.out + output.err
    assert len(preflights) == 2 and len(injected) == 1
    assert target.is_dir() and not any(target.iterdir())
    assert directory_marker(target.lstat()) == injected[0]
    assert _closed_database_rows(tmp_path) == before_rows
    assert _tree_identity(prepared["staging"]) == before_staging
    after_node = _tree_identity(tmp_path / "node")
    after_node.pop("A/output")
    assert after_node == before_node
    assert _preparation_receipt(tmp_path, prepared["operation_id"])[5] == "prepared"


def _leave_committed_component_cleanup(tmp_path, monkeypatch):
    workflow = _established_workflow(tmp_path)
    storage = workflow.storage
    known = {row["operation_id"] for row in _rows(storage)["preparation_receipts"]}
    cleanup_calls = []
    remove_recorded = PreparationStaging._remove_recorded_tree

    def interrupt_cleanup(files, relative, expected):
        if not cleanup_calls and (files.root / relative).exists():
            assert relative.startswith(files.relative + "/")
            cleanup_calls.append(relative)
            raise OSError("simulated committed cleanup interruption")
        return remove_recorded(files, relative, expected)

    with monkeypatch.context() as patch:
        patch.setattr(PreparationStaging, "_remove_recorded_tree", interrupt_cleanup)
        _run_preparation(tmp_path, workflow, "reset")
    assert len(cleanup_calls) == 1
    receipt, = [
        row for row in _rows(storage)["preparation_receipts"]
        if row["operation_id"] not in known
    ]
    assert receipt["state"] == "committed"
    staging = tmp_path / ".mwf" / "preparation-trash" / receipt["operation_id"]
    assert staging.is_dir() and any(staging.iterdir())
    _close(storage)
    return receipt, staging


@pytest.mark.parametrize("change", ["added", "replaced"])
def test_committed_cleanup_refuses_private_content_changed_at_recursive_delete_boundary(
    tmp_path,
    monkeypatch,
    capsys,
    change,
):
    receipt, staging = _leave_committed_component_cleanup(tmp_path, monkeypatch)
    before_rows = _closed_database_rows(tmp_path)
    before_node = _tree_identity(tmp_path / "node")
    saved = next(path for path in staging.rglob("*") if path.is_file())
    rglob = Path.rglob
    injected = []
    content = b"independent private content at recursive cleanup boundary"

    def change_private_tree(path, pattern):
        if path == staging and not injected:
            if change == "added":
                affected = staging / "independent-private.txt"
            else:
                affected = saved
                affected.chmod(stat.S_IMODE(affected.stat().st_mode) | stat.S_IWRITE)
                affected.unlink()
            affected.write_bytes(content)
            injected.append(affected)
        return rglob(path, pattern)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "rglob", change_private_tree)
        capsys.readouterr()
        assert cli.main(["recover"]) == 1
    output = capsys.readouterr()
    assert receipt["operation_id"] in output.out + output.err
    assert len(injected) == 1
    assert injected[0].read_bytes() == content
    assert _closed_database_rows(tmp_path) == before_rows
    assert _tree_identity(tmp_path / "node") == before_node
    assert _preparation_receipt(tmp_path, receipt["operation_id"])[5] == "committed"
