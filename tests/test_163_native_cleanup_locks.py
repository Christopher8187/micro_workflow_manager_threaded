from __future__ import annotations

import json
import stat
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.storage import FileStorage, preparation_attempts
from micro_workflow_manager.storage.operation_lock import operation_lock
from micro_workflow_manager.storage.preparation_attempts import hold_preparation_attempt
from tests.test_090_component_session_settlement import _close
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows
from tests.test_148_native_cleanup_recovery import (
    _assert_preparation_restored,
    _input_receipt,
    _leave_prepared_component_cleanup,
    _leave_prepared_input_publication,
    _storage_database_rows,
    _tree_identity,
)


@pytest.fixture(autouse=True)
def _run_public_recovery_from_project(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


def _rows_without_advisory(root):
    storage = FileStorage(root)
    try:
        rows = _storage_database_rows(storage)
    finally:
        _close(storage)
    rows.pop("advisory_locks", None)
    return rows


def _read_row(root, table, column, value):
    storage = FileStorage(root)
    try:
        row = storage.db_connection().execute(
            f"SELECT * FROM {table} WHERE {column}=?",
            (value,),
        ).fetchone()
        assert row is not None
        return dict(row)
    finally:
        _close(storage)


def _preparation_target(tmp_path, monkeypatch):
    prepared = _leave_prepared_component_cleanup(tmp_path, monkeypatch)
    receipt = _read_row(tmp_path, "preparation_receipts", "operation_id", prepared["operation_id"])
    receivers = tuple(json.loads(receipt["manifest_json"])["receivers"])
    assert receivers
    storage = FileStorage(tmp_path)
    try:
        with operation_lock(storage, "preparation-operations", prepared["guard_id"]):
            pass
    finally:
        _close(storage)
    return {
        "kind": "preparation",
        "prepared": prepared,
        "operation_id": prepared["operation_id"],
        "receiver": receivers[0],
    }


def _input_target(tmp_path, monkeypatch):
    prepared = _leave_prepared_input_publication(tmp_path, monkeypatch)
    receipt = _read_row(tmp_path, "input_publications", "operation_id", prepared["operation_id"])
    storage = FileStorage(tmp_path)
    try:
        owner = storage.get_job_execution_owner(receipt["execution_id"])
        assert owner is not None
        with operation_lock(storage, "input-publication-operations", prepared["operation_id"]):
            pass
    finally:
        _close(storage)
    return {
        "kind": "input",
        "prepared": prepared,
        "operation_id": prepared["operation_id"],
        "receiver": receipt["receiver_node"],
        "owner": owner,
    }


def _lock_context(storage, held, target):
    if held == "producer-fence":
        owner = target["owner"]
        return storage.filesystem_interprocess_lock(
            "execution-fences",
            storage.job_execution_lock_name(owner["node_name"], owner["job_id"]),
        )
    if held == "active-run-state":
        return storage.interprocess_lock("active-run-state")
    suffix = "input" if held == "receiver-input" else "jobs"
    return storage.interprocess_lock(f"node-{target['receiver']}-{suffix}")


def _start_lock_holder(root, held, target):
    ready, release = threading.Event(), threading.Event()
    errors = []

    def hold():
        storage = FileStorage(root)
        try:
            with _lock_context(storage, held, target):
                ready.set()
                if not release.wait(15):
                    raise TimeoutError("cleanup lock holder was not released")
        except BaseException as error:
            errors.append(error)
            ready.set()
        finally:
            _close(storage)

    thread = threading.Thread(target=hold, name="native-cleanup-lock-holder", daemon=True)
    thread.start()
    assert ready.wait(10), "cleanup lock holder did not report acquisition"
    assert errors == []
    return release, thread, errors


def _recover_without_waiting_for_holder(release, holder, holder_errors):
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="bounded-native-cleanup")
    future = pool.submit(cli.main, ["recover"])
    completed_while_held = True
    outcome = None
    try:
        try:
            outcome = future.result(timeout=2)
        except FutureTimeout:
            completed_while_held = False
            outcome = None
    finally:
        release.set()
        holder.join(10)
        if outcome is None:
            outcome = future.result(timeout=10)
        pool.shutdown(wait=True)
        assert not holder.is_alive()
        assert holder_errors == []
    assert completed_while_held, "native cleanup blocked on an active recovery resource"
    return outcome


def _assert_input_restored(tmp_path, prepared, expected_business=None):
    target = tmp_path / "node" / "B" / "input" / "A" / "keep.txt"
    assert target.read_bytes() == b"original"
    assert stat.S_IMODE(target.stat().st_mode) == prepared["original_mode"]
    assert not (target.parent / "later.txt").exists()
    assert _input_receipt(tmp_path, prepared["operation_id"])[3] == "aborted"
    assert not prepared["staging"].exists()
    rows = _closed_database_rows(tmp_path)
    assert rows["job_execution_owners"] == prepared["owner_rows"]
    rows.pop("input_publications")
    assert rows == (prepared["business"] if expected_business is None else expected_business)


@pytest.mark.parametrize(
    "held",
    ["producer-fence", "active-run-state", "receiver-input", "receiver-jobs"],
)
def test_public_cleanup_boundedly_refuses_an_active_required_lock_without_mutation(
    tmp_path,
    monkeypatch,
    capsys,
    held,
):
    target = (
        _input_target(tmp_path, monkeypatch)
        if held == "producer-fence"
        else _preparation_target(tmp_path, monkeypatch)
    )
    before_rows = _rows_without_advisory(tmp_path)
    before_node = _tree_identity(tmp_path / "node")
    before_staging = _tree_identity(target["prepared"]["staging"])
    release, holder, holder_errors = _start_lock_holder(tmp_path, held, target)

    capsys.readouterr()
    assert _recover_without_waiting_for_holder(release, holder, holder_errors) == 1
    output = capsys.readouterr()
    assert target["operation_id"] in output.out + output.err
    assert _rows_without_advisory(tmp_path) == before_rows
    assert _tree_identity(tmp_path / "node") == before_node
    assert _tree_identity(target["prepared"]["staging"]) == before_staging

    assert cli.main(["recover"]) == 0
    if target["kind"] == "preparation":
        _assert_preparation_restored(tmp_path, target["prepared"])
    else:
        _assert_input_restored(tmp_path, target["prepared"])


def _leave_interrupted_attempt(storage, monkeypatch):
    original = OSError("simulated disjoint preparation attempt loss")
    release_error = OSError("simulated attempt release loss")
    operation_ids = []
    component_key = encode_component_key(("A",))

    def validate(connection):
        if connection.execute(
            "SELECT 1 FROM component_states WHERE component_key=?",
            (component_key,),
        ).fetchone() is None:
            raise RuntimeError("native A component is missing")

    def fail_release(*args, **kwargs):
        raise release_error

    with monkeypatch.context() as patch:
        patch.setattr(preparation_attempts, "_finish", fail_release)
        with pytest.raises(OSError) as caught:
            with hold_preparation_attempt(
                storage,
                None,
                ("A",),
                ("A",),
                ({"operation": "reset", "component_key": component_key},),
                validate,
            ) as operation_id:
                operation_ids.append(operation_id)
                raise original
    assert caught.value is original
    assert any(str(release_error) in note for note in original.__notes__)
    assert len(operation_ids) == 1
    return operation_ids[0]


def test_cleanup_continues_a_compatible_attempt_while_an_input_producer_fence_is_held(
    tmp_path,
    monkeypatch,
    capsys,
):
    target = _input_target(tmp_path, monkeypatch)
    storage = FileStorage(tmp_path)
    try:
        safe_operation = _leave_interrupted_attempt(storage, monkeypatch)
        with operation_lock(storage, "preparation-operations", safe_operation):
            pass
    finally:
        _close(storage)
    before_rows = _rows_without_advisory(tmp_path)
    before_attempt = _read_row(tmp_path, "preparation_attempts", "operation_id", safe_operation)
    before_receipt = _input_receipt(tmp_path, target["operation_id"])
    before_node = _tree_identity(tmp_path / "node")
    before_staging = _tree_identity(target["prepared"]["staging"])
    release, holder, holder_errors = _start_lock_holder(tmp_path, "producer-fence", target)

    capsys.readouterr()
    assert _recover_without_waiting_for_holder(release, holder, holder_errors) == 1
    output = capsys.readouterr()
    assert target["operation_id"] in output.out + output.err
    after_rows = _rows_without_advisory(tmp_path)
    for table in before_rows:
        if table not in {"preparation_attempts", "receiver_mutation_guards"}:
            assert after_rows[table] == before_rows[table]
    after_attempt = _read_row(tmp_path, "preparation_attempts", "operation_id", safe_operation)
    assert after_attempt == dict(before_attempt, state="aborted", finished_at=after_attempt["finished_at"])
    assert after_attempt["finished_at"] is not None
    assert not [row for row in after_rows["receiver_mutation_guards"] if safe_operation in row]
    assert _input_receipt(tmp_path, target["operation_id"]) == before_receipt
    assert _tree_identity(tmp_path / "node") == before_node
    assert _tree_identity(target["prepared"]["staging"]) == before_staging

    expected_business = _closed_database_rows(tmp_path)
    expected_business.pop("input_publications")
    assert cli.main(["recover"]) == 0
    _assert_input_restored(tmp_path, target["prepared"], expected_business)
