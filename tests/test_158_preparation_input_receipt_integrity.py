from __future__ import annotations

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.storage import FileStorage
from tests.test_090_component_session_settlement import _close
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows
from tests.test_148_native_cleanup_recovery import (
    _leave_prepared_component_cleanup,
    _leave_prepared_input_publication,
    _tree_identity,
)


@pytest.fixture(autouse=True)
def _run_public_recovery_from_project(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


def _prepared_operation(tmp_path, monkeypatch, operation_kind):
    if operation_kind == "preparation":
        prepared = _leave_prepared_component_cleanup(tmp_path, monkeypatch)
        return {
            "operation_id": prepared["operation_id"],
            "staging": prepared["staging"],
            "table": "preparation_receipts",
            "state_position": 5,
        }
    prepared = _leave_prepared_input_publication(tmp_path, monkeypatch)
    return {
        "operation_id": prepared["operation_id"],
        "staging": prepared["staging"],
        "table": "input_publications",
        "state_position": 3,
    }


def _receipt(rows, table, operation_id):
    matches = [row for row in rows[table] if row[0] == operation_id]
    assert len(matches) == 1
    return matches[0]


def _assert_only_receipt_state_changed(before, after, operation):
    table = operation["table"]
    operation_id = operation["operation_id"]
    state_position = operation["state_position"]
    assert {name: rows for name, rows in after.items() if name != table} == {
        name: rows for name, rows in before.items() if name != table
    }
    before_receipt = _receipt(before, table, operation_id)
    after_receipt = _receipt(after, table, operation_id)
    assert before_receipt[state_position] == "prepared"
    assert before_receipt[:state_position] + before_receipt[state_position + 1:] == (
        after_receipt[:state_position] + after_receipt[state_position + 1:]
    )
    assert [row for row in before[table] if row[0] != operation_id] == [
        row for row in after[table] if row[0] != operation_id
    ]


@pytest.mark.parametrize("operation_kind", ["preparation", "input"])
@pytest.mark.parametrize("forged_state", ["committed", "aborted"])
def test_forged_terminal_receipt_state_does_not_authorize_prepared_file_discard(
    tmp_path,
    monkeypatch,
    capsys,
    operation_kind,
    forged_state,
):
    operation = _prepared_operation(tmp_path, monkeypatch, operation_kind)
    operation_id = operation["operation_id"]
    staging = operation["staging"]
    assert staging.is_dir() and any(staging.iterdir())
    prepared_rows = _closed_database_rows(tmp_path)
    prepared_node = _tree_identity(tmp_path / "node")
    prepared_staging = _tree_identity(staging)
    receipt = _receipt(prepared_rows, operation["table"], operation_id)
    assert receipt[operation["state_position"]] == "prepared"

    storage = FileStorage(tmp_path)

    def forge_state(connection):
        changed = connection.execute(
            f"UPDATE {operation['table']} SET state=? WHERE operation_id=? AND state='prepared'",
            (forged_state, operation_id),
        ).rowcount
        if changed != 1:
            raise RuntimeError("Prepared receipt changed before the deliberate state-field fault")

    storage.submit_db_mutation(forge_state)
    storage.db_mutation_barrier()
    _close(storage)

    altered_rows = _closed_database_rows(tmp_path)
    _assert_only_receipt_state_changed(prepared_rows, altered_rows, operation)
    assert _receipt(altered_rows, operation["table"], operation_id)[operation["state_position"]] == forged_state
    assert _tree_identity(tmp_path / "node") == prepared_node
    assert _tree_identity(staging) == prepared_staging

    capsys.readouterr()
    assert cli.main(["recover"]) == 1
    output = capsys.readouterr()
    assert operation_id in output.out + output.err
    assert _closed_database_rows(tmp_path) == altered_rows
    assert _tree_identity(tmp_path / "node") == prepared_node
    assert _tree_identity(staging) == prepared_staging
    assert _receipt(
        _closed_database_rows(tmp_path), operation["table"], operation_id
    )[operation["state_position"]] == forged_state
