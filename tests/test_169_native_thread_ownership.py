"""The threads command resolves one exact native session owner or pending value."""

from __future__ import annotations

import os
import json
import math
import socket
import threading

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.models import Job, now
from micro_workflow_manager.processes import process_identity
from micro_workflow_manager.storage import FileStorage
from tests.test_064_read_only_previews import _close_without_sidecars, _snapshot, _wait_writer
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows
from tests.test_146_native_applied_recovery import _create_active_scope, _scope_rows
from tests.test_149_interrupt_declarations import _make_interrupt_project


_CORE_TABLES = (
    "graph_shapes",
    "component_definitions",
    "active_component_partition",
    "active_component_members",
    "component_states",
    "component_successful_results",
    "execution_sessions",
    "session_components",
    "session_jobs",
    "component_reservations",
    "component_holds",
    "pending_component_executions",
    "jobs",
    "job_instances",
    "job_execution_owners",
    "job_events",
)


def _core_rows(storage):
    connection = storage.db_connection()
    present = {
        row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    return {
        table: sorted(
            (tuple(row) for row in connection.execute(f'SELECT * FROM "{table}"')),
            key=repr,
        )
        for table in _CORE_TABLES
        if table in present
    }


def _non_database_snapshot(root, storage):
    _wait_writer(storage)
    snapshot = _snapshot(root, mutable_existing_shm=True)
    return {
        path: value
        for path, value in snapshot.items()
        if not path.as_posix().startswith(".mwf/state.sqlite3")
        and path.as_posix() != ".mwf/mutation_writer.json"
    }


def _assert_writer_diagnostics(root):
    path = root / ".mwf" / "mutation_writer.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    assert set(data) == {
        "pid", "hostname", "process_identity", "updated_at", "queued",
        "urgent", "queued_by_priority", "submitted_serial",
        "completed_through", "durability_backlog", "pending_mutations",
        "completion_heap_entries", "writer_alive", "active_priority",
        "active_batch_size", "last_batch_seconds", "max_batch",
        "claim_transaction_rows",
    }
    assert type(data["pid"]) is int and data["pid"] > 0
    assert type(data["hostname"]) is str and data["hostname"]
    assert type(data["process_identity"]) is str and data["process_identity"]
    assert type(data["updated_at"]) in {int, float}
    assert math.isfinite(data["updated_at"])
    for field in (
        "queued", "urgent", "submitted_serial", "completed_through",
        "durability_backlog", "pending_mutations", "completion_heap_entries",
        "active_batch_size", "max_batch", "claim_transaction_rows",
    ):
        assert type(data[field]) is int and data[field] >= 0
    assert type(data["queued_by_priority"]) is dict
    assert all(
        type(priority) is str and type(count) is int and count >= 0
        for priority, count in data["queued_by_priority"].items()
    )
    assert type(data["writer_alive"]) is bool
    assert data["active_priority"] is None or type(data["active_priority"]) is int
    assert (
        data["last_batch_seconds"] is None
        or type(data["last_batch_seconds"]) in {int, float}
    )
    if data["last_batch_seconds"] is not None:
        assert math.isfinite(data["last_batch_seconds"])
        assert data["last_batch_seconds"] >= 0
    return data


def _setup_project(tmp_path, monkeypatch, *, edges):
    _make_interrupt_project(tmp_path, monkeypatch, edges=edges)
    workflow = load_workflow(tmp_path, "direct")
    workflow.storage.register_component_topology(workflow.topology.snapshot())
    return workflow, workflow.storage, workflow.topology.snapshot().shape_json


def _create_live_session(storage, shape, component, session_id, *, kind):
    started = now()
    storage.create_execution_session(
        session_id,
        session_kind=kind,
        command="run",
        start_component=component,
        selected_components=[component],
        started_at=started,
        hostname=socket.gethostname(),
        pid=os.getpid(),
        process_identity=process_identity(os.getpid()),
        expected_shape=shape,
    )


def _inject_unreserved_active_owner(storage, shape):
    """Create valid native rows whose active claim contradicts A's reservation."""
    _create_live_session(
        storage, shape, ("A",), "interrupt-A", kind="interrupt",
    )
    storage.create_job(Job(node_name="A", job_id=2, params={"damaged": True}))
    connection = storage.db_connection()
    session = connection.execute(
        "SELECT admitted_shape_id FROM execution_sessions WHERE session_id=?",
        ("interrupt-A",),
    ).fetchone()
    instance = connection.execute(
        "SELECT instance_id FROM job_instances WHERE node_name='A' AND job_id=2"
    ).fetchone()
    assert session is not None and instance is not None
    execution_id = "f" * 32
    started = now()
    with storage.db_transaction() as writer:
        writer.execute(
            "INSERT INTO job_execution_owners("
            "execution_id, node_name, job_id, job_instance_id, generation, "
            "session_id, component_key, shape_id, alignment_generation, "
            "created_by_execution_id) VALUES(?, 'A', 2, ?, 1, ?, ?, ?, 0, NULL)",
            (
                execution_id,
                instance[0],
                "interrupt-A",
                encode_component_key(("A",)),
                session[0],
            ),
        )
        assert writer.execute(
            "UPDATE jobs SET status='running', status_json='{}', generation=1, "
            "active_execution_id=?, active_pid=?, active_thread_id=?, "
            "active_started_at=? WHERE node_name='A' AND job_id=2 "
            "AND status='queued' AND generation=0 AND active_execution_id IS NULL",
            (execution_id, os.getpid(), threading.get_ident(), started),
        ).rowcount == 1
        assert writer.execute(
            "UPDATE job_instances SET last_execution_id=? "
            "WHERE node_name='A' AND job_id=2 AND last_execution_id IS NULL",
            (execution_id,),
        ).rowcount == 1


@pytest.mark.parametrize(
    "node,value,owner,other",
    [
        ("A", 7, "main-A", "interrupt-B"),
        ("B", 9, "interrupt-B", "main-A"),
    ],
)
def test_threads_update_inspect_and_reset_the_exact_disjoint_live_owner(
    tmp_path, monkeypatch, capsys, node, value, owner, other,
):
    workflow, storage, shape = _setup_project(
        tmp_path, monkeypatch, edges=[("A", "A"), ("B", "B")],
    )
    try:
        owners = {
            "A": _create_active_scope(storage, shape, "A", "main-A", live=True),
            "B": _create_active_scope(storage, shape, "B", "interrupt-B", live=True),
        }
        other_node = "B" if node == "A" else "A"
        _wait_writer(storage)
        core_before = _core_rows(storage)
        files_before = _non_database_snapshot(tmp_path, storage)
        other_scope_before = _scope_rows(storage, other_node, other)
        capsys.readouterr()

        assert cli.main(["threads", node, str(value)]) == 0
        _wait_writer(storage)
        changed = capsys.readouterr().out
        assert owner in changed and str(value) in changed
        assert storage.read_thread_override_observation(node) == {
            "node": node, "value": value, "session_id": owner,
        }
        assert storage.read_thread_override_observation(other_node) == {
            "node": other_node, "value": None, "session_id": other,
        }
        assert _scope_rows(storage, other_node, other) == other_scope_before
        assert _core_rows(storage) == core_before
        assert _non_database_snapshot(tmp_path, storage) == files_before
        _assert_writer_diagnostics(tmp_path)

        inspect_files_before = _snapshot(tmp_path)
        assert cli.main(["threads", node]) == 0
        inspected = capsys.readouterr().out
        assert owner in inspected and str(value) in inspected
        assert _snapshot(tmp_path) == inspect_files_before

        other_scope_before_reset = _scope_rows(storage, other_node, other)
        reset_files_before = _non_database_snapshot(tmp_path, storage)
        assert cli.main(["threads", node, "reset"]) == 0
        _wait_writer(storage)
        reset = capsys.readouterr().out
        assert owner in reset
        assert storage.read_thread_override_observation(node) == {
            "node": node, "value": None, "session_id": owner,
        }
        assert _non_database_snapshot(tmp_path, storage) == reset_files_before
        _assert_writer_diagnostics(tmp_path)
        assert _scope_rows(storage, other_node, other) == other_scope_before_reset
        assert _core_rows(storage) == core_before
        assert storage.read_job_current_owner(node, 1) == owners[node]["owner"]
        assert storage.read_job_current_owner(other_node, 1) == owners[other_node]["owner"]
        assert _non_database_snapshot(tmp_path, storage) == files_before
        assert not (tmp_path / ".mwf" / "threads.json").exists()
        assert not (tmp_path / ".mwf" / "run.json").exists()
    finally:
        _close_without_sidecars(storage, tmp_path)


def test_pending_node_override_binds_only_when_the_next_session_claims_that_node(
    tmp_path, monkeypatch, capsys,
):
    workflow, storage, shape = _setup_project(
        tmp_path, monkeypatch, edges=[("A", "A"), ("B", "B")],
    )
    try:
        files_before_update = _non_database_snapshot(tmp_path, storage)
        capsys.readouterr()
        assert cli.main(["threads", "A", "4"]) == 0
        pending_output = capsys.readouterr().out.lower()
        assert "pending" in pending_output and "4" in pending_output
        assert storage.read_thread_override_observation("A") == {
            "node": "A", "value": 4, "session_id": None,
        }
        assert _non_database_snapshot(tmp_path, storage) == files_before_update
        _assert_writer_diagnostics(tmp_path)

        _create_active_scope(storage, shape, "B", "interrupt-B", live=True)
        assert storage.read_thread_override_observation("A") == {
            "node": "A", "value": 4, "session_id": None,
        }
        assert storage.read_thread_override_observation("B") == {
            "node": "B", "value": None, "session_id": "interrupt-B",
        }

        _create_active_scope(storage, shape, "A", "main-A", live=True)
        assert storage.read_thread_override_observation("A") == {
            "node": "A", "value": 4, "session_id": "main-A",
        }
        _wait_writer(storage)
        inspect_files_before = _snapshot(tmp_path)
        assert cli.main(["threads", "A"]) == 0
        inspected = capsys.readouterr().out
        assert "main-A" in inspected and "4" in inspected
        assert _snapshot(tmp_path) == inspect_files_before
        assert not (tmp_path / ".mwf" / "threads.json").exists()
    finally:
        _close_without_sidecars(storage, tmp_path)


@pytest.mark.parametrize("arguments", [[], ["reset"], ["8"]])
def test_threads_refuse_damaged_several_owner_state_without_guessing(
    tmp_path, monkeypatch, capsys, arguments,
):
    workflow, storage, shape = _setup_project(
        tmp_path, monkeypatch, edges=[("A", "A")],
    )
    _create_active_scope(storage, shape, "A", "main-A", live=True)
    _inject_unreserved_active_owner(storage, shape)
    _close_without_sidecars(storage, tmp_path)
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main(["threads", "A", *arguments]) == 1

    captured = capsys.readouterr()
    output = (captured.out + captured.err).lower()
    assert "owner" in output and "ambiguous" in output
    assert "main-a" in output and "interrupt-a" in output
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files
    assert not (tmp_path / ".mwf" / "threads.json").exists()


def test_raw_node_peers_resolve_one_component_owner_with_distinct_node_values(
    tmp_path, monkeypatch, capsys,
):
    workflow, storage, shape = _setup_project(
        tmp_path, monkeypatch, edges=[("A", "Apeer"), ("Apeer", "A")],
    )
    component = tuple(sorted(workflow.component_for("A")))
    try:
        assert component == ("A", "Apeer")
        storage.create_job(Job(node_name="A", job_id=1, params={"peer": True}))
        _create_live_session(storage, shape, component, "main-cycle", kind="main")
        storage.reserve_execution_components("main-cycle", expected_shape=shape)
        storage.begin_queued_component_execution(
            "main-cycle", component,
            expected_shape=shape,
            expected_alignment_generation=0,
            successful_lineage=("stable", None),
        )
        storage.claim_job_execution(
            "A", 1, started_at=now(),
            session_id="main-cycle", component=component,
        )
        _wait_writer(storage)
        core_before = _core_rows(storage)
        files_before = _non_database_snapshot(tmp_path, storage)
        capsys.readouterr()

        assert cli.main(["threads", "Apeer", "6"]) == 0
        _wait_writer(storage)
        output = capsys.readouterr().out
        assert "main-cycle" in output and "6" in output
        assert storage.read_thread_override_observation("Apeer") == {
            "node": "Apeer", "value": 6, "session_id": "main-cycle",
        }
        assert storage.read_thread_override_observation("A") == {
            "node": "A", "value": None, "session_id": "main-cycle",
        }
        assert _core_rows(storage) == core_before
        assert _non_database_snapshot(tmp_path, storage) == files_before
        _assert_writer_diagnostics(tmp_path)
        assert not (tmp_path / ".mwf" / "threads.json").exists()
    finally:
        _close_without_sidecars(storage, tmp_path)
