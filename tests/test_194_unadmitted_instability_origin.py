"""An unstable component result requires an admitted interrupt origin."""

from __future__ import annotations

import json
import math
import time
from pathlib import Path

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_064_read_only_previews import (
    _close,
    _close_without_sidecars,
    _initialize_native_project,
    _install_import_sentinels,
    _live_execution_session_identity,
    _snapshot,
    _wait_writer,
)
from tests.test_121_native_preview_recovery import _rows
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows
from tests.test_149_interrupt_declarations import _router_source


def _create_session(storage, shape, session_id, *, kind):
    return storage.create_execution_session(
        session_id,
        session_kind=kind,
        command="run A --interrupt" if kind == "interrupt" else "run A",
        start_component=("A",),
        selected_components=[("A",)],
        **_live_execution_session_identity(),
        expected_shape=shape,
    )


def _wait_for_subscribers(root):
    deadline = time.monotonic() + 10
    directory = root / ".mwf" / "state_subscribers"
    while list(directory.glob("*.json")):
        assert time.monotonic() < deadline, "Finished execution retained a state subscriber"
        time.sleep(0.01)


def _validate_writer_diagnostics(data):
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
    assert type(data["updated_at"]) in (int, float)
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
    assert data["last_batch_seconds"] is None or (
        type(data["last_batch_seconds"]) in (int, float)
        and math.isfinite(data["last_batch_seconds"])
        and data["last_batch_seconds"] >= 0
    )
    return data


def _require_drained_writer(data, *, require_retired):
    if require_retired:
        assert data["writer_alive"] is False
    assert data["active_priority"] is None
    assert data["active_batch_size"] == 0
    assert data["queued"] == 0
    assert data["urgent"] == 0
    assert data["queued_by_priority"] == {}
    assert data["durability_backlog"] == 0
    assert data["pending_mutations"] == 0
    assert data["completion_heap_entries"] == 0
    assert data["completed_through"] == data["submitted_serial"]


def _retired_writer_diagnostics(storage, root):
    current = _validate_writer_diagnostics(storage.mutation_writer_diagnostics())
    _require_drained_writer(current, require_retired=True)
    persisted = _validate_writer_diagnostics(json.loads(
        (root / ".mwf" / "mutation_writer.json").read_text(encoding="utf-8")
    ))
    # Saved diagnostics are frequency-limited observations. A short final batch
    # can retire before its post-batch observation is published, so the saved
    # active fields and serial watermarks may describe the earlier in-flight
    # batch even though the exact live observation above is fully drained.
    assert {
        name: persisted[name]
        for name in (
            "pid", "hostname", "process_identity", "max_batch",
            "claim_transaction_rows",
        )
    } == {
        name: current[name]
        for name in (
            "pid", "hostname", "process_identity", "max_batch",
            "claim_transaction_rows",
        )
    }
    assert persisted["completed_through"] <= persisted["submitted_serial"]
    assert persisted["submitted_serial"] <= current["submitted_serial"]
    assert persisted["updated_at"] <= current["updated_at"]
    return current


def test_component_start_refuses_an_unadmitted_interrupt_origin_without_mutation(
    tmp_path, monkeypatch,
):
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=[("A", "A")])
    storage = workflow.storage
    shape = workflow.topology.snapshot().shape_json
    _create_session(storage, shape, "main-owner", kind="main")
    assert storage.reserve_execution_components("main-owner", expected_shape=shape) is True
    unadmitted = _create_session(storage, shape, "created-interrupt", kind="interrupt")
    assert unadmitted["session_kind"] == "interrupt"
    assert storage.get_execution_session("created-interrupt")["status"] == "running"
    assert storage.db_connection().execute(
        "SELECT scope_admitted FROM execution_sessions WHERE session_id=?",
        ("created-interrupt",),
    ).fetchone()[0] == 0
    storage.db_mutation_barrier()
    _wait_writer(storage)
    before_rows = _rows(storage)
    before_files = _snapshot(tmp_path, mutable_existing_shm=True)
    writer_path = Path(".mwf/mutation_writer.json")
    assert before_files.pop(writer_path)[0] == "file"
    before_writer = _retired_writer_diagnostics(storage, tmp_path)

    try:
        with pytest.raises(RuntimeError, match="admitted interrupt origin"):
            storage.begin_queued_component_execution(
                "main-owner",
                ("A",),
                expected_shape=shape,
                expected_alignment_generation=0,
                successful_lineage=("unstable", "created-interrupt"),
            )
        storage.db_mutation_barrier()
        _wait_writer(storage)
        assert _rows(storage) == before_rows
        after_files = _snapshot(tmp_path, mutable_existing_shm=True)
        assert after_files.pop(writer_path)[0] == "file"
        assert after_files == before_files
        after_writer = _retired_writer_diagnostics(storage, tmp_path)
        assert {
            name: after_writer[name]
            for name in ("pid", "hostname", "process_identity", "max_batch", "claim_transaction_rows")
        } == {
            name: before_writer[name]
            for name in ("pid", "hostname", "process_identity", "max_batch", "claim_transaction_rows")
        }
        assert after_writer["submitted_serial"] == before_writer["submitted_serial"] + 1
        assert after_writer["completed_through"] == before_writer["completed_through"] + 1
        assert after_writer["updated_at"] >= before_writer["updated_at"]
        assert storage.get_execution_session("created-interrupt")["status"] == "running"
        assert storage.db_connection().execute(
            "SELECT scope_admitted FROM execution_sessions WHERE session_id=?",
            ("created-interrupt",),
        ).fetchone()[0] == 0
    finally:
        _close(storage)


def test_lineage_refuses_a_stored_unadmitted_origin_before_import_or_mutation(
    tmp_path, monkeypatch, capsys,
):
    edges = [("A", "A")]
    make_project(
        tmp_path,
        monkeypatch,
        edges=f"EDGES = {edges!r}",
        files={"A": _router_source("A", jobs=1)},
    )
    assert cli.main(["run", "A", "--runner", "direct"]) == 0
    capsys.readouterr()
    _wait_for_subscribers(tmp_path)
    storage = FileStorage(tmp_path)
    try:
        state = storage.get_component_state(("A",))
        assert (state["lifecycle"], state["stability"], state["instability_origin"]) == (
            "done", "stable", None,
        )
        shape = state["shape_json"]
        unadmitted = _create_session(storage, shape, "stored-created-interrupt", kind="interrupt")
        assert unadmitted["status"] == "running"

        def damage(connection):
            session = connection.execute(
                "SELECT scope_admitted FROM execution_sessions WHERE session_id=?",
                ("stored-created-interrupt",),
            ).fetchone()
            assert session is not None and session["scope_admitted"] == 0
            current = connection.execute(
                "SELECT shape_id, alignment_generation, retained_result_shape_id, "
                "retained_result_alignment_generation FROM component_states WHERE component_key=?",
                ('["A"]',),
            ).fetchone()
            assert current is not None
            assert (current["shape_id"], current["alignment_generation"]) == (
                current["retained_result_shape_id"],
                current["retained_result_alignment_generation"],
            )
            changed_result = connection.execute(
                "UPDATE component_successful_results SET stability='unstable', instability_origin=? "
                "WHERE component_key=? AND shape_id=? AND alignment_generation=? "
                "AND lifecycle='done' AND stability='stable' AND instability_origin IS NULL",
                (
                    "stored-created-interrupt", '["A"]',
                    current["shape_id"], current["alignment_generation"],
                ),
            ).rowcount
            changed_state = connection.execute(
                "UPDATE component_states SET stability='unstable', instability_origin=? "
                "WHERE component_key=? AND shape_id=? AND alignment_generation=? "
                "AND lifecycle='done' AND stability='stable' AND instability_origin IS NULL",
                (
                    "stored-created-interrupt", '["A"]',
                    current["shape_id"], current["alignment_generation"],
                ),
            ).rowcount
            assert (changed_result, changed_state) == (1, 1)

        storage.submit_db_mutation(damage, wait=True, priority=0)
        before_rows = _rows(storage)
    finally:
        _close_without_sidecars(storage, tmp_path)

    external = _install_import_sentinels(tmp_path, edges)
    before_files = _snapshot(tmp_path)
    assert cli.main(["trace", "A", "job", "1", "--lineage", "--json"]) == 1
    captured = capsys.readouterr()
    message = captured.out + captured.err
    assert "successful" in message.lower() or "component state" in message.lower()
    assert "invalid" in message.lower()
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files
    assert not external.exists()


def test_terminal_admitted_interrupt_remains_a_valid_historical_origin(
    tmp_path, monkeypatch, capsys,
):
    edges = [("P", "I")]
    make_project(
        tmp_path,
        monkeypatch,
        edges=f"EDGES = {edges!r}",
        files={
            "P": _router_source("P", jobs=1),
            "I": _router_source("I", interrupt="True", jobs=1),
        },
    )
    assert cli.main(["run", "I", "--interrupt", "--runner", "direct"]) == 0
    capsys.readouterr()
    _wait_for_subscribers(tmp_path)
    storage = FileStorage(tmp_path)
    try:
        interrupt, = [
            session for session in storage.list_execution_sessions()
            if session["session_kind"] == "interrupt"
        ]
        session_id = interrupt["session_id"]
        assert (interrupt["status"], interrupt["outcome"]) == ("terminal", "done")
        assert storage.db_connection().execute(
            "SELECT scope_admitted FROM execution_sessions WHERE session_id=?", (session_id,),
        ).fetchone()[0] == 1
        state = storage.get_component_state(("I",))
        assert (state["lifecycle"], state["stability"], state["instability_origin"]) == (
            "done", "unstable", session_id,
        )
        assert storage.get_component_reservation(("I",)) is None
        before_rows = _rows(storage)
    finally:
        _close_without_sidecars(storage, tmp_path)

    external = _install_import_sentinels(tmp_path, edges)
    before_files = _snapshot(tmp_path)
    assert cli.main(["trace", "I", "job", "1", "--lineage", "--json"]) == 0
    lineage = json.loads(capsys.readouterr().out)
    assert lineage["interrupt_session_id"] == session_id
    assert lineage["component"] == {
        "members": ["I"],
        "state": "done",
        "stability": "unstable",
        "instability_origin": session_id,
        "misaligned": False,
        "misalignment_causes": [],
    }
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files
    assert not external.exists()
