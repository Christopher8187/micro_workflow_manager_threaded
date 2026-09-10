"""Durable recovery distinction between unadmitted and damaged admitted sessions."""

from __future__ import annotations

import json

from micro_workflow_manager import cli
from micro_workflow_manager.component_identity import encode_component_key
from tests.test_064_read_only_previews import (
    _close,
    _initialize_native_project,
    _install_import_sentinels,
    _live_execution_session_identity,
    _mark_execution_session_stale,
    _snapshot,
    _wait_writer,
)
from tests.test_121_native_preview_recovery import _rows
from tests.test_154_interrupt_scope_transfers import (
    _admit_interrupt,
    _fixture,
    _freeze_interrupt,
    _reservation_owners,
    _session,
)


def _create_running_session(storage, shape, session_id):
    return storage.create_execution_session(
        session_id,
        session_kind="main",
        command="run",
        start_component=("A",),
        selected_components=[("A",)],
        **_live_execution_session_identity(),
        expected_shape=shape,
    )


def test_recover_refuses_an_admitted_session_that_lost_every_reservation(
    tmp_path, monkeypatch, capsys,
):
    edges = [("A", "A")]
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    storage = workflow.storage
    shape = workflow.topology.snapshot().shape_json
    session_id = "admitted-without-scope"
    _create_running_session(storage, shape, session_id)
    assert storage.reserve_execution_components(session_id, expected_shape=shape) is True
    assert storage.submit_db_mutation(lambda connection: connection.execute(
        "DELETE FROM component_reservations WHERE session_id=?", (session_id,),
    ).rowcount) == 1
    _mark_execution_session_stale(storage, session_id)
    storage.db_mutation_barrier()
    _wait_writer(storage)

    external = _install_import_sentinels(tmp_path, edges)
    before_rows = _rows(storage)
    before_files = _snapshot(tmp_path, mutable_existing_shm=True)
    capsys.readouterr()
    try:
        assert cli.main(["recover"]) == 1
        captured = capsys.readouterr()
        message = captured.out + captured.err
        assert session_id in message
        assert "reservation" in message.lower()
        assert "scope" in message.lower()
        assert _rows(storage) == before_rows
        assert _snapshot(tmp_path, mutable_existing_shm=True) == before_files
        assert not external.exists()

        assert cli.main(["recover"]) == 1
        capsys.readouterr()
        assert _rows(storage) == before_rows
        assert _snapshot(tmp_path, mutable_existing_shm=True) == before_files
        assert not external.exists()
    finally:
        _close(storage)


def test_recover_settles_a_stale_session_whose_reservation_never_committed(
    tmp_path, monkeypatch, capsys,
):
    edges = [("A", "A")]
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    storage = workflow.storage
    shape = workflow.topology.snapshot().shape_json
    session_id = "created-before-reservation"
    _create_running_session(storage, shape, session_id)
    _mark_execution_session_stale(storage, session_id)
    storage.db_mutation_barrier()
    _wait_writer(storage)
    assert storage.get_component_reservation(("A",)) is None

    external = _install_import_sentinels(tmp_path, edges)
    before_rows = _rows(storage)
    before_node = _snapshot(tmp_path / "node" / "A")
    capsys.readouterr()
    try:
        assert cli.main(["recover"]) == 0
        captured = capsys.readouterr()
        message = captured.out + captured.err
        assert f"Recovered session {session_id}" in message
        assert not external.exists()
        storage.db_mutation_barrier()
        after_rows = _rows(storage)
        before_session = before_rows["execution_sessions"][0]
        after_session = after_rows["execution_sessions"][0]
        assert len(before_session) == len(after_session)
        changed = {
            position for position, pair in enumerate(zip(before_session, after_session, strict=True))
            if pair[0] != pair[1]
        }
        columns = [row[1] for row in storage.db_connection().execute(
            "PRAGMA table_info(execution_sessions)",
        )]
        assert {columns[position] for position in changed} == {
            "status", "finished_at", "outcome", "failures_json",
        }
        assert after_session[columns.index("status")] == "terminal"
        assert after_session[columns.index("outcome")] == "failed"
        assert json.loads(after_session[columns.index("failures_json")])
        for table, rows in before_rows.items():
            if table not in {"execution_sessions", "recovery_receipts"}:
                assert after_rows[table] == rows, table
        assert [tuple(row) for row in storage.db_connection().execute(
            "SELECT session_id, state FROM recovery_receipts ORDER BY operation_id",
        )] == [(session_id, "committed")]
        assert _snapshot(tmp_path / "node" / "A") == before_node

        after_files = _snapshot(tmp_path, mutable_existing_shm=True)
        assert cli.main(["recover"]) == 0
        capsys.readouterr()
        storage.db_mutation_barrier()
        assert _rows(storage) == after_rows
        assert _snapshot(tmp_path, mutable_existing_shm=True) == after_files
        assert not external.exists()
    finally:
        _close(storage)


def test_terminal_scope_validation_follows_a_nested_interrupt_transfer_chain(tmp_path):
    storage, topology = _fixture(tmp_path)
    selected = [("P",), ("I",), ("D",)]
    _session(storage, topology, "main-owner", kind="main", selected=selected)
    _admit_interrupt(
        storage,
        topology,
        "first-child",
        selected=[("I",), ("D",)],
        predecessors=[("P",)],
    )
    _freeze_interrupt(storage, topology, "first-child", ("I",))
    storage.begin_queued_component_execution(
        "first-child",
        ("I",),
        expected_shape=topology.shape_json,
        expected_alignment_generation=0,
        successful_lineage=("unstable", "first-child"),
    )
    storage.claim_job_execution(
        "I",
        1,
        started_at="2026-09-08T08:00:03+00:00",
        session_id="first-child",
        component=("I",),
    )
    second = _admit_interrupt(
        storage,
        topology,
        "second-child",
        selected=[("D",)],
        predecessors=[("I",)],
    )
    assert second["parent_session_ids"] == ["first-child"]
    p_key = encode_component_key(("P",))
    i_key = encode_component_key(("I",))
    d_key = encode_component_key(("D",))
    assert _reservation_owners(storage) == {
        p_key: "main-owner",
        i_key: "first-child",
        d_key: "second-child",
    }

    try:
        decision = storage.decide_execution_session_exit(
            "main-owner",
            outcome="stopped",
            finished_at="2026-09-08T08:00:04+00:00",
        )
        assert decision == {"restarts": {}, "released": 1}
        assert storage.get_execution_session("main-owner")["status"] == "terminal"
        assert storage.get_execution_session("first-child")["status"] == "running"
        assert storage.get_execution_session("second-child")["status"] == "running"
        assert _reservation_owners(storage) == {
            i_key: "first-child",
            d_key: "second-child",
        }
        assert [tuple(row) for row in storage.db_connection().execute(
            "SELECT source_session_id, child_session_id, component_key, state "
            "FROM interrupt_scope_transfers WHERE component_key=? "
            "ORDER BY child_session_id",
            (d_key,),
        )] == [
            ("main-owner", "first-child", d_key, "active"),
            ("first-child", "second-child", d_key, "active"),
        ]
    finally:
        _close(storage)
