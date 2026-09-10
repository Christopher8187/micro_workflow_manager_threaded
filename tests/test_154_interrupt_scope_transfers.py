"""Sensitive RED cases for explicit-interrupt admission and scope transfer.

These tests state the proposed storage seam before it exists. They use only
current native topology, session, reservation, component, and job writers for
their setup. ``admit_interrupt_execution`` must be one writer transaction.
"""

from __future__ import annotations

import json
import math
import os
import socket
import time
from pathlib import Path

import networkx as nx
import pytest

from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.models import Job, now
from micro_workflow_manager.processes import process_identity
from micro_workflow_manager.storage import FileStorage, InterruptAdmissionPaused
from micro_workflow_manager.storage.interrupt_admission import read_interrupt_admission_conflicts
from micro_workflow_manager.storage.session_scope import require_admitted_reservation_scope
from micro_workflow_manager.topology import ComponentTopology


def _close(storage):
    storage.db_mutation_barrier()
    deadline = time.monotonic() + 10
    while storage.mutation_writer_diagnostics()["writer_alive"]:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    storage.close_database_connections()


def _fixture(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    topology = ComponentTopology(nx.DiGraph([("P", "I"), ("I", "D")]), []).snapshot()
    storage.register_component_topology(topology)
    for node, count in (("P", 1), ("I", 2), ("D", 1)):
        for job_id in range(1, count + 1):
            storage.create_job(Job(node_name=node, job_id=job_id, params={"node": node, "id": job_id}))
    return storage, topology


def _session(storage, topology, session_id, *, kind, selected, reserve=True):
    session = storage.create_execution_session(
        session_id,
        session_kind=kind,
        command="runfrom P" if kind == "main" else "runfrom P --interrupt",
        start_component=selected[0],
        selected_components=selected,
        started_at="2026-09-08T08:00:00+00:00",
        hostname=socket.gethostname(),
        pid=os.getpid(),
        process_identity=process_identity(os.getpid()),
        details={"fixture": session_id},
        expected_shape=topology.shape_json,
    )
    if reserve:
        storage.reserve_execution_components(session_id, expected_shape=topology.shape_json)
    return session


def _begin_and_claim(storage, topology, session_id, component, node, job_id=1):
    storage.begin_queued_component_execution(
        session_id,
        component,
        expected_shape=topology.shape_json,
        expected_alignment_generation=0,
        successful_lineage=("stable", None),
    )
    generation, execution_id = storage.claim_job_execution(
        node,
        job_id,
        started_at="2026-09-08T08:00:01+00:00",
        session_id=session_id,
        component=component,
    )
    return generation, execution_id


def _admit_interrupt(storage, topology, session_id, *, selected, predecessors):
    return storage.admit_interrupt_execution(
        session_id,
        command=f"run {selected[0][0]} --interrupt",
        start_component=selected[0],
        selected_components=selected,
        selected_jobs=(),
        direct_predecessors=predecessors,
        readiness_overridden=True,
        started_at="2026-09-08T08:00:02+00:00",
        hostname=socket.gethostname(),
        pid=os.getpid(),
        process_identity=process_identity(os.getpid()),
        details={"explicit_interrupt": True},
        expected_shape=topology.shape_json,
    )


def _freeze_interrupt(storage, topology, session_id, component):
    by_node = {node: members for members in topology.components for node in members}
    predecessors = {by_node[source] for source, receiver in json.loads(topology.shape_json)["edges"]
                    if receiver in component and source not in component}
    parents = {parent: storage.get_component_state(parent) for parent in sorted(predecessors)}
    frozen = storage.freeze_interrupt_target(
        session_id,
        expected_shape=topology.shape_json,
        expected_parent_states=parents,
        successful_lineage=("unstable", session_id),
        readiness_overridden=True,
        frozen_at="2026-09-08T08:00:03+00:00",
    )
    assert frozen["state"] == "frozen"
    assert frozen["frozen_parent_states"] == parents
    return frozen


def _rows(storage):
    connection = storage.db_connection()
    names = [
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    return {
        name: [dict(row) for row in connection.execute(f'SELECT * FROM "{name}" ORDER BY rowid')]
        for name in names
    }


def _files(root: Path):
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
        and not path.name.startswith("state.sqlite3")
        and path.name != "mutation_writer.json"
        and not path.name.startswith(".mutation_writer.json.")
    }


def _settle_writer(storage, root):
    storage.db_mutation_barrier()
    deadline = time.monotonic() + 10
    while True:
        diagnostics = storage.mutation_writer_diagnostics()
        temporary = list((root / ".mwf").glob(".mutation_writer.json.*.tmp"))
        if (
            not diagnostics["writer_alive"]
            and diagnostics["queued"] == 0
            and diagnostics["urgent"] == 0
            and diagnostics["durability_backlog"] == 0
            and diagnostics["pending_mutations"] == 0
            and diagnostics["active_priority"] is None
            and diagnostics["active_batch_size"] == 0
            and not temporary
        ):
            return
        assert time.monotonic() < deadline, "Mutation writer did not become idle"
        time.sleep(0.01)


def _writer_identity(root):
    data = json.loads((root / ".mwf" / "mutation_writer.json").read_text(encoding="utf-8"))
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
    assert type(data["writer_alive"]) is bool and data["writer_alive"]
    assert data["active_priority"] is None or type(data["active_priority"]) is int
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
    assert (
        data["last_batch_seconds"] is None
        or type(data["last_batch_seconds"]) in (int, float)
    )
    if data["last_batch_seconds"] is not None:
        assert math.isfinite(data["last_batch_seconds"])
        assert data["last_batch_seconds"] >= 0
    return {
        field: data[field]
        for field in (
            "pid", "hostname", "process_identity",
            "max_batch", "claim_transaction_rows",
        )
    }


def _reservation_owners(storage):
    return {
        row["component_key"]: row["session_id"]
        for row in storage.db_connection().execute(
            "SELECT component_key, session_id FROM component_reservations ORDER BY component_key"
        )
    }


def test_read_only_interrupt_preflight_allows_unregistered_shape_but_writer_stays_strict(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    topology = ComponentTopology(nx.DiGraph([("P", "I")]), []).snapshot()
    before_rows, before_files = _rows(storage), _files(tmp_path)

    observed = read_interrupt_admission_conflicts(
        storage.db_connection(), start_component=("I",),
        selected_components=(("I",),), direct_predecessors=(("P",),),
        expected_shape=topology.shape_json,
    )

    assert observed == {
        "shape_id": None,
        "partition_revision": None,
        "parents": (),
        "predecessor_executions": (),
        "reservations": {encode_component_key(("I",)): None},
        "transfers": (),
    }
    assert _rows(storage) == before_rows
    assert _files(tmp_path) == before_files

    with pytest.raises(RuntimeError, match="registered graph shape"):
        _admit_interrupt(
            storage, topology, "unregistered-child",
            selected=[("I",)], predecessors=[("P",)],
        )
    assert storage.get_execution_session("unregistered-child") is None
    assert _rows(storage) == before_rows
    storage.register_component_topology(topology)
    assert _admit_interrupt(
        storage, topology, "registered-child",
        selected=[("I",)], predecessors=[("P",)],
    )["session_id"] == "registered-child"
    _close(storage)


def test_future_admission_transfer_has_no_false_parent_and_returns_scope_to_main(tmp_path):
    storage, topology = _fixture(tmp_path)
    selected = [("P",), ("I",), ("D",)]
    _session(storage, topology, "main-owner", kind="main", selected=selected)

    child = _admit_interrupt(
        storage, topology, "child-interrupt", selected=[("I",), ("D",)], predecessors=[("P",)]
    )
    assert child["parent_session_ids"] == []
    assert child["selected_components"] == [("I",), ("D",)]
    assert _reservation_owners(storage) == {
        encode_component_key(("P",)): "main-owner",
        encode_component_key(("I",)): "child-interrupt",
        encode_component_key(("D",)): "child-interrupt",
    }
    transfers = [
        dict(row)
        for row in storage.db_connection().execute(
            "SELECT child_session_id, source_session_id, component_key, transfer_kind, state "
            "FROM interrupt_scope_transfers ORDER BY component_key"
        )
    ]
    assert transfers == [
        {
            "child_session_id": "child-interrupt",
            "source_session_id": "main-owner",
            "component_key": encode_component_key(("D",)),
            "transfer_kind": "future-admission",
            "state": "active",
        },
        {
            "child_session_id": "child-interrupt",
            "source_session_id": "main-owner",
            "component_key": encode_component_key(("I",)),
            "transfer_kind": "future-admission",
            "state": "active",
        },
    ]

    decision = storage.decide_execution_session_exit(
        "child-interrupt",
        outcome="done",
        finished_at="2026-09-08T08:00:03+00:00",
    )
    assert decision["released"] == 0
    assert _reservation_owners(storage) == {
        encode_component_key(component): "main-owner" for component in selected
    }
    assert storage.get_execution_session("main-owner")["status"] == "running"
    assert storage.get_execution_session("child-interrupt")["status"] == "terminal"
    assert {
        row["state"]
        for row in storage.db_connection().execute(
            "SELECT state FROM interrupt_scope_transfers WHERE child_session_id='child-interrupt'"
        )
    } == {"returned"}
    _close(storage)


@pytest.mark.parametrize("parent_kind", ["main", "interrupt"])
def test_active_direct_predecessor_records_its_actual_main_or_interrupt_parent(tmp_path, parent_kind):
    storage, topology = _fixture(tmp_path)
    selected = [("P",), ("I",), ("D",)]
    _session(storage, topology, "active-parent", kind=parent_kind, selected=selected)
    generation, execution_id = _begin_and_claim(
        storage, topology, "active-parent", ("P",), "P"
    )

    child = _admit_interrupt(
        storage, topology, "child-interrupt", selected=[("I",), ("D",)], predecessors=[("P",)]
    )
    assert child["parent_session_ids"] == ["active-parent"]
    assert _reservation_owners(storage)[encode_component_key(("P",))] == "active-parent"
    assert _reservation_owners(storage)[encode_component_key(("I",))] == "child-interrupt"
    request = dict(storage.db_connection().execute(
        "SELECT child_session_id, owner_session_id, component_key, state "
        "FROM interrupt_pause_requests WHERE child_session_id='child-interrupt'"
    ).fetchone())
    assert request == {
        "child_session_id": "child-interrupt",
        "owner_session_id": "active-parent",
        "component_key": encode_component_key(("P",)),
        "state": "requested",
    }
    owner = storage.get_job_execution_owner(execution_id)
    assert (owner["session_id"], owner["generation"], owner["component"]) == (
        "active-parent", generation, ("P",)
    )
    assert storage.get_execution_session("active-parent")["status"] == "running"
    _close(storage)


def test_two_active_direct_predecessor_owners_are_both_recorded_and_paused(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    topology = ComponentTopology(
        nx.DiGraph([("P1", "I"), ("P2", "I"), ("I", "D")]), []
    ).snapshot()
    storage.register_component_topology(topology)
    for node in ("P1", "P2", "I", "D"):
        storage.create_job(Job(node_name=node, job_id=1, params={"node": node}))
    _session(storage, topology, "main-parent", kind="main", selected=[("P1",)])
    _session(storage, topology, "nested-parent", kind="interrupt", selected=[("P2",)])
    first = _begin_and_claim(storage, topology, "main-parent", ("P1",), "P1")
    second = _begin_and_claim(storage, topology, "nested-parent", ("P2",), "P2")

    child = _admit_interrupt(
        storage,
        topology,
        "multi-parent-child",
        selected=[("I",)],
        predecessors=[("P1",), ("P2",)],
    )
    assert child["parent_session_ids"] == ["main-parent", "nested-parent"]
    assert [
        tuple(row)
        for row in storage.db_connection().execute(
            "SELECT child_session_id, parent_session_id FROM execution_session_parents "
            "WHERE child_session_id='multi-parent-child' ORDER BY parent_session_id"
        )
    ] == [
        ("multi-parent-child", "main-parent"),
        ("multi-parent-child", "nested-parent"),
    ]
    assert [
        tuple(row)
        for row in storage.db_connection().execute(
            "SELECT owner_session_id, component_key, state FROM interrupt_pause_requests "
            "WHERE child_session_id='multi-parent-child' ORDER BY owner_session_id"
        )
    ] == [
        ("main-parent", encode_component_key(("P1",)), "requested"),
        ("nested-parent", encode_component_key(("P2",)), "requested"),
    ]
    assert storage.get_component_reservation(("P1",))["session_id"] == "main-parent"
    assert storage.get_component_reservation(("P2",))["session_id"] == "nested-parent"
    assert storage.get_component_reservation(("I",))["session_id"] == "multi-parent-child"
    assert {
        storage.get_job_execution_owner(first[1])["session_id"],
        storage.get_job_execution_owner(second[1])["session_id"],
    } == {"main-parent", "nested-parent"}
    _close(storage)


def test_active_claim_in_child_target_scope_refuses_before_any_admission_mutation(tmp_path):
    storage, topology = _fixture(tmp_path)
    selected = [("P",), ("I",), ("D",)]
    _session(storage, topology, "main-owner", kind="main", selected=selected)
    _begin_and_claim(storage, topology, "main-owner", ("I",), "I")
    _settle_writer(storage, tmp_path)
    before_writer = _writer_identity(tmp_path)
    before_rows, before_files = _rows(storage), _files(tmp_path)

    with pytest.raises(
        RuntimeError,
        match=r'active job I/1 owned by session main-owner in component \["I"\]',
    ):
        _admit_interrupt(
            storage, topology, "refused-child", selected=[("I",), ("D",)], predecessors=[("P",)]
        )

    _settle_writer(storage, tmp_path)
    assert _writer_identity(tmp_path) == before_writer
    assert _rows(storage) == before_rows
    assert _files(tmp_path) == before_files
    assert storage.get_execution_session("refused-child") is None
    assert _reservation_owners(storage) == {
        encode_component_key(component): "main-owner" for component in selected
    }
    _close(storage)


def test_independent_interrupt_overlap_refuses_without_yielding_or_creating_a_child(tmp_path):
    storage, topology = _fixture(tmp_path)
    _session(storage, topology, "first-interrupt", kind="interrupt", selected=[("I",), ("D",)])
    _settle_writer(storage, tmp_path)
    before_writer = _writer_identity(tmp_path)
    before_rows, before_files = _rows(storage), _files(tmp_path)

    with pytest.raises(RuntimeError, match="overlap|reservation"):
        _admit_interrupt(
            storage, topology, "second-interrupt", selected=[("I",)], predecessors=[("P",)]
        )

    _settle_writer(storage, tmp_path)
    assert _writer_identity(tmp_path) == before_writer
    assert _rows(storage) == before_rows
    assert _files(tmp_path) == before_files
    assert storage.get_execution_session("second-interrupt") is None
    assert _reservation_owners(storage) == {
        encode_component_key(("I",)): "first-interrupt",
        encode_component_key(("D",)): "first-interrupt",
    }
    _close(storage)


def test_transferred_component_rejects_parent_claim_and_records_child_as_exact_owner(tmp_path):
    storage, topology = _fixture(tmp_path)
    _session(storage, topology, "main-owner", kind="main", selected=[("P",), ("I",), ("D",)])
    _admit_interrupt(
        storage, topology, "child-interrupt", selected=[("I",)], predecessors=[("P",)]
    )
    _freeze_interrupt(storage, topology, "child-interrupt", ("I",))
    storage.begin_queued_component_execution(
        "child-interrupt",
        ("I",),
        expected_shape=topology.shape_json,
        expected_alignment_generation=0,
        successful_lineage=("unstable", "child-interrupt"),
    )
    before_second = (
        storage.read_job_control("I", 2),
        storage.read_job_events("I", 2),
        storage.read_job_current_owner("I", 2),
    )
    with pytest.raises(InterruptAdmissionPaused) as paused:
        storage.claim_job_execution(
            "I", 2, started_at=now(), session_id="main-owner", component=("I",)
        )
    assert paused.value.child_session_ids == ("child-interrupt",)
    assert (
        storage.read_job_control("I", 2),
        storage.read_job_events("I", 2),
        storage.read_job_current_owner("I", 2),
    ) == before_second

    generation, execution_id = storage.claim_job_execution(
        "I", 1, started_at=now(), session_id="child-interrupt", component=("I",)
    )
    owner = storage.get_job_execution_owner(execution_id)
    assert (generation, owner["session_id"], owner["component"]) == (
        0, "child-interrupt", ("I",)
    )
    assert storage.read_job_current_owner("I", 1) == owner
    _close(storage)


def test_suppressed_interrupt_admission_row_rolls_back_every_child_row_and_file(tmp_path):
    storage, topology = _fixture(tmp_path)
    _session(
        storage, topology, "main-owner", kind="main",
        selected=[("P",), ("I",), ("D",)],
    )
    connection = storage.db_connection()
    connection.execute(
        "CREATE TRIGGER suppress_interrupt_admission BEFORE INSERT ON interrupt_admissions "
        "BEGIN SELECT RAISE(IGNORE); END"
    )
    _settle_writer(storage, tmp_path)
    before_writer = _writer_identity(tmp_path)
    before_rows, before_files = _rows(storage), _files(tmp_path)

    with pytest.raises(RuntimeError, match="admission was not recorded"):
        _admit_interrupt(
            storage, topology, "suppressed-child",
            selected=[("I",)], predecessors=[("P",)],
        )

    _settle_writer(storage, tmp_path)
    assert _writer_identity(tmp_path) == before_writer
    assert _rows(storage) == before_rows
    assert _files(tmp_path) == before_files
    assert storage.get_execution_session("suppressed-child") is None
    _close(storage)


def test_child_releases_transferred_scope_when_its_source_has_ended(tmp_path):
    storage, topology = _fixture(tmp_path)
    selected = [("P",), ("I",), ("D",)]
    _session(storage, topology, "main-owner", kind="main", selected=selected)
    _admit_interrupt(
        storage, topology, "child-interrupt",
        selected=[("I",)], predecessors=[("P",)],
    )

    parent = storage.decide_execution_session_exit(
        "main-owner", outcome="stopped",
        finished_at="2026-09-08T08:00:03+00:00",
    )
    assert parent["released"] == 2
    assert storage.get_execution_session("main-owner")["status"] == "terminal"
    assert storage.get_component_reservation(("I",))["session_id"] == "child-interrupt"

    child = storage.decide_execution_session_exit(
        "child-interrupt", outcome="done",
        finished_at="2026-09-08T08:00:04+00:00",
    )
    assert child == {"restarts": {}, "released": 1, "returned": 0}
    assert storage.get_component_reservation(("I",)) is None
    transfer = dict(storage.db_connection().execute(
        "SELECT state, finished_at FROM interrupt_scope_transfers "
        "WHERE child_session_id='child-interrupt' AND component_key=?",
        (encode_component_key(("I",)),),
    ).fetchone())
    assert transfer == {
        "state": "released", "finished_at": "2026-09-08T08:00:04+00:00",
    }
    _close(storage)


def test_settled_target_keeps_parent_blocked_until_live_child_returns_remaining_scope(tmp_path):
    from micro_workflow_manager.storage.component_states import ComponentTerminalOutcome

    storage, topology = _fixture(tmp_path)
    selected = [("P",), ("I",), ("D",)]
    _session(storage, topology, "main-owner", kind="main", selected=selected)
    _admit_interrupt(
        storage, topology, "child-interrupt",
        selected=[("I",), ("D",)], predecessors=[("P",)],
    )
    _freeze_interrupt(storage, topology, "child-interrupt", ("I",))
    storage.begin_queued_component_execution(
        "child-interrupt", ("I",),
        expected_shape=topology.shape_json,
        expected_alignment_generation=0,
        successful_lineage=("unstable", "child-interrupt"),
    )
    for job_id in (1, 2):
        generation, execution_id = storage.claim_job_execution(
            "I", job_id, started_at=now(),
            session_id="child-interrupt", component=("I",),
        )
        storage.finalize_job_execution(
            "I", job_id, generation, execution_id, "done",
        )
    assert storage.finish_successful_component_execution(
        "child-interrupt",
        ComponentTerminalOutcome(
            ("I",), topology.shape_json, 0, "done",
            "unstable", "child-interrupt",
        ),
    ) is True
    assert storage.get_interrupt_execution_admission("child-interrupt")["state"] == "settled"
    connection = storage.db_connection()
    assert dict(connection.execute(
        "SELECT status, scope_admitted FROM execution_sessions WHERE session_id=?",
        ("child-interrupt",),
    ).fetchone()) == {"status": "running", "scope_admitted": 1}
    assert [
        tuple(row)
        for row in connection.execute(
            "SELECT position, component_key FROM session_components "
            "WHERE session_id=? ORDER BY position",
            ("child-interrupt",),
        )
    ] == [
        (0, encode_component_key(("I",))),
        (1, encode_component_key(("D",))),
    ]
    assert [
        tuple(row)
        for row in connection.execute(
            "SELECT child_session_id, source_session_id, component_key, state "
            "FROM interrupt_scope_transfers WHERE child_session_id=? "
            "ORDER BY component_key",
            ("child-interrupt",),
        )
    ] == [
        ("child-interrupt", "main-owner", encode_component_key(("D",)), "active"),
        ("child-interrupt", "main-owner", encode_component_key(("I",)), "active"),
    ]
    assert require_admitted_reservation_scope(connection, "main-owner") is True
    assert storage.interrupt_component_admission_blockers(
        "main-owner", ("I",),
    ) == ("child-interrupt",)
    assert storage.interrupt_component_admission_blockers(
        "main-owner", ("D",),
    ) == ("child-interrupt",)
    with pytest.raises(InterruptAdmissionPaused) as paused:
        storage.claim_job_execution(
            "D", 1, started_at=now(), session_id="main-owner", component=("D",),
        )
    assert paused.value.child_session_ids == ("child-interrupt",)

    decision = storage.decide_execution_session_exit(
        "child-interrupt", outcome="stopped",
        finished_at="2026-09-08T08:00:05+00:00",
    )
    assert decision == {"restarts": {}, "released": 0, "returned": 2}
    assert storage.interrupt_component_admission_blockers("main-owner", ("I",)) == ()
    assert storage.interrupt_component_admission_blockers("main-owner", ("D",)) == ()
    assert _reservation_owners(storage) == {
        encode_component_key(component): "main-owner" for component in selected
    }
    _close(storage)
