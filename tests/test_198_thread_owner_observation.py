"""Live limit observations validate every active member and reread changed state."""

from __future__ import annotations

import os
import socket

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.models import Job, now
from micro_workflow_manager.processes import process_identity
from tests.test_064_read_only_previews import _close_without_sidecars
from tests.test_154_interrupt_scope_transfers import (
    _admit_interrupt,
    _begin_and_claim,
    _freeze_interrupt,
    _session,
)


@pytest.fixture
def owned_component(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "Apeer"), ("Apeer", "A")])
    storage = workflow.storage
    shape = workflow.topology.snapshot().shape_json
    component = ("A", "Apeer")
    storage.register_component_topology(workflow.topology.snapshot())
    for node in component:
        for job_id in range(1, 4):
            storage.create_job(Job(node_name=node, job_id=job_id, params={}))
    storage.create_execution_session(
        "main-component", session_kind="main", command="run",
        start_component=component, selected_components=[component],
        started_at=now(), hostname=socket.gethostname(), pid=os.getpid(),
        process_identity=process_identity(os.getpid()), expected_shape=shape,
    )
    storage.reserve_execution_components("main-component", expected_shape=shape)
    storage.begin_queued_component_execution(
        "main-component", component, expected_shape=shape,
        expected_alignment_generation=0, successful_lineage=("stable", None),
    )
    for node in component:
        storage.claim_job_executions_batch(
            node, [1, 2, 3], started_at=now(),
            session_id="main-component", component=component,
        )
    workflow.execution_session_context = (
        "main-component", {node: component for node in component},
    )
    observed = storage.read_thread_override_observation("A")
    storage.set_thread_override("A", 7, expected=observed)
    try:
        yield workflow
    finally:
        workflow.execution_session_context = None
        _close_without_sidecars(storage, tmp_path)


def _ownership_rows(storage):
    connection = storage.db_connection()
    return {
        table: [tuple(row) for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')]
        for table in ("jobs", "job_instances", "job_execution_owners")
    }


def test_limit_observation_keeps_node_values_distinct_and_refreshes_updates(owned_component):
    workflow = owned_component
    storage = workflow.storage
    before = _ownership_rows(storage)
    assert workflow.thread_override("A") == 7
    assert workflow.thread_override("Apeer") is None
    assert storage.read_thread_override_for_session("A", "another-session") is None
    expected = storage.read_thread_override_observation("A")
    assert expected == {"node": "A", "value": 7, "session_id": "main-component"}
    storage.set_thread_override("A", 11, expected=expected)
    assert workflow.thread_override("A") == 11
    assert workflow.thread_override("Apeer") is None
    assert _ownership_rows(storage) == before


@pytest.mark.parametrize(
    "table,column,replacement",
    [
        ("jobs", "active_thread_id", 0),
        ("jobs", "status", "queued"),
        ("jobs", "active_started_at", "not-a-time"),
        ("job_execution_owners", "generation", "next-generation"),
        ("job_execution_owners", "job_instance_id", "other-instance"),
        ("job_instances", "last_execution_id", "other-execution"),
    ],
)
def test_limit_observation_refuses_a_damaged_later_peer_and_rereads_after_repair(
    owned_component, table, column, replacement,
):
    workflow = owned_component
    storage = workflow.storage
    connection = storage.db_connection()
    original = connection.execute(
        f'SELECT "{column}" FROM "{table}" WHERE node_name=? AND job_id=?',
        ("Apeer", 3),
    ).fetchone()[0]
    if replacement == "next-generation":
        replacement = original + 1
    elif replacement == "other-instance":
        replacement = storage.read_job_instance_id("Apeer", 2)
    elif replacement == "other-execution":
        replacement = storage.read_job_current_owner("Apeer", 2)["execution_id"]

    assert workflow.thread_override("A") == 7
    statement = f'UPDATE "{table}" SET "{column}"=? WHERE node_name=? AND job_id=?'
    with storage.db_transaction() as writer:
        assert writer.execute(statement, (replacement, "Apeer", 3)).rowcount == 1
    damaged = _ownership_rows(storage)
    try:
        with pytest.raises(RuntimeError, match="[Oo]wnership|[Oo]wner|[Ii]dentity"):
            workflow.thread_override("A")
        assert _ownership_rows(storage) == damaged
    finally:
        with storage.db_transaction() as writer:
            assert writer.execute(statement, (original, "Apeer", 3)).rowcount == 1
    assert workflow.thread_override("A") == 7
    assert storage.read_thread_override_observation("Apeer") == {
        "node": "Apeer", "value": None, "session_id": "main-component",
    }


def test_grouped_owner_observation_retains_exact_interrupt_parent_sessions(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="api")
    storage = workflow.storage
    try:
        workflow.graph([
            ("P1", "A"), ("P2", "Apeer"),
            ("A", "Apeer"), ("Apeer", "A"),
        ])
        topology = workflow.topology.snapshot()
        component = ("A", "Apeer")
        storage.register_component_topology(topology)
        for node in ("P1", "P2"):
            storage.create_job(Job(node_name=node, job_id=1, params={}))
        for node in component:
            for job_id in range(1, 4):
                storage.create_job(Job(node_name=node, job_id=job_id, params={}))

        _session(
            storage, topology, "main-parent", kind="main", selected=[("P1",)],
        )
        main_generation, main_execution = _begin_and_claim(
            storage, topology, "main-parent", ("P1",), "P1",
        )
        _session(
            storage, topology, "nested-parent", kind="interrupt", selected=[("P2",)],
        )
        nested_generation, nested_execution = _begin_and_claim(
            storage, topology, "nested-parent", ("P2",), "P2",
        )
        child = _admit_interrupt(
            storage, topology, "child-interrupt",
            selected=[component], predecessors=[("P1",), ("P2",)],
        )
        parents = ["main-parent", "nested-parent"]
        assert child["parent_session_ids"] == parents
        assert storage.acknowledge_interrupt_pauses(
            "main-parent", "P1", 1, main_generation, main_execution,
        ) == ("child-interrupt",)
        assert storage.acknowledge_interrupt_pauses(
            "nested-parent", "P2", 1, nested_generation, nested_execution,
        ) == ("child-interrupt",)
        _freeze_interrupt(storage, topology, "child-interrupt", component)
        storage.begin_queued_component_execution(
            "child-interrupt", component,
            expected_shape=topology.shape_json,
            expected_alignment_generation=0,
            successful_lineage=("unstable", "child-interrupt"),
        )
        for node in component:
            storage.claim_job_executions_batch(
                node, [1, 2, 3], started_at=now(),
                session_id="child-interrupt", component=component,
            )

        expected = storage.read_thread_override_observation("A")
        storage.set_thread_override("A", 13, expected=expected)
        assert storage.read_thread_override_observation("A") == {
            "node": "A", "value": 13, "session_id": "child-interrupt",
        }
        for node in component:
            for job_id in range(1, 4):
                observed = storage.read_job_owner_observation(node, job_id)
                assert observed["state"] == "active"
                assert observed["owner"]["session_id"] == "child-interrupt"
                assert observed["owner"]["component"] == component
                assert observed["session"]["session_id"] == "child-interrupt"
                assert observed["session"]["parent_session_ids"] == parents
    finally:
        _close_without_sidecars(storage, tmp_path)
