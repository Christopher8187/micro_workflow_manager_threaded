"""Deletion-only membership reconciliation retains native history and user work."""

from __future__ import annotations

import os
import socket

import networkx as nx
import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.models import now
from micro_workflow_manager.processes import process_identity
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.component_membership import read_active_component_partition
from micro_workflow_manager.topology import ComponentTopology
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_064_read_only_previews import _snapshot
from tests.test_090_component_session_settlement import _close
from tests.test_121_native_preview_recovery import _rows
from tests.test_137_between_run_membership import _close_finished_project, _graph, _owner_rows


TASKS = {
    "A": """
from micro_workflow_manager import NodeRouter
router = NodeRouter('A', runner='direct')
router.create_job(number=1)
@router.task
def run(ctx):
    ctx.write_output('result.txt', 'A')
    return 'A'
""",
    **{
        node: f"""
from micro_workflow_manager import NodeRouter
router = NodeRouter({node!r}, runner='direct')
{'router.create_job(number=1)' if node == 'U' else ''}
@router.task
def run(ctx):
    return {node!r}
"""
        for node in ("B", "U", "V")
    },
}


OLD_EDGES = [("A", "B"), ("B", "A"), ("U", "V")]
REMOVED_COMPONENT = ("A", "B")


def _target_snapshot():
    return ComponentTopology(nx.DiGraph([("U", "V")]), []).snapshot()


def _component_rows(storage, table, component):
    key = encode_component_key(component)
    return [
        tuple(row)
        for row in storage.db_connection().execute(
            f"SELECT * FROM {table} WHERE component_key=? ORDER BY rowid",
            (key,),
        )
    ]


def _establish_removed_component(tmp_path, monkeypatch, capsys, *, reusable):
    make_project(
        tmp_path,
        monkeypatch,
        edges=_graph(OLD_EDGES),
        files=TASKS,
        runner="direct",
    )
    capsys.readouterr()
    assert cli.main(["run", "A", "--runner", "direct"]) == 0
    capsys.readouterr()
    if not reusable:
        assert cli.main(["reset", "A", "--yes"]) == 0
        capsys.readouterr()
    _close_finished_project(tmp_path)

    storage = FileStorage(tmp_path)
    state = storage.get_component_state(REMOVED_COMPONENT)
    assert state["lifecycle"] == ("done" if reusable else "queued")
    current_owner = storage.read_job_current_owner("A", 1)
    assert (current_owner is not None) is reusable
    results = _component_rows(storage, "component_successful_results", REMOVED_COMPONENT)
    assert results
    definitions = _component_rows(storage, "component_definitions", REMOVED_COMPONENT)
    assert definitions
    (storage.node_input_dir("A") / "unowned.txt").write_text(
        "removed component user input", encoding="utf-8",
    )
    (storage.node_input_dir("U") / "unowned.txt").write_text(
        "unrelated user input", encoding="utf-8",
    )
    (tmp_path / "user-data.txt").write_text("project user data", encoding="utf-8")
    active = read_active_component_partition(storage.db_connection())
    assert active is not None
    assert REMOVED_COMPONENT in {item.members for item in active.components}
    retained = {
        "state": state,
        "active": active,
        "job": storage.load_job("A", 1),
        "job_status": storage.read_job_status_data("A", 1),
        "job_instance": storage.read_job_instance_id("A", 1),
        "current_owner": current_owner,
        "owners": _owner_rows(storage),
        "results": results,
        "definitions": definitions,
        "a_tree": _snapshot(tmp_path / "node" / "A"),
        "u_job": storage.load_job("U", 1),
        "u_status": storage.read_job_status_data("U", 1),
        "u_instance": storage.read_job_instance_id("U", 1),
        "u_tree": _snapshot(tmp_path / "node" / "U"),
        "user_data": (tmp_path / "user-data.txt").read_bytes(),
    }
    return storage, retained


def _assert_retained(storage, tmp_path, retained):
    assert storage.get_component_state(REMOVED_COMPONENT) == retained["state"]
    assert storage.load_job("A", 1) == retained["job"]
    assert storage.read_job_status_data("A", 1) == retained["job_status"]
    assert storage.read_job_instance_id("A", 1) == retained["job_instance"]
    assert storage.read_job_current_owner("A", 1) == retained["current_owner"]
    assert _owner_rows(storage) == retained["owners"]
    assert _component_rows(
        storage, "component_successful_results", REMOVED_COMPONENT,
    ) == retained["results"]
    assert _component_rows(
        storage, "component_definitions", REMOVED_COMPONENT,
    ) == retained["definitions"]
    assert _snapshot(tmp_path / "node" / "A") == retained["a_tree"]
    assert storage.load_job("U", 1) == retained["u_job"]
    assert storage.read_job_status_data("U", 1) == retained["u_status"]
    assert storage.read_job_instance_id("U", 1) == retained["u_instance"]
    assert _snapshot(tmp_path / "node" / "U") == retained["u_tree"]
    assert (tmp_path / "user-data.txt").read_bytes() == retained["user_data"]


def test_registration_removes_only_unproduced_absent_active_membership(
    tmp_path, monkeypatch, capsys,
):
    storage, retained = _establish_removed_component(
        tmp_path, monkeypatch, capsys, reusable=False,
    )
    try:
        assert storage.register_component_topology(_target_snapshot()) is True

        active = read_active_component_partition(storage.db_connection())
        assert active is not None
        assert active.revision == retained["active"].revision + 1
        assert {item.members for item in active.components} == {("U",), ("V",)}
        _assert_retained(storage, tmp_path, retained)
        assert storage.read_job_current_owner("A", 1) is None
    finally:
        _close(storage)


def test_registration_refuses_deletion_under_overlapping_interrupt_without_mutation(
    tmp_path, monkeypatch, capsys,
):
    storage, retained = _establish_removed_component(
        tmp_path, monkeypatch, capsys, reusable=False,
    )
    session_id = "removed-region-interrupt"
    pid = os.getpid()
    storage.create_execution_session(
        session_id,
        session_kind="interrupt",
        command="run",
        start_component=REMOVED_COMPONENT,
        selected_components=[REMOVED_COMPONENT],
        started_at=now(),
        hostname=socket.gethostname(),
        pid=pid,
        process_identity=process_identity(pid),
        expected_shape=retained["state"]["shape_json"],
    )
    assert storage.reserve_execution_components(
        session_id,
        expected_shape=retained["state"]["shape_json"],
    ) is True
    session = storage.get_execution_session(session_id)
    rows = _rows(storage)
    files = _snapshot(tmp_path / "node")

    try:
        with pytest.raises(RuntimeError, match="[Mm]embership"):
            storage.register_component_topology(_target_snapshot())

        assert _rows(storage) == rows
        assert _snapshot(tmp_path / "node") == files
        assert storage.get_execution_session(session_id) == session
        assert storage.get_component_reservation(REMOVED_COMPONENT) == {
            "members": REMOVED_COMPONENT,
            "session_id": session_id,
        }
        _assert_retained(storage, tmp_path, retained)
    finally:
        _close(storage)


def test_registration_keeps_entirely_removed_membership_with_reusable_work(
    tmp_path, monkeypatch, capsys,
):
    storage, retained = _establish_removed_component(
        tmp_path, monkeypatch, capsys, reusable=True,
    )
    try:
        assert storage.register_component_topology(_target_snapshot()) is True

        active = read_active_component_partition(storage.db_connection())
        assert active == retained["active"]
        assert REMOVED_COMPONENT in {item.members for item in active.components}
        _assert_retained(storage, tmp_path, retained)
        assert storage.read_job_current_owner("A", 1) is not None
    finally:
        _close(storage)
