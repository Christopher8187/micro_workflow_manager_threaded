"""Raw-node observers derive lifecycle from the current native component."""

import json

import pytest

from micro_workflow_manager import MicroWorkflow, cli
from micro_workflow_manager.cli.preview import PreviewStorage
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_149_interrupt_declarations import _router_source


def test_native_node_status_readers_ignore_divergent_scheduler_cache(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('A', 'B'), ('B', 'A')]",
        files={"A": _router_source("A", jobs=2), "B": _router_source("B", jobs=1)},
    )
    assert cli.main(["run", "A", "sample", "50%", "--seed", "state", "--runner", "direct"]) == 0
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    try:
        assert storage.get_component_state(("A", "B"))["lifecycle"] == "sampled"
        storage.set_node_statuses({"A": "done", "B": "waiting"})
        storage.db_mutation_barrier()
        assert {row["status"] for row in storage.db_connection().execute(
            "SELECT status FROM nodes WHERE node_name IN ('A','B')",
        )} == {"done", "waiting"}
        assert storage.get_node_status("A") == storage.get_node_status("B") == "sampled"
        assert storage.get_node_statuses(["B", "A", "absent"]) == {"A": "sampled", "B": "sampled"}
        assert storage.get_node_status("absent") is None
    finally:
        _close(storage)
    preview = PreviewStorage(tmp_path)
    try:
        assert preview.get_node_status("A") == preview.get_node_status("B") == "sampled"
    finally:
        preview.close()
    assert cli.main(["top", "--once", "--json"]) == 0
    snapshot = json.loads(capsys.readouterr().out)
    for row in snapshot["nodes"]:
        assert row["node_status"] == row["state"] == "sampled"
        assert row["component"] == ["A", "B"] and row["stability"] == "stable"
        assert row["misaligned"] is False and row["misalignment_causes"] == []
    assert cli.main(["top", "--once"]) == 0
    assert "state=sampled stability=stable" in capsys.readouterr().out


def test_native_readiness_observes_uninitialized_parents_without_creating_state(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "B")])
    storage = workflow.storage
    try:
        storage.db_mutation_barrier()
        before = _rows(storage)
        assert storage.get_component_state(("A",)) is None
        for _ in range(2):
            assert workflow.node_ready("A") is True
            assert workflow.node_ready("B") is False
            assert workflow.component_ready({"B"}) is False
        assert _rows(storage) == before
    finally:
        _close(storage)


def test_native_readiness_refuses_missing_state_for_an_established_parent(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "B")])
    storage = workflow.storage
    try:
        storage.register_component_topology(workflow.topology.snapshot())
        assert storage.submit_db_mutation(lambda connection: connection.execute(
            "DELETE FROM component_states WHERE component_key=?", ('["A"]',),
        ).rowcount) == 1
        before = _rows(storage)
        with pytest.raises(RuntimeError, match="Component definition has no current state"):
            workflow.node_ready("B")
        assert _rows(storage) == before
    finally:
        _close(storage)
