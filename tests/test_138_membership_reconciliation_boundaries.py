"""Public preservation and refusal boundaries for between-run membership."""

from __future__ import annotations

import sqlite3

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_064_read_only_previews import _close_without_sidecars, _snapshot
from tests.test_090_component_session_settlement import _close
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows
from tests.test_137_between_run_membership import (
    _close_finished_project,
    _establish_old_membership,
    _graph,
    _owner_rows,
)


UNCHANGED_TASKS = {
    "A": """
from micro_workflow_manager import NodeRouter
router = NodeRouter('A', runner='direct')
router.create_job(number=1)
@router.task
def run(ctx):
    ctx.write_output('retained.txt', 'A established result')
    return 'A'
""",
    "B": """
from micro_workflow_manager import NodeRouter
router = NodeRouter('B', runner='direct')
@router.task
def run(ctx):
    return 'B'
""",
    "U": """
from micro_workflow_manager import NodeRouter
router = NodeRouter('U', runner='direct')
@router.task
def run(ctx):
    return 'U'
""",
    "Z": """
from micro_workflow_manager import NodeRouter
router = NodeRouter('Z', runner='direct')
@router.task
def run(ctx):
    return 'Z'
""",
    "W": """
from micro_workflow_manager import NodeRouter
router = NodeRouter('W', runner='direct')
@router.task
def run(ctx):
    return 'W'
""",
}


def test_unrelated_graph_edit_preserves_unchanged_component_result_and_owner(
    tmp_path, monkeypatch, capsys,
):
    old_edges = [("A", "B"), ("U", "Z")]
    current_edges = [("A", "B"), ("U", "W")]
    make_project(
        tmp_path,
        monkeypatch,
        edges=_graph(old_edges),
        files=UNCHANGED_TASKS,
        runner="direct",
    )
    capsys.readouterr()
    assert cli.main(["run", "A"]) == 0
    capsys.readouterr()

    storage = FileStorage(tmp_path)
    before = {
        "state": storage.get_component_state(("A",)),
        "control": storage.read_job_control("A", 1),
        "owner": storage.read_job_current_owner("A", 1),
        "events": storage.read_job_events("A", 1),
        "tree": _snapshot(tmp_path / "node" / "A"),
        "owners": _owner_rows(storage),
    }
    _close_without_sidecars(storage, tmp_path)
    _close_finished_project(tmp_path)
    (tmp_path / "src" / "graph.py").write_text(
        _graph(current_edges) + "\n", encoding="utf-8",
    )

    assert cli.main(["graph", "--update"]) == 0
    capsys.readouterr()
    preview_rows = _closed_database_rows(tmp_path)
    preview_files = _snapshot(tmp_path)
    assert cli.main(["resume", "A", "--plan"]) == 0
    preview = capsys.readouterr().out
    assert "membership repair preparation:" not in preview
    assert "membership" not in "\n".join(
        line for line in preview.splitlines() if "would refuse:" in line
    )
    assert _closed_database_rows(tmp_path) == preview_rows
    assert _snapshot(tmp_path) == preview_files

    assert cli.main(["resume", "A"]) == 0

    workflow = load_workflow(tmp_path)
    storage = workflow.storage
    try:
        assert workflow.component_for("A") == {"A"}
        assert storage.get_component_state(("A",)) == before["state"]
        assert storage.read_job_control("A", 1) == before["control"]
        assert storage.read_job_current_owner("A", 1) == before["owner"]
        assert storage.read_job_events("A", 1) == before["events"]
        assert _snapshot(tmp_path / "node" / "A") == before["tree"]
        assert _owner_rows(storage) == before["owners"]
    finally:
        _close(storage)


EMPTY_TASKS = {
    node: f"""
from micro_workflow_manager import NodeRouter
router = NodeRouter({node!r}, runner='direct')
@router.task
def run(ctx):
    return {node!r}
"""
    for node in ("A", "B", "U", "Z")
}


def test_no_history_membership_change_reconciles_without_expansion_or_data_loss(
    tmp_path, monkeypatch, capsys,
):
    old_edges = [("A", "B"), ("B", "A"), ("U", "Z")]
    current_edges = [("A", "B"), ("U", "Z")]
    make_project(
        tmp_path,
        monkeypatch,
        edges=_graph(old_edges),
        files=EMPTY_TASKS,
        runner="direct",
    )
    workflow = load_workflow(tmp_path)
    storage = workflow.storage
    storage.register_component_topology(workflow.topology.snapshot())
    assert storage.get_component_state(("A", "B"))["lifecycle"] == "queued"
    for node in ("A", "B"):
        path = storage.node_input_dir(node) / "project-owned.txt"
        path.write_text(f"retained {node}", encoding="utf-8")
    _close_without_sidecars(storage, tmp_path)

    (tmp_path / "src" / "graph.py").write_text(
        _graph(current_edges) + "\n", encoding="utf-8",
    )
    assert cli.main(["graph", "--update"]) == 0
    capsys.readouterr()
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)

    assert cli.main(["reset", "A", "--dry-run"]) == 0

    preview = capsys.readouterr().out
    assert "selected Hoeflein components: {A}" in preview
    assert "historical membership overlap:" not in preview
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files

    assert cli.main(["reset", "A", "--yes"]) == 0

    workflow = load_workflow(tmp_path)
    storage = workflow.storage
    try:
        assert workflow.component_for("A") == {"A"}
        assert workflow.component_for("B") == {"B"}
        state_a = storage.get_component_state(("A",))
        state_b = storage.get_component_state(("B",))
        assert (state_a["lifecycle"], state_a["alignment_generation"]) == ("queued", 1)
        assert (state_b["lifecycle"], state_b["alignment_generation"]) == ("queued", 0)
        assert storage.list_job_ids("A") == []
        assert storage.list_job_ids("B") == []
        for node in ("A", "B"):
            assert (storage.node_input_dir(node) / "project-owned.txt").read_text(
                encoding="utf-8",
            ) == f"retained {node}"
        assert storage.read_component_misalignment_causes(("A",)) == []
        assert storage.read_component_misalignment_causes(("B",)) == []
    finally:
        _close(storage)


@pytest.mark.parametrize("arguments", [
    ["resume", "A"],
    ["run", "A", "job", "1"],
])
def test_nonfresh_execution_refuses_changed_membership_before_mutation(
    tmp_path, monkeypatch, capsys, arguments,
):
    _establish_old_membership(tmp_path, monkeypatch, capsys, "split")
    before_rows = _closed_database_rows(tmp_path)
    before_nodes = _snapshot(tmp_path / "node")
    before_log = (tmp_path / "component-executions.txt").read_bytes()
    capsys.readouterr()

    assert cli.main(arguments) == 1

    captured = capsys.readouterr()
    text = (captured.out + captured.err).lower()
    assert "membership" in text
    assert "mwf reset a" in text
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path / "node") == before_nodes
    assert (tmp_path / "component-executions.txt").read_bytes() == before_log


def test_damaged_native_historical_shape_refuses_without_reconstruction(
    tmp_path, monkeypatch, capsys,
):
    _establish_old_membership(tmp_path, monkeypatch, capsys, "split")
    database = tmp_path / ".mwf" / "state.sqlite3"
    connection = sqlite3.connect(database)
    try:
        trigger_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' "
            "AND name='prevent_graph_shape_update'",
        ).fetchone()
        assert trigger_sql is not None and trigger_sql[0]
        connection.execute("DROP TRIGGER prevent_graph_shape_update")
        row = connection.execute(
            "SELECT shape_id FROM component_definitions WHERE component_key=?",
            (encode_component_key(("A", "B")),),
        ).fetchone()
        assert row is not None
        connection.execute(
            "UPDATE graph_shapes SET shape_json='{}' WHERE shape_id=?",
            (row[0],),
        )
        connection.execute(trigger_sql[0])
        connection.commit()
    finally:
        connection.close()
    before_rows = _closed_database_rows(tmp_path)
    before_nodes = _snapshot(tmp_path / "node")
    capsys.readouterr()

    assert cli.main(["run", "A", "--plan"]) == 1

    captured = capsys.readouterr()
    text = (captured.out + captured.err).lower()
    assert "damaged" in text or "shape" in text
    assert "reconstruct" not in text
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path / "node") == before_nodes
