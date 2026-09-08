"""Public split/merge membership repair through full fresh preparation."""

from __future__ import annotations

from pathlib import Path
import time
import os

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_064_read_only_previews import _close_without_sidecars, _snapshot
from tests.test_090_component_session_settlement import _close
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows


def _graph(edges):
    return "EDGES = " + repr(edges)


def _producer_source(node):
    return f"""
from pathlib import Path
from micro_workflow_manager import NodeRouter
router = NodeRouter({node!r}, runner='direct')
router.create_job(number=1)
@router.task
def run(ctx):
    root = Path(ctx.system.storage.project_dir)
    with (root / 'component-executions.txt').open('a', encoding='utf-8') as stream:
        stream.write({node!r} + '\\n')
    if not (root / 'disable-membership-publications').exists():
        ctx.node('R').write_input('owned.txt', {node!r} + '-input')
        ctx.node('R').add(producer={node!r})
    ctx.write_output('result.txt', {node!r})
    return {node!r}
"""


TASKS = {
    "A": _producer_source("A"),
    "B": _producer_source("B"),
    "R": """
from micro_workflow_manager import NodeRouter
router = NodeRouter('R', runner='direct')
@router.task
def run(ctx, producer):
    ctx.write_output(f'{producer}-{ctx.job_id}.txt', producer)
    return producer
""",
    "U": """
from micro_workflow_manager import NodeRouter
router = NodeRouter('U', runner='direct')
router.create_job(number=1)
@router.task
def run(ctx):
    ctx.write_output('retained.txt', 'unrelated')
    return 'unrelated'
""",
    "Z": """
from micro_workflow_manager import NodeRouter
router = NodeRouter('Z', runner='direct')
@router.task
def run(ctx):
    return 'Z'
""",
}


def _membership_edges(direction):
    tail = [("A", "R"), ("B", "R"), ("U", "Z")]
    if direction == "split":
        return [("A", "B"), ("B", "A"), *tail], [("A", "B"), *tail]
    return [("A", "B"), *tail], [("A", "B"), ("B", "A"), *tail]


def _owner_rows(storage):
    return {
        row["execution_id"]: dict(row)
        for row in storage.db_connection().execute(
            "SELECT * FROM job_execution_owners ORDER BY execution_id"
        )
    }


def _close_finished_project(root):
    deadline = time.monotonic() + 10
    while any((root / '.mwf' / 'state_subscribers').glob('*.json')):
        assert time.monotonic() < deadline, 'Finished setup retained a notification listener'
        time.sleep(0.01)
    _close_without_sidecars(FileStorage(root), root)


def _establish_old_membership(tmp_path, monkeypatch, capsys, direction):
    old_edges, current_edges = _membership_edges(direction)
    make_project(
        tmp_path,
        monkeypatch,
        edges=_graph(old_edges),
        files=TASKS,
        runner="direct",
    )
    capsys.readouterr()
    if direction == "split":
        assert cli.main(["run", "A"]) == 0
    else:
        assert cli.main(["run", "A"]) == 0
        assert cli.main(["run", "B"]) == 0
    assert cli.main(["run", "R"]) == 0
    assert cli.main(["run", "U"]) == 0
    capsys.readouterr()

    storage = FileStorage(tmp_path)
    assert storage.get_component_state(("R",))["lifecycle"] == "done"
    assert storage.get_component_state(("U",))["lifecycle"] == "done"
    assert storage.read_component_misalignment_causes(("R",)) == []
    assert storage.list_job_ids("R") == [1, 2]
    jobs = {
        storage.load_job("R", job_id).params["producer"]: {
            "job_id": job_id,
            "job_instance_id": storage.read_job_instance_id("R", job_id),
            "created_by_execution_id": storage.read_job_current_owner(
                "R", job_id,
            )["created_by_execution_id"],
        }
        for job_id in storage.list_job_ids("R")
    }
    assert set(jobs) == {"A", "B"}
    inputs = {
        node: storage.read_node_input_owner("R", f"{node}/owned.txt")
        for node in ("A", "B")
    }
    assert all(owner is not None for owner in inputs.values())
    retained = {
        "jobs": jobs,
        "inputs": inputs,
        "owners": _owner_rows(storage),
        "receiver_state": storage.get_component_state(("R",)),
        "receiver_output": _snapshot(tmp_path / "node" / "R" / "output"),
        "unrelated_state": storage.get_component_state(("U",)),
        "unrelated_owner": storage.read_job_current_owner("U", 1),
        "unrelated_tree": _snapshot(tmp_path / "node" / "U"),
        "execution_log": (tmp_path / "component-executions.txt").read_bytes(),
        "sessions": storage.list_execution_sessions(),
    }
    (tmp_path / "disable-membership-publications").write_text(
        "suppress replacement publications", encoding="utf-8",
    )
    _close_without_sidecars(storage, tmp_path)

    graph_path = tmp_path / "src" / "graph.py"
    graph_path.write_text(_graph(current_edges) + "\n", encoding="utf-8")
    assert cli.main(["graph", "--update"]) == 0
    _close_finished_project(tmp_path)
    capsys.readouterr()
    return retained


def _assert_membership_plan(output, direction):
    assert "membership repair preparation:" in output
    assert "historical membership overlap:" in output
    if direction == "split":
        assert "membership repair preparation: {A}, {B}" in output
        assert "historical membership overlap: {A, B}" in output
    else:
        assert "membership repair preparation: {A, B}" in output
        assert "historical membership overlap: {A}, {B}" in output


@pytest.mark.parametrize("direction", ["split", "merge"])
@pytest.mark.parametrize("operation", ["reset", "run"])
def test_full_fresh_command_repairs_membership_without_expanding_execution(
    tmp_path, monkeypatch, capsys, direction, operation,
):
    retained = _establish_old_membership(
        tmp_path, monkeypatch, capsys, direction,
    )
    preview = (
        ["reset", "A", "--dry-run"]
        if operation == "reset"
        else ["run", "A", "--plan"]
    )
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main(preview) == 0

    plan = capsys.readouterr().out
    _assert_membership_plan(plan, direction)
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files
    assert not (tmp_path / ".mwf" / "state.sqlite3-wal").exists()
    assert not (tmp_path / ".mwf" / "state.sqlite3-shm").exists()

    arguments = (
        ["reset", "A", "--yes"]
        if operation == "reset"
        else ["run", "A", "--runner", "direct"]
    )
    assert cli.main(arguments) == 0

    applied = capsys.readouterr().out
    _assert_membership_plan(applied, direction)
    workflow = load_workflow(tmp_path)
    storage = workflow.storage
    try:
        expected_component = {"A"} if direction == "split" else {"A", "B"}
        assert workflow.component_for("A") == expected_component
        assert workflow.component_for("B") == (
            {"B"} if direction == "split" else expected_component
        )
        for node in ("A", "B"):
            assert not (storage.node_input_dir("R") / node / "owned.txt").exists()
            assert storage.read_node_input_owner("R", f"{node}/owned.txt") is None
        for item in retained["jobs"].values():
            assert not storage.job_exists("R", item["job_id"])
            assert not storage.job_base_dir("R", item["job_id"]).exists()

        receiver = storage.get_component_state(("R",))
        assert receiver == dict(retained["receiver_state"], misaligned=True)
        causes = storage.read_component_misalignment_causes(("R",))
        assert len(causes) == 1
        cause = causes[0]
        producer = cause["producer_node"]
        assert producer in {"A", "B"}
        expected_cause = {
            "component": ("R",),
            "receiver_node": "R",
            "alignment_generation": retained["receiver_state"]["alignment_generation"],
            "producer_node": producer,
            "producer_job_id": 1,
            "preparation_kind": "preparation-removal",
            "operation": operation,
            "action": "delete",
            "affected_kind": cause["affected_kind"],
        }
        if cause["affected_kind"] == "managed-input":
            expected_cause["path"] = f"{producer}/owned.txt"
        else:
            assert cause["affected_kind"] == "managed-job"
            expected_cause.update(
                job_id=retained["jobs"][producer]["job_id"],
                job_instance_id=retained["jobs"][producer]["job_instance_id"],
            )
        assert cause == expected_cause
        assert _snapshot(tmp_path / "node" / "R" / "output") == retained["receiver_output"]
        assert storage.get_component_state(("U",)) == retained["unrelated_state"]
        assert storage.read_job_current_owner("U", 1) == retained["unrelated_owner"]
        assert _snapshot(tmp_path / "node" / "U") == retained["unrelated_tree"]

        after_owners = _owner_rows(storage)
        assert retained["owners"].keys() <= after_owners.keys()
        for execution_id, owner in retained["owners"].items():
            assert after_owners[execution_id] == owner

        expected_append = b""
        if operation == "run":
            expected_nodes = ("A",) if direction == "split" else ("A", "B")
            expected_append = "".join(node + os.linesep for node in expected_nodes).encode()
        assert (tmp_path / "component-executions.txt").read_bytes() == (
            retained["execution_log"] + expected_append
        )
        if direction == "split":
            assert storage.get_component_state(("B",))["lifecycle"] == "queued"
            assert storage.read_job_current_owner("B", 1) is None
            assert storage.get_component_state(("A",))["lifecycle"] == (
                "queued" if operation == "reset" else "done"
            )
        else:
            assert storage.get_component_state(("A", "B"))["lifecycle"] == (
                "queued" if operation == "reset" else "done"
            )
        sessions = storage.list_execution_sessions()
        if operation == "reset":
            assert sessions == retained["sessions"]
        else:
            old_ids = {session["session_id"] for session in retained["sessions"]}
            added = [session for session in sessions if session["session_id"] not in old_ids]
            assert len(added) == 1
            assert added[0]["selected_components"] == [tuple(sorted(expected_component))]
            assert (added[0]["status"], added[0]["outcome"]) == ("terminal", "done")
    finally:
        _close(storage)
