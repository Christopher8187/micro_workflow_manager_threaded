"""Regional between-run membership repair and commit boundaries."""

from __future__ import annotations

from pathlib import Path
import os

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage import component_membership as membership_storage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_064_read_only_previews import _close_without_sidecars, _snapshot
from tests.test_090_component_session_settlement import _close
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows
from tests.test_137_between_run_membership import (
    TASKS,
    _establish_old_membership,
    _close_finished_project,
    _graph,
    _owner_rows,
)


def _producer_source(node, receiver):
    return f"""
from pathlib import Path
from micro_workflow_manager import NodeRouter
router = NodeRouter({node!r}, runner='direct')
router.create_job(number=1)
@router.task
def run(ctx):
    root = Path(ctx.system.storage.project_dir)
    with (root / 'regional-executions.txt').open('a', encoding='utf-8') as stream:
        stream.write({node!r} + '\\n')
    if not (root / 'disable-membership-publications').exists():
        ctx.node({receiver!r}).write_input('owned.txt', {node!r} + '-input')
        ctx.node({receiver!r}).add(producer={node!r})
    ctx.write_output('result.txt', {node!r})
    return {node!r}
"""


def _receiver_source(node):
    return f"""
from micro_workflow_manager import NodeRouter
router = NodeRouter({node!r}, runner='direct')
@router.task
def run(ctx, producer):
    ctx.write_output(f'{{producer}}-{{ctx.job_id}}.txt', producer)
    return producer
"""


DUAL_TASKS = {
    "A": _producer_source("A", "R"),
    "B": _producer_source("B", "R"),
    "X": _producer_source("X", "S"),
    "Y": _producer_source("Y", "S"),
    "R": _receiver_source("R"),
    "S": _receiver_source("S"),
    "U": """
from pathlib import Path
from micro_workflow_manager import NodeRouter
router = NodeRouter('U', runner='direct')
router.create_job(number=1)
@router.task
def run(ctx):
    root = Path(ctx.system.storage.project_dir)
    with (root / 'regional-executions.txt').open('a', encoding='utf-8') as stream:
        stream.write('U\\n')
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


def _dual_edges(split):
    ab = [("A", "B")] if split else [("A", "B"), ("B", "A")]
    xy = [("X", "Y")] if split else [("X", "Y"), ("Y", "X")]
    return [
        *ab,
        *xy,
        ("A", "R"),
        ("B", "R"),
        ("X", "S"),
        ("Y", "S"),
        ("U", "Z"),
    ]


def _job_snapshot(storage, nodes):
    result = {}
    for node in nodes:
        result[node] = {}
        for job_id in storage.list_job_ids(node):
            job = storage.load_job(node, job_id)
            result[node][job_id] = {
                "params": job.params,
                "parent": job.parent,
                "control": storage.read_job_control(node, job_id),
                "status": storage.read_job_status_data(node, job_id),
                "owner": storage.read_job_current_owner(node, job_id),
                "events": storage.read_job_events(node, job_id),
                "tree": _snapshot(storage.job_base_dir(node, job_id)),
            }
    return result


def _active_component(storage, node):
    active = membership_storage.read_active_component_partition(storage.db_connection())
    assert active is not None
    return next((item.members for item in active.components if node in item.members), None)


def _region_snapshot(storage, root, component, receiver):
    nodes = tuple(component) + (receiver,)
    owner_ids = {
        item["owner"]["execution_id"]
        for jobs in _job_snapshot(storage, nodes).values()
        for item in jobs.values()
        if item["owner"] is not None
    }
    all_owners = _owner_rows(storage)
    return {
        "component_state": storage.get_component_state(component),
        "receiver_state": storage.get_component_state((receiver,)),
        "receiver_causes": storage.read_component_misalignment_causes((receiver,)),
        "jobs": _job_snapshot(storage, nodes),
        "inputs": {
            node: storage.read_node_input_owner(receiver, f"{node}/owned.txt")
            for node in component
        },
        "trees": {node: _snapshot(root / "node" / node) for node in nodes},
        "owners": {owner_id: all_owners[owner_id] for owner_id in owner_ids},
    }


def _establish_two_old_regions(tmp_path, monkeypatch, capsys):
    make_project(
        tmp_path,
        monkeypatch,
        edges=_graph(_dual_edges(False)),
        files=DUAL_TASKS,
        runner="direct",
    )
    capsys.readouterr()
    for node in ("A", "R", "X", "S", "U"):
        assert cli.main(["run", node]) == 0
        capsys.readouterr()

    storage = FileStorage(tmp_path)
    assert storage.get_component_state(("A", "B"))["lifecycle"] == "done"
    assert storage.get_component_state(("X", "Y"))["lifecycle"] == "done"
    assert storage.get_component_state(("R",))["lifecycle"] == "done"
    assert storage.get_component_state(("S",))["lifecycle"] == "done"
    retained = {
        "ab": _region_snapshot(storage, tmp_path, ("A", "B"), "R"),
        "xy": _region_snapshot(storage, tmp_path, ("X", "Y"), "S"),
        "u_state": storage.get_component_state(("U",)),
        "u_owner": storage.read_job_current_owner("U", 1),
        "u_tree": _snapshot(tmp_path / "node" / "U"),
        "owners": _owner_rows(storage),
        "sessions": storage.list_execution_sessions(),
        "log": (tmp_path / "regional-executions.txt").read_bytes(),
    }
    (tmp_path / "disable-membership-publications").write_text(
        "suppress replacement publications", encoding="utf-8",
    )
    _close_without_sidecars(storage, tmp_path)

    (tmp_path / "src" / "graph.py").write_text(
        _graph(_dual_edges(True)) + "\n", encoding="utf-8",
    )
    assert cli.main(["graph", "--update"]) == 0
    _close_finished_project(tmp_path)
    capsys.readouterr()
    return retained


def _repair_line(output):
    lines = [
        line.strip()
        for line in output.splitlines()
        if line.strip().startswith("membership repair preparation:")
    ]
    assert len(lines) == 1
    return lines[0]


def test_independent_membership_regions_repair_and_reuse_separately(
    tmp_path, monkeypatch, capsys,
):
    retained = _establish_two_old_regions(tmp_path, monkeypatch, capsys)
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)

    assert cli.main(["reset", "A", "--dry-run"]) == 0

    preview = capsys.readouterr().out
    assert _repair_line(preview) == "membership repair preparation: {A}, {B}"
    assert "{X}" not in _repair_line(preview) and "{Y}" not in _repair_line(preview)
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files

    assert cli.main(["reset", "A", "--yes"]) == 0

    workflow = load_workflow(tmp_path)
    storage = workflow.storage
    try:
        assert _active_component(storage, "A") == ("A",)
        assert _active_component(storage, "B") == ("B",)
        assert _active_component(storage, "X") == ("X", "Y")
        assert _active_component(storage, "Y") == ("X", "Y")
        assert _region_snapshot(storage, tmp_path, ("X", "Y"), "S") == retained["xy"]
        assert storage.get_component_state(("U",)) == retained["u_state"]
        assert storage.read_job_current_owner("U", 1) == retained["u_owner"]
        assert _snapshot(tmp_path / "node" / "U") == retained["u_tree"]
        for node in ("A", "B"):
            assert storage.read_node_input_owner("R", f"{node}/owned.txt") is None
        assert storage.get_component_state(("R",))["misaligned"] is True
    finally:
        _close_without_sidecars(storage, tmp_path)

    assert cli.main(["run", "A", "--runner", "direct"]) == 0
    capsys.readouterr()
    workflow = load_workflow(tmp_path)
    storage = workflow.storage
    try:
        repaired_ab = {
            "state_a": storage.get_component_state(("A",)),
            "state_b": storage.get_component_state(("B",)),
            "jobs": _job_snapshot(storage, ("A", "B")),
            "trees": {
                node: _snapshot(tmp_path / "node" / node) for node in ("A", "B")
            },
        }
        assert repaired_ab["state_a"]["lifecycle"] == "done"
        assert repaired_ab["state_b"]["lifecycle"] == "queued"
        assert _region_snapshot(storage, tmp_path, ("X", "Y"), "S") == retained["xy"]
        _close_without_sidecars(storage, tmp_path)
    except BaseException:
        _close(storage)
        raise

    assert cli.main(["reset", "X", "--dry-run"]) == 0
    preview = capsys.readouterr().out
    assert _repair_line(preview) == "membership repair preparation: {X}, {Y}"
    assert "{A}" not in _repair_line(preview) and "{B}" not in _repair_line(preview)
    assert cli.main(["reset", "X", "--yes"]) == 0

    workflow = load_workflow(tmp_path)
    storage = workflow.storage
    try:
        for node in ("A", "B", "X", "Y"):
            assert _active_component(storage, node) == (node,)
        assert storage.get_component_state(("A",)) == repaired_ab["state_a"]
        assert storage.get_component_state(("B",)) == repaired_ab["state_b"]
        assert _job_snapshot(storage, ("A", "B")) == repaired_ab["jobs"]
        assert {
            node: _snapshot(tmp_path / "node" / node) for node in ("A", "B")
        } == repaired_ab["trees"]
        for node in ("X", "Y"):
            assert storage.read_node_input_owner("S", f"{node}/owned.txt") is None
        after_owners = _owner_rows(storage)
        for execution_id, owner in retained["owners"].items():
            assert after_owners[execution_id] == owner
        assert (tmp_path / "regional-executions.txt").read_bytes() == (
            retained["log"] + b"A" + os.linesep.encode()
        )
    finally:
        _close(storage)


def test_later_membership_preparation_failure_keeps_region_unresolved_for_retry(
    tmp_path, monkeypatch, capsys,
):
    retained = _establish_old_membership(tmp_path, monkeypatch, capsys, "split")
    storage = FileStorage(tmp_path)
    b_job = retained["jobs"]["B"]["job_id"]
    b_job_dir = storage.job_base_dir("R", b_job)
    b_job_before = _snapshot(b_job_dir)
    b_input = storage.node_input_dir("R") / "B" / "owned.txt"
    b_input_before = b_input.read_bytes()
    b_owner_before = storage.read_node_input_owner("R", "B/owned.txt")
    owners_before = _owner_rows(storage)
    sessions_before = storage.list_execution_sessions()
    _close(storage)

    preparation_trash = tmp_path / ".mwf" / "preparation-trash"
    rename = Path.rename
    injected = False

    def fail_b_unit(path, target):
        nonlocal injected
        target = Path(target)
        if (
            not injected
            and path == b_job_dir
            and target.is_relative_to(preparation_trash)
        ):
            injected = True
            raise OSError("later membership preparation unit failed")
        return rename(path, target)

    monkeypatch.setattr(Path, "rename", fail_b_unit)
    assert cli.main(["reset", "A", "--yes"]) == 1
    captured = capsys.readouterr()
    assert "later membership preparation unit failed" in captured.err
    assert injected

    storage = FileStorage(tmp_path)
    try:
        assert _active_component(storage, "A") == ("A", "B")
        assert _active_component(storage, "B") == ("A", "B")
        assert storage.read_node_input_owner("R", "A/owned.txt") is None
        assert not (storage.node_input_dir("R") / "A" / "owned.txt").exists()
        assert storage.read_node_input_owner("R", "B/owned.txt") == b_owner_before
        assert b_input.read_bytes() == b_input_before
        assert _snapshot(b_job_dir) == b_job_before
        assert _owner_rows(storage) == owners_before
        assert storage.list_execution_sessions() == sessions_before
        assert storage.get_component_state(("R",))["misaligned"] is True
        _close_without_sidecars(storage, tmp_path)
    except BaseException:
        _close(storage)
        raise

    failed_rows = _closed_database_rows(tmp_path)
    failed_files = _snapshot(tmp_path)
    assert cli.main(["run", "A", "--plan"]) == 0
    assert _repair_line(capsys.readouterr().out) == (
        "membership repair preparation: {A}, {B}"
    )
    assert _closed_database_rows(tmp_path) == failed_rows
    assert _snapshot(tmp_path) == failed_files

    assert cli.main(["reset", "A", "--yes"]) == 0
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    try:
        for node in ("A", "B"):
            assert _active_component(storage, node) == (node,)
            assert storage.read_node_input_owner("R", f"{node}/owned.txt") is None
        assert _owner_rows(storage) == owners_before
        _close_without_sidecars(storage, tmp_path)
    except BaseException:
        _close(storage)
        raise
    assert cli.main(["run", "A", "--plan"]) == 0
    assert "membership repair preparation:" not in capsys.readouterr().out


def test_final_membership_switch_failure_never_exposes_partial_active_mapping(
    tmp_path, monkeypatch, capsys,
):
    retained = _establish_old_membership(tmp_path, monkeypatch, capsys, "split")
    original = membership_storage._replace_active_component_closure
    injected = False

    def fail_final_switch(connection, change):
        nonlocal injected
        injected = True
        raise OSError("regional membership switch failed")

    monkeypatch.setattr(
        membership_storage, "_replace_active_component_closure", fail_final_switch,
    )
    assert cli.main(["reset", "A", "--yes"]) == 1
    captured = capsys.readouterr()
    assert "regional membership switch failed" in captured.err
    assert injected

    storage = FileStorage(tmp_path)
    try:
        assert _active_component(storage, "A") == ("A", "B")
        assert _active_component(storage, "B") == ("A", "B")
        for node in ("A", "B"):
            assert storage.read_node_input_owner("R", f"{node}/owned.txt") is None
            assert not (storage.node_input_dir("R") / node / "owned.txt").exists()
        after_owners = _owner_rows(storage)
        for execution_id, owner in retained["owners"].items():
            assert after_owners[execution_id] == owner
        assert storage.list_execution_sessions() == retained["sessions"]
        _close_without_sidecars(storage, tmp_path)
    except BaseException:
        _close(storage)
        raise

    failed_rows = _closed_database_rows(tmp_path)
    failed_files = _snapshot(tmp_path)
    assert cli.main(["run", "A", "--plan"]) == 0
    assert _repair_line(capsys.readouterr().out) == (
        "membership repair preparation: {A}, {B}"
    )
    assert _closed_database_rows(tmp_path) == failed_rows
    assert _snapshot(tmp_path) == failed_files

    monkeypatch.setattr(
        membership_storage, "_replace_active_component_closure", original,
    )
    assert cli.main(["reset", "A", "--yes"]) == 0
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    try:
        assert _active_component(storage, "A") == ("A",)
        assert _active_component(storage, "B") == ("B",)
        _close_without_sidecars(storage, tmp_path)
    except BaseException:
        _close(storage)
        raise
    assert cli.main(["run", "A", "--plan"]) == 0
    assert "membership repair preparation:" not in capsys.readouterr().out


def test_removed_historical_member_publications_are_prepared_from_immutable_owners(
    tmp_path, monkeypatch, capsys,
):
    old_edges = [
        ("A", "B"),
        ("B", "A"),
        ("A", "R"),
        ("B", "R"),
        ("U", "Z"),
    ]
    current_edges = [("A", "R"), ("U", "Z")]
    make_project(
        tmp_path,
        monkeypatch,
        edges=_graph(old_edges),
        files=TASKS,
        runner="direct",
    )
    capsys.readouterr()
    for node in ("A", "R", "U"):
        assert cli.main(["run", node]) == 0
        capsys.readouterr()

    storage = FileStorage(tmp_path)
    jobs = {
        storage.load_job("R", job_id).params["producer"]: job_id
        for job_id in storage.list_job_ids("R")
    }
    assert set(jobs) == {"A", "B"}
    owners_before = _owner_rows(storage)
    b_created_by = storage.read_job_current_owner("R", jobs["B"])[
        "created_by_execution_id"
    ]
    assert owners_before[b_created_by]["node_name"] == "B"
    receiver_output = _snapshot(storage.node_output_dir("R"))
    unrelated = {
        "state": storage.get_component_state(("U",)),
        "owner": storage.read_job_current_owner("U", 1),
        "tree": _snapshot(tmp_path / "node" / "U"),
    }
    (tmp_path / "disable-membership-publications").write_text(
        "suppress replacement publications", encoding="utf-8",
    )
    _close_without_sidecars(storage, tmp_path)

    (tmp_path / "src" / "graph.py").write_text(
        _graph(current_edges) + "\n", encoding="utf-8",
    )
    assert cli.main(["graph", "--update"]) == 0
    _close_finished_project(tmp_path)
    capsys.readouterr()
    assert not (tmp_path / "node" / "B").exists()

    storage = FileStorage(tmp_path)
    try:
        assert _owner_rows(storage)[b_created_by] == owners_before[b_created_by]
        assert _active_component(storage, "A") == ("A", "B")
        assert _active_component(storage, "B") == ("A", "B")
        _close_without_sidecars(storage, tmp_path)
    except BaseException:
        _close(storage)
        raise
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)
    assert cli.main(["reset", "A", "--dry-run"]) == 0
    preview = capsys.readouterr().out
    assert _repair_line(preview) == "membership repair preparation: {A}"
    assert "historical membership overlap: {A, B}" in preview
    assert "removed historical members: B" in preview
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files

    assert cli.main(["reset", "A", "--yes"]) == 0
    workflow = load_workflow(tmp_path)
    storage = workflow.storage
    try:
        assert _active_component(storage, "A") == ("A",)
        assert _active_component(storage, "B") is None
        for node in ("A", "B"):
            assert storage.read_node_input_owner("R", f"{node}/owned.txt") is None
            assert not (storage.node_input_dir("R") / node / "owned.txt").exists()
        for job_id in jobs.values():
            assert not storage.job_exists("R", job_id)
        assert _snapshot(storage.node_output_dir("R")) == receiver_output
        assert storage.get_component_state(("R",))["misaligned"] is True
        assert storage.get_component_state(("U",)) == unrelated["state"]
        assert storage.read_job_current_owner("U", 1) == unrelated["owner"]
        assert _snapshot(tmp_path / "node" / "U") == unrelated["tree"]
        after_owners = _owner_rows(storage)
        for execution_id, owner in owners_before.items():
            assert after_owners[execution_id] == owner
        assert not (tmp_path / "node" / "B").exists()
    finally:
        _close(storage)
