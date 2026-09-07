"""Read-only graph previews distinguish valid busy work from damaged state."""

from __future__ import annotations

import os
import socket

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.models import now
from micro_workflow_manager.processes import process_identity
from tests.test_064_read_only_previews import (
    _close_without_sidecars,
    _initialize_native_project,
    _install_import_sentinels,
    _snapshot,
    _wait_restart_listener_retired,
)


GRAPH_PREVIEWS = (
    ("run", ("A", "--plan")),
    ("runfrom", ("A", "--plan")),
    ("runbetween", ("A", "B", "--plan")),
    ("resume", ("A", "--plan")),
    ("resumefrom", ("A", "--plan")),
    ("resumebetween", ("A", "B", "--plan")),
    ("reset", ("A", "--dry-run")),
    ("resetfrom", ("A", "--dry-run")),
    ("resetbetween", ("A", "B", "--dry-run")),
)


def _create_live_session(storage, shape, component, *, session_id):
    storage.create_execution_session(
        session_id,
        session_kind="main",
        command="run",
        start_component=component,
        selected_components=[component],
        started_at=now(),
        hostname=socket.gethostname(),
        pid=os.getpid(),
        process_identity=process_identity(os.getpid()),
    )
    storage.reserve_execution_components(session_id, expected_shape=shape)


@pytest.mark.parametrize("command,arguments", GRAPH_PREVIEWS)
@pytest.mark.parametrize("activity", ("reservation", "hold", "pending"))
def test_graph_preview_reports_valid_selected_component_activity_without_changes(
    tmp_path, monkeypatch, capsys, command, arguments, activity,
):
    edges = [("A", "B")]
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    storage = workflow.storage
    shape = workflow.topology.snapshot().shape_json
    _create_live_session(storage, shape, ("A",), session_id="preview-busy")
    if activity == "hold":
        assert storage.acquire_component_holds("preview-busy", [("A",)]) == {
            ("A",): 1,
        }
    elif activity == "pending":
        storage.begin_queued_component_execution(
            "preview-busy",
            ("A",),
            expected_shape=shape,
            expected_alignment_generation=0,
            successful_lineage=("stable", None),
        )
    _close_without_sidecars(storage, tmp_path)
    sentinel = _install_import_sentinels(tmp_path, edges)
    before = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main([command, *arguments]) == 0

    output = capsys.readouterr().out
    assert "would refuse: component ('A',) is reserved by main session preview-busy" in output
    if activity == "hold":
        assert "would refuse: component ('A',) has 1 hold(s) from session preview-busy" in output
    elif activity == "pending":
        assert "would refuse: component ('A',) has a pending full execution in session preview-busy" in output
    assert "user code was not loaded" in output
    assert _snapshot(tmp_path) == before
    assert not sentinel.exists()


@pytest.mark.parametrize("damage", ("pending-owner", "pending-state", "hold-owner"))
def test_graph_preview_refuses_damaged_busy_links_without_changes(
    tmp_path, monkeypatch, capsys, damage,
):
    edges = [("A", "B")]
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    storage = workflow.storage
    shape = workflow.topology.snapshot().shape_json
    _create_live_session(storage, shape, ("A",), session_id="damaged-busy")
    if damage == "hold-owner":
        storage.acquire_component_holds("damaged-busy", [("A",)])
        storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE execution_sessions SET status='terminal', outcome='failed', "
            "finished_at=? WHERE session_id=?",
            (now(), "damaged-busy"),
        ))
        storage.submit_db_mutation(lambda connection: connection.execute(
            "DELETE FROM component_reservations WHERE component_key=?",
            (encode_component_key(("A",)),),
        ))
    else:
        storage.begin_queued_component_execution(
            "damaged-busy",
            ("A",),
            expected_shape=shape,
            expected_alignment_generation=0,
            successful_lineage=("stable", None),
        )
        if damage == "pending-owner":
            storage.submit_db_mutation(lambda connection: connection.execute(
                "DELETE FROM component_reservations WHERE component_key=?",
                (encode_component_key(("A",)),),
            ))
        else:
            storage.submit_db_mutation(lambda connection: connection.execute(
                "UPDATE component_states SET lifecycle='queued', stability=NULL, "
                "instability_origin=NULL WHERE component_key=?",
                (encode_component_key(("A",)),),
            ))
    _close_without_sidecars(storage, tmp_path)
    sentinel = _install_import_sentinels(tmp_path, edges)
    before = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main(["run", "A", "--plan"]) == 1

    error = capsys.readouterr().err
    assert "Damaged" in error or "lost its pending execution" in error
    assert _snapshot(tmp_path) == before
    assert not sentinel.exists()


def _establish_retained_receiver(workflow, capsys):
    storage = workflow.storage
    capsys.readouterr()
    workflow.run_node("A")
    capsys.readouterr()
    assert storage.get_job_status("Hidden", 1) == "queued"
    _wait_restart_listener_retired(storage)
    return storage


@pytest.mark.parametrize("command,flag", (("runbetween", "--plan"), ("resetbetween", "--dry-run")))
@pytest.mark.parametrize("activity", ("guard", "active-job"))
def test_fresh_preview_reports_busy_excluded_retained_receiver_without_changes(
    tmp_path, monkeypatch, capsys, command, flag, activity,
):
    edges = [("A", "B"), ("Hidden", "Other")]
    workflow = _initialize_native_project(
        tmp_path,
        monkeypatch,
        edges=edges,
        task_sources={
            "A": """
from micro_workflow_manager import NodeRouter
router = NodeRouter('A', runner='direct')
router.create_job(number=1)
@router.task
def run(ctx):
    ctx.system.start('Hidden')
    return 'A'
""",
        },
    )
    storage = _establish_retained_receiver(workflow, capsys)
    if activity == "guard":
        identity = process_identity(os.getpid())
        assert identity
        storage.submit_db_mutation(lambda connection: connection.execute(
            "INSERT INTO receiver_mutation_guards "
            "(receiver_node, operation_id, session_id, owner_pid, process_identity, hostname) "
            "VALUES(?,?,?,?,?,?)",
            ("Hidden", "c" * 32, None, os.getpid(), identity, socket.gethostname()),
        ))
        expected = "receiver Hidden has mutation guard " + "c" * 32
    else:
        shape = workflow.topology.snapshot().shape_json
        _create_live_session(storage, shape, ("Hidden",), session_id="hidden-owner")
        storage.begin_queued_component_execution(
            "hidden-owner",
            ("Hidden",),
            expected_shape=shape,
            expected_alignment_generation=0,
            successful_lineage=("stable", None),
        )
        generation, execution_id = storage.claim_job_execution(
            "Hidden",
            1,
            started_at=now(),
            session_id="hidden-owner",
            component=("Hidden",),
        )
        assert generation == 0
        expected = f"receiver Hidden has active job Hidden/1 execution {execution_id}"
    _close_without_sidecars(storage, tmp_path)
    sentinel = _install_import_sentinels(tmp_path, edges)
    before = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main([command, "A", "B", flag]) == 0

    output = capsys.readouterr().out
    assert "Hidden/1" in output
    assert "would refuse: " + expected in output
    assert "user code was not loaded" in output
    assert _snapshot(tmp_path) == before
    assert not sentinel.exists()
