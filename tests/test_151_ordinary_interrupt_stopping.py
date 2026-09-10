"""Ordinary interrupt stops retain queued work and settle the owning session."""

from __future__ import annotations

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.storage import FileStorage
from tests.test_064_read_only_previews import _close_without_sidecars
from tests.test_090_component_session_settlement import _close
from tests.test_117_execution_sampling import _job_snapshot
from tests.test_149_interrupt_declarations import _make_interrupt_project


def _terminal_session(storage, *, kind="main", outcome="stopped"):
    sessions = storage.list_execution_sessions()
    assert len(sessions) == 1
    session = sessions[0]
    assert session["session_kind"] == kind
    assert (session["status"], session["outcome"], session["failures"]) == (
        "terminal", outcome, [],
    )
    assert storage.get_live_main_session() is None
    return session


def _interrupt_record(session):
    record = session["details"]["interrupt_preflight"]
    assert record["policy"] in {"run-all", "stop-all", "individual"}
    return record


def _decision(record, key):
    matches = [item for item in record["decisions"] if item["canonical_key"] == key]
    assert len(matches) == 1
    return matches[0]


def _assert_never_executed(storage, node, job_id=1):
    assert storage.get_job_status(node, job_id) == "queued"
    assert storage.read_job_current_owner(node, job_id) is None
    events = storage.read_job_events(node, job_id)
    assert not {
        event["event"] for event in events
    }.intersection({"started", "task_started", "output_written", "done", "failed", "skipped"})
    assert not storage.output_file(node, job_id).exists()


def test_individual_stop_blocks_its_branch_while_independent_selected_work_finishes_and_slot_releases(
    tmp_path, monkeypatch, capsys,
):
    _make_interrupt_project(
        tmp_path,
        monkeypatch,
        edges=[("S", "I"), ("I", "D"), ("S", "R")],
        interrupt_nodes={"I"},
        jobs={"S", "I", "D", "R"},
    )
    capsys.readouterr()

    assert cli.main([
        "runfrom", "S", "--runner", "direct",
        "--interrupt-policy", "individual", "--interrupt-choice", "I=stop",
    ]) == 0

    storage = FileStorage(tmp_path)
    try:
        assert storage.get_job_status("S", 1) == "done"
        assert storage.get_job_status("R", 1) == "done"
        _assert_never_executed(storage, "I")
        _assert_never_executed(storage, "D")
        assert storage.get_component_state(("I",))["lifecycle"] == "queued"
        assert storage.get_component_state(("D",))["lifecycle"] == "queued"
        assert storage.db_connection().execute(
            "SELECT COUNT(*) FROM pending_component_executions",
        ).fetchone()[0] == 0
        assert storage.db_connection().execute(
            "SELECT COUNT(*) FROM component_reservations",
        ).fetchone()[0] == 0
        session = _terminal_session(storage)
        record = _interrupt_record(session)
        assert _decision(record, "I") == {
            "canonical_key": "I", "members": ["I"], "action": "stop",
        }
        assert record["stopped_components"] == [["I"]]
        assert ["I"] in record["blocked_components"]
        assert ["D"] in record["blocked_components"]
    finally:
        _close(storage)

    # The stopped session released the single-main slot.
    assert cli.main(["run", "R", "--runner", "direct"]) == 0
    reopened = FileStorage(tmp_path)
    try:
        sessions = reopened.list_execution_sessions()
        assert len(sessions) == 2
        assert sorted((item["status"], item["outcome"]) for item in sessions) == [
            ("terminal", "done"),
            ("terminal", "stopped"),
        ]
        assert reopened.get_live_main_session() is None
    finally:
        _close(reopened)


def test_stop_all_records_each_independent_boundary_without_skipping_descendants(
    tmp_path, monkeypatch, capsys,
):
    _make_interrupt_project(
        tmp_path,
        monkeypatch,
        edges=[("S", "I1"), ("I1", "D1"), ("S", "I2"), ("I2", "D2")],
        interrupt_nodes={"I1", "I2"},
        jobs={"S", "I1", "D1", "I2", "D2"},
    )
    capsys.readouterr()

    assert cli.main([
        "runfrom", "S", "--runner", "direct", "--interrupt-policy", "stop-all",
    ]) == 0

    storage = FileStorage(tmp_path)
    try:
        assert storage.get_job_status("S", 1) == "done"
        for node in ("I1", "D1", "I2", "D2"):
            _assert_never_executed(storage, node)
        session = _terminal_session(storage)
        record = _interrupt_record(session)
        assert [
            (item["canonical_key"], item["members"], item["action"])
            for item in record["decisions"]
        ] == [
            ("I1", ["I1"], "stop"),
            ("I2", ["I2"], "stop"),
        ]
        assert record["stopped_components"] == [["I1"], ["I2"]]
        assert storage.db_connection().execute(
            "SELECT COUNT(*) FROM component_reservations",
        ).fetchone()[0] == 0
    finally:
        _close(storage)


@pytest.mark.parametrize(
    "selection",
    [
        pytest.param(["job", "1"], id="selected-job"),
        pytest.param(["sample", "100%", "--seed", "interrupt-stop"], id="sample"),
    ],
)
def test_selected_job_and_sample_stop_before_claim_but_retain_exact_session_selection(
    tmp_path, monkeypatch, capsys, selection,
):
    _make_interrupt_project(
        tmp_path,
        monkeypatch,
        edges=[("I", "I")],
        interrupt_nodes={"I"},
        jobs={"I"},
    )
    workflow = load_workflow(tmp_path, "direct")
    workflow.storage.register_component_topology(workflow.topology.snapshot())
    before_job = _job_snapshot(workflow.storage, "I", 1)
    _close_without_sidecars(workflow.storage, tmp_path)
    capsys.readouterr()

    assert cli.main([
        "run", "I", *selection, "--interrupt-policy", "stop-all",
    ]) == 0

    storage = FileStorage(tmp_path)
    try:
        assert _job_snapshot(storage, "I", 1) == before_job
        session = _terminal_session(storage)
        assert session["selected_jobs"] == [("I", 1)]
        record = _interrupt_record(session)
        assert _decision(record, "I")["action"] == "stop"
        assert record["stopped_components"] == [["I"]]
        assert storage.db_connection().execute(
            "SELECT COUNT(*) FROM pending_component_executions",
        ).fetchone()[0] == 0
        assert storage.get_component_reservation(("I",)) is None
    finally:
        _close(storage)


def test_explicit_start_bridge_excludes_only_start_and_stops_later_interrupt_component(
    tmp_path, monkeypatch, capsys,
):
    """Required later explicit-executor bridge: this is not ordinary-only acceptance."""
    _make_interrupt_project(
        tmp_path,
        monkeypatch,
        edges=[("I", "J")],
        interrupt_nodes={"I", "J"},
        jobs={"I", "J"},
    )
    capsys.readouterr()

    assert cli.main([
        "runfrom", "I", "--interrupt", "--runner", "direct",
        "--interrupt-policy", "stop-all",
    ]) == 0

    storage = FileStorage(tmp_path)
    try:
        assert storage.get_job_status("I", 1) == "done"
        _assert_never_executed(storage, "J")
        session = _terminal_session(storage, kind="interrupt", outcome="stopped")
        assert session["start_component"] == ("I",)
        assert session["selected_components"] == [("I",), ("J",)]
        record = _interrupt_record(session)
        assert [item["canonical_key"] for item in record["decisions"]] == ["J"]
        assert _decision(record, "J")["action"] == "stop"
        assert all(item["canonical_key"] != "I" for item in record["decisions"])
        assert len([item for item in storage.list_execution_sessions()
                    if item["session_kind"] == "interrupt"]) == 1
    finally:
        _close(storage)


def test_explicit_sample_bridge_excludes_selected_start_without_losing_sample_roots(
    tmp_path, monkeypatch, capsys,
):
    """Required later explicit-executor bridge for sampling."""
    _make_interrupt_project(
        tmp_path,
        monkeypatch,
        edges=[("I", "I")],
        interrupt_nodes={"I"},
        jobs={"I"},
    )
    workflow = load_workflow(tmp_path, "direct")
    workflow.storage.register_component_topology(workflow.topology.snapshot())
    _close_without_sidecars(workflow.storage, tmp_path)
    capsys.readouterr()

    assert cli.main([
        "run", "I", "sample", "100%", "--seed", "explicit-sample",
        "--interrupt", "--interrupt-policy", "stop-all", "--runner", "direct",
    ]) == 0

    storage = FileStorage(tmp_path)
    try:
        assert storage.get_job_status("I", 1) == "done"
        session = _terminal_session(storage, kind="interrupt", outcome="done")
        assert session["selected_jobs"] == [("I", 1)]
        record = _interrupt_record(session)
        assert record["decisions"] == []
        assert record["stopped_components"] == []
        assert storage.get_component_reservation(("I",)) is None
    finally:
        _close(storage)
