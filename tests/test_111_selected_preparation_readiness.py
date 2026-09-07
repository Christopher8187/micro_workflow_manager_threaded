"""Draft regressions for selected-job readiness before and after admission."""

from __future__ import annotations

import pytest

from micro_workflow_manager import MicroWorkflow, cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.errors import InvalidGraphError
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.workflow.preparation import prepare_fresh_components
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_093_native_cli_readiness import _node_files


def _selected_root(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("P", "A")])
    storage = workflow.storage
    calls = []

    @workflow.task("P")
    def parent(ctx):
        return ctx.job_id

    @workflow.task("A")
    def selected(ctx):
        calls.append(ctx.job_id)
        ctx.write_output("selected-root.txt", str(ctx.job_id))
        return ctx.job_id

    workflow.start("P", job_id=1)
    workflow.start("A", job_id=1)
    workflow.run_node("P")
    workflow.run_node("A")
    assert calls == [1]
    assert storage.get_component_state(("P",))["lifecycle"] == "done"
    assert storage.get_component_state(("A",))["lifecycle"] == "done"
    return workflow, storage, calls


def _run_selected(workflow, entry, ignore_readiness):
    if entry == "run_job":
        return workflow.run_job("A", 1, ignore_readiness=ignore_readiness)
    if entry == "run_jobs":
        return workflow.run_jobs("A", [1], ignore_readiness=ignore_readiness)
    return workflow.run_node_jobs(
        "A", [workflow.storage.load_job("A", 1)],
        ignore_readiness=ignore_readiness,
    )


def _receipt_rows(storage):
    return [dict(row) for row in storage.db_connection().execute(
        "SELECT * FROM preparation_receipts ORDER BY operation_id"
    )]


@pytest.mark.parametrize("entry", ["run_job", "run_jobs", "run_node_jobs"])
@pytest.mark.parametrize("ignore_readiness", [False, True])
def test_selected_job_blocked_parent_refuses_before_session_or_preparation(
    tmp_path, entry, ignore_readiness,
):
    workflow, storage, calls = _selected_root(tmp_path)
    try:
        prepare_fresh_components(tmp_path, workflow, [{"P"}])
        assert storage.get_component_state(("P",))["lifecycle"] == "queued"
        before_rows = _rows(storage)
        before_files = _node_files(tmp_path)
        before_owner = storage.read_job_current_owner("A", 1)

        with pytest.raises(InvalidGraphError, match="ready"):
            _run_selected(workflow, entry, ignore_readiness)

        assert calls == [1]
        assert _rows(storage) == before_rows
        assert _node_files(tmp_path) == before_files
        assert storage.read_job_current_owner("A", 1) == before_owner
        assert storage.get_live_main_session() is None
        assert storage.get_component_reservation(("A",)) is None
    finally:
        _close(storage)


@pytest.mark.parametrize("entry", ["run_job", "run_jobs", "run_node_jobs"])
@pytest.mark.parametrize("ignore_readiness", [False, True])
def test_selected_job_parent_change_after_admission_precedes_preparation(
    tmp_path, monkeypatch, entry, ignore_readiness,
):
    workflow, storage, calls = _selected_root(tmp_path)
    reserve = FileStorage.reserve_execution_components
    changed = []
    before_control = storage.read_job_control("A", 1)
    before_status = storage.read_job_status_data("A", 1)
    before_job = storage.load_job("A", 1)
    before_owner = storage.read_job_current_owner("A", 1)
    before_events = storage.read_job_events("A", 1)
    before_component = storage.get_component_state(("A",))
    before_files = _node_files(tmp_path)
    before_receipts = _receipt_rows(storage)
    before_sessions = {item["session_id"] for item in storage.list_execution_sessions()}

    def change_parent_after_reservation(self, *args, **kwargs):
        result = reserve(self, *args, **kwargs)
        if not changed:
            changed.append(True)
            self.submit_db_mutation(lambda connection: connection.execute(
                "UPDATE component_states SET lifecycle='failed', stability=NULL, "
                "instability_origin=NULL WHERE component_key='[\"P\"]'"
            ))
        return result

    monkeypatch.setattr(FileStorage, "reserve_execution_components", change_parent_after_reservation)
    try:
        with pytest.raises((InvalidGraphError, RuntimeError), match="ready|parent|changed"):
            _run_selected(workflow, entry, ignore_readiness)

        assert changed == [True]
        assert calls == [1]
        assert storage.get_component_state(("P",))["lifecycle"] == "failed"
        assert storage.read_job_control("A", 1) == before_control
        assert storage.read_job_status_data("A", 1) == before_status
        assert storage.load_job("A", 1) == before_job
        assert storage.read_job_current_owner("A", 1) == before_owner
        assert storage.read_job_events("A", 1) == before_events
        assert storage.get_component_state(("A",)) == before_component
        assert _node_files(tmp_path) == before_files
        assert _receipt_rows(storage) == before_receipts
        added = [item for item in storage.list_execution_sessions()
                 if item["session_id"] not in before_sessions]
        assert len(added) == 1
        assert added[0]["selection_kind"] == "jobs"
        assert added[0]["selected_jobs"] == [("A", 1)]
        assert (added[0]["status"], added[0]["outcome"]) == ("terminal", "failed")
        assert storage.get_live_main_session() is None
        assert storage.get_component_reservation(("A",)) is None
    finally:
        _close(storage)


def _cli_selected_root(tmp_path, monkeypatch, capsys):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('P', 'A')]",
        files={
            "P": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("P")
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    return ctx.job_id
            """,
            "A": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A")
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    ctx.write_output("selected-root.txt", str(ctx.job_id))
                    return ctx.job_id
            """,
        },
    )
    capsys.readouterr()
    assert cli.main(["run", "P", "--runner", "direct"]) == 0
    assert cli.main(["run", "A", "--runner", "direct"]) == 0
    capsys.readouterr()


def test_cli_selected_job_blocked_parent_refuses_before_session_or_preparation(
    tmp_path, monkeypatch, capsys,
):
    _cli_selected_root(tmp_path, monkeypatch, capsys)
    workflow = load_workflow(tmp_path)
    try:
        prepare_fresh_components(tmp_path, workflow, [{"P"}])
    finally:
        _close(workflow.storage)
    storage = FileStorage(tmp_path)
    try:
        before_rows = _rows(storage)
        before_files = _node_files(tmp_path)

        assert cli.main(["run", "A", "job", "1", "--runner", "direct"]) == 1

        output = capsys.readouterr()
        assert "ready" in (output.out + output.err).lower()
        assert _rows(storage) == before_rows
        assert _node_files(tmp_path) == before_files
        assert storage.get_live_main_session() is None
        assert storage.get_component_reservation(("A",)) is None
    finally:
        _close(storage)


def test_cli_selected_job_parent_change_after_admission_precedes_preparation(
    tmp_path, monkeypatch, capsys,
):
    _cli_selected_root(tmp_path, monkeypatch, capsys)
    storage = FileStorage(tmp_path)
    reserve = FileStorage.reserve_execution_components
    changed = []
    before_control = storage.read_job_control("A", 1)
    before_status = storage.read_job_status_data("A", 1)
    before_job = storage.load_job("A", 1)
    before_owner = storage.read_job_current_owner("A", 1)
    before_events = storage.read_job_events("A", 1)
    before_component = storage.get_component_state(("A",))
    before_files = _node_files(tmp_path)
    before_receipts = _receipt_rows(storage)
    before_sessions = {item["session_id"] for item in storage.list_execution_sessions()}

    def change_parent_after_reservation(self, *args, **kwargs):
        result = reserve(self, *args, **kwargs)
        if not changed:
            changed.append(True)
            self.submit_db_mutation(lambda connection: connection.execute(
                "UPDATE component_states SET lifecycle='failed', stability=NULL, "
                "instability_origin=NULL WHERE component_key='[\"P\"]'"
            ))
        return result

    monkeypatch.setattr(FileStorage, "reserve_execution_components", change_parent_after_reservation)
    try:
        assert cli.main(["run", "A", "job", "1", "--runner", "direct"]) == 1

        assert changed == [True]
        assert storage.get_component_state(("P",))["lifecycle"] == "failed"
        assert storage.read_job_control("A", 1) == before_control
        assert storage.read_job_status_data("A", 1) == before_status
        assert storage.load_job("A", 1) == before_job
        assert storage.read_job_current_owner("A", 1) == before_owner
        assert storage.read_job_events("A", 1) == before_events
        assert storage.get_component_state(("A",)) == before_component
        assert _node_files(tmp_path) == before_files
        assert _receipt_rows(storage) == before_receipts
        added = [item for item in storage.list_execution_sessions()
                 if item["session_id"] not in before_sessions]
        assert len(added) == 1
        assert added[0]["selection_kind"] == "jobs"
        assert added[0]["selected_jobs"] == [("A", 1)]
        assert (added[0]["status"], added[0]["outcome"]) == ("terminal", "failed")
        assert storage.get_live_main_session() is None
        assert storage.get_component_reservation(("A",)) is None
    finally:
        _close(storage)
