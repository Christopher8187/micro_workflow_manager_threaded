"""Disjoint native sessions survive an unrelated regional membership repair."""

from __future__ import annotations

from datetime import datetime, timezone
import os
import socket

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.processes import process_identity
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.component_membership import read_active_component_partition
from micro_workflow_manager.storage.component_states import ComponentTerminalOutcome
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_064_read_only_previews import _snapshot
from tests.test_090_component_session_settlement import _close
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows
from tests.test_137_between_run_membership import _close_finished_project, _graph


TASKS = {
    node: f"""
from pathlib import Path
from micro_workflow_manager import NodeRouter
router = NodeRouter({node!r}, runner='direct')
router.create_job(number=1)
@router.task
def run(ctx):
    root = Path(ctx.system.storage.project_dir)
    with (root / 'regional-session-executions.txt').open('a', encoding='utf-8') as stream:
        stream.write({node!r} + '\\n')
    ctx.write_output('result.txt', {node!r})
    return {node!r}
"""
    for node in ("A", "B")
}
TASKS.update({
    node: f"""
from micro_workflow_manager import NodeRouter
router = NodeRouter({node!r}, runner='direct')
@router.task
def run(ctx):
    return {node!r}
"""
    for node in ("U", "Z")
})


OLD_EDGES = [("A", "B"), ("B", "A"), ("U", "Z")]
CURRENT_EDGES = [("A", "B"), ("U", "Z")]


def _now():
    return datetime.now(timezone.utc).isoformat()


def _make_old_project(tmp_path, monkeypatch, capsys):
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
    storage = FileStorage(tmp_path)
    assert storage.get_component_state(("A", "B"))["lifecycle"] == "done"
    _close(storage)


def _update_graph(tmp_path, capsys):
    (tmp_path / "src" / "graph.py").write_text(
        _graph(CURRENT_EDGES) + "\n",
        encoding="utf-8",
    )
    assert cli.main(["graph", "--update"]) == 0
    capsys.readouterr()
    _close_finished_project(tmp_path)
    workflow = load_workflow(tmp_path)
    try:
        return workflow.topology.snapshot()
    finally:
        _close(workflow.storage)


def _create_interrupt(storage, session_id, component, shape):
    started = _now()
    pid = os.getpid()
    storage.create_execution_session(
        session_id,
        session_kind="interrupt",
        command="run",
        start_component=component,
        selected_components=[component],
        started_at=started,
        hostname=socket.gethostname(),
        pid=pid,
        process_identity=process_identity(pid),
        expected_shape=shape,
    )
    assert storage.reserve_execution_components(
        session_id,
        expected_shape=shape,
    ) is True
    return storage.get_execution_session(session_id)


def test_disjoint_interrupt_continues_after_main_repairs_another_membership_region(
    tmp_path, monkeypatch, capsys,
):
    _make_old_project(tmp_path, monkeypatch, capsys)
    target = _update_graph(tmp_path, capsys)
    storage = FileStorage(tmp_path)
    storage.register_component_topology(target)
    interrupt_before = _create_interrupt(
        storage,
        "regional-interrupt",
        ("U",),
        target.shape_json,
    )
    active_before = read_active_component_partition(storage.db_connection())
    assert active_before is not None
    assert interrupt_before["partition_revision"] == active_before.revision
    _close(storage)

    assert cli.main(["run", "A", "--runner", "direct"]) == 0
    output = capsys.readouterr().out
    assert "membership repair preparation: {A}, {B}" in output

    storage = FileStorage(tmp_path)
    try:
        active_after = read_active_component_partition(storage.db_connection())
        assert active_after is not None
        assert active_after.revision == active_before.revision + 1
        assert storage.get_execution_session("regional-interrupt") == interrupt_before
        state = storage.get_component_state(("U",))
        assert state["lifecycle"] == "queued"
        assert storage.begin_queued_component_execution(
            "regional-interrupt",
            ("U",),
            expected_shape=target.shape_json,
            expected_alignment_generation=state["alignment_generation"],
            successful_lineage=("stable", None),
        ) is True
        outcome = ComponentTerminalOutcome(
            component=("U",),
            expected_shape=target.shape_json,
            expected_alignment_generation=state["alignment_generation"],
            lifecycle="done",
            stability="stable",
            instability_origin=None,
        )
        assert storage.finish_successful_component_execution(
            "regional-interrupt",
            outcome,
        ) is True
        decision = storage.decide_execution_session_exit(
            "regional-interrupt",
            outcome="done",
            finished_at=_now(),
        )
        assert decision["released"] == 1
        terminal = storage.get_execution_session("regional-interrupt")
        assert terminal["status"] == "terminal"
        assert terminal["outcome"] == "done"
        assert terminal["partition_revision"] == interrupt_before["partition_revision"]
        result = storage.get_component_state(("U",))
        assert result == dict(
            state,
            shape_json=target.shape_json,
            lifecycle="done",
            stability="stable",
            instability_origin=None,
        )
        assert storage.get_component_reservation(("U",)) is None
    finally:
        _close(storage)


def test_overlapping_interrupt_refuses_membership_repair_before_project_mutation(
    tmp_path, monkeypatch, capsys,
):
    _make_old_project(tmp_path, monkeypatch, capsys)
    workflow = load_workflow(tmp_path)
    old_shape = workflow.topology.snapshot().shape_json
    storage = workflow.storage
    interrupt = _create_interrupt(
        storage,
        "overlapping-interrupt",
        ("A", "B"),
        old_shape,
    )
    _close(storage)
    _update_graph(tmp_path, capsys)
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)

    assert cli.main(["run", "A", "--runner", "direct"]) == 1

    error = capsys.readouterr().err
    assert "overlapping-interrupt" in error
    assert "A" in error and "B" in error
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files
    storage = FileStorage(tmp_path)
    try:
        assert storage.get_execution_session("overlapping-interrupt") == interrupt
        assert storage.get_component_reservation(("A", "B")) == {
            "members": ("A", "B"),
            "session_id": "overlapping-interrupt",
        }
    finally:
        _close(storage)
