"""Stopped components retain their established failures across graph commands."""
from __future__ import annotations

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.storage import FileStorage
from tests.test_064_read_only_previews import _close_without_sidecars
from tests.test_117_execution_sampling import _job_snapshot
from tests.test_149_interrupt_declarations import _make_interrupt_project, _router_source


@pytest.mark.parametrize("arguments", [
    ["run", "I"], ["runfrom", "S"], ["runbetween", "S", "D"],
    ["resume", "I"], ["resumefrom", "S"], ["resumebetween", "S", "D"],
])
def test_stopped_failed_component_keeps_job_owner_generation_output_and_trace(
    tmp_path, monkeypatch, capsys, arguments,
):
    _make_interrupt_project(
        tmp_path, monkeypatch,
        edges=[("S", "I"), ("I", "D")],
        interrupt_nodes={"I"}, jobs={"S", "I", "D"},
    )
    source = _router_source("I", interrupt="True", jobs=1)
    source = source.replace("    return 'I'", "    raise RuntimeError('retained failure')")
    (tmp_path / "src" / "node_behavior" / "I.py").write_text(source, encoding="utf-8")
    assert cli.main([
        "runfrom", "S", "--runner", "direct", "--interrupt-policy", "run-all",
    ]) == 1
    storage = FileStorage(tmp_path)
    before_job = _job_snapshot(storage, "I", 1)
    before_component = storage.get_component_state(("I",))
    before_descendant = _job_snapshot(storage, "D", 1)
    assert storage.get_job_status("I", 1) == "failed"
    assert before_job["owner"] is not None
    assert before_component["lifecycle"] == "failed"
    _close_without_sidecars(storage, tmp_path)
    capsys.readouterr()

    assert cli.main([
        *arguments, "--runner", "direct", "--interrupt-policy", "stop-all",
    ]) == 0

    storage = FileStorage(tmp_path)
    try:
        assert _job_snapshot(storage, "I", 1) == before_job
        assert storage.get_component_state(("I",)) == before_component
        assert _job_snapshot(storage, "D", 1) == before_descendant
        sessions = storage.list_execution_sessions()
        assert len(sessions) == 2
        stopped = [session for session in sessions if session["outcome"] == "stopped"]
        assert len(stopped) == 1
        assert stopped[0]["status"] == "terminal"
        assert stopped[0]["failures"] == []
        assert stopped[0]["details"]["interrupt_preflight"]["stopped_components"] == [["I"]]
        assert storage.get_live_main_session() is None
        assert storage.get_component_reservation(("I",)) is None
    finally:
        _close_without_sidecars(storage, tmp_path)
