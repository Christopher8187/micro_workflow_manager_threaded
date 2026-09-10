"""Interrupt samples capture input after predecessor safe points."""

from __future__ import annotations

import subprocess

import networkx as nx
import pytest

from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.sample_planning import plan_sample
from micro_workflow_manager.topology import ComponentTopology
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_090_component_session_settlement import _close
from tests.test_149_interrupt_declarations import _router_source
from tests.test_155_explicit_interrupt_execution import _start, _wait_until


@pytest.mark.parametrize("replay", [False, True], ids=["capture-current", "reject-drifted-replay"])
def test_final_sample_population_includes_publication_before_predecessor_acknowledgement(
    tmp_path, monkeypatch, replay,
):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('P', 'I')]",
        files={
            "P": '''
                import time
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('P', runner='direct')
                router.create_job(params={})
                @router.task
                def work(ctx):
                    root = ctx.system.storage.project_dir
                    deadline = time.monotonic() + 45
                    with ctx.side_effects():
                        (root / 'publisher-active').write_text(ctx.execution_id, encoding='utf-8')
                        while not (root / 'publish-last-job').exists():
                            assert time.monotonic() < deadline
                            ctx.checkpoint('inside publication operation')
                            time.sleep(0.01)
                        ctx.node('I').add()
                    ctx.checkpoint('published final input before pausing')
                    return 'P'
            ''',
            "I": _router_source("I", interrupt="True", jobs=2),
        },
    )
    parent = _start(tmp_path, ["run", "P", "--runner", "direct"])
    child = storage = None
    release = tmp_path / "publish-last-job"
    try:
        _wait_until(lambda: (tmp_path / "publisher-active").exists() or parent.poll() is not None)
        assert parent.poll() is None, parent.communicate(timeout=5)
        storage = FileStorage(tmp_path)
        snapshot = ComponentTopology(nx.DiGraph([("P", "I")]), []).snapshot()
        prior = plan_sample(
            storage.db_connection(), tmp_path, snapshot, "I", ("100%",), seed="frozen-input",
        )
        assert prior.selected_jobs == (("I", 1), ("I", 2))
        before_state = storage.get_component_state(("I",))
        before_controls = {job_id: storage.read_job_control("I", job_id) for job_id in (1, 2)}
        before_events = {job_id: storage.read_job_events("I", job_id) for job_id in (1, 2)}
        arguments = ["run", "I", "sample", "100%", "--seed", "frozen-input", "--interrupt",
                     "--runner", "direct"]
        if replay:
            arguments.extend(["--expect-population", prior.combined_digest])
        child = _start(tmp_path, arguments)

        def interrupt_session():
            sessions = [session for session in storage.list_execution_sessions()
                        if session["session_kind"] == "interrupt"]
            assert len(sessions) <= 1
            return None if not sessions else sessions[0]

        _wait_until(lambda: child.poll() is not None or interrupt_session() is not None)
        assert child.poll() is None, child.communicate(timeout=5)
        provisional = interrupt_session()
        assert provisional["selected_jobs"] == [("I", 1), ("I", 2)]
        assert storage.get_component_holds(("I",)) == []
        assert all(storage.read_job_current_owner("I", job_id) is None for job_id in (1, 2))
        release.write_text("publish", encoding="utf-8")

        child_out, child_err = child.communicate(timeout=30)
        parent_out, parent_err = parent.communicate(timeout=30)
        assert parent.returncode == 0, parent_out + parent_err
        assert storage.list_job_ids("I") == [1, 2, 3]
        assert storage.get_component_holds(("I",)) == []
        assert storage.get_component_reservation(("I",)) is None
        session = storage.get_execution_session(provisional["session_id"])
        assert session["status"] == "terminal"
        if replay:
            assert child.returncode == 1, child_out + child_err
            assert "sample population or input changed" in (child_out + child_err).lower()
            assert storage.get_component_state(("I",)) == before_state
            assert {job_id: storage.read_job_control("I", job_id) for job_id in (1, 2)} == before_controls
            assert {job_id: storage.read_job_events("I", job_id) for job_id in (1, 2)} == before_events
            assert all(storage.get_job_status("I", job_id) == "queued" for job_id in (1, 2, 3))
            assert all(storage.read_job_current_owner("I", job_id) is None for job_id in (1, 2, 3))
        else:
            assert child.returncode == 0, child_out + child_err
            assert session["outcome"] == "done"
            assert session["selected_jobs"] == [("I", 1), ("I", 2), ("I", 3)]
            selection = session["details"]["selection"]
            assert selection["members"]["I"]["population_count"] == 3
            assert selection["members"]["I"]["selected_job_ids"] == [1, 2, 3]
            assert selection["combined_digest"] != prior.combined_digest
            assert selection["full_starting_coverage"] is True
            assert storage.get_component_state(("I",))["lifecycle"] == "done"
            assert all(storage.get_job_status("I", job_id) == "done" for job_id in (1, 2, 3))
            assert all(storage.read_job_current_owner("I", job_id)["session_id"] == session["session_id"]
                       for job_id in (1, 2, 3))
    finally:
        release.write_text("publish", encoding="utf-8")
        try:
            for process in (child, parent):
                if process is None:
                    continue
                try:
                    process.communicate(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=10)
                    raise
        finally:
            if storage is not None:
                _close(storage)
