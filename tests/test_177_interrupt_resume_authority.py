"""Approved sampled-resume origin and later ordinary command authority."""

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.models import now
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.component_definitions import component_snapshot_from_shape
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_090_component_session_settlement import _close
from tests.test_117_execution_sampling import _job_snapshot
from tests.test_125_sampled_resume import _seed_unstable_parent
from tests.test_149_interrupt_declarations import _router_source
from tests.test_154_interrupt_scope_transfers import _session


@pytest.mark.parametrize("command,descendants", [
    (["resume", "I"], ()),
    (["resumefrom", "I"], ("D", "E")),
    (["resumebetween", "I", "E"], ("D",)),
])
def test_aligned_sampled_interrupt_resume_retains_origin_and_later_ordinary_authority(
    tmp_path, monkeypatch, capsys, command, descendants,
):
    make_project(
        tmp_path, monkeypatch,
        edges="EDGES = [('P', 'I'), ('I', 'D'), ('D', 'E')]",
        files={name: _router_source(name, jobs=4 if name == "I" else 1,
                                   interrupt="True" if name == "I" else None)
               for name in ("P", "I", "D", "E")},
    )
    assert cli.main(["run", "I", "sample", "50%", "--seed", "retained-origin",
                     "--interrupt", "--runner", "direct"]) == 0
    storage = FileStorage(tmp_path)
    try:
        first, = storage.list_execution_sessions()
        origin = first["session_id"]
        assert first["session_kind"] == "interrupt"
        sampled = storage.get_component_state(("I",))
        assert (sampled["lifecycle"], sampled["stability"], sampled["instability_origin"],
                sampled["misaligned"]) == ("sampled", "unstable", origin, False)
        done = {job_id: _job_snapshot(storage, "I", job_id) for job_id in range(1, 5)
                if storage.get_job_status("I", job_id) == "done"}
        assert len(done) == 2
        before_sessions = storage.list_execution_sessions()
        capsys.readouterr()
        assert cli.main(["run", "D", "--runner", "direct"]) == 1
        assert storage.list_execution_sessions() == before_sessions
        assert storage.get_component_state(("D",))["lifecycle"] == "queued"

        assert cli.main([*command, "--interrupt", "--runner", "direct"]) == 0

        second, = [session for session in storage.list_execution_sessions()
                   if session["session_id"] != origin]
        assert second["session_kind"] == "interrupt"
        assert second["status"] == "terminal" and second["outcome"] == "done"
        assert second["parent_session_ids"] == []
        result = storage.get_component_state(("I",))
        assert (result["lifecycle"], result["stability"], result["instability_origin"],
                result["alignment_generation"], result["misaligned"]) == (
                    "done", "unstable", origin, sampled["alignment_generation"], False)
        for job_id in range(1, 5):
            assert storage.get_job_status("I", job_id) == "done"
            if job_id in done:
                assert _job_snapshot(storage, "I", job_id) == done[job_id]
            else:
                assert storage.read_job_current_owner("I", job_id)["session_id"] == second["session_id"]
        assert storage.get_component_state(("P",))["lifecycle"] == "queued"
        for node in ("D", "E"):
            state = storage.get_component_state((node,))
            assert state["lifecycle"] == ("done" if node in descendants else "queued")
            if node in descendants:
                assert state["instability_origin"] == origin
        retained_jobs = {job_id: _job_snapshot(storage, "I", job_id) for job_id in range(1, 5)}

        # A later ordinary command may cross the earlier fence once its direct
        # parent is done. It must preserve that parent's exact result and work.
        assert cli.main(["run", "D", "--runner", "direct"]) == 0
        assert storage.get_component_state(("I",)) == result
        assert {job_id: _job_snapshot(storage, "I", job_id) for job_id in range(1, 5)} == retained_jobs
        downstream = storage.get_component_state(("D",))
        assert (downstream["lifecycle"], downstream["stability"],
                downstream["instability_origin"]) == ("done", "unstable", origin)
        assert storage.get_component_holds(("I",)) == []
        assert storage.get_component_reservation(("I",)) is None
        assert storage.get_component_reservation(("D",)) is None
    finally:
        _close(storage)


def test_incomplete_parent_does_not_hide_an_incompatible_completed_parent(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path, monkeypatch,
        edges="EDGES = [('P', 'I'), ('Q', 'I')]",
        files={name: _router_source(name, jobs=4 if name == "I" else 1,
                                   interrupt="True" if name == "I" else None)
               for name in ("P", "Q", "I")},
    )
    workflow = load_workflow(tmp_path, "direct")
    storage = workflow.storage
    storage.register_component_topology(workflow.topology.snapshot())
    try:
        assert cli.main(["run", "I", "sample", "50%", "--seed", "retained-origin",
                         "--interrupt", "--runner", "direct"]) == 0
        _seed_unstable_parent(storage, "different-completed-parent-origin")
        before_state = storage.get_component_state(("I",))
        before_sessions = storage.list_execution_sessions()
        before_jobs = {job_id: _job_snapshot(storage, "I", job_id)
                       for job_id in range(1, 5)}
        assert storage.get_component_state(("Q",))["lifecycle"] == "queued"
        capsys.readouterr()

        assert cli.main(["resume", "I", "--interrupt", "--runner", "direct"]) == 1

        assert "incompatible" in capsys.readouterr().err.lower()
        assert storage.list_execution_sessions() == before_sessions
        assert storage.get_component_state(("I",)) == before_state
        assert {job_id: _job_snapshot(storage, "I", job_id)
                for job_id in range(1, 5)} == before_jobs
    finally:
        _close(storage)


def test_session_admitted_between_interrupts_cannot_cross_the_second_boundary(
    tmp_path, monkeypatch,
):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('P', 'I'), ('I', 'D')]",
        files={name: _router_source(name, jobs=4 if name == "I" else 1,
                                   interrupt="True" if name == "I" else None)
               for name in ("P", "I", "D")},
    )
    assert cli.main(["run", "I", "sample", "50%", "--seed", "first-boundary",
                     "--interrupt", "--runner", "direct"]) == 0
    storage = FileStorage(tmp_path)
    try:
        first, = storage.list_execution_sessions()
        shape = storage.get_component_definition(("I",))["shape_json"]
        _session(storage, component_snapshot_from_shape(shape), "between-interrupts",
                 kind="main", selected=[("D",)], reserve=False)
        assert storage.read_interrupt_execution_fences("between-interrupts", ("D",)) == ()

        assert cli.main(["resume", "I", "--interrupt", "--runner", "direct"]) == 0

        second, = [session for session in storage.list_execution_sessions()
                   if session["session_id"] not in (first["session_id"], "between-interrupts")]
        fences = storage.read_interrupt_execution_fences("between-interrupts", ("D",))
        assert len(fences) == 1 and fences[0]["source_session_id"] == second["session_id"]
        all_fences = [tuple(row) for row in storage.db_connection().execute(
            "SELECT component_key, shape_id, alignment_generation, source_session_id "
            "FROM post_interrupt_fences ORDER BY source_session_id",
        )]
        assert len(all_fences) == 2 and all_fences[0][:3] == all_fences[1][:3]
        assert {row[3] for row in all_fences} == {first["session_id"], second["session_id"]}
        storage.decide_execution_session_exit("between-interrupts", outcome="stopped", finished_at=now())

        assert cli.main(["run", "D", "--runner", "direct"]) == 0
        downstream, = [session for session in storage.list_execution_sessions()
                       if session["command"] == "run" and session["start_component"] == ("D",)]
        assert storage.read_interrupt_execution_fences(downstream["session_id"], ("D",)) == ()
        authorized = storage.db_connection().execute(
            "SELECT source_session_id FROM session_fence_authorizations WHERE session_id=?",
            (downstream["session_id"],),
        ).fetchall()
        assert {row[0] for row in authorized} == {first["session_id"], second["session_id"]}
    finally:
        _close(storage)


def test_failed_interrupt_resume_keeps_sampled_work_and_origin_for_next_resume(
    tmp_path, monkeypatch,
):
    target = _router_source("I", jobs=4, interrupt="True").replace(
        "def run(ctx):\n",
        "def run(ctx):\n"
        "    if (ctx.system.storage.project_dir / 'fail-resume').exists():\n"
        "        raise RuntimeError('requested resume failure')\n",
    )
    make_project(tmp_path, monkeypatch, edges="EDGES = [('P', 'I')]",
                 files={"P": _router_source("P", jobs=1), "I": target})
    assert cli.main(["run", "I", "sample", "50%", "--seed", "failed-resume",
                     "--interrupt", "--runner", "direct"]) == 0
    storage = FileStorage(tmp_path)
    try:
        sampled = storage.get_component_state(("I",))
        done = {job_id: _job_snapshot(storage, "I", job_id) for job_id in range(1, 5)
                if storage.get_job_status("I", job_id) == "done"}
        assert len(done) == 2
        marker = tmp_path / "fail-resume"
        marker.touch()
        assert cli.main(["resume", "I", "--interrupt", "--runner", "direct"]) == 1
        assert storage.get_component_state(("I",))["lifecycle"] == "failed"
        assert {job_id: _job_snapshot(storage, "I", job_id) for job_id in done} == done
        marker.unlink()

        assert cli.main(["resume", "I", "--interrupt", "--runner", "direct"]) == 0

        result = storage.get_component_state(("I",))
        assert result["lifecycle"] == "done"
        assert result["instability_origin"] == sampled["instability_origin"]
        assert result["alignment_generation"] == sampled["alignment_generation"]
        assert {job_id: _job_snapshot(storage, "I", job_id) for job_id in done} == done
        assert all(storage.get_job_status("I", job_id) == "done" for job_id in range(1, 5))
        assert storage.get_component_state(("P",))["lifecycle"] == "queued"
    finally:
        _close(storage)
