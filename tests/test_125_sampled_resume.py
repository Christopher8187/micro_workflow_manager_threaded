"""Public native resume regressions for aligned sampled components."""

from __future__ import annotations

import os
import socket

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.models import now
from micro_workflow_manager.processes import process_identity
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_064_read_only_previews import (
    _live_execution_session_identity,
    _mark_execution_session_stale,
)
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_093_native_cli_readiness import _node_files
from tests.test_117_execution_sampling import _component_result, _job_snapshot


def _make_single_component(tmp_path, monkeypatch, *, behavior=None):
    behavior = behavior or '''
        from pathlib import Path
        from micro_workflow_manager import NodeRouter
        router = NodeRouter("A", runner="direct")
        router.create_job(number=4, params={"role": "work"})
        @router.task
        def run(ctx, role):
            marker = Path(ctx.system.storage.project_dir) / f"fail-{ctx.job_id}"
            if marker.exists():
                raise RuntimeError(f"expected resume failure {ctx.job_id}")
            return f"A/{ctx.job_id}/{role}"
    '''
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'A')]",
        files={"A": behavior},
        runner="direct",
    )
    return FileStorage(tmp_path)


def _sample_half(storage, capsys):
    capsys.readouterr()
    assert cli.main([
        "run", "A", "sample", "50%", "--seed", "sampled-resume", "--runner", "direct",
    ]) == 0
    capsys.readouterr()
    done = [job_id for job_id in range(1, 5) if storage.get_job_status("A", job_id) == "done"]
    queued = [job_id for job_id in range(1, 5) if storage.get_job_status("A", job_id) == "queued"]
    assert len(done) == len(queued) == 2
    assert _component_result(storage, ("A",))["lifecycle"] == "sampled"
    return done, queued


def _new_session(storage, before, command):
    sessions = [
        session for session in storage.list_execution_sessions()
        if session["session_id"] not in before and session["command"] == command
    ]
    assert len(sessions) == 1
    return sessions[0]


def _set_done_result(storage, component, stability, origin):
    key = encode_component_key(component)

    def seed(connection):
        state = connection.execute(
            'SELECT shape_id, alignment_generation FROM component_states WHERE component_key=?',
            (key,),
        ).fetchone()
        assert state is not None
        connection.execute(
            'INSERT INTO component_successful_results '
            '(component_key, shape_id, alignment_generation, lifecycle, stability, instability_origin) '
            "VALUES(?,?,?,'done',?,?) ON CONFLICT(component_key, shape_id, alignment_generation) "
            'DO UPDATE SET lifecycle=excluded.lifecycle, stability=excluded.stability, '
            'instability_origin=excluded.instability_origin',
            (key, state['shape_id'], state['alignment_generation'], stability, origin),
        )
        return connection.execute(
            "UPDATE component_states SET lifecycle='done', stability=?, instability_origin=?, "
            'retained_result_shape_id=shape_id, retained_result_alignment_generation=alignment_generation '
            'WHERE component_key=?',
            (stability, origin, key),
        ).rowcount

    return storage.submit_db_mutation(seed)


def _seed_unstable_parent(storage, origin):
    storage.create_execution_session(
        origin,
        session_kind="interrupt",
        command="run",
        start_component=("P",),
        selected_components=[("P",)],
        started_at=now(),
        hostname=socket.gethostname(),
        pid=os.getpid(),
        process_identity=process_identity(os.getpid()),
        expected_shape=storage.get_component_definition(("P",))["shape_json"],
    )
    assert storage.reserve_execution_components(
        origin, expected_shape=storage.get_component_definition(("P",))["shape_json"],
    ) is True
    assert storage.db_connection().execute(
        "SELECT scope_admitted FROM execution_sessions WHERE session_id=?", (origin,),
    ).fetchone()[0] == 1
    assert storage.finish_execution_session(origin, outcome="done", finished_at=now()) is True
    assert storage.release_execution_components(origin) == 1
    assert storage.get_component_reservation(("P",)) is None
    assert _set_done_result(storage, ("P",), "unstable", origin) == 1
    storage.set_node_status("P", "done")


def _make_parented_component(tmp_path, monkeypatch):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('P', 'A')]",
        files={
            "P": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("P", runner="direct")
                @router.task
                def run(ctx): return "P"
            ''',
            "A": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A", runner="direct")
                router.create_job(number=4)
                @router.task
                def run(ctx): return f"A/{ctx.job_id}"
            ''',
        },
        runner="direct",
    )
    workflow = load_workflow(tmp_path, "direct")
    storage = workflow.storage
    storage.register_component_topology(workflow.topology.snapshot())
    assert storage.get_component_state(("P",)) is not None
    assert storage.get_component_state(("A",)) is not None
    return storage


def test_resume_runs_remaining_sampled_work_and_publishes_done_once(tmp_path, monkeypatch, capsys):
    storage = _make_single_component(tmp_path, monkeypatch)
    try:
        selected, remaining = _sample_half(storage, capsys)
        selected_before = {job_id: _job_snapshot(storage, "A", job_id) for job_id in selected}
        sampled = storage.get_component_state(("A",))
        sessions = {item["session_id"] for item in storage.list_execution_sessions()}

        assert cli.main(["resume", "A"]) == 0
        capsys.readouterr()

        assert {job_id: _job_snapshot(storage, "A", job_id) for job_id in selected} == selected_before
        assert all(storage.get_job_status("A", job_id) == "done" for job_id in remaining)
        assert _component_result(storage, ("A",)) == {
            "lifecycle": "done",
            "stability": "stable",
            "instability_origin": None,
            "misaligned": False,
            "alignment_generation": sampled["alignment_generation"],
        }
        session = _new_session(storage, sessions, "resume")
        assert (session["status"], session["outcome"], session["selected_jobs"]) == (
            "terminal", "done", [],
        )
        assert storage.get_component_reservation(("A",)) is None
        assert storage.db_connection().execute(
            "SELECT COUNT(*) FROM pending_component_executions WHERE session_id=?",
            (session["session_id"],),
        ).fetchone()[0] == 0
        retained = storage.db_connection().execute(
            "SELECT lifecycle, stability, instability_origin FROM component_successful_results "
            "WHERE component_key=? AND alignment_generation=?",
            (encode_component_key(("A",)), sampled["alignment_generation"]),
        ).fetchone()
        assert tuple(retained) == ("done", "stable", None)
    finally:
        _close(storage)


def test_resume_promotes_sampled_result_with_no_remaining_work_without_replay(
    tmp_path, monkeypatch, capsys,
):
    storage = _make_single_component(tmp_path, monkeypatch)
    try:
        capsys.readouterr()
        assert cli.main(["run", "A", "--runner", "direct"]) == 0
        capsys.readouterr()
        assert cli.main([
            "run", "A", "sample", "50%", "--seed", "already-done", "--runner", "direct",
        ]) == 0
        capsys.readouterr()
        assert storage.get_component_state(("A",))["lifecycle"] == "sampled"
        jobs = {job_id: _job_snapshot(storage, "A", job_id) for job_id in range(1, 5)}
        sessions = {item["session_id"] for item in storage.list_execution_sessions()}

        assert cli.main(["resume", "A"]) == 0
        capsys.readouterr()

        assert {job_id: _job_snapshot(storage, "A", job_id) for job_id in range(1, 5)} == jobs
        assert storage.get_component_state(("A",))["lifecycle"] == "done"
        session = _new_session(storage, sessions, "resume")
        assert (session["status"], session["outcome"]) == ("terminal", "done")
        assert storage.get_component_reservation(("A",)) is None
    finally:
        _close(storage)


def test_resume_preserves_exact_sampled_instability_origin(tmp_path, monkeypatch, capsys):
    storage = _make_parented_component(tmp_path, monkeypatch)
    try:
        _seed_unstable_parent(storage, "interrupt-parent")
        selected, _ = _sample_half(storage, capsys)
        assert _component_result(storage, ("A",)) == {
            "lifecycle": "sampled",
            "stability": "unstable",
            "instability_origin": "interrupt-parent",
            "misaligned": False,
            "alignment_generation": 0,
        }
        selected_before = {job_id: _job_snapshot(storage, "A", job_id) for job_id in selected}
        parent = storage.get_component_state(("P",))
        observed = {}
        begin = FileStorage.begin_sampled_component_execution

        def capture_running(self, *args, **kwargs):
            result = begin(self, *args, **kwargs)
            state = self.get_component_state(("A",))
            pending = self.db_connection().execute(
                "SELECT execution_kind, starting_lifecycle, stability, instability_origin "
                "FROM pending_component_executions WHERE component_key=?",
                (encode_component_key(("A",)),),
            ).fetchone()
            observed.update(state=dict(state), pending=dict(pending))
            return result

        monkeypatch.setattr(FileStorage, "begin_sampled_component_execution", capture_running)

        assert cli.main(["resume", "A"]) == 0
        capsys.readouterr()

        assert (
            observed["state"]["lifecycle"], observed["state"]["stability"],
            observed["state"]["instability_origin"], observed["state"]["alignment_generation"],
        ) == ("running", "unstable", "interrupt-parent", 0)
        assert observed["pending"] == {
            "execution_kind": "resume",
            "starting_lifecycle": "sampled",
            "stability": "unstable",
            "instability_origin": "interrupt-parent",
        }
        assert storage.get_component_state(("P",)) == parent
        assert _component_result(storage, ("A",)) == {
            "lifecycle": "done",
            "stability": "unstable",
            "instability_origin": "interrupt-parent",
            "misaligned": False,
            "alignment_generation": 0,
        }
        assert {job_id: _job_snapshot(storage, "A", job_id) for job_id in selected} == selected_before
    finally:
        _close(storage)


def test_stable_parent_after_unstable_sample_preserves_component_origin(
    tmp_path, monkeypatch, capsys,
):
    storage = _make_parented_component(tmp_path, monkeypatch)
    try:
        _seed_unstable_parent(storage, "interrupt-own-result")
        _sample_half(storage, capsys)
        assert _set_done_result(storage, ("P",), "stable", None) == 1

        assert cli.main(["resume", "A"]) == 0
        capsys.readouterr()

        result = storage.get_component_state(("A",))
        assert (
            result["lifecycle"], result["stability"], result["instability_origin"],
            result["misaligned"], result["alignment_generation"],
        ) == ("done", "unstable", "interrupt-own-result", False, 0)
    finally:
        _close(storage)


def test_resume_refuses_changed_unstable_parent_before_preparation(tmp_path, monkeypatch, capsys):
    storage = _make_parented_component(tmp_path, monkeypatch)
    try:
        _seed_unstable_parent(storage, "interrupt-first")
        _sample_half(storage, capsys)
        _seed_unstable_parent(storage, "interrupt-second")
        storage.db_mutation_barrier()
        before_rows = _rows(storage)
        before_files = _node_files(tmp_path)
        sessions = storage.list_execution_sessions()

        assert cli.main(["resume", "A"]) == 1

        error = capsys.readouterr().err.lower()
        assert "incompatible" in error and "parent" in error
        assert _rows(storage) == before_rows
        assert _node_files(tmp_path) == before_files
        assert storage.list_execution_sessions() == sessions
        assert storage.get_component_reservation(("A",)) is None
    finally:
        _close(storage)


def test_resume_refuses_misaligned_sampled_component_before_preparation(tmp_path, monkeypatch, capsys):
    storage = _make_single_component(tmp_path, monkeypatch)
    try:
        _sample_half(storage, capsys)
        assert storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE component_states SET misaligned=1 WHERE component_key=?",
            (encode_component_key(("A",)),),
        ).rowcount) == 1
        storage.db_mutation_barrier()
        before_rows = _rows(storage)
        before_files = _node_files(tmp_path)
        sessions = storage.list_execution_sessions()

        assert cli.main(["resume", "A"]) == 1

        assert "misaligned" in capsys.readouterr().err.lower()
        assert _rows(storage) == before_rows
        assert _node_files(tmp_path) == before_files
        assert storage.list_execution_sessions() == sessions
        assert storage.get_component_reservation(("A",)) is None
    finally:
        _close(storage)


def test_failed_sampled_resume_settles_component_and_session_together(tmp_path, monkeypatch, capsys):
    storage = _make_single_component(tmp_path, monkeypatch)
    try:
        selected, remaining = _sample_half(storage, capsys)
        selected_before = {job_id: _job_snapshot(storage, "A", job_id) for job_id in selected}
        failing = remaining[0]
        (tmp_path / f"fail-{failing}").write_text("fail", encoding="utf-8")
        sessions = {item["session_id"] for item in storage.list_execution_sessions()}

        assert cli.main(["resume", "A"]) == 1

        assert f"Job A/{failing} failed" in capsys.readouterr().err
        assert {job_id: _job_snapshot(storage, "A", job_id) for job_id in selected} == selected_before
        failed_output = storage.read_json(storage.output_file("A", failing))
        assert failed_output["status"] == "failed"
        assert f"expected resume failure {failing}" in failed_output["error"]
        state = storage.get_component_state(("A",))
        assert (state["lifecycle"], state["stability"], state["instability_origin"]) == (
            "failed", None, None,
        )
        session = _new_session(storage, sessions, "resume")
        assert (session["status"], session["outcome"]) == ("terminal", "failed")
        assert f"Job A/{failing} failed" in session["failures"][0]["error"]
        owner = storage.read_job_current_owner("A", failing)
        assert owner["session_id"] == session["session_id"]
        assert failed_output["execution_id"] == owner["execution_id"]
        assert storage.get_component_reservation(("A",)) is None
        assert storage.db_connection().execute(
            "SELECT COUNT(*) FROM pending_component_executions WHERE session_id=?",
            (session["session_id"],),
        ).fetchone()[0] == 0
    finally:
        _close(storage)


def test_resumefrom_finishes_sampled_start_then_runs_quotient_descendant(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B')]",
        files={
            "A": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A", runner="direct")
                router.create_job(number=4)
                @router.task
                def run(ctx):
                    ctx.node("B").add(value=ctx.job_id)
                    return ctx.job_id
            ''',
            "B": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B", runner="direct")
                @router.task
                def run(ctx, value): return value
            ''',
        },
        runner="direct",
    )
    storage = FileStorage(tmp_path)
    try:
        _sample_half(storage, capsys)
        assert len(storage.list_job_ids("B")) == 2

        assert cli.main(["resumefrom", "A"]) == 0
        capsys.readouterr()

        assert storage.list_job_ids("B") == [1, 2, 3, 4]
        assert all(storage.get_job_status("A", job_id) == "done" for job_id in range(1, 5))
        assert all(storage.get_job_status("B", job_id) == "done" for job_id in range(1, 5))
        assert storage.get_component_state(("A",))["lifecycle"] == "done"
        assert storage.get_component_state(("B",))["lifecycle"] == "done"
        sessions = [item for item in storage.list_execution_sessions() if item["command"] == "resumefrom"]
        assert len(sessions) == 1
        assert sessions[0]["selected_components"] == [("A",), ("B",)]
        assert (sessions[0]["status"], sessions[0]["outcome"]) == ("terminal", "done")
    finally:
        _close(storage)


def test_recovery_preview_accepts_rootless_sampled_resume_pending_kind(
    tmp_path, monkeypatch, capsys,
):
    storage = _make_single_component(tmp_path, monkeypatch)
    try:
        _, remaining = _sample_half(storage, capsys)
        state = storage.get_component_state(("A",))
        storage.create_execution_session(
            "abandoned-resume",
            session_kind="main",
            command="resume",
            start_component=("A",),
            selected_components=[("A",)],
            **_live_execution_session_identity(),
            expected_shape=state["shape_json"],
        )
        assert storage.reserve_execution_components(
            "abandoned-resume", expected_shape=state["shape_json"],
        ) is True
        assert storage.begin_sampled_component_execution(
            "abandoned-resume", ("A",), expected_shape=state["shape_json"],
            expected_alignment_generation=state["alignment_generation"],
            successful_lineage=(state["stability"], state["instability_origin"]),
        ) is True
        generation, execution_id = storage.claim_job_execution(
            "A", remaining[0], started_at=now(), session_id="abandoned-resume", component=("A",),
        )
        assert (generation, execution_id) == (
            storage.current_job_generation("A", remaining[0]),
            storage.read_job_current_owner("A", remaining[0])["execution_id"],
        )
        _mark_execution_session_stale(storage, "abandoned-resume")
        storage.db_mutation_barrier()
        before_rows = _rows(storage)
        before_files = _node_files(tmp_path)

        assert cli.main(["recover", "--dry-run"]) == 0

        output = capsys.readouterr().out
        assert "abandoned session abandoned-resume" in output
        assert f"would requeue abandoned execution: A/{remaining[0]}" in output
        assert "pending execution kind disagrees" not in output
        assert _rows(storage) == before_rows
        assert _node_files(tmp_path) == before_files
    finally:
        _close(storage)
