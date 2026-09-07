"""Selected lifecycle writer atomicity and identity refusal."""

from __future__ import annotations

import json
from threading import Event, Thread

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.component_identity import encode_component_key
from tests.test_090_component_session_settlement import (
    _close,
    _outcome as _full_outcome,
    _rows as _settlement_rows,
    running_component,
)


def test_ready_selected_result_survives_an_error_after_its_durable_ready_write(
    tmp_path, monkeypatch,
):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "A")])
    storage = workflow.storage
    returned = object()
    after_ready_error = OSError("caller failed after selected result became ready")

    @workflow.task("A")
    def work(ctx):
        assert ctx.job_id == 1
        return returned

    workflow.add_job(None, "A", job_id=1)
    workflow.add_job(None, "A", job_id=2)
    mark_ready = storage.mark_selected_component_execution_ready
    ready_session = []

    def mark_ready_then_fail(session_id, proposal):
        mark_ready(session_id, proposal)
        row = storage.db_connection().execute(
            "SELECT execution_kind, completion_ready FROM pending_component_executions "
            "WHERE session_id=?",
            (session_id,),
        ).fetchone()
        assert tuple(row) == ("jobs", 1)
        ready_session.append(session_id)
        raise after_ready_error

    monkeypatch.setattr(storage, "mark_selected_component_execution_ready", mark_ready_then_fail)
    try:
        with pytest.raises(OSError) as caught:
            workflow.run_job("A", 1)
        assert caught.value is after_ready_error
        assert len(ready_session) == 1
        session_id = ready_session[0]

        owner = storage.read_job_current_owner("A", 1)
        assert owner["session_id"] == session_id
        assert storage.get_job_status("A", 1) == "done"
        assert json.loads(storage.output_file("A", 1).read_text(encoding="utf-8")) == {
            "status": "done",
            "result_type": "object",
            "result_repr": repr(returned),
            "generation": owner["generation"],
            "execution_id": owner["execution_id"],
        }
        assert storage.get_job_status("A", 2) == "queued"
        assert storage.read_job_current_owner("A", 2) is None

        state = storage.get_component_state(("A",))
        assert (
            state["lifecycle"], state["stability"], state["instability_origin"],
            state["misaligned"], state["alignment_generation"],
        ) == ("sampled", "stable", None, False, 0)
        retained = storage.db_connection().execute(
            "SELECT lifecycle, stability, instability_origin FROM component_successful_results "
            "WHERE component_key=? AND alignment_generation=0",
            (encode_component_key(("A",)),),
        ).fetchone()
        assert tuple(retained) == ("sampled", "stable", None)

        session = storage.get_execution_session(session_id)
        assert session["selected_jobs"] == [("A", 1)]
        assert (session["status"], session["outcome"]) == ("terminal", "failed")
        assert storage.get_component_reservation(("A",)) is None
        assert storage.db_connection().execute(
            "SELECT COUNT(*) FROM pending_component_executions WHERE session_id=?",
            (session_id,),
        ).fetchone()[0] == 0
    finally:
        _close(storage)


def _two_job_selected_workflow(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "A")])

    @workflow.task("A")
    def work(ctx):
        return ctx.job_id

    workflow.add_job(None, "A", job_id=1)
    workflow.add_job(None, "A", job_id=2)
    workflow.storage.register_component_topology(workflow.topology.snapshot())
    return workflow


def _job_snapshot(storage, job_id):
    job = storage.load_job("A", job_id)
    events = storage.read_job_events("A", job_id)
    owner = storage.read_job_current_owner("A", job_id)
    base = storage.job_base_dir("A", job_id)
    files = {
        path.relative_to(base).as_posix(): path.read_bytes()
        for path in base.rglob("*") if path.is_file()
    }
    return job, events, owner, files


def test_suppressed_selected_pending_insert_rolls_back_the_component_start(tmp_path, monkeypatch):
    workflow = _two_job_selected_workflow(tmp_path)
    storage = workflow.storage
    before_state = storage.get_component_state(("A",))
    before_jobs = []
    begin_selected = storage.begin_selected_component_execution

    def capture_prepared_jobs(*args, **kwargs):
        before_jobs.extend(_job_snapshot(storage, job_id) for job_id in (1, 2))
        return begin_selected(*args, **kwargs)

    monkeypatch.setattr(storage, 'begin_selected_component_execution', capture_prepared_jobs)
    before_history = [dict(row) for row in storage.db_connection().execute(
        "SELECT * FROM component_successful_results ORDER BY component_key, alignment_generation",
    )]
    storage.db_connection().execute(
        "CREATE TRIGGER suppress_selected_pending BEFORE INSERT ON pending_component_executions "
        "WHEN NEW.execution_kind='jobs' BEGIN SELECT RAISE(IGNORE); END"
    )
    try:
        with pytest.raises(RuntimeError):
            workflow.run_job("A", 1)

        assert storage.get_component_state(("A",)) == before_state
        assert [_job_snapshot(storage, job_id) for job_id in (1, 2)] == before_jobs
        assert [dict(row) for row in storage.db_connection().execute(
            "SELECT * FROM component_successful_results ORDER BY component_key, alignment_generation",
        )] == before_history
        assert storage.db_connection().execute(
            "SELECT COUNT(*) FROM pending_component_executions",
        ).fetchone()[0] == 0
        session, = storage.list_execution_sessions()
        assert (session["status"], session["outcome"]) == ("terminal", "failed")
        assert storage.get_component_reservation(("A",)) is None
        assert storage.get_live_main_session() is None
    finally:
        _close(storage)


def test_suppressed_success_history_write_rolls_back_selected_terminal_settlement(
    tmp_path, monkeypatch,
):
    workflow = _two_job_selected_workflow(tmp_path)
    storage = workflow.storage
    storage.db_connection().execute(
        "CREATE TRIGGER suppress_selected_history BEFORE INSERT ON component_successful_results "
        "BEGIN SELECT RAISE(IGNORE); END"
    )
    mark_ready = storage.mark_selected_component_execution_ready
    ready = {}

    def capture_ready_state(session_id, proposal):
        mark_ready(session_id, proposal)
        ready["session_id"] = session_id
        ready["state"] = storage.get_component_state(("A",))
        ready["pending"] = dict(storage.db_connection().execute(
            "SELECT * FROM pending_component_executions WHERE session_id=?",
            (session_id,),
        ).fetchone())
        ready["jobs"] = [_job_snapshot(storage, job_id) for job_id in (1, 2)]

    monkeypatch.setattr(storage, "mark_selected_component_execution_ready", capture_ready_state)
    try:
        with pytest.raises(RuntimeError):
            workflow.run_job("A", 1)

        session_id = ready["session_id"]
        assert ready["pending"]["execution_kind"] == "jobs"
        assert ready["pending"]["completion_ready"] == 1
        assert storage.get_component_state(("A",)) == ready["state"]
        assert [_job_snapshot(storage, job_id) for job_id in (1, 2)] == ready["jobs"]
        assert dict(storage.db_connection().execute(
            "SELECT * FROM pending_component_executions WHERE session_id=?",
            (session_id,),
        ).fetchone()) == ready["pending"]
        assert storage.db_connection().execute(
            "SELECT COUNT(*) FROM component_successful_results",
        ).fetchone()[0] == 0
        session = storage.get_execution_session(session_id)
        assert session["status"] == "running" and session["outcome"] is None
        assert storage.get_component_reservation(("A",))["session_id"] == session_id
        assert storage.get_live_main_session()["session_id"] == session_id
    finally:
        _close(storage)


def test_full_terminal_settlement_refuses_a_nonqueued_start_record(running_component):
    storage, topology, generation, execution_id = running_component
    storage.finalize_job_execution("A", 1, generation, execution_id, "done")
    storage.submit_db_mutation(lambda connection: connection.execute(
        "UPDATE pending_component_executions SET starting_lifecycle='done' "
        "WHERE session_id='ordinary-result'",
    ))
    before = _settlement_rows(storage)

    with pytest.raises(RuntimeError, match="pending|starting state"):
        storage.finish_successful_component_execution(
            "ordinary-result", _full_outcome(topology),
        )

    assert _settlement_rows(storage) == before


@pytest.mark.parametrize("damage", ["queued-misaligned", "queued-history"])
def test_selected_terminal_settlement_refuses_impossible_queued_start_history(
    tmp_path, monkeypatch, damage,
):
    workflow = _two_job_selected_workflow(tmp_path)
    storage = workflow.storage
    mark_ready = storage.mark_selected_component_execution_ready
    captured = {}

    def damage_after_ready(session_id, proposal):
        mark_ready(session_id, proposal)

        def damage_pending(connection):
            pending = connection.execute(
                "SELECT component_key, shape_id, alignment_generation "
                "FROM pending_component_executions WHERE session_id=?",
                (session_id,),
            ).fetchone()
            assert pending is not None
            if damage == "queued-misaligned":
                connection.execute(
                    "UPDATE pending_component_executions SET starting_misaligned=1 "
                    "WHERE session_id=?",
                    (session_id,),
                )
            else:
                connection.execute(
                    "INSERT INTO component_successful_results "
                    "(component_key, shape_id, alignment_generation, lifecycle, stability, instability_origin) "
                    "VALUES(?,?,?,?,?,NULL)",
                    (
                        pending["component_key"], pending["shape_id"],
                        pending["alignment_generation"], "sampled", "stable",
                    ),
                )

        storage.submit_db_mutation(damage_pending, wait=True, priority=0)
        captured["rows"] = _settlement_rows(storage)
        captured["jobs"] = [_job_snapshot(storage, job_id) for job_id in (1, 2)]

    monkeypatch.setattr(
        storage, "mark_selected_component_execution_ready", damage_after_ready,
    )
    try:
        with pytest.raises(RuntimeError, match="pending|retained|queued|aligned running state"):
            workflow.run_job("A", 1)

        assert _settlement_rows(storage) == captured["rows"]
        assert [_job_snapshot(storage, job_id) for job_id in (1, 2)] == captured["jobs"]
    finally:
        _close(storage)


def _root_job_snapshot(storage):
    connection = storage.db_connection()
    rows = {
        "job": [dict(row) for row in connection.execute(
            "SELECT * FROM jobs WHERE node_name='A' AND job_id=1",
        )],
        "instance": [dict(row) for row in connection.execute(
            "SELECT * FROM job_instances WHERE node_name='A' AND job_id=1",
        )],
        "owners": [dict(row) for row in connection.execute(
            "SELECT * FROM job_execution_owners WHERE node_name='A' AND job_id=1 "
            "ORDER BY generation, execution_id",
        )],
        "events": [dict(row) for row in connection.execute(
            "SELECT * FROM job_events WHERE node_name='A' AND job_id=1 ORDER BY event_id",
        )],
    }
    root = storage.job_base_dir("A", 1)
    files = {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }
    return {
        "rows": rows,
        "status": storage.get_job_status("A", 1),
        "control": storage.read_job_control("A", 1),
        "instance_id": storage.read_job_instance_id("A", 1),
        "current_owner": storage.read_job_current_owner("A", 1),
        "params": storage.load_job("A", 1).params,
        "files": files,
    }


def test_selected_root_claim_refuses_alignment_drift_after_frontier_observation(
    tmp_path, monkeypatch,
):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "A")])
    storage = workflow.storage
    calls = []
    at_claim = Event()
    release_claim = Event()
    result = {}

    @workflow.task("A")
    def root(ctx, value):
        calls.append((ctx.job_id, ctx.execution_generation, value))
        return value

    workflow.add_job(None, "A", job_id=1, value="must not run")
    original_claim = storage.claim_job_execution

    def pause_first_root_claim(node_name, job_id, *args, **kwargs):
        if node_name == "A" and job_id == 1 and not at_claim.is_set():
            at_claim.set()
            assert release_claim.wait(30)
        return original_claim(node_name, job_id, *args, **kwargs)

    monkeypatch.setattr(storage, "claim_job_execution", pause_first_root_claim)

    def run_selected():
        try:
            result["value"] = workflow.run_job("A", 1, ignore_readiness=True)
        except BaseException as error:
            result["error"] = error

    thread = Thread(target=run_selected, name="selected-root-claim-drift", daemon=True)
    thread.start()
    try:
        assert at_claim.wait(20), result

        # The public operation has observed its finite frontier and begun its
        # selected component, but the first job claim has not entered storage.
        before = _root_job_snapshot(storage)
        assert before["status"] == "queued"
        assert before["control"]["active_execution_id"] is None
        assert before["current_owner"] is None
        assert before["rows"]["owners"] == []
        assert before["params"] == {"value": "must not run"}
        assert set(before["files"]) == {"input.json"}
        assert not storage.output_file("A", 1).exists()
        assert calls == []

        key = encode_component_key(("A",))
        connection = storage.db_connection()
        pending = dict(connection.execute(
            "SELECT session_id, shape_id, alignment_generation, execution_kind, completion_ready "
            "FROM pending_component_executions WHERE component_key=?",
            (key,),
        ).fetchone())
        state = storage.get_component_state(("A",))
        assert state["lifecycle"] == "running"
        assert pending["execution_kind"] == "jobs"
        assert pending["completion_ready"] == 0
        assert pending["alignment_generation"] == state["alignment_generation"]

        changed = storage.submit_db_mutation(lambda writer: writer.execute(
            "UPDATE component_states SET alignment_generation=alignment_generation+1 "
            "WHERE component_key=? AND alignment_generation=?",
            (key, pending["alignment_generation"]),
        ).rowcount, priority=0)
        assert changed == 1
        assert storage.get_component_state(("A",))["alignment_generation"] == (
            pending["alignment_generation"] + 1
        )
        assert _root_job_snapshot(storage) == before

        release_claim.set()
        thread.join(timeout=30)
        assert not thread.is_alive(), result
        error = result.get("error")
        assert isinstance(error, RuntimeError), result
        assert "alignment" in str(error).lower() or "aligned running state" in str(error).lower()
        assert "value" not in result

        # Claim validation and the rejected terminal decision must not mutate
        # the selected root or manufacture an execution owner.
        assert _root_job_snapshot(storage) == before
        assert storage.read_job_current_owner("A", 1) is None
        assert not storage.output_file("A", 1).exists()
        assert calls == []
        sessions = storage.list_execution_sessions()
        assert len(sessions) == 1
        assert sessions[0]["session_id"] == pending["session_id"]
        assert sessions[0]["status"] == "running"
        assert storage.get_component_reservation(("A",)) == {
            "members": ("A",), "session_id": pending["session_id"],
        }
    finally:
        release_claim.set()
        thread.join(timeout=30)
        assert not thread.is_alive(), result
        _close(storage)
