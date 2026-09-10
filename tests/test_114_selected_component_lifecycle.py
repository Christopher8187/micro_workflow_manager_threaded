"""Selected component lifecycle, lineage, failure repair, and cumulative coverage."""

from __future__ import annotations

import os
import socket
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.errors import InvalidGraphError, JobFailedError
from micro_workflow_manager.models import now
from micro_workflow_manager.session_liveness import process_identity
from tests.test_090_component_session_settlement import _close, _rows


def _result(state):
    return {
        key: state[key]
        for key in (
            "lifecycle", "stability", "instability_origin",
            "misaligned", "alignment_generation",
        )
    }


def _assert_result(
    storage,
    component,
    lifecycle,
    *,
    stability,
    origin=None,
    misaligned=False,
    generation=0,
):
    assert _result(storage.get_component_state(component)) == {
        "lifecycle": lifecycle,
        "stability": stability,
        "instability_origin": origin,
        "misaligned": misaligned,
        "alignment_generation": generation,
    }


def _job_snapshot(storage, node, job_id):
    connection = storage.db_connection()
    rows = {}
    for table in ("jobs", "job_instances", "job_execution_owners", "job_events"):
        rows[table] = [dict(row) for row in connection.execute(
            f"SELECT * FROM {table} WHERE node_name=? AND job_id=? ORDER BY rowid",
            (node, job_id),
        )]
    base = storage.job_base_dir(node, job_id)
    files = {
        path.relative_to(base).as_posix(): path.read_bytes()
        for path in base.rglob("*") if path.is_file()
    }
    return {"job": storage.load_job(node, job_id), "rows": rows, "files": files}


def _assert_root_output(storage, previous_execution_id=None):
    owner = storage.read_job_current_owner("A", 1)
    assert owner is not None
    if previous_execution_id is not None:
        assert owner["execution_id"] != previous_execution_id
    assert json.loads(storage.output_file("A", 1).read_text(encoding="utf-8")) == {
        "status": "done",
        "result_type": "str",
        "result_repr": repr("root result"),
        "generation": owner["generation"],
        "execution_id": owner["execution_id"],
    }
    return owner


def _selected_component(tmp_path, *, cyclic=False):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "B"), ("B", "A")] if cyclic else [("A", "A")])
    values = {1: object(), 2: object()}
    failures = set()
    pauses = set()
    entered = Event()
    release = Event()

    @workflow.task("A")
    def node_a(ctx):
        if ctx.job_id in pauses:
            entered.set()
            assert release.wait(20)
        if ctx.job_id in failures:
            raise ValueError(f"selected failure {ctx.job_id}")
        return values[ctx.job_id]

    if cyclic:
        @workflow.task("B")
        def node_b(ctx):
            raise AssertionError(f"zero-population member ran as job {ctx.job_id}")

    workflow.add_job(None, "A", job_id=1)
    workflow.add_job(None, "A", job_id=2)
    return workflow, values, failures, pauses, entered, release


@pytest.mark.parametrize(
    "selected,expected_lifecycle",
    [pytest.param([2], "sampled", id="proper-subset"),
     pytest.param([2, 1], "done", id="full-current-population")],
)
def test_queued_selected_coverage_sets_sampled_or_done(tmp_path, selected, expected_lifecycle):
    workflow, values, _, _, _, _ = _selected_component(tmp_path, cyclic=True)
    storage = workflow.storage
    try:
        returned = workflow.run_jobs("A", selected, ignore_readiness=True)

        assert len(returned) == len(selected)
        assert all(actual is values[job_id] for actual, job_id in zip(returned, selected))
        _assert_result(
            storage, ("A", "B"), expected_lifecycle,
            stability="stable", generation=0,
        )
        for job_id in (1, 2):
            expected = "done" if job_id in selected else "queued"
            assert storage.get_job_status("A", job_id) == expected
            if expected == "queued":
                assert storage.read_job_current_owner("A", job_id) is None
        assert storage.list_job_ids("B") == []
        session_id = storage.read_job_current_owner("A", selected[0])["session_id"]
        session = storage.get_execution_session(session_id)
        assert session["selected_jobs"] == [("A", job_id) for job_id in selected]
        assert (session["status"], session["outcome"]) == ("terminal", "done")
    finally:
        _close(storage)


def test_selected_success_accumulates_sampled_coverage_then_promotes_to_done(tmp_path):
    workflow, values, _, _, _, _ = _selected_component(tmp_path)
    storage = workflow.storage
    try:
        assert workflow.run_job("A", 1, ignore_readiness=True) is values[1]
        first_owner = storage.read_job_current_owner("A", 1)
        _assert_result(storage, ("A",), "sampled", stability="stable")
        assert storage.get_job_status("A", 2) == "queued"
        assert storage.read_job_current_owner("A", 2) is None

        assert workflow.run_job("A", 2, ignore_readiness=True) is values[2]
        second_owner = storage.read_job_current_owner("A", 2)
        assert second_owner["session_id"] != first_owner["session_id"]
        _assert_result(storage, ("A",), "done", stability="stable")
        assert storage.get_job_status("A", 1) == storage.get_job_status("A", 2) == "done"
    finally:
        _close(storage)


def test_selected_rerun_keeps_sampled_lineage_while_running(tmp_path):
    workflow, values, _, pauses, entered, release = _selected_component(tmp_path)
    storage = workflow.storage
    try:
        assert workflow.run_job("A", 1, ignore_readiness=True) is values[1]
        _assert_result(storage, ("A",), "sampled", stability="stable")
        pauses.add(1)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(workflow.run_job, "A", 1, True)
            try:
                assert entered.wait(20)
                _assert_result(storage, ("A",), "running", stability="stable")
                assert storage.get_job_status("A", 2) == "queued"
                assert storage.read_job_current_owner("A", 2) is None
            finally:
                release.set()
            assert future.result(timeout=20) is values[1]
        _assert_result(storage, ("A",), "sampled", stability="stable")
    finally:
        release.set()
        _close(storage)


def test_sampled_failure_and_repair_restore_sampled_until_coverage_is_full(tmp_path):
    workflow, values, failures, _, _, _ = _selected_component(tmp_path)
    storage = workflow.storage
    try:
        workflow.run_job("A", 1, ignore_readiness=True)
        _assert_result(storage, ("A",), "sampled", stability="stable")

        failures.add(1)
        with pytest.raises(JobFailedError) as caught:
            workflow.run_job("A", 1, ignore_readiness=True)
        assert isinstance(caught.value.__cause__, ValueError)
        _assert_result(storage, ("A",), "failed", stability=None)
        assert storage.get_job_status("A", 2) == "queued"

        failures.clear()
        assert workflow.run_job("A", 1, ignore_readiness=True) is values[1]
        _assert_result(storage, ("A",), "sampled", stability="stable")
        assert storage.get_job_status("A", 2) == "queued"

        assert workflow.run_job("A", 2, ignore_readiness=True) is values[2]
        _assert_result(storage, ("A",), "done", stability="stable")
    finally:
        _close(storage)


def _done_then_misaligned(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("P", "A")])
    publish = {"enabled": False}
    failures = set()
    pauses = set()
    entered = Event()
    release = Event()

    @workflow.task("P")
    def producer(ctx):
        if publish["enabled"]:
            ctx.node("A").add(job_id=2, role="unrelated")

    @workflow.task("A")
    def receiver(ctx, role):
        if role == "unrelated":
            raise AssertionError("unselected late job executed")
        if ctx.job_id in pauses:
            entered.set()
            assert release.wait(20)
        if ctx.job_id in failures:
            raise ValueError("done selected rerun failed")
        return "root result"

    storage = workflow.storage
    try:
        workflow.add_job(None, "P", job_id=1)
        workflow.add_job(None, "A", job_id=1, role="root")
        workflow.run_node("P")
        workflow.run_node("A")
        _assert_result(storage, ("A",), "done", stability="stable", generation=1)
        established_owner = _assert_root_output(storage)
        established_job = storage.load_job("A", 1)
        publish["enabled"] = True
        workflow.run_node("P")
        _assert_result(
            storage, ("A",), "done", stability="stable", misaligned=True, generation=1,
        )
        assert storage.get_job_status("A", 2) == "queued"
        assert storage.read_job_current_owner("A", 2) is None
        causes = storage.read_component_misalignment_causes(("A",))
        assert len(causes) == 1
        assert storage.load_job("A", 1) == established_job
        assert _assert_root_output(storage) == established_owner
        unselected = _job_snapshot(storage, "A", 2)
        return (
            workflow, failures, pauses, entered, release, causes,
            established_owner, established_job, unselected,
        )
    except BaseException:
        _close(storage)
        raise


def test_done_misaligned_selected_rerun_is_running_without_clearing_result_or_cause(tmp_path):
    (workflow, _, pauses, entered, release, causes,
     established_owner, established_job, unselected) = _done_then_misaligned(tmp_path)
    storage = workflow.storage
    pauses.add(1)
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(workflow.run_job, "A", 1)
            try:
                assert entered.wait(20)
                _assert_result(
                    storage, ("A",), "running", stability="stable",
                    misaligned=True, generation=1,
                )
                assert storage.read_component_misalignment_causes(("A",)) == causes
                assert _job_snapshot(storage, "A", 2) == unselected
            finally:
                release.set()
            assert future.result(timeout=20) == "root result"
        _assert_result(
            storage, ("A",), "done", stability="stable", misaligned=True, generation=1,
        )
        assert storage.read_component_misalignment_causes(("A",)) == causes
        assert _job_snapshot(storage, "A", 2) == unselected
        new_owner = _assert_root_output(storage, established_owner["execution_id"])
        assert new_owner["job_instance_id"] == established_owner["job_instance_id"]
        assert storage.load_job("A", 1) == established_job
    finally:
        release.set()
        _close(storage)


def test_done_misaligned_failure_and_repair_restore_prior_done_result(tmp_path):
    (workflow, failures, _, _, _, causes,
     established_owner, established_job, unselected) = _done_then_misaligned(tmp_path)
    storage = workflow.storage
    try:
        failures.add(1)
        with pytest.raises(JobFailedError):
            workflow.run_job("A", 1)
        _assert_result(
            storage, ("A",), "failed", stability=None, misaligned=True, generation=1,
        )
        assert storage.read_component_misalignment_causes(("A",)) == causes
        assert _job_snapshot(storage, "A", 2) == unselected

        failures.clear()
        assert workflow.run_job("A", 1) == "root result"
        _assert_result(
            storage, ("A",), "done", stability="stable", misaligned=True, generation=1,
        )
        assert storage.read_component_misalignment_causes(("A",)) == causes
        assert _job_snapshot(storage, "A", 2) == unselected
        new_owner = _assert_root_output(storage, established_owner["execution_id"])
        assert new_owner["job_instance_id"] == established_owner["job_instance_id"]
        assert storage.load_job("A", 1) == established_job
    finally:
        _close(storage)


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


def _seed_unstable_parent(storage, component, origin):
    storage.create_execution_session(
        origin,
        session_kind="interrupt",
        command="run",
        start_component=component,
        selected_components=[component],
        started_at=now(),
        hostname=socket.gethostname(),
        pid=os.getpid(),
        process_identity=process_identity(os.getpid()),
        expected_shape=storage.get_component_definition(component)["shape_json"],
    )
    assert storage.reserve_execution_components(
        origin, expected_shape=storage.get_component_definition(component)["shape_json"],
    ) is True
    assert storage.db_connection().execute(
        "SELECT scope_admitted FROM execution_sessions WHERE session_id=?", (origin,),
    ).fetchone()[0] == 1
    assert storage.finish_execution_session(origin, outcome="done", finished_at=now()) is True
    assert storage.release_execution_components(origin) == 1
    assert storage.get_component_reservation(component) is None
    assert _set_done_result(storage, component, "unstable", origin) == 1
    storage.set_node_status(component[0], "done")


def test_selected_partial_and_full_results_keep_exact_unstable_parent_origin(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("P", "A")])
    values = {1: object(), 2: object()}

    @workflow.task("A")
    def child(ctx):
        return values[ctx.job_id]

    workflow.add_job(None, "A", job_id=1)
    workflow.add_job(None, "A", job_id=2)
    storage = workflow.storage
    try:
        storage.register_component_topology(workflow.topology.snapshot())
        _seed_unstable_parent(storage, ("P",), "interrupt-parent")

        assert workflow.run_job("A", 1) is values[1]
        _assert_result(
            storage, ("A",), "sampled",
            stability="unstable", origin="interrupt-parent",
        )
        assert workflow.run_job("A", 2) is values[2]
        _assert_result(
            storage, ("A",), "done",
            stability="unstable", origin="interrupt-parent",
        )
    finally:
        _close(storage)


def test_selected_start_refuses_incompatible_parent_results_before_mutation(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("P", "A"), ("Q", "A")])

    @workflow.task("A")
    def child(ctx):
        raise AssertionError("incompatible parents admitted selected work")

    workflow.add_job(None, "A", job_id=1)
    storage = workflow.storage
    try:
        storage.register_component_topology(workflow.topology.snapshot())
        _seed_unstable_parent(storage, ("P",), "interrupt-p")
        _seed_unstable_parent(storage, ("Q",), "interrupt-q")
        storage.db_mutation_barrier()
        before = _rows(storage)

        with pytest.raises(InvalidGraphError, match="not ready"):
            workflow.run_job("A", 1)

        assert _rows(storage) == before
        assert storage.get_job_status("A", 1) == "queued"
        assert storage.read_job_current_owner("A", 1) is None
        _assert_result(storage, ("A",), "queued", stability=None)
    finally:
        _close(storage)


def test_zero_selected_jobs_leave_registered_component_unchanged(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "A")])
    storage = workflow.storage
    try:
        storage.register_component_topology(workflow.topology.snapshot())
        before = _rows(storage)
        assert workflow.run_jobs("A", []) == []
        assert _rows(storage) == before
        _assert_result(storage, ("A",), "queued", stability=None)
    finally:
        _close(storage)
