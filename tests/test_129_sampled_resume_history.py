"""Sensitive public refusal for conflicting retained sampled history.

External draft only. Root may copy this into Direct after review.
"""

import os
import socket

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.models import now
from micro_workflow_manager.processes import process_identity
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_093_native_cli_readiness import _node_files
from tests.test_117_execution_sampling import _component_result, _job_snapshot
from tests.test_125_sampled_resume import _make_single_component, _sample_half


def test_resume_refuses_conflicting_retained_sample_history_before_admission(
    tmp_path, monkeypatch, capsys,
):
    storage = _make_single_component(tmp_path, monkeypatch)
    try:
        selected, queued = _sample_half(storage, capsys)
        state = storage.get_component_state(("A",))
        assert (
            state["lifecycle"], state["stability"], state["instability_origin"]
        ) == ("sampled", "stable", None)
        key = encode_component_key(("A",))
        assert storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE component_successful_results SET lifecycle='done' "
            "WHERE component_key=? AND shape_id=(SELECT shape_id FROM component_definitions "
            "WHERE component_key=?) AND alignment_generation=? "
            "AND lifecycle='sampled' AND stability='stable' AND instability_origin IS NULL",
            (key, key, state["alignment_generation"]),
        ).rowcount) == 1
        storage.db_mutation_barrier()
        before_rows = _rows(storage)
        before_files = _node_files(tmp_path)
        before_jobs = {
            job_id: _job_snapshot(storage, "A", job_id)
            for job_id in (*selected, *queued)
        }
        before_sessions = storage.list_execution_sessions()

        assert cli.main(["resume", "A", "--runner", "direct"]) == 1

        assert "retained successful" in capsys.readouterr().err.lower()
        assert _rows(storage) == before_rows
        assert _node_files(tmp_path) == before_files
        assert {
            job_id: _job_snapshot(storage, "A", job_id)
            for job_id in (*selected, *queued)
        } == before_jobs
        assert storage.list_execution_sessions() == before_sessions
        assert storage.get_component_reservation(("A",)) is None
    finally:
        _close(storage)


def _make_failing_parented_component(tmp_path, monkeypatch):
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


def _set_unstable_result(storage, component, origin):
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
    assert storage.finish_execution_session(origin, outcome="done", finished_at=now()) is True
    key = encode_component_key(component)
    assert _set_done_result(storage, component, "unstable", origin) == 1
    for node in component:
        storage.set_node_status(node, "done")


def test_failed_sampled_resume_repair_retains_exact_origin_with_stable_parent(
    tmp_path, monkeypatch, capsys,
):
    storage = _make_failing_parented_component(tmp_path, monkeypatch)
    try:
        _set_unstable_result(storage, ("P",), "interrupt-own-result")
        selected, queued = _sample_half(storage, capsys)
        sampled = storage.get_component_state(("A",))
        assert (
            sampled["lifecycle"], sampled["stability"], sampled["instability_origin"],
            sampled["alignment_generation"],
        ) == ("sampled", "unstable", "interrupt-own-result", 0)
        assert _set_done_result(storage, ("P",), "stable", None) == 1
        failing = queued[0]
        marker = tmp_path / f"fail-{failing}"
        marker.write_text("fail", encoding="utf-8")

        assert cli.main(["resume", "A", "--runner", "direct"]) == 1

        assert f"Job A/{failing} failed" in capsys.readouterr().err
        failed = storage.get_component_state(("A",))
        assert (
            failed["lifecycle"], failed["stability"], failed["instability_origin"],
            failed["alignment_generation"],
        ) == ("failed", None, None, sampled["alignment_generation"])
        retained = storage.db_connection().execute(
            "SELECT lifecycle, stability, instability_origin FROM component_successful_results "
            "WHERE component_key=? AND alignment_generation=?",
            (encode_component_key(("A",)), sampled["alignment_generation"]),
        ).fetchone()
        assert tuple(retained) == ("sampled", "unstable", "interrupt-own-result")
        selected_before = {
            job_id: _job_snapshot(storage, "A", job_id) for job_id in selected
        }
        marker.unlink()

        assert cli.main(["resume", "A", "--runner", "direct"]) == 0

        capsys.readouterr()
        assert _component_result(storage, ("A",)) == {
            "lifecycle": "done",
            "stability": "unstable",
            "instability_origin": "interrupt-own-result",
            "misaligned": False,
            "alignment_generation": sampled["alignment_generation"],
        }
        retained = storage.db_connection().execute(
            "SELECT lifecycle, stability, instability_origin FROM component_successful_results "
            "WHERE component_key=? AND alignment_generation=?",
            (encode_component_key(("A",)), sampled["alignment_generation"]),
        ).fetchone()
        assert tuple(retained) == ("done", "unstable", "interrupt-own-result")
        assert {
            job_id: _job_snapshot(storage, "A", job_id) for job_id in selected
        } == selected_before
    finally:
        _close(storage)


def test_resumefrom_refuses_incompatible_selected_parent_lineage_before_admission(
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
                @router.task
                def run(ctx): return "A"
            ''',
            "B": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B", runner="direct")
                router.create_job(number=4)
                @router.task
                def run(ctx): return f"B/{ctx.job_id}"
            ''',
        },
        runner="direct",
    )
    workflow = load_workflow(tmp_path, "direct")
    storage = workflow.storage
    try:
        storage.register_component_topology(workflow.topology.snapshot())
        _set_unstable_result(storage, ("A",), "interrupt-first")
        capsys.readouterr()
        assert cli.main([
            "run", "B", "sample", "50%", "--seed", "selected-parent-lineage",
            "--runner", "direct",
        ]) == 0
        capsys.readouterr()
        assert _component_result(storage, ("B",)) == {
            "lifecycle": "sampled",
            "stability": "unstable",
            "instability_origin": "interrupt-first",
            "misaligned": False,
            "alignment_generation": 0,
        }
        _set_unstable_result(storage, ("A",), "interrupt-second")
        before_rows = _rows(storage)
        before_files = _node_files(tmp_path)
        before_jobs = {
            job_id: _job_snapshot(storage, "B", job_id) for job_id in range(1, 5)
        }
        before_sessions = storage.list_execution_sessions()

        assert cli.main(["resumefrom", "A", "--runner", "direct"]) == 1

        assert "incompatible" in capsys.readouterr().err.lower()
        assert _rows(storage) == before_rows
        assert _node_files(tmp_path) == before_files
        assert {
            job_id: _job_snapshot(storage, "B", job_id) for job_id in range(1, 5)
        } == before_jobs
        assert storage.list_execution_sessions() == before_sessions
        assert storage.get_component_reservation(("A",)) is None
        assert storage.get_component_reservation(("B",)) is None
    finally:
        _close(storage)
