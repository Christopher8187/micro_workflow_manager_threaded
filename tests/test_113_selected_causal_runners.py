"""Selected causal execution across CLI, waiting, and runner boundaries."""

from __future__ import annotations

from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys
from threading import Event, Thread, get_ident

import pytest

from micro_workflow_manager import MicroWorkflow, NodeRouter, cli
from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.errors import JobFailedError
from micro_workflow_manager.storage import FileStorage
from tests.test_090_component_session_settlement import _close


def _environment():
    return dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))


def _restart(tmp_path, node, job_id):
    command = subprocess.run(
        [sys.executable, "-m", "micro_workflow_manager", "restart", node, "job", str(job_id)],
        cwd=tmp_path,
        env=_environment(),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert command.returncode == 0, command.stdout + command.stderr
    return command


def _assert_unclaimed(storage, node, job_id):
    assert storage.get_job_status(node, job_id) == "queued"
    assert storage.read_job_current_owner(node, job_id) is None


def _assert_terminal_done(storage, session_id, component):
    session = storage.get_execution_session(session_id)
    assert (session["status"], session["outcome"]) == ("terminal", "done")
    assert storage.get_component_reservation(component) is None
    assert storage.get_live_main_session() is None


def _write_cli_selected_project(tmp_path, runner):
    source = tmp_path / "src"
    behaviors = source / "node_behavior"
    behaviors.mkdir(parents=True)
    (source / "graph.py").write_text(
        "EDGES = [('A', 'B'), ('B', 'A'), ('B', 'C')]\n",
        encoding="utf-8",
    )
    (behaviors / "A.py").write_text(
        f'''from micro_workflow_manager import NodeRouter
router = NodeRouter("A", runner={runner!r}, max_threads=2)
router.create_job(number=7, params={{"role": "existing"}})
@router.task
def work(ctx, role):
    if ctx.job_id == 7 and role == "existing":
        ctx.node("B").add(role="child")
        return "selected-root"
    if role == "grandchild":
        return "causal-grandchild"
    raise AssertionError("unrelated A job executed")
''',
        encoding="utf-8",
    )
    (behaviors / "B.py").write_text(
        f'''from micro_workflow_manager import NodeRouter
router = NodeRouter("B", runner={runner!r}, max_threads=2)
router.create_job(params={{"role": "existing"}})
@router.task
def work(ctx, role):
    if role != "child":
        raise AssertionError("unrelated B job executed")
    ctx.node("A").add(role="grandchild")
    ctx.node("C").add(role="quotient")
    return "causal-child"
''',
        encoding="utf-8",
    )
    (behaviors / "C.py").write_text(
        f'''from micro_workflow_manager import NodeRouter
router = NodeRouter("C", runner={runner!r})
router.create_job(params={{"role": "existing"}})
@router.task
def work(ctx, role):
    raise AssertionError("quotient or unrelated C job executed")
''',
        encoding="utf-8",
    )


@pytest.mark.parametrize("runner", ["direct", "process"])
def test_cli_exact_selection_runs_recursive_same_component_causal_jobs_only(
    tmp_path, monkeypatch, capsys, runner,
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[1]))
    _write_cli_selected_project(tmp_path, runner)
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", runner]) == 0
    capsys.readouterr()

    assert cli.main(["run", "A", "job", "7", "--runner", runner]) == 0
    output = capsys.readouterr().out

    storage = FileStorage(tmp_path)
    try:
        assert "Ran jobs for A:" in output and "  7" in output
        for node, job_id in (("A", 7), ("A", 8), ("B", 2)):
            assert storage.get_job_status(node, job_id) == "done"
        for job_id in range(1, 7):
            _assert_unclaimed(storage, "A", job_id)
        _assert_unclaimed(storage, "B", 1)
        _assert_unclaimed(storage, "C", 1)
        _assert_unclaimed(storage, "C", 2)

        root = storage.read_job_current_owner("A", 7)
        child = storage.read_job_current_owner("B", 2)
        grandchild = storage.read_job_current_owner("A", 8)
        assert child["created_by_execution_id"] == root["execution_id"]
        assert grandchild["created_by_execution_id"] == child["execution_id"]
        assert root["session_id"] == child["session_id"] == grandchild["session_id"]
        assert root["component"] == child["component"] == grandchild["component"] == ("A", "B")
        quotient_creator = storage.db_connection().execute(
            "SELECT created_by_execution_id FROM job_instances WHERE node_name='C' AND job_id=2",
        ).fetchone()["created_by_execution_id"]
        assert quotient_creator == child["execution_id"]
        session = storage.get_execution_session(root["session_id"])
        assert session["selected_jobs"] == [("A", 7)]
        _assert_terminal_done(storage, root["session_id"], ("A", "B"))
    finally:
        _close(storage)


@pytest.mark.parametrize("runner", ["threaded", "api"])
def test_selected_execution_waits_for_the_filtered_causal_frontier_to_quiesce(
    tmp_path, runner,
):
    workflow = MicroWorkflow(tmp_path, runner=runner, persist_graph=False)
    workflow.graph([("A", "B"), ("B", "A"), ("B", "C")])
    storage = workflow.storage
    child_started, release_child, grandchild_done = Event(), Event(), Event()
    root_value = object()
    created = {}
    calls = []

    @workflow.task("A")
    def node_a(ctx, role):
        calls.append(("A", ctx.job_id, role))
        if role == "root":
            created["child"] = ctx.node("B").add(role="child").job_id
            return root_value
        if role == "grandchild":
            grandchild_done.set()
            return "grandchild"
        raise AssertionError("unrelated A job executed")

    @workflow.task("B", waiting=True, wait_for=["A"])
    def node_b(ctx, role):
        calls.append(("B", ctx.job_id, role))
        if role != "child":
            raise AssertionError("unrelated B job executed")
        child_started.set()
        assert release_child.wait(30)
        created["grandchild"] = ctx.node("A").add(role="grandchild").job_id
        created["quotient"] = ctx.node("C").add(role="quotient").job_id
        return "child"

    @workflow.task("C")
    def node_c(ctx, role):
        calls.append(("C", ctx.job_id, role))
        raise AssertionError("quotient or unrelated C job executed")

    workflow.add_job(None, "A", job_id=1, role="root")
    workflow.add_job(None, "A", job_id=2, role="unrelated")
    workflow.add_job(None, "B", job_id=1, role="unrelated")
    workflow.add_job(None, "C", job_id=1, role="unrelated")
    result = {}

    def run():
        try:
            result["value"] = workflow.run_jobs("A", [1])
        except BaseException as error:
            result["error"] = error

    thread = Thread(target=run, name=f"selected-{runner}-quiescence", daemon=True)
    thread.start()
    try:
        assert child_started.wait(20), result
        assert thread.is_alive()
        root = storage.read_job_current_owner("A", 1)
        child = storage.read_job_current_owner("B", created["child"])
        assert storage.get_job_status("A", 1) == "done"
        assert storage.get_job_status("B", created["child"]) == "running"
        assert child["created_by_execution_id"] == root["execution_id"]
        assert storage.get_execution_session(root["session_id"])["status"] == "running"
        _assert_unclaimed(storage, "A", 2)
        _assert_unclaimed(storage, "B", 1)
        _assert_unclaimed(storage, "C", 1)

        release_child.set()
        thread.join(timeout=30)
        assert not thread.is_alive(), result
        assert "error" not in result
        assert len(result["value"]) == 1 and result["value"][0] is root_value
        assert grandchild_done.is_set()
        assert calls == [
            ("A", 1, "root"),
            ("B", created["child"], "child"),
            ("A", created["grandchild"], "grandchild"),
        ]
        _assert_unclaimed(storage, "C", created["quotient"])
        _assert_terminal_done(storage, root["session_id"], ("A", "B"))
    finally:
        release_child.set()
        thread.join(timeout=30)
        assert not thread.is_alive(), result
        _close(storage)


@pytest.mark.parametrize("runner", ["direct", "threaded", "api"])
def test_run_node_jobs_preserves_supplied_root_payload_and_python_value(tmp_path, runner):
    workflow = MicroWorkflow(tmp_path, runner=runner, persist_graph=False)
    workflow.graph([("A", "A")])
    storage = workflow.storage
    marker = object()
    seen = []

    @workflow.task("A")
    def work(ctx, payload):
        seen.append(payload)
        return payload

    workflow.add_job(None, "A", job_id=1, payload="stored")
    supplied = replace(storage.load_job("A", 1), params={"payload": marker})
    try:
        values = workflow.run_node_jobs("A", [supplied])
        assert len(values) == 1 and values[0] is marker
        assert seen == [marker]
        assert storage.load_job("A", 1).params == {"payload": "stored"}
        owner = storage.read_job_current_owner("A", 1)
        assert json.loads(storage.output_file("A", 1).read_text(encoding="utf-8")) == {
            "status": "done",
            "result_type": "object",
            "result_repr": repr(marker),
            "generation": owner["generation"],
            "execution_id": owner["execution_id"],
        }
        _assert_terminal_done(storage, owner["session_id"], ("A",))
    finally:
        _close(storage)


def test_scalar_run_job_stays_in_the_main_process_when_process_is_configured(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="process", persist_graph=False)
    workflow.graph([("A", "A")])
    storage = workflow.storage
    marker = object()
    caller_pid = os.getpid()
    observations = []

    @workflow.task("A")
    def work(ctx):
        observations.append((os.getpid(), get_ident()))
        return marker

    workflow.add_job(None, "A", job_id=1)
    try:
        result = workflow.run_job("A", 1)
        assert result is marker
        assert len(observations) == 1 and observations[0][0] == caller_pid
        owner = storage.read_job_current_owner("A", 1)
        assert storage.get_job_status("A", 1) == "done"
        _assert_terminal_done(storage, owner["session_id"], ("A",))
    finally:
        _close(storage)


@pytest.mark.parametrize("runner", ["direct", "threaded", "api"])
def test_selected_failed_decision_runs_accepted_new_causal_child_successor(
    tmp_path, monkeypatch, runner,
):
    workflow = MicroWorkflow(tmp_path, runner=runner, persist_graph=False)
    workflow.graph([("A", "B"), ("B", "A")])
    storage = workflow.storage
    at_failed_decision, proceed = Event(), Event()
    root_value = object()
    created = {}
    calls = []

    @workflow.task("A")
    def root(ctx, role):
        calls.append(("A", ctx.job_id, ctx.execution_generation, role))
        if role != "root":
            raise AssertionError("unrelated A job executed")
        created["child"] = ctx.node("B").add(role="child").job_id
        return root_value

    @workflow.task("B")
    def child(ctx, role):
        calls.append(("B", ctx.job_id, ctx.execution_generation, role))
        if role != "child":
            raise AssertionError("unrelated B job executed")
        if ctx.execution_generation == 0:
            raise ValueError("causal child needs restart")
        return "repaired-child"

    workflow.add_job(None, "A", job_id=1, role="root")
    workflow.add_job(None, "A", job_id=2, role="unrelated")
    workflow.add_job(None, "B", job_id=1, role="unrelated")
    decide = storage.decide_execution_session_exit

    def pause_failed_decision(*args, **kwargs):
        if kwargs.get("outcome") == "failed" and not at_failed_decision.is_set():
            at_failed_decision.set()
            assert proceed.wait(30)
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, "decide_execution_session_exit", pause_failed_decision)
    result = {}

    def run():
        try:
            result["value"] = workflow.run_jobs("A", [1])
        except BaseException as error:
            result["error"] = error

    thread = Thread(target=run, name=f"selected-{runner}-restart", daemon=True)
    thread.start()
    try:
        assert at_failed_decision.wait(20), result
        child_id = created["child"]
        failed_owner = storage.read_job_current_owner("B", child_id)
        assert storage.get_job_status("B", child_id) == "failed"
        _restart(tmp_path, "B", child_id)
        proceed.set()
        thread.join(timeout=30)
        assert not thread.is_alive(), result
        assert "error" not in result
        assert len(result["value"]) == 1 and result["value"][0] is root_value
        assert calls == [
            ("A", 1, 0, "root"),
            ("B", child_id, 0, "child"),
            ("B", child_id, 1, "child"),
        ]
        root_owner = storage.read_job_current_owner("A", 1)
        repaired = storage.read_job_current_owner("B", child_id)
        assert repaired["session_id"] == root_owner["session_id"] == failed_owner["session_id"]
        assert repaired["job_instance_id"] == failed_owner["job_instance_id"]
        assert repaired["generation"] == failed_owner["generation"] + 1
        assert repaired["created_by_execution_id"] == root_owner["execution_id"]
        _assert_unclaimed(storage, "A", 2)
        _assert_unclaimed(storage, "B", 1)
        _assert_terminal_done(storage, root_owner["session_id"], ("A", "B"))
    finally:
        proceed.set()
        thread.join(timeout=30)
        assert not thread.is_alive(), result
        _close(storage)


def test_first_exit_decision_nested_restart_preserves_the_selected_roots_original_value(
    tmp_path, monkeypatch,
):
    """A child replacement result does not replace its already-returned root value.

    Restarting a completed root is not eligible. A root catches a nested
    same-component child's failure and returns. The test pauses the first outer
    decision because selected-causal discovery may now
    surface that failure before the formerly successful exit decision. In both
    routes, the child's accepted successor cannot replace the root's value.
    """
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "B"), ("B", "A")])
    storage = workflow.storage
    original_root_value = object()
    at_first_decision, proceed = Event(), Event()
    first_outcome = []
    calls = []

    @workflow.task("A")
    def root(ctx):
        calls.append(("A", ctx.job_id, ctx.execution_generation))
        child = ctx.node("B").add()
        try:
            workflow.run_job("B", child.job_id, ignore_readiness=True)
        except JobFailedError:
            pass
        return original_root_value

    @workflow.task("B")
    def child(ctx):
        calls.append(("B", ctx.job_id, ctx.execution_generation))
        if ctx.execution_generation == 0:
            raise ValueError("nested child needs restart")
        return "replacement-child-value"

    workflow.add_job(None, "A", job_id=1)
    decide = storage.decide_execution_session_exit

    def pause_first_decision(*args, **kwargs):
        if not at_first_decision.is_set():
            first_outcome.append(kwargs.get("outcome"))
            at_first_decision.set()
            assert proceed.wait(30)
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, "decide_execution_session_exit", pause_first_decision)
    result = {}

    def run():
        try:
            result["value"] = workflow.run_job("A", 1)
        except BaseException as error:
            result["error"] = error

    thread = Thread(target=run, name="selected-clean-exit-restart", daemon=True)
    thread.start()
    try:
        assert at_first_decision.wait(20), result
        assert len(first_outcome) == 1
        root_owner = storage.read_job_current_owner("A", 1)
        failed_child = storage.read_job_current_owner("B", 1)
        root_output = storage.output_file("A", 1).read_bytes()
        root_events = storage.read_job_events("A", 1)
        assert failed_child["created_by_execution_id"] == root_owner["execution_id"]
        assert storage.get_job_status("B", 1) == "failed"
        _restart(tmp_path, "B", 1)
        proceed.set()
        thread.join(timeout=30)
        assert not thread.is_alive(), result
        assert result.get("value") is original_root_value and "error" not in result
        assert calls == [("A", 1, 0), ("B", 1, 0), ("B", 1, 1)]
        assert storage.read_job_current_owner("A", 1) == root_owner
        assert storage.output_file("A", 1).read_bytes() == root_output
        assert storage.read_job_events("A", 1) == root_events
        child = storage.read_job_current_owner("B", 1)
        assert child["session_id"] == failed_child["session_id"] == root_owner["session_id"]
        assert child["generation"] == failed_child["generation"] + 1
        assert json.loads(storage.output_file("B", 1).read_text(encoding="utf-8"))["result_repr"] == repr(
            "replacement-child-value"
        )
        _assert_terminal_done(storage, root_owner["session_id"], ("A", "B"))
    finally:
        proceed.set()
        thread.join(timeout=30)
        assert not thread.is_alive(), result
        _close(storage)


def test_selected_frontier_refuses_component_realignment_before_claiming_new_child(
    tmp_path, monkeypatch,
):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "B"), ("B", "A")])
    storage = workflow.storage
    root_value = object()
    created = {}

    @workflow.task("A")
    def root(ctx, role):
        assert role == "root"
        created["child"] = ctx.node("B").add(role="child").job_id
        return root_value

    @workflow.task("B")
    def child(ctx, role):
        raise AssertionError("child claimed after selected component realigned")

    workflow.add_job(None, "A", job_id=1, role="root")
    original_finalize = storage.finalize_job_execution
    realigned = Event()

    def finalize_then_realign(*args, **kwargs):
        result = original_finalize(*args, **kwargs)
        node, job_id, status = args[0], args[1], args[4]
        if node == "A" and job_id == 1 and status == "done" and not realigned.is_set():
            storage.submit_db_mutation(lambda connection: connection.execute(
                "UPDATE component_states SET alignment_generation=alignment_generation+1 WHERE component_key=?",
                (encode_component_key(("A", "B")),),
            ))
            realigned.set()
        return result

    monkeypatch.setattr(storage, "finalize_job_execution", finalize_then_realign)
    try:
        with pytest.raises(RuntimeError, match="captured|alignment|component"):
            workflow.run_job("A", 1)
        assert realigned.is_set()
        root_owner = storage.read_job_current_owner("A", 1)
        assert storage.get_job_status("A", 1) == "done"
        assert json.loads(storage.output_file("A", 1).read_text(encoding="utf-8")) == {
            "status": "done",
            "result_type": "object",
            "result_repr": repr(root_value),
            "generation": root_owner["generation"],
            "execution_id": root_owner["execution_id"],
        }
        _assert_unclaimed(storage, "B", created["child"])
    finally:
        _close(storage)
