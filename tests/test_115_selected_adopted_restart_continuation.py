"""Selected execution continuation after accepted nested repairs."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Event

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.errors import JobFailedError
from tests.test_090_component_session_settlement import _close


def _restart(project, node, job_id):
    command = subprocess.run(
        [sys.executable, "-m", "micro_workflow_manager", "restart", node, "job", str(job_id)],
        cwd=project,
        env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert command.returncode == 0, command.stdout + command.stderr


def _assert_terminal_done(storage, session_id, component):
    session = storage.get_execution_session(session_id)
    assert (session["status"], session["outcome"]) == ("terminal", "done")
    assert storage.get_component_reservation(component) is None
    assert storage.get_live_main_session() is None


def test_selected_adopted_child_can_fail_again_and_restart_without_replaying_root(
    tmp_path, monkeypatch,
):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "B"), ("B", "A")])
    storage = workflow.storage
    root_value = object()
    calls = []
    failed_decisions = [Event(), Event()]
    releases = [Event(), Event()]
    decision_count = 0

    @workflow.task("A")
    def root(ctx):
        calls.append(("A", ctx.job_id, ctx.execution_generation))
        assert calls.count(("A", 1, 0)) == 1
        child = ctx.node("B").add()
        try:
            workflow.run_job("B", child.job_id, ignore_readiness=True)
        except JobFailedError:
            pass
        return root_value

    @workflow.task("B")
    def child(ctx):
        calls.append(("B", ctx.job_id, ctx.execution_generation))
        if ctx.execution_generation < 2:
            raise ValueError(f"child attempt {ctx.execution_generation} failed")
        return "second accepted repair"

    workflow.add_job(None, "A", job_id=1)
    workflow.add_job(None, "A", job_id=2)
    workflow.add_job(None, "B", job_id=1)
    decide = storage.decide_execution_session_exit

    def pause_failed_decisions(*args, **kwargs):
        nonlocal decision_count
        if kwargs.get("outcome") == "failed" and decision_count < 2:
            position = decision_count
            decision_count += 1
            failed_decisions[position].set()
            assert releases[position].wait(30)
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, "decide_execution_session_exit", pause_failed_decisions)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(workflow.run_job, "A", 1)
            try:
                assert failed_decisions[0].wait(20)
                root_owner = storage.read_job_current_owner("A", 1)
                root_output = storage.output_file("A", 1).read_bytes()
                root_events = storage.read_job_events("A", 1)
                failed_zero = storage.read_job_current_owner("B", 2)
                assert failed_zero["created_by_execution_id"] == root_owner["execution_id"]
                assert storage.get_job_status("B", 2) == "failed"
                _restart(tmp_path, "B", 2)
                releases[0].set()

                assert failed_decisions[1].wait(20)
                failed_one = storage.read_job_current_owner("B", 2)
                assert storage.get_job_status("B", 2) == "failed"
                assert failed_one["job_instance_id"] == failed_zero["job_instance_id"]
                assert failed_one["generation"] == failed_zero["generation"] + 1
                assert failed_one["session_id"] == root_owner["session_id"]
                assert storage.read_job_current_owner("A", 1) == root_owner
                assert storage.output_file("A", 1).read_bytes() == root_output
                assert storage.read_job_events("A", 1) == root_events
                _restart(tmp_path, "B", 2)
                releases[1].set()

                result = future.result(timeout=30)
            finally:
                for release in releases:
                    release.set()

        assert result is root_value
        assert calls == [("A", 1, 0), ("B", 2, 0), ("B", 2, 1), ("B", 2, 2)]
        repaired = storage.read_job_current_owner("B", 2)
        assert repaired["job_instance_id"] == failed_zero["job_instance_id"]
        assert repaired["generation"] == failed_one["generation"] + 1
        assert repaired["session_id"] == root_owner["session_id"]
        assert json.loads(storage.output_file("B", 2).read_text(encoding="utf-8")) == {
            "status": "done",
            "result_type": "str",
            "result_repr": repr("second accepted repair"),
            "generation": repaired["generation"],
            "execution_id": repaired["execution_id"],
        }
        assert storage.get_job_status("A", 2) == "queued"
        assert storage.read_job_current_owner("A", 2) is None
        assert storage.get_job_status("B", 1) == "queued"
        assert storage.read_job_current_owner("B", 1) is None
        _assert_terminal_done(storage, root_owner["session_id"], ("A", "B"))
    finally:
        for release in releases:
            release.set()
        _close(storage)


def test_nested_full_repair_created_causal_child_finishes_before_selected_return(
    tmp_path, monkeypatch,
):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "B"), ("B", "A"), ("B", "C")])
    storage = workflow.storage
    root_value = object()
    calls = []
    at_decision, proceed = Event(), Event()

    @workflow.task("A")
    def node_a(ctx, role):
        calls.append(("A", ctx.job_id, ctx.execution_generation, role))
        if role == "root":
            child = ctx.node("B").add(role="repair")
            try:
                workflow.run_component({"A", "B"})
            except JobFailedError:
                pass
            return root_value
        assert role == "repair-child"
        return "causal child finished"

    @workflow.task("B")
    def node_b(ctx, role):
        calls.append(("B", ctx.job_id, ctx.execution_generation, role))
        assert role == "repair"
        if ctx.execution_generation == 0:
            raise ValueError("nested full child failed")
        ctx.node("A").add(role="repair-child")
        return "nested full child repaired"

    @workflow.task("C")
    def node_c(ctx, role):
        raise AssertionError("quotient work entered selected causal execution")

    workflow.add_job(None, "A", job_id=1, role="root")
    decide = storage.decide_execution_session_exit

    def pause_first_decision(*args, **kwargs):
        if not at_decision.is_set():
            at_decision.set()
            assert proceed.wait(30)
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, "decide_execution_session_exit", pause_first_decision)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(workflow.run_job, "A", 1)
            try:
                assert at_decision.wait(20)
                root_owner = storage.read_job_current_owner("A", 1)
                root_output = storage.output_file("A", 1).read_bytes()
                root_events = storage.read_job_events("A", 1)
                failed_child = storage.read_job_current_owner("B", 1)
                assert failed_child["created_by_execution_id"] == root_owner["execution_id"]
                assert storage.get_job_status("B", 1) == "failed"
                _restart(tmp_path, "B", 1)
                proceed.set()
                result = future.result(timeout=30)
            finally:
                proceed.set()

        assert result is root_value
        assert calls == [
            ("A", 1, 0, "root"),
            ("B", 1, 0, "repair"),
            ("B", 1, 1, "repair"),
            ("A", 2, 0, "repair-child"),
        ]
        repaired = storage.read_job_current_owner("B", 1)
        descendant = storage.read_job_current_owner("A", 2)
        assert repaired["generation"] == failed_child["generation"] + 1
        assert repaired["session_id"] == descendant["session_id"] == root_owner["session_id"]
        assert descendant["created_by_execution_id"] == repaired["execution_id"]
        assert storage.read_job_current_owner("A", 1) == root_owner
        assert storage.output_file("A", 1).read_bytes() == root_output
        assert storage.read_job_events("A", 1) == root_events
        assert storage.get_job_status("A", 2) == "done"
        assert storage.get_component_state(("C",))["lifecycle"] == "queued"
        _assert_terminal_done(storage, root_owner["session_id"], ("A", "B"))
    finally:
        proceed.set()
        _close(storage)
