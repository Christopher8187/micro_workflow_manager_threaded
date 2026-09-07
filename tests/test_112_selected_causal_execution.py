"""Recursive selected-job circulation and causal failure."""

from __future__ import annotations

from collections import Counter
import json

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.errors import JobFailedError
from tests.test_090_component_session_settlement import _close


def _invoke_selected(workflow, entry, root_ids):
    if entry == "run_job":
        assert len(root_ids) == 1
        return workflow.run_job("A", root_ids[0], ignore_readiness=True)
    if entry == "run_jobs":
        return workflow.run_jobs("A", root_ids, ignore_readiness=True)
    return workflow.run_node_jobs(
        "A", [workflow.storage.load_job("A", job_id) for job_id in root_ids],
        ignore_readiness=True,
    )


def _instance_creator(storage, node, job_id):
    row = storage.db_connection().execute(
        "SELECT instance_id, created_by_execution_id FROM job_instances "
        "WHERE node_name=? AND job_id=?",
        (node, job_id),
    ).fetchone()
    assert row is not None
    return dict(row)


def _assert_unclaimed(storage, node, job_id):
    assert storage.get_job_status(node, job_id) == "queued"
    assert storage.read_job_current_owner(node, job_id) is None


@pytest.mark.parametrize("entry", ["run_job", "run_jobs", "run_node_jobs"])
def test_selected_roots_run_new_same_component_causal_descendants_only(tmp_path, entry):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "B"), ("B", "A"), ("B", "C")])
    storage = workflow.storage
    calls = []
    created = {}
    root_values = {"seven": object(), "five": object()}

    @workflow.task("A")
    def node_a(ctx, role, token):
        calls.append(("A", ctx.job_id, role, token))
        if role == "unrelated":
            raise AssertionError("Unrelated preexisting A job executed")
        if role == "grandchild":
            return "grandchild:" + token
        child = ctx.node("B").add(role="child", token=token)
        created.setdefault(token, {})["child"] = child.job_id
        return root_values[token]

    @workflow.task("B")
    def node_b(ctx, role, token):
        calls.append(("B", ctx.job_id, role, token))
        if role == "unrelated":
            raise AssertionError("Unrelated preexisting B job executed")
        grandchild = ctx.node("A").add(role="grandchild", token=token)
        quotient = ctx.node("C").add(role="quotient", token=token)
        created[token].update(grandchild=grandchild.job_id, quotient=quotient.job_id)
        return "child:" + token

    @workflow.task("C")
    def node_c(ctx, role, token):
        calls.append(("C", ctx.job_id, role, token))
        raise AssertionError("Quotient descendant executed during selected-job circulation")

    workflow.add_job(None, "A", job_id=1, role="unrelated", token="old-a")
    workflow.add_job(None, "B", job_id=1, role="unrelated", token="old-b")
    workflow.add_job(None, "C", job_id=1, role="unrelated", token="old-c")
    workflow.add_job(None, "A", job_id=7, role="root", token="seven")
    workflow.add_job(None, "A", job_id=5, role="root", token="five")
    roots = [7] if entry == "run_job" else [7, 5]
    tokens = ["seven"] if entry == "run_job" else ["seven", "five"]

    try:
        returned = _invoke_selected(workflow, entry, roots)

        if entry == "run_job":
            assert returned is root_values["seven"]
        else:
            assert len(returned) == 2
            assert returned[0] is root_values["seven"]
            assert returned[1] is root_values["five"]
        assert [(job_id, token) for node, job_id, role, token in calls
                if node == "A" and role == "root"] == list(zip(roots, tokens))
        assert Counter((node, role, token) for node, _, role, token in calls) == Counter(
            [("A", "root", token) for token in tokens]
            + [("B", "child", token) for token in tokens]
            + [("A", "grandchild", token) for token in tokens]
        )

        root_owners = {token: storage.read_job_current_owner("A", job_id)
                       for token, job_id in zip(tokens, roots)}
        for token in tokens:
            root = root_owners[token]
            child = storage.read_job_current_owner("B", created[token]["child"])
            grandchild = storage.read_job_current_owner("A", created[token]["grandchild"])
            assert root["created_by_execution_id"] is None
            assert child["created_by_execution_id"] == root["execution_id"]
            assert grandchild["created_by_execution_id"] == child["execution_id"]
            assert root["session_id"] == child["session_id"] == grandchild["session_id"]
            assert root["component"] == child["component"] == grandchild["component"] == ("A", "B")
            for owner in (root, child, grandchild):
                instance = _instance_creator(storage, owner["node_name"], owner["job_id"])
                assert instance == {
                    "instance_id": owner["job_instance_id"],
                    "created_by_execution_id": owner["created_by_execution_id"],
                }
            quotient = created[token]["quotient"]
            _assert_unclaimed(storage, "C", quotient)
            quotient_instance = _instance_creator(storage, "C", quotient)
            assert quotient_instance["created_by_execution_id"] == child["execution_id"]

        session_id = root_owners[tokens[0]]["session_id"]
        session = storage.get_execution_session(session_id)
        assert session["selected_jobs"] == [("A", job_id) for job_id in roots]
        assert (session["status"], session["outcome"]) == ("terminal", "done")
        selected_rows = storage.db_connection().execute(
            "SELECT position, node_name, job_id, job_instance_id FROM session_jobs "
            "WHERE session_id=? ORDER BY position", (session_id,),
        ).fetchall()
        assert [tuple(row) for row in selected_rows] == [
            (position, "A", job_id, storage.read_job_instance_id("A", job_id))
            for position, job_id in enumerate(roots)
        ]
        _assert_unclaimed(storage, "A", 1)
        _assert_unclaimed(storage, "B", 1)
        _assert_unclaimed(storage, "C", 1)
        if entry == "run_job":
            _assert_unclaimed(storage, "A", 5)
        assert storage.get_component_reservation(("A", "B")) is None
        assert storage.get_live_main_session() is None
    finally:
        _close(storage)


@pytest.mark.parametrize("entry", ["run_job", "run_jobs", "run_node_jobs"])
def test_selected_causal_child_failure_is_attributed_without_ordinary_admission(tmp_path, entry):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "B"), ("B", "A"), ("B", "C")])
    storage = workflow.storage
    child_failure = ValueError("selected causal child failed")
    created = {}
    calls = []

    @workflow.task("A")
    def root(ctx, role):
        calls.append(("A", ctx.job_id, role))
        if role == "unrelated":
            raise AssertionError("Unrelated selected-component job executed")
        child = ctx.node("B").add(role="child")
        created["child"] = child.job_id
        return "root completed"

    @workflow.task("B")
    def child(ctx, role):
        calls.append(("B", ctx.job_id, role))
        if role == "unrelated":
            raise AssertionError("Unrelated selected-component job executed")
        quotient = ctx.node("C").add(role="quotient")
        created["quotient"] = quotient.job_id
        raise child_failure

    @workflow.task("C")
    def quotient(ctx, role):
        calls.append(("C", ctx.job_id, role))
        raise AssertionError("Quotient descendant executed after selected child failure")

    workflow.add_job(None, "A", job_id=1, role="unrelated")
    workflow.add_job(None, "B", job_id=1, role="unrelated")
    workflow.add_job(None, "C", job_id=1, role="unrelated")
    workflow.add_job(None, "A", job_id=7, role="root")

    try:
        with pytest.raises(JobFailedError) as caught:
            _invoke_selected(workflow, entry, [7])

        assert caught.value.__cause__ is child_failure
        assert calls == [("A", 7, "root"), ("B", created["child"], "child")]
        root_owner = storage.read_job_current_owner("A", 7)
        child_owner = storage.read_job_current_owner("B", created["child"])
        assert child_owner["created_by_execution_id"] == root_owner["execution_id"]
        assert child_owner["session_id"] == root_owner["session_id"]
        assert child_owner["component"] == root_owner["component"] == ("A", "B")
        assert _instance_creator(storage, "B", created["child"]) == {
            "instance_id": child_owner["job_instance_id"],
            "created_by_execution_id": root_owner["execution_id"],
        }
        assert caught.value.execution_attempt == (
            "B", created["child"], child_owner["generation"], child_owner["execution_id"],
        )
        assert storage.get_job_status("A", 7) == "done"
        assert json.loads(storage.output_file("A", 7).read_text(encoding="utf-8")) == {
            "status": "done", "result_type": "str", "result_repr": repr("root completed"),
            "generation": root_owner["generation"], "execution_id": root_owner["execution_id"],
        }
        assert storage.get_job_status("B", created["child"]) == "failed"
        quotient_id = created["quotient"]
        _assert_unclaimed(storage, "C", quotient_id)
        assert (
            _instance_creator(storage, "C", quotient_id)["created_by_execution_id"]
            == child_owner["execution_id"]
        )
        _assert_unclaimed(storage, "A", 1)
        _assert_unclaimed(storage, "B", 1)
        _assert_unclaimed(storage, "C", 1)
        session = storage.get_execution_session(root_owner["session_id"])
        assert session["selected_jobs"] == [("A", 7)]
        assert (session["status"], session["outcome"]) == ("terminal", "failed")
        assert storage.get_component_reservation(("A", "B")) is None
        assert storage.get_live_main_session() is None
    finally:
        _close(storage)
