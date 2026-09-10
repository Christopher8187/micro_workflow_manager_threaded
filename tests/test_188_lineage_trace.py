"""Public compact lineage from persisted jobs without project execution."""

from __future__ import annotations

import json
import time

from micro_workflow_manager import cli
from micro_workflow_manager.models import Job
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_090_component_session_settlement import _close
from tests.test_119_native_preview_storage import _files
from tests.test_149_interrupt_declarations import _router_source


def test_lineage_reports_direct_relations_and_component_state_without_loading_code(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('A', 'B'), ('B', 'C')]",
        files={
            "A": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A", runner="direct")
                router.create_job(params={"secret": "private parameters"})
                @router.task
                def run(ctx, secret):
                    ctx.trace("private trace", content=secret)
                    ctx.node("B").add_many([
                        {"private": "private child parameters"},
                        {"private": "private child parameters"},
                    ])
                    return "private output"
            ''',
            "B": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B", runner="direct")
                @router.task
                def run(ctx, private):
                    ctx.node("C").add(private=private)
                    return private
            ''',
            "C": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("C", runner="direct")
                @router.task
                def run(ctx, private): return private
            ''',
        },
    )
    assert cli.main(["runfrom", "A", "--runner", "direct"]) == 0
    capsys.readouterr()
    (tmp_path / "src" / "node_behavior" / "A.py").write_text(
        "raise RuntimeError('lineage must not import project code')\n", encoding="utf-8",
    )
    deadline = time.monotonic() + 5
    while list((tmp_path / ".mwf" / "state_subscribers").glob("*.json")):
        assert time.monotonic() < deadline, "Finished execution retained a state subscriber"
        time.sleep(0.01)
    before = _files(tmp_path, allow_existing_shm=True)

    assert cli.main(["trace", "A", "job", "1", "--lineage", "--json"]) == 0
    output = capsys.readouterr().out
    assert "private" not in output
    assert json.loads(output) == {
        "schema_version": 1, "node": "A", "job_id": 1, "job_status": "done",
        "component": {
            "members": ["A"], "state": "done", "stability": "stable",
            "instability_origin": None, "misaligned": False,
            "misalignment_causes": [],
        },
        "sample_id": None, "interrupt_session_id": None, "created_by": None,
        "created_jobs": [{"node": "B", "job_id": 1}, {"node": "B", "job_id": 2}],
    }
    assert cli.main(["trace", "B", "job", "1", "--lineage", "--json"]) == 0
    child = json.loads(capsys.readouterr().out)
    assert child["created_by"] == {"node": "A", "job_id": 1}
    assert child["created_jobs"] == [{"node": "C", "job_id": 1}]
    assert cli.main(["trace", "C", "job", "1", "--lineage", "--json"]) == 0
    grandchild = json.loads(capsys.readouterr().out)
    assert grandchild["created_by"] == {"node": "B", "job_id": 1}
    assert grandchild["created_jobs"] == []

    assert cli.main(["trace", "A", "job", "1", "--lineage"]) == 0
    text = capsys.readouterr().out
    assert "Job A/1" in text and "job-status: done" in text
    assert "state: done" in text and "stability: stable" in text
    assert "B/1" in text and "B/2" in text and "C/1" not in text
    assert "private" not in text
    assert _files(tmp_path, allow_existing_shm=True) == before


def test_lineage_keeps_sample_execution_and_retained_interrupt_origin_distinct(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('P', 'I')]",
        files={"P": _router_source("P", jobs=1),
               "I": _router_source("I", jobs=2, interrupt="True")},
    )
    assert cli.main([
        "run", "I", "sample", "50%", "--seed", "lineage",
        "--interrupt", "--runner", "direct",
    ]) == 0
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    try:
        first, = storage.list_execution_sessions()
        first_id = first["session_id"]
        sample_id = first["details"]["selection"]["sample_id"]
        done, = [job for job in (1, 2) if storage.get_job_status("I", job) == "done"]
        queued, = [job for job in (1, 2) if storage.get_job_status("I", job) == "queued"]
    finally:
        _close(storage)
    assert cli.main(["trace", "I", "job", str(done), "--lineage", "--json"]) == 0
    sampled = json.loads(capsys.readouterr().out)
    assert sampled["sample_id"] == sample_id
    assert sampled["interrupt_session_id"] == first_id
    assert sampled["component"] == {
        "members": ["I"], "state": "sampled", "stability": "unstable",
        "instability_origin": first_id, "misaligned": False, "misalignment_causes": [],
    }
    assert cli.main(["resume", "I", "--interrupt", "--runner", "direct"]) == 0
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    try:
        second_id = storage.read_job_current_owner("I", queued)["session_id"]
        assert second_id != first_id
    finally:
        _close(storage)
    assert cli.main(["trace", "I", "job", str(queued), "--lineage", "--json"]) == 0
    resumed = json.loads(capsys.readouterr().out)
    assert resumed["sample_id"] is None
    assert resumed["interrupt_session_id"] == second_id
    assert resumed["component"] == {
        **sampled["component"], "state": "done",
    }


def test_lineage_lists_each_receivers_current_first_cause_and_forgets_old_alignment(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path, monkeypatch,
        edges="EDGES = [('P', 'A'), ('P', 'B'), ('A', 'B'), ('B', 'A')]",
        files={
            "P": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("P", runner="direct")
                router.create_job()
                @router.task
                def run(ctx):
                    for receiver in ("B", "A"):
                        ctx.node(receiver).write_input("source.txt", "new input", overwrite=True)
                    return "P"
            ''',
            "A": _router_source("A", jobs=1),
            "B": _router_source("B", jobs=1),
        },
    )
    assert cli.main(["runfrom", "P", "--runner", "direct"]) == 0
    assert cli.main(["run", "P", "job", "1", "--runner", "direct"]) == 0
    capsys.readouterr()
    assert cli.main(["trace", "A", "job", "1", "--lineage", "--json"]) == 0
    previous = json.loads(capsys.readouterr().out)
    assert previous["component"]["members"] == ["A", "B"]
    assert previous["component"]["misaligned"] is True
    causes = previous["component"]["misalignment_causes"]
    assert [cause["receiver_node"] for cause in causes] == ["A", "B"]
    assert all(cause["producer_node"] == "P" and cause["producer_job_id"] == 1
               for cause in causes)
    old_generation = causes[0]["alignment_generation"]
    assert cli.main(["reset", "A", "--yes"]) == 0
    assert cli.main(["resume", "A", "--runner", "direct"]) == 0
    capsys.readouterr()
    assert cli.main(["trace", "A", "job", "1", "--lineage", "--json"]) == 0
    aligned = json.loads(capsys.readouterr().out)
    assert aligned["component"]["misaligned"] is False
    assert aligned["component"]["misalignment_causes"] == []
    assert cli.main(["run", "P", "job", "1", "--runner", "direct"]) == 0
    capsys.readouterr()
    assert cli.main(["trace", "A", "job", "1", "--lineage", "--json"]) == 0
    current = json.loads(capsys.readouterr().out)
    current_causes = current["component"]["misalignment_causes"]
    assert [cause["receiver_node"] for cause in current_causes] == ["A", "B"]
    assert all(cause["alignment_generation"] > old_generation for cause in current_causes)


def test_lineage_does_not_assign_old_children_to_a_recreated_numeric_job_id(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('A', 'B')]",
        files={
            "A": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A", runner="direct")
                router.create_job()
                @router.task
                def run(ctx):
                    ctx.node("B").add()
            ''',
            "B": _router_source("B", jobs=0),
        },
    )
    assert cli.main(["run", "A", "--runner", "direct"]) == 0
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    try:
        previous = storage.read_job_instance_id("A", 1)
        assert storage.delete_job("A", 1) is True
        storage.create_job(Job(node_name="A", job_id=1, params={}))
        assert storage.read_job_instance_id("A", 1) != previous
    finally:
        _close(storage)
    assert cli.main(["trace", "A", "job", "1", "--lineage", "--json"]) == 0
    recreated = json.loads(capsys.readouterr().out)
    assert recreated["job_status"] == "queued"
    assert recreated["created_by"] is None and recreated["created_jobs"] == []
    assert cli.main(["trace", "B", "job", "1", "--lineage", "--json"]) == 0
    old_child = json.loads(capsys.readouterr().out)
    assert old_child["created_by"] == {"node": "A", "job_id": 1}
    assert old_child["created_jobs"] == []
