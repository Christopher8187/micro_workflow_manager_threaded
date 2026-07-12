from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from micro_workflow_manager import MicroWorkflow, NodeRouter, cli
from micro_workflow_manager.models import DONE, FAILED, QUEUED, RUNNING


def write_project(tmp_path: Path, monkeypatch, *, graph="EDGES = [('A', 'B')]\n") -> Path:
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(graph, encoding="utf-8")
    (behavior / "A.py").write_text(
        '''from micro_workflow_manager import NodeRouter
router = NodeRouter("A")
router.create_job(number=1)
@router.task
def run(ctx):
    return 1
''',
        encoding="utf-8",
    )
    (behavior / "B.py").write_text(
        '''from micro_workflow_manager import NodeRouter
router = NodeRouter("B")
@router.task
def run(ctx, value=1):
    return value * 2
''',
        encoding="utf-8",
    )
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0
    return behavior


def test_graph_path_is_stored_with_slashes_and_accepts_backslashes(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text("EDGES = [('A', 'B')]\n", encoding="utf-8")
    router_source = (
        'from micro_workflow_manager import NodeRouter\n'
        'router = NodeRouter("{name}")\n'
        '@router.task\n'
        'def run(ctx):\n'
        '    return None\n'
    )
    for name in ("A", "B"):
        (behavior / f"{name}.py").write_text(router_source.format(name=name), encoding="utf-8")
    assert cli.main(["init"]) == 0
    # A Windows-style command path must also work when this test runs on Linux.
    assert cli.main(["graph", "src\\graph.py", "--runner", "direct"]) == 0
    config_path = tmp_path / ".mwf"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    assert config["graph_path"] == "src/graph.py"

    config["graph_path"] = "src\\graph.py"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    assert cli.main(["monitor", "--once"]) == 0
    capsys.readouterr()

    assert cli.main(["graph", "--update"]) == 0
    config = json.loads(config_path.read_text(encoding="utf-8"))
    assert config["graph_path"] == "src/graph.py"


def test_doctor_detects_missing_router_without_mutating_project(tmp_path, monkeypatch, capsys):
    behavior = write_project(tmp_path, monkeypatch)
    capsys.readouterr()
    before = (tmp_path / ".mwf").read_bytes()

    assert cli.main(["doctor"]) == 0
    assert "Healthy" in capsys.readouterr().out

    (behavior / "B.py").unlink()
    assert cli.main(["doctor"]) == 1
    out = capsys.readouterr().out
    assert "without node_behavior files: B" in out
    assert (tmp_path / ".mwf").read_bytes() == before



def test_doctor_reports_malformed_status_and_continues(tmp_path, monkeypatch, capsys):
    write_project(tmp_path, monkeypatch)
    capsys.readouterr()
    status = tmp_path / "node" / "A" / "jobs" / "1" / "status.json"
    status.write_text("{not json", encoding="utf-8")

    assert cli.main(["doctor"]) == 1
    out = capsys.readouterr().out
    assert "malformed JSON" in out
    assert str(status) in out


def test_events_and_inspect_show_job_history(tmp_path, monkeypatch, capsys):
    write_project(tmp_path, monkeypatch)
    capsys.readouterr()
    assert cli.main(["run", "A"]) == 0
    capsys.readouterr()

    events = tmp_path / "node" / "A" / "jobs" / "1" / "events.jsonl"
    rows = [json.loads(line) for line in events.read_text(encoding="utf-8").splitlines()]
    names = [row["event"] for row in rows]
    assert "created" in names
    assert "started" in names
    assert "done" in names

    assert cli.main(["inspect", "A", "job", "1"]) == 0
    out = capsys.readouterr().out
    assert "Job A/1" in out
    assert "Events:" in out
    assert "started" in out
    assert "done" in out


def test_recover_requeues_only_abandoned_running_jobs(tmp_path, monkeypatch, capsys):
    write_project(tmp_path, monkeypatch)
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct", persist_graph=False, initialize_node_folders=False)
    workflow.graph([("A", "B")])
    workflow.storage.set_job_status("A", 1, RUNNING, pid=99999999, started_at="2020-01-01T00:00:00")
    workflow.storage.atomic_write_json(
        workflow.storage.job_control_file("A", 1),
        {"version": 1, "generation": 0, "active_execution_id": "dead", "active_pid": 99999999},
    )
    workflow.storage.write_run_state(
        {
            "run_id": "dead-run",
            "status": "running",
            "command": "runfrom",
            "nodes": ["A", "B"],
            "pid": 99999999,
            "hostname": os.uname().nodename if hasattr(os, "uname") else "local",
            "heartbeat_at": "2020-01-01T00:00:00",
        }
    )
    capsys.readouterr()

    assert cli.main(["recover"]) == 0
    assert workflow.storage.get_job_status("A", 1) == QUEUED
    state = workflow.storage.get_run_state()
    assert state["status"] == "recovered"
    assert "A/1" in state["recovered_jobs"]


def test_timeout_moves_to_fallback_and_blocks_late_context_write(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B")])

    @workflow.task("A", timeout=0.03)
    def slow(ctx):
        time.sleep(0.08)
        ctx.write("late.txt", "must not commit")
        return "late"

    @workflow.fallback("A", name="quick")
    def quick(ctx, error=None):
        return "fallback"

    @workflow.task("B")
    def b(ctx):
        return None

    job = workflow.start("A")
    assert workflow.run_job("A", job.job_id, ignore_readiness=True) == "fallback"
    time.sleep(0.1)
    assert not (workflow.storage.files_dir("A", job.job_id) / "late.txt").exists()
    events = workflow.storage.read_job_events("A", job.job_id)
    assert any(event.get("event") == "timeout" for event in events)
    assert any(event.get("event") == "fallback_started" for event in events)


def test_context_sleep_and_cancellation_alias(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B")])

    @workflow.task("A")
    def a(ctx):
        ctx.raise_if_cancelled()
        ctx.sleep(0.01)
        assert not ctx.is_cancelled()
        return "ok"

    @workflow.task("B")
    def b(ctx):
        return None

    assert workflow.run_one("A") == "ok"


def test_transaction_stages_jobs_and_idempotency_prevents_duplicates(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B")])

    @workflow.task("A")
    def a(ctx):
        with ctx.transaction():
            first = ctx.node("B").add(value=1)
            second = ctx.node("B").add(value=2)
            assert not first.committed
            assert not second.committed
        assert first.committed and second.committed
        return [first.job_id, second.job_id]

    @workflow.task("B")
    def b(ctx, value):
        return value

    first_parent = workflow.start("A")
    assert workflow.run_job("A", first_parent.job_id, ignore_readiness=True) == [1, 2]
    assert workflow.storage.list_job_ids("B") == [1, 2]

    # A resume/restart generation must reuse the same deterministic transaction
    # keys, including jobs committed before a previous attempt failed.
    workflow.storage.request_job_restart("A", first_parent.job_id, reason="test resume")
    assert workflow.run_job("A", first_parent.job_id, ignore_readiness=True) == [1, 2]
    assert workflow.storage.list_job_ids("B") == [1, 2]


def test_transaction_aborts_before_creating_downstream_jobs(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B")])

    @workflow.task("A")
    def a(ctx):
        with ctx.transaction():
            ctx.node("B").add(value=1)
            raise ValueError("stop")

    @workflow.task("B")
    def b(ctx, value):
        return value

    with pytest.raises(Exception):
        workflow.run_one("A")
    assert workflow.storage.list_job_ids("B") == []


def test_resumefrom_preserves_done_jobs_and_continues_failed_descendant(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text("EDGES = [('A', 'B')]\n", encoding="utf-8")
    (behavior / "A.py").write_text(
        '''from micro_workflow_manager import NodeRouter
router = NodeRouter("A")
router.create_job(number=1)
@router.task
def run(ctx):
    ctx.node("B").add(value=5)
    return "A done"
''', encoding="utf-8")
    (behavior / "B.py").write_text(
        '''from pathlib import Path
from micro_workflow_manager import NodeRouter
router = NodeRouter("B")
@router.task
def run(ctx, value):
    marker = Path(ctx.system.storage.project_dir) / "failed_once.txt"
    if not marker.exists():
        marker.write_text("yes", encoding="utf-8")
        raise RuntimeError("first failure")
    return value * 2
''', encoding="utf-8")
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0
    capsys.readouterr()

    assert cli.main(["runfrom", "A"]) == 1
    capsys.readouterr()
    a_status_before = json.loads((tmp_path / "node" / "A" / "jobs" / "1" / "status.json").read_text())
    assert a_status_before["status"] == DONE
    a_events_before = (tmp_path / "node" / "A" / "jobs" / "1" / "events.jsonl").read_text().count('"event":"started"')

    assert cli.main(["resumefrom", "A"]) == 0
    capsys.readouterr()
    a_events_after = (tmp_path / "node" / "A" / "jobs" / "1" / "events.jsonl").read_text().count('"event":"started"')
    assert a_events_after == a_events_before
    b_status = json.loads((tmp_path / "node" / "B" / "jobs" / "1" / "status.json").read_text())
    assert b_status["status"] == DONE


def test_describe_is_longer_than_help_and_uses_abstract_examples(capsys):
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["run", "--help"])
    assert exit_info.value.code == 0
    help_text = capsys.readouterr().out
    assert cli.main(["--describe", "run"]) == 0
    describe = capsys.readouterr().out
    assert "Run deliberately starts fresh work" in describe
    assert "Run deliberately starts fresh work" not in help_text
    assert "random integer" in describe
    assert "explode" not in describe.lower()


def test_resume_command_retries_failed_job_without_rerunning_done_job(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text("EDGES = [('A', 'B')]\n", encoding="utf-8")
    (behavior / "A.py").write_text(
        '''from pathlib import Path
from micro_workflow_manager import NodeRouter
router = NodeRouter("A")
router.create_job(number=2)
@router.task
def run(ctx):
    root = Path(ctx.system.storage.project_dir)
    if ctx.job_id == 2 and not (root / "allow_two.txt").exists():
        raise RuntimeError("job two fails once")
    count = root / f"count_{ctx.job_id}.txt"
    value = int(count.read_text() if count.exists() else "0") + 1
    count.write_text(str(value), encoding="utf-8")
    return ctx.job_id
''', encoding="utf-8")
    (behavior / "B.py").write_text(
        '''from micro_workflow_manager import NodeRouter
router = NodeRouter("B")
@router.task
def run(ctx):
    return None
''', encoding="utf-8")
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0
    capsys.readouterr()

    assert cli.main(["run", "A"]) == 1
    capsys.readouterr()
    assert (tmp_path / "count_1.txt").read_text() == "1"
    (tmp_path / "allow_two.txt").write_text("yes", encoding="utf-8")

    assert cli.main(["resume", "A"]) == 0
    capsys.readouterr()
    assert (tmp_path / "count_1.txt").read_text() == "1"
    assert (tmp_path / "count_2.txt").read_text() == "1"


def test_node_router_timeout_is_written_to_schema(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B")])
    router = NodeRouter("A", timeout=1.5)

    @router.task
    def a(ctx):
        return 1

    workflow.include_router(router)

    @workflow.task("B")
    def b(ctx):
        return None

    schema = workflow.storage.read_json(workflow.storage.node_schema_file("A"))
    assert schema["timeout"] == 1.5
    assert workflow.nodes["A"].main_task.timeout == 1.5


def test_active_run_state_contains_ownership_and_heartbeat(tmp_path, monkeypatch, capsys):
    write_project(tmp_path, monkeypatch)
    capsys.readouterr()
    assert cli.main(["run", "A"]) == 0
    state = json.loads((tmp_path / ".mwf_run.json").read_text(encoding="utf-8"))
    assert state["hostname"]
    assert state["pid"] > 0
    assert state["heartbeat_at"]
    assert state["mwf_version"] == "0.2.2"
    assert state["status"] == "done"


def test_migrate_versions_only_framework_metadata(tmp_path, monkeypatch, capsys):
    write_project(tmp_path, monkeypatch)
    capsys.readouterr()
    config_path = tmp_path / ".mwf"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config.pop("schema_version", None)
    config_path.write_text(json.dumps(config), encoding="utf-8")

    job_path = tmp_path / "node" / "A" / "jobs" / "1" / "job.json"
    job_data = json.loads(job_path.read_text(encoding="utf-8"))
    job_data.pop("schema_version", None)
    job_path.write_text(json.dumps(job_data), encoding="utf-8")
    input_path = tmp_path / "node" / "A" / "jobs" / "1" / "input.json"
    output_path = tmp_path / "node" / "A" / "jobs" / "1" / "output.json"
    output_path.write_text('{"custom": true}', encoding="utf-8")
    input_before = input_path.read_bytes()
    output_before = output_path.read_bytes()

    assert cli.main(["migrate", "--dry-run"]) == 0
    assert "Would migrate" in capsys.readouterr().out
    assert "schema_version" not in json.loads(config_path.read_text(encoding="utf-8"))

    assert cli.main(["migrate"]) == 0
    capsys.readouterr()
    assert json.loads(config_path.read_text(encoding="utf-8"))["schema_version"] == 1
    assert json.loads(job_path.read_text(encoding="utf-8"))["schema_version"] == 1
    assert input_path.read_bytes() == input_before
    assert output_path.read_bytes() == output_before


def test_runfrom_plan_is_read_only(tmp_path, monkeypatch, capsys):
    write_project(tmp_path, monkeypatch)
    capsys.readouterr()
    watched = [
        tmp_path / ".mwf",
        tmp_path / "node" / "A" / "node_state.json",
        tmp_path / "node" / "A" / "jobs" / "1" / "job.json",
    ]
    before = {path: path.read_bytes() for path in watched}

    assert cli.main(["runfrom", "A", "--plan"]) == 0
    out = capsys.readouterr().out
    assert "Plan for: mwf runfrom A" in out
    assert "no state, jobs, inputs, outputs, or node folders were changed" in out
    assert {path: path.read_bytes() for path in watched} == before
    assert not (tmp_path / ".mwf_run.json").exists()


def test_graph_update_dry_run_does_not_add_or_delete_nodes(tmp_path, monkeypatch, capsys):
    write_project(tmp_path, monkeypatch)
    capsys.readouterr()
    config_before = (tmp_path / ".mwf").read_bytes()
    (tmp_path / "src" / "graph.py").write_text("EDGES = [('A', 'C')]\n", encoding="utf-8")

    assert cli.main(["graph", "--update", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "nodes to add: C" in out
    assert "nodes to delete: B" in out
    assert (tmp_path / ".mwf").read_bytes() == config_before
    assert (tmp_path / "node" / "B").is_dir()
    assert not (tmp_path / "node" / "C").exists()


def test_cleanup_and_recover_dry_runs_do_not_mutate(tmp_path, monkeypatch, capsys):
    write_project(tmp_path, monkeypatch)
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct", persist_graph=False, initialize_node_folders=False)
    workflow.graph([("A", "B")])
    workflow.storage.set_job_status("A", 1, RUNNING, pid=99999999, started_at="2020-01-01T00:00:00")
    workflow.storage.atomic_write_json(
        workflow.storage.job_control_file("A", 1),
        {"version": 1, "generation": 0, "active_execution_id": "dead", "active_pid": 99999999},
    )
    workflow.storage.write_run_state(
        {
            "run_id": "dead-run",
            "status": "running",
            "command": "runfrom",
            "nodes": ["A", "B"],
            "pid": 99999999,
            "hostname": os.uname().nodename if hasattr(os, "uname") else "local",
            "heartbeat_at": "2020-01-01T00:00:00",
        }
    )
    status_before = workflow.storage.status_file("A", 1).read_bytes()
    control_before = workflow.storage.job_control_file("A", 1).read_bytes()
    run_before = (tmp_path / ".mwf_run.json").read_bytes()
    capsys.readouterr()

    assert cli.main(["recover", "--dry-run"]) == 0
    assert "Would recover" in capsys.readouterr().out
    assert workflow.storage.status_file("A", 1).read_bytes() == status_before
    assert workflow.storage.job_control_file("A", 1).read_bytes() == control_before
    assert (tmp_path / ".mwf_run.json").read_bytes() == run_before

    node_before = sorted(str(path.relative_to(tmp_path)) for path in (tmp_path / "node" / "A").rglob("*"))
    assert cli.main(["clean", "A", "--dry-run"]) == 0
    assert "Dry run" in capsys.readouterr().out
    node_after = sorted(str(path.relative_to(tmp_path)) for path in (tmp_path / "node" / "A").rglob("*"))
    assert node_after == node_before


def test_every_describe_page_extends_help_with_abstract_examples(capsys):
    from micro_workflow_manager.cli.constants import COMMAND_NAMES

    forbidden = ("explode", "tagify", "attachfragment", "preexplode", "ocr_pages", "zoning")
    for command in COMMAND_NAMES:
        assert cli.main(["--describe", command]) == 0
        text = capsys.readouterr().out
        assert "Help summary:" in text
        assert "Extended explanation:" in text
        assert "mwf " in text
        lowered = text.lower()
        for term in forbidden:
            assert term not in lowered
