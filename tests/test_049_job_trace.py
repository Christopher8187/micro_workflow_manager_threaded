from __future__ import annotations

from importlib import import_module
import pytest

from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.cli.parser import build_parser
from micro_workflow_manager.cli.trace import trace_command


def _make_trace_workflow(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="direct")
    workflow.graph([("source", "next")])

    source = NodeRouter("source", runner="direct")
    source.create_job(number=1)

    @source.task
    def run(ctx):
        ctx.trace(
            "primary llm",
            input={"prompt": "hello"},
            output={"reply": "bad"},
            node_name="user node label",
            job_id="user job label",
        )
        ctx.trace("validator", status="error", content="bad response")
        ctx.write_output("main.txt", "main diagnostic")
        raise ValueError("main failed")

    @source.fallback(name="repair")
    def repair(ctx, error=None):
        ctx.trace("repair llm", input={"prompt": "repair"}, output={"reply": "good"})
        ctx.trace("validator", status="passed")
        ctx.write_output("published.txt", "published diagnostic")
        target = ctx.node("next")
        target.write_input("forwarded.txt", "forwarded input", overwrite=True)
        target.add(value="child")
        return {"ok": True}

    next_node = NodeRouter("next", runner="direct")

    @next_node.task
    def run_next(ctx, value=None):
        return value

    workflow.include_router(source)
    workflow.include_router(next_node)
    return workflow


def test_trace_events_and_renderer_are_chronological(tmp_path, capsys):
    workflow = _make_trace_workflow(tmp_path)
    workflow.run_node("source", ignore_readiness=True)

    events = workflow.storage.read_job_events("source", 1)
    primary = next(event for event in events if event.get("name") == "primary llm")
    assert primary["trace_node_name"] == "user node label"
    assert primary["trace_job_id"] == "user job label"
    kinds = [event["event"] for event in events]
    required = [
        "task_started",
        "trace",
        "trace",
        "output_written",
        "task_started",
        "trace",
        "trace",
        "output_written",
        "input_forwarded",
        "jobs_created",
        "done",
    ]
    cursor = 0
    for kind in required:
        cursor = kinds.index(kind, cursor) + 1

    assert trace_command(workflow, "source", 1) == 0
    output = capsys.readouterr().out
    assert "ORIGIN" in output
    assert "source MAIN TASK STARTED" in output
    assert "source FALLBACK repair STARTED" in output
    assert "TRACE primary llm FOR MAIN TASK" in output
    assert "TRACE repair llm FOR repair" in output
    assert "OUTPUT FOR repair" in output
    assert "INPUT FORWARD FOR repair" in output
    assert "next job 1" in output
    assert "JOB ENDED" in output
    assert '"state": "done"' in output
    assert '"path": "jobs/1/output.json"' in output
    assert '"path": "output/jobs/1/output.json"' not in output
    assert output.index("TRACE repair llm") < output.index("INPUT FORWARD FOR repair")
    assert output.index("INPUT FORWARD FOR repair") < output.index("JOBS CREATED")


def test_trace_errors_renders_ordered_failures_and_only_failure_context(
    tmp_path,
    capsys,
    monkeypatch,
):
    workflow = MicroWorkflow(tmp_path, runner="direct")
    workflow.graph([("source", "next")])

    source = NodeRouter("source", runner="direct")
    source.create_job(number=1)

    @source.task(retries=1)
    def run(ctx):
        if ctx.attempt == 1:
            raise ValueError("main attempt one")
        raise TypeError("main attempt two")

    @source.fallback(name="final")
    def final(ctx):
        raise RuntimeError("fallback terminal")

    next_node = NodeRouter("next", runner="direct")

    @next_node.task
    def run_next(ctx):
        return None

    workflow.include_router(source)
    workflow.include_router(next_node)

    with pytest.raises(Exception):
        workflow.run_job("source", 1, ignore_readiness=True)

    assert trace_command(workflow, "source", 1) == 0
    normal_output = capsys.readouterr().out
    assert normal_output.count("source MAIN TASK run FAILED") == 2
    assert normal_output.count("source FALLBACK final FAILED") == 1
    assert (
        normal_output.index("main attempt one")
        < normal_output.index("main attempt two")
        < normal_output.index("fallback terminal")
    )

    assert trace_command(workflow, "source", 1, errors_only=True) == 0
    errors_output = capsys.readouterr().out
    assert "JOB source/1" in errors_output
    assert "ORIGIN" in errors_output
    assert "script (root job defined by node behavior/router)" in errors_output
    assert errors_output.count("source MAIN TASK run FAILED") == 2
    assert errors_output.count("source FALLBACK final FAILED") == 1
    assert (
        errors_output.index("main attempt one")
        < errors_output.index("main attempt two")
        < errors_output.index("fallback terminal")
    )
    assert '"attempt": 1' in errors_output
    assert '"attempt": 2' in errors_output
    assert '"repeat_index": 1' in errors_output
    assert '"state": "failed"' in errors_output
    assert errors_output.count("fallback terminal") == 2
    assert "STARTED" not in errors_output
    assert "TRACE " not in errors_output
    assert "OUTPUT FOR" not in errors_output
    assert "INPUT FORWARD" not in errors_output
    assert "JOBS CREATED" not in errors_output

    args = build_parser().parse_args(
        ["trace", "source", "job", "17", "--errors"]
    )
    assert args.errors is True

    cli_package = import_module("micro_workflow_manager.cli")
    cli_entrypoint = cli_package.main
    cli_main = import_module("micro_workflow_manager.cli.main")
    dispatched = []
    monkeypatch.setattr(cli_main, "find_root", lambda: tmp_path)
    monkeypatch.setattr(cli_main, "ensure_runtime_layout", lambda root: None)
    monkeypatch.setattr(cli_main, "load_workflow", lambda root, runner: workflow)
    monkeypatch.setattr(cli_main, "require_node", lambda loaded, node: None)

    def capture_trace(loaded, node, job_id, *, errors_only=False):
        dispatched.append((loaded, node, job_id, errors_only))
        return 0

    monkeypatch.setattr(cli_main, "trace_command", capture_trace)
    cli_package.main = cli_entrypoint
    assert cli_package.main(["trace", "source", "job", "1", "--errors"]) == 0
    assert dispatched == [
        (
            workflow,
            "source",
            1,
            True,
        )
    ]


def test_trace_cli_syntax_parses():
    args = build_parser().parse_args(["trace", "source", "job", "17"])
    assert args.command == "trace"
    assert args.node == "source"
    assert args.job_mode == "job"
    assert args.job_id == 17
