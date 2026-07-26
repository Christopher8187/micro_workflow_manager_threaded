from __future__ import annotations

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
        ctx.trace("primary llm", input={"prompt": "hello"}, output={"reply": "bad"})
        ctx.trace("validator", status="error", content="bad response")
        ctx.write("main.txt", "main diagnostic")
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
    assert output.index("TRACE repair llm") < output.index("INPUT FORWARD FOR repair")
    assert output.index("INPUT FORWARD FOR repair") < output.index("JOBS CREATED")


def test_trace_cli_syntax_parses():
    args = build_parser().parse_args(["trace", "source", "job", "17"])
    assert args.command == "trace"
    assert args.node == "source"
    assert args.job_mode == "job"
    assert args.job_id == 17
