from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from micro_workflow_manager.models import DONE, FAILED, QUEUED, RUNNING, SKIPPED


def _print_json(label: str, value: Any):
    print(f"{label}:")
    text = json.dumps(value, indent=2, ensure_ascii=False, default=str)
    for line in text.splitlines():
        print(f"  {line}")


def _format_progress(value: Any) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f"{float(value) * 100:.1f}%"
    return "(not reported)"


def _print_runtime(runtime: dict[str, Any]):
    if not runtime:
        print("Runtime:")
        print("  (no checkpoint or supervised timeout data recorded)")
        return
    print("Runtime:")
    print(f"  state: {runtime.get('state', '?')}")
    print(f"  task: {runtime.get('task', '?')}")
    print(f"  attempt: {runtime.get('attempt', '?')} repeat: {runtime.get('repeat_index', '?')}")
    print(f"  started_at: {runtime.get('started_at') or '(unknown)'}")
    print(f"  updated_at: {runtime.get('updated_at') or '(unknown)'}")
    print(f"  total timeout: {runtime.get('total_timeout_seconds')}")
    print(f"  total deadline: {runtime.get('total_deadline_at') or '(none)'}")
    print(f"  checkpoint: {runtime.get('checkpoint_name') or '(none)'}")
    print(f"  checkpoint_at: {runtime.get('checkpoint_at') or '(none)'}")
    print(f"  checkpoint timeout: {runtime.get('checkpoint_timeout_seconds')}")
    print(f"  checkpoint deadline: {runtime.get('checkpoint_deadline_at') or '(none)'}")
    print(f"  progress: {_format_progress(runtime.get('progress'))}")
    if runtime.get("progress_detail"):
        print(f"  progress detail: {runtime['progress_detail']}")
    if runtime.get("timeout_message"):
        print(f"  timeout: {runtime['timeout_message']}")


def _node_explanation(workflow, node: str) -> str:
    status = workflow.storage.get_node_status(node) or "missing"
    summary = workflow.storage.node_job_summary(node)
    counts = summary["counts"]
    if counts.get(RUNNING, 0):
        return f"The node is active because {counts[RUNNING]} job(s) are running."
    if counts.get(QUEUED, 0):
        blockers = [p for p in workflow.graph_obj.predecessors(node) if not workflow.node_complete(p)]
        if blockers:
            return "Queued jobs are waiting for incomplete predecessors: " + ", ".join(sorted(blockers)) + "."
        return f"The node has {counts[QUEUED]} queued job(s) ready for scheduling."
    if counts.get(FAILED, 0):
        return f"The node cannot complete because {counts[FAILED]} job(s) failed. Use mwf resume {node}."
    if status in {DONE, SKIPPED}:
        return f"The node is complete with status {status}."
    if summary["total"] == 0:
        return "The node has no jobs. An upstream node or router.create_job(...) must create one."
    return f"The node is in status {status}; inspect its latest job events for details."


def inspect_node(workflow, node: str) -> int:
    summary = workflow.storage.node_job_summary(node)
    schema = workflow.storage.read_json(workflow.storage.node_schema_file(node), default={})
    component = sorted(workflow.component_for(node))
    print(f"Node {node}")
    print(f"  status: {workflow.storage.get_node_status(node) or 'missing'}")
    print(f"  component: {', '.join(component)}")
    print(f"  predecessors: {', '.join(sorted(workflow.graph_obj.predecessors(node))) or '(none)'}")
    print(f"  successors: {', '.join(sorted(workflow.graph_obj.successors(node))) or '(none)'}")
    print(f"  jobs: total={summary['total']} " + " ".join(f"{key}={value}" for key, value in sorted(summary['counts'].items()) if value))
    if schema:
        print(f"  runner: {schema.get('runner_override') or workflow.runner}")
        print(f"  max_threads: {schema.get('max_threads')}")
        print(f"  timeout: {schema.get('timeout')}")
        print(f"  checkpoint_timeout: {schema.get('checkpoint_timeout')}")
        print(f"  fallbacks: {', '.join(schema.get('fallbacks') or []) or '(none)'}")
    print(f"  explanation: {_node_explanation(workflow, node)}")
    return 0


def inspect_job(workflow, node: str, job_id: int) -> int:
    storage = workflow.storage
    if not storage.job_exists(node, job_id):
        raise RuntimeError(f"Job does not exist: {node}/{job_id}")
    job = storage.load_job(node, job_id)
    status = storage.read_json(storage.status_file(node, job_id), default={"status": QUEUED})
    control = storage.read_job_control(node, job_id)
    output = storage.read_json(storage.output_file(node, job_id), default=None)
    runtime = storage.read_job_runtime(node, job_id)
    events = storage.read_job_events(node, job_id)
    print(f"Job {node}/{job_id}")
    print(f"  status: {status.get('status', QUEUED)}")
    print(f"  parent: {job.parent or '(none)'}")
    print(f"  generation: {control.get('generation', 0)}")
    if control.get("active_execution_id"):
        print(f"  active process: {control.get('active_pid')}")
        print(f"  active since: {control.get('active_started_at')}")
    _print_runtime(runtime)
    _print_json("Input", job.params)
    if output is not None:
        _print_json("Output", output)

    children: list[str] = []
    for target in workflow.graph_obj.successors(node):
        for child_id in storage.list_job_ids(target):
            child = storage.load_job(target, child_id)
            parent = child.parent or {}
            if parent.get("from_node") == node and parent.get("from_job_id") == job_id:
                children.append(f"{target}/{child_id}")
    print(f"  downstream jobs created: {', '.join(children) if children else '(none)'}")

    print("Events:")
    if not events:
        print("  (none recorded)")
    else:
        for event in events:
            details = {k: v for k, v in event.items() if k not in {"time", "event"}}
            suffix = f" {json.dumps(details, ensure_ascii=False, default=str)}" if details else ""
            print(f"  {event.get('time', '?')} {event.get('event', '?')}{suffix}")
    return 0


def inspect_command(workflow, node: str, job_id: int | None = None) -> int:
    if job_id is None:
        return inspect_node(workflow, node)
    return inspect_job(workflow, node, job_id)
