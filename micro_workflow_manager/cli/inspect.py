from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .active_run import live_active_run

from micro_workflow_manager.models import (
    CANCELLED,
    DONE,
    FAILED,
    QUEUED,
    RUNNING,
    SKIPPED,
)


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
        blockers = [p for p in workflow.component_predecessors(workflow.component_for(node)) if not workflow.node_complete(p)]
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
    print(f"  Hoeflein component: {', '.join(component)}")
    print(f"  predecessors: {', '.join(sorted(workflow.graph_obj.predecessors(node))) or '(none)'}")
    print(f"  successors: {', '.join(sorted(workflow.graph_obj.successors(node))) or '(none)'}")
    print(f"  jobs: total={summary['total']} " + " ".join(f"{key}={value}" for key, value in sorted(summary['counts'].items()) if value))
    if schema:
        print(f"  runner: {schema.get('runner_override') or workflow.runner}")
        declared_threads = schema.get("max_threads")
        override_threads = workflow.thread_override(node)
        print(f"  declared max_threads: {declared_threads}")
        print(f"  runtime max_threads override: {override_threads if override_threads is not None else '(none)'}")
        print(f"  effective max_threads: {workflow.effective_max_threads(node)}")
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
    status = storage.read_job_status_data(node, job_id) or {"status": QUEUED}
    control = storage.read_job_control(node, job_id)
    output = storage.read_json(storage.output_file(node, job_id), default=None)
    runtime = storage.read_job_runtime(node, job_id)
    events = storage.read_job_events(node, job_id)
    print(f"Job {node}/{job_id}")
    print(f"  status: {status.get('status', QUEUED)}")
    print(f"  parent: {job.parent or '(none)'}")
    print(f"  producer component: {', '.join(job.producer_component or ()) or '(none)'}")
    print(f"  job kind: {job.job_kind or 'root'}")
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


def _one_line(value: Any, *, limit: int = 240) -> str:
    text = " ".join(str(value).split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."



def _current_execution_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return the latest execution segment for one job.

    A manual restart or a fresh reset/run appends another ``started`` event.
    Filter inspection is intended to explain the current funnel, so old
    generations must not inflate later retry/fallback counts.
    """
    start_index: int | None = None
    for index, event in enumerate(events):
        if event.get("event") == "started":
            start_index = index
    if start_index is None:
        return []
    return events[start_index:]


def _filter_stages(workflow, node: str) -> list[dict[str, Any]]:
    mounted_node = workflow.nodes[node]
    main = mounted_node.main_task
    if main is None:
        return []

    stages: list[dict[str, Any]] = []

    def add_task_stages(kind: str, name: str, retries: int, repeats: int):
        attempts = retries + 1
        for attempt in range(1, attempts + 1):
            if kind == "main":
                role = "main"
            elif kind == name:
                role = "fallback"
            else:
                role = f"fallback {kind}"
            label = f"{role}: {name} — attempt {attempt}/{attempts}"
            if repeats > 1:
                label += f" ({repeats} repeats)"
            stages.append(
                {
                    "kind": kind,
                    "task": name,
                    "attempt": attempt,
                    "label": label,
                }
            )

    add_task_stages("main", main.name, main.retries, main.repeats)
    for fallback_name in mounted_node.fallback_order:
        fallback = mounted_node.fallbacks[fallback_name]
        add_task_stages(fallback_name, fallback.name, fallback.retries, fallback.repeats)
    return stages


def _stage_indexes_for_events(
    events: list[dict[str, Any]],
    stages: list[dict[str, Any]],
) -> set[int]:
    if not events or not stages:
        return set()

    indexes = {0}
    lookup = {
        (stage["kind"], stage["attempt"]): index
        for index, stage in enumerate(stages)
    }
    current_kind = "main"

    for event in events[1:]:
        event_name = event.get("event")
        if event_name == "fallback_started":
            fallback_name = event.get("fallback")
            if isinstance(fallback_name, str):
                current_kind = fallback_name
                index = lookup.get((current_kind, 1))
                if index is not None:
                    indexes.add(index)
        elif event_name == "retry_started":
            try:
                attempt = int(event.get("attempt"))
            except (TypeError, ValueError):
                continue
            index = lookup.get((current_kind, attempt))
            if index is not None:
                indexes.add(index)
    return indexes


def _failed_job_rows(workflow, node: str) -> list[dict[str, Any]]:
    return workflow.storage.list_jobs(node, status=FAILED)


def inspect_filter(workflow, node: str) -> int:
    """Show how many jobs survive each retry/fallback stage.

    The command deliberately reconstructs the funnel from per-job event logs
    only when requested. It adds no shared manifest and no execution-time writes.
    """
    storage = workflow.storage
    stages = _filter_stages(workflow, node)
    job_ids = list(storage.iter_job_ids(node))
    entered: list[set[int]] = [set() for _ in stages]
    latest_stage: dict[int, int] = {}
    statuses: dict[int, str] = {}

    for job_id in job_ids:
        status = storage.get_job_status(node, job_id) or QUEUED
        statuses[job_id] = status
        events = _current_execution_events(storage.read_job_events(node, job_id))
        indexes = _stage_indexes_for_events(events, stages)
        if not indexes:
            continue
        for index in indexes:
            entered[index].add(job_id)
        latest_stage[job_id] = max(indexes)

    print(f"Filter funnel for node {node}")
    print(f"  jobs discovered: {len(job_ids)}")
    print("  scope: latest execution segment for each job")
    print("  remaining = entered - jobs that passed at that stage")

    if not stages:
        print("Stages:")
        print("  (node has no mounted task)")
    else:
        rows: list[tuple[int, str, int, int, int]] = []
        for index, stage in enumerate(stages):
            stage_entered = len(entered[index])
            passed = sum(
                1
                for job_id in entered[index]
                if latest_stage.get(job_id) == index
                and statuses.get(job_id) in {DONE, SKIPPED}
            )
            remaining = stage_entered - passed
            rows.append((index + 1, stage["label"], stage_entered, passed, remaining))

        number_width = max(1, len(str(len(rows))))
        label_width = max(5, min(72, max(len(row[1]) for row in rows)))
        count_width = max(7, len(str(max((row[2] for row in rows), default=0))))
        print("Stages:")
        print(
            f"  {'#':>{number_width}}  {'stage':<{label_width}}  "
            f"{'entered':>{count_width}}  {'passed':>{count_width}}  {'remaining':>{count_width}}"
        )
        print(
            f"  {'-' * number_width}  {'-' * label_width}  "
            f"{'-' * count_width}  {'-' * count_width}  {'-' * count_width}"
        )
        for number, label, stage_entered, passed, remaining in rows:
            print(
                f"  {number:>{number_width}}  {label:<{label_width}}  "
                f"{stage_entered:>{count_width}}  {passed:>{count_width}}  {remaining:>{count_width}}"
            )

    not_started = [job_id for job_id in job_ids if job_id not in latest_stage]
    terminal_counts = {
        status: sum(1 for value in statuses.values() if value == status)
        for status in (DONE, SKIPPED, RUNNING, QUEUED, FAILED, CANCELLED)
    }
    print("Terminal/current state:")
    print(
        "  "
        + " ".join(
            f"{status}={count}"
            for status, count in terminal_counts.items()
            if count
        )
        if any(terminal_counts.values())
        else "  (no jobs)"
    )
    print(f"  not started in latest execution: {len(not_started)}")

    failed_rows = _failed_job_rows(workflow, node)
    failed_ids = [int(row["job_id"]) for row in failed_rows]
    if failed_ids:
        joined = " ".join(str(job_id) for job_id in failed_ids)
        print("Commands:")
        print(f"  inspect one: mwf inspect {node} job {failed_ids[0]}")
        print(f"  restart all: mwf restart {node} jobs {joined}")

    # Keep the terminal failures as the final section so a long funnel can be
    # scanned top-to-bottom and end on the exact jobs that still need attention.
    print("Failed jobs:")
    if not failed_rows:
        print("  (none)")
        return 0
    for row in failed_rows:
        job_id = int(row["job_id"])
        output = storage.read_json(storage.output_file(node, job_id), default={})
        error = output.get("error") if isinstance(output, dict) else None
        if not error:
            runtime = storage.read_job_runtime(node, job_id)
            error = runtime.get("timeout_message") if isinstance(runtime, dict) else None
        print(f"  {job_id}: {_one_line(error or '(error not recorded)')}")
    return 0


def inspect_failed(workflow, node: str) -> int:
    """Print a compact, copyable summary of failed jobs in one node.

    This is an explicit diagnostic command, so it may scan that node's job
    folders. The scheduler's normal fast paths and job index remain unchanged.
    """
    storage = workflow.storage
    failed_rows = _failed_job_rows(workflow, node)

    print(f"Failed jobs for node {node}")
    print(f"  count: {len(failed_rows)}")
    if not failed_rows:
        print("  IDs: (none)")
        return 0

    job_ids = [int(row["job_id"]) for row in failed_rows]
    print("  IDs: " + " ".join(str(job_id) for job_id in job_ids))
    print("Jobs:")

    for row in failed_rows:
        job_id = int(row["job_id"])
        output = storage.read_json(storage.output_file(node, job_id), default={})
        error = output.get("error") if isinstance(output, dict) else None
        if not error:
            runtime = storage.read_job_runtime(node, job_id)
            error = runtime.get("timeout_message") if isinstance(runtime, dict) else None
        duration = row.get("duration_seconds")
        duration_text = f"{float(duration):.3f}s" if isinstance(duration, int | float) else "?"
        finished_at = row.get("finished_at") or "?"
        print(f"  {job_id}: finished={finished_at} duration={duration_text}")
        print(f"     error: {_one_line(error or '(error not recorded)')}")

    joined = " ".join(str(job_id) for job_id in job_ids)
    print("Commands:")
    print(f"  inspect one: mwf inspect {node} job {job_ids[0]}")
    active = live_active_run(storage)
    if active is not None and node in set(active.get("nodes") or []):
        print(f"  restart all in the active run: mwf restart {node} jobs {joined}")
    else:
        print(f"  retry this node after the run: mwf resume {node}")
        print("  retry a descendant sequence: mwf resumefrom <start-node>")
    return 0


def inspect_debug(workflow, node: str) -> int:
    path = workflow.storage.debug_file(node)
    print(f"Debug file for node {node}")
    print(f"  path: {path}")
    if not path.exists():
        print("  (debug file does not exist yet)")
        return 0
    text = path.read_text(encoding="utf-8", errors="replace")
    if not text:
        print("  (debug file is empty)")
        return 0
    print("--- debug.txt ---")
    print(text, end="" if text.endswith("\n") else "\n")
    return 0


def inspect_command(
    workflow,
    node: str,
    job_id: int | None = None,
    *,
    debug: bool = False,
    failed: bool = False,
    filter_funnel: bool = False,
) -> int:
    if debug:
        return inspect_debug(workflow, node)
    if failed:
        return inspect_failed(workflow, node)
    if filter_funnel:
        return inspect_filter(workflow, node)
    if job_id is None:
        return inspect_node(workflow, node)
    return inspect_job(workflow, node, job_id)
