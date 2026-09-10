from __future__ import annotations

import json
from typing import Any

from micro_workflow_manager.session_liveness import execution_session_liveness
from micro_workflow_manager.node_observation import node_observation

from micro_workflow_manager.models import (
    CANCELLED,
    DONE,
    FAILED,
    QUEUED,
    RUNNING,
    SKIPPED,
    WAITING,
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


def _node_explanation(workflow, node: str, observation: dict) -> str:
    status = observation["state"]
    summary = workflow.storage.node_job_summary(node)
    counts = summary["counts"]
    if status == "sampled":
        return "The component completed its selected sample; remaining jobs stay queued for resume."
    if status == DONE:
        return "The component has completed."
    if status == FAILED:
        return f"The component failed. Inspect failed jobs before using mwf resume {node}."
    if counts.get(RUNNING, 0):
        return f"The node is active because {counts[RUNNING]} job(s) are running."
    if counts.get(QUEUED, 0):
        waiting_blockers = sorted(workflow.waiting_blockers(node))
        if status == RUNNING and waiting_blockers:
            return (
                f"The node is waiting with {counts[QUEUED]} queued job(s) until "
                + ", ".join(waiting_blockers)
                + " has no queued work left."
            )
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
    observation = node_observation(workflow, node)
    print(f"Node {node}")
    print(f"  status: {observation['status']}")
    print(f"  state: {observation['state']}")
    print(f"  stability: {observation['stability'] or '(none)'}")
    print(f"  instability origin: {observation['instability_origin'] or '(none)'}")
    print(f"  misaligned: {'yes' if observation['misaligned'] else 'no'}")
    if observation["misalignment_causes"]:
        _print_json("  first causes", observation["misalignment_causes"])
    print(f"  Hoeflein component: {', '.join(component)}")
    print(f"  predecessors: {', '.join(sorted(workflow.graph_obj.predecessors(node))) or '(none)'}")
    print(f"  successors: {', '.join(sorted(workflow.graph_obj.successors(node))) or '(none)'}")
    print(f"  jobs: total={summary['total']} " + " ".join(f"{key}={value}" for key, value in sorted(summary['counts'].items()) if value))
    if schema:
        print(f"  runner: {schema.get('runner_override') or workflow.runner}")
        declared_threads = schema.get("max_threads")
        override_threads = observation["thread_override"]
        print(f"  declared max_threads: {declared_threads}")
        print(f"  runtime max_threads override: {override_threads if override_threads is not None else '(none)'}")
        print(f"  requested max_threads: {observation['requested_max_threads']}")
        if observation["runner"] == "api":
            limit = workflow.api_total_limit_override()
            print(f"  project-wide API limit: {limit if limit is not None else '(none)'}")
        if override_threads is not None:
            owner = observation["thread_override_session_id"]
            print(f"  override scope: {'session ' + owner if owner else 'pending next claimant'}")
        print(f"  timeout: {schema.get('timeout')}")
        print(f"  checkpoint_timeout: {schema.get('checkpoint_timeout')}")
        print(f"  waiting node: {'yes' if schema.get('waiting') else 'no'}")
        declared_wait = schema.get('wait_for')
        if schema.get('waiting'):
            declared_text = 'all component peers' if declared_wait is None else (', '.join(declared_wait) or '(empty subset)')
            print(f"  declared wait_for: {declared_text}")
            print(f"  resolved wait_for: {', '.join(schema.get('resolved_wait_for') or []) or '(none)'}")
            print(f"  currently waiting on: {', '.join(sorted(workflow.waiting_blockers(node))) or '(none)'}")
        print(f"  fallbacks: {', '.join(schema.get('fallbacks') or []) or '(none)'}")
    print(f"  explanation: {_node_explanation(workflow, node, observation)}")
    return 0


def inspect_job(workflow, node: str, job_id: int) -> int:
    storage = workflow.storage
    ownership = storage.read_job_owner_observation(node, job_id)
    if ownership is None:
        raise RuntimeError(f"Job does not exist: {node}/{job_id}")
    job = storage.load_job(node, job_id)
    if storage.read_job_instance_id(node, job_id) != ownership['job_instance_id']:
        raise RuntimeError(f"Job {node}/{job_id} was replaced during inspection; retry the command")
    output = storage.read_json(storage.output_file(node, job_id), default=None)
    runtime = storage.read_job_runtime(node, job_id)
    events = storage.read_job_events(node, job_id)
    print(f"Job {node}/{job_id}")
    print(f"  status: {ownership['status']}")
    print(f"  job instance: {ownership['job_instance_id']}")
    print(f"  parent: {job.parent or '(none)'}")
    print(f"  producer component: {', '.join(job.producer_component or ()) or '(none)'}")
    print(f"  job kind: {job.job_kind or 'root'}")
    print(f"  generation: {ownership['generation']}")
    print(f"  execution ownership: {ownership['state']}")
    owner = ownership['owner']
    if owner is not None:
        session = ownership['session']
        liveness = execution_session_liveness(session)
        print(f"  execution ID: {owner['execution_id']}")
        print(f"  session ID: {owner['session_id']}")
        print(f"  session kind: {session['session_kind']}")
        print(f"  session status: {session['status']}")
        print(f"  session liveness: {'live' if liveness['live'] else 'not live'}")
        print(f"  session liveness reason: {liveness['reason']}")
        print(f"  owner component: {', '.join(owner['component'])}")
        print(f"  claimed generation: {owner['generation']}")
        if session['parent_session_ids']:
            print(f"  parent sessions: {', '.join(session['parent_session_ids'])}")
        if session['outcome'] is not None:
            print(f"  session outcome: {session['outcome']}")
    if ownership['active_execution_id'] is not None:
        pid = ownership['active_pid']
        started_at = ownership['active_started_at']
        print(f"  active process: {pid if pid is not None else '(not recorded)'}")
        print(f"  active since: {started_at if started_at is not None else '(not recorded)'}")
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



def _failed_job_rows(workflow, node: str) -> list[dict[str, Any]]:
    return workflow.storage.list_jobs(node, status=FAILED)


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

    print("Commands:")
    print(f"  inspect one: mwf inspect {node} job {job_ids[0]}")
    live_owners = {}
    for job_id in job_ids:
        observed = storage.read_job_owner_observation(node, job_id)
        session = None if observed is None else observed['session']
        if session is not None and execution_session_liveness(session)['live']:
            live_owners.setdefault(session['session_id'], []).append(job_id)
    for session_id, owned_jobs in sorted(live_owners.items()):
        selected = ' '.join(map(str, owned_jobs))
        print(f"  restart jobs in session {session_id}: mwf restart {node} jobs {selected}")
    if sum(map(len, live_owners.values())) < len(job_ids):
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
) -> int:
    if debug:
        return inspect_debug(workflow, node)
    if failed:
        return inspect_failed(workflow, node)
    if job_id is None:
        return inspect_node(workflow, node)
    return inspect_job(workflow, node, job_id)
