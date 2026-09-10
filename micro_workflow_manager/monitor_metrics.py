from __future__ import annotations

from datetime import datetime
from typing import Any

from .models import CANCELLED, DONE, FAILED, QUEUED, RUNNING, SKIPPED, WAITING
from .node_observation import node_observation

STATUSES = [QUEUED, RUNNING, DONE, FAILED, SKIPPED, CANCELLED]
TERMINAL = {DONE, FAILED, SKIPPED, CANCELLED}


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")

def parse_iso(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None

    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None

def seconds_since(value: Any) -> float | None:
    start = parse_iso(value)
    if start is None:
        return None
    return max(0.0, (datetime.now(start.tzinfo) - start).total_seconds())

def human_seconds(seconds: float | int | None) -> str:
    if seconds is None:
        return "?"

    seconds = max(0, int(round(float(seconds))))
    if seconds < 60:
        return f"{seconds}s"

    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m{sec:02d}s"

    hours, minutes = divmod(minutes, 60)
    if hours < 48:
        return f"{hours}h{minutes:02d}m"

    days, hours = divmod(hours, 24)
    return f"{days}d{hours:02d}h"

def _duration(row: dict[str, Any]) -> float | None:
    value = row.get("duration_seconds")
    if isinstance(value, int | float) and value >= 0:
        return float(value)
    return None

def node_stats(
    workflow,
    node_name: str,
    *,
    summary: dict[str, Any] | None = None,
    max_parallel_jobs: int | None = None,
    observation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    # Fast path: use FileStorage's per-node job index. This makes monitor
    # snapshots O(number of nodes + running jobs), not O(all job folders/status
    # files). On large cyclic autostart runs, the old monitor could itself
    # compete with the runner by rereading 10k+ status files every refresh.
    summary = summary or workflow.storage.node_job_summary(node_name)
    observation = observation or node_observation(workflow, node_name)
    counts = {status: 0 for status in STATUSES}
    counts.update(summary.get("counts") or {})

    running_jobs = []
    running_elapsed = []
    for raw_job_id, data in (summary.get("running_jobs") or {}).items():
        try:
            running_jobs.append(int(raw_job_id))
        except (TypeError, ValueError):
            continue
        if isinstance(data, dict):
            elapsed = seconds_since(data.get("started_at"))
            if elapsed is not None:
                running_elapsed.append(elapsed)

    total = int(summary.get("total") or 0)
    completed = counts.get(DONE, 0) + counts.get(SKIPPED, 0)
    remaining = counts.get(QUEUED, 0) + counts.get(RUNNING, 0)
    failed = counts.get(FAILED, 0)
    avg_duration = summary.get("avg_duration_seconds")
    max_parallel = (
        observation["requested_max_threads"]
        if max_parallel_jobs is None
        else max(1, int(max_parallel_jobs))
    )
    eta_seconds = None

    if avg_duration is not None and remaining > 0:
        slots = max(1, min(max_parallel, remaining))
        eta_seconds = remaining * avg_duration / slots

    progress = (completed / total * 100.0) if total else 0.0

    return {
        **observation,
        "node": node_name,
        "total": total,
        "queued": counts.get(QUEUED, 0),
        "running": counts.get(RUNNING, 0),
        "done": counts.get(DONE, 0),
        "failed": failed,
        "skipped": counts.get(SKIPPED, 0),
        "cancelled": counts.get(CANCELLED, 0),
        "remaining": remaining,
        "completed": completed,
        "progress_percent": round(progress, 1),
        "avg_duration_seconds": avg_duration,
        "completed_last_60_seconds": int(summary.get("completed_last_60_seconds") or 0),
        "eta_seconds": eta_seconds,
        "max_parallel_jobs": max_parallel,
        "running_jobs": sorted(running_jobs),
        "running_elapsed_seconds": running_elapsed,
    }

def workflow_snapshot(workflow, nodes: list[str] | None = None) -> dict[str, Any]:
    selected = list(nodes) if nodes is not None else list(workflow.graph_obj.nodes)
    sessions = workflow.storage.list_execution_sessions()
    summaries = workflow.storage.node_job_summaries(selected)
    observations = {
        node: node_observation(workflow, node) for node in workflow.graph_obj.nodes
    }
    api_node_names = {
        node_name
        for node_name in observations
        if node_name in workflow.nodes
        and (workflow.nodes[node_name].runner_override or workflow.runner) == "api"
    }
    api_total = workflow.api_total_limit_override()
    requested_api_limits = {
        node_name: observations[node_name]["requested_max_threads"]
        for node_name in api_node_names
    }
    active_api_nodes = {
        node_name
        for node_name in api_node_names
        if observations[node_name]["state"] == RUNNING
    }
    active_requested_capacity = sum(
        requested_api_limits[name] for name in active_api_nodes
    )
    active_capacity = (
        active_requested_capacity if api_total is None
        else min(active_requested_capacity, api_total)
    )
    node_rows = [
        node_stats(
            workflow,
            node,
            summary=summaries.get(node),
            observation=observations[node],
            max_parallel_jobs=(
                requested_api_limits[node] if node in api_node_names else None
            ),
        )
        for node in selected
    ]

    totals = {
        "nodes": len(node_rows),
        "jobs": sum(row["total"] for row in node_rows),
        "queued": sum(row["queued"] for row in node_rows),
        "running": sum(row["running"] for row in node_rows),
        "done": sum(row["done"] for row in node_rows),
        "failed": sum(row["failed"] for row in node_rows),
        "skipped": sum(row["skipped"] for row in node_rows),
        "cancelled": sum(row["cancelled"] for row in node_rows),
        "remaining": sum(row["remaining"] for row in node_rows),
    }
    totals["completed"] = totals["done"] + totals["skipped"]
    totals["progress_percent"] = (
        round(totals["completed"] / totals["jobs"] * 100.0, 1)
        if totals["jobs"]
        else 0.0
    )

    etas = [row["eta_seconds"] for row in node_rows if row["eta_seconds"] is not None]
    totals["rough_eta_seconds"] = sum(etas) if etas else None

    running_nodes = [row["node"] for row in node_rows if row["running"] > 0 or row["status"] == RUNNING]
    waiting_nodes = [row["node"] for row in node_rows if row["status"] == WAITING]
    api_rows = [row for row in node_rows if row["node"] in api_node_names]
    api_runtime = {
        "mode": "cooperative",
        "aggregate_limit": api_total,
        "requested_capacity": sum(requested_api_limits.values()),
        "active_capacity": active_capacity,
        "running": sum(row["running"] for row in api_rows),
        "queued": sum(row["queued"] for row in api_rows),
        "completed_last_60_seconds": sum(
            row.get("completed_last_60_seconds", 0) for row in api_rows
        ),
        "nodes": len(api_rows),
    }

    return {
        "generated_at": now_iso(),
        "project_dir": str(workflow.storage.project_dir),
        "runner": workflow.runner,
        "sessions": sessions,
        "running_nodes": running_nodes,
        "waiting_nodes": waiting_nodes,
        "totals": totals,
        "api_runtime": api_runtime,
        "nodes": node_rows,
    }
