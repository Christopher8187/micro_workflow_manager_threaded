from __future__ import annotations

import json
import sys
import threading
import time
from datetime import datetime
from typing import Any

from .models import CANCELLED, DONE, FAILED, QUEUED, RUNNING, SKIPPED

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
    return max(0.0, (datetime.now() - start).total_seconds())


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


def _max_parallel_jobs(workflow, node_name: str) -> int:
    node = workflow.nodes.get(node_name)
    if node is None:
        return 1

    if node.runner_override == "direct" or workflow.runner == "direct":
        return 1

    return max(1, int(getattr(node, "max_threads", 1) or 1))


def node_stats(workflow, node_name: str) -> dict[str, Any]:
    rows = workflow.storage.list_jobs(node_name)
    counts = {status: 0 for status in STATUSES}
    running_jobs: list[int] = []
    running_elapsed: list[float] = []
    durations: list[float] = []

    for row in rows:
        status = row.get("status") or QUEUED
        counts[status] = counts.get(status, 0) + 1

        if status == RUNNING:
            job_id = row.get("job_id")
            if isinstance(job_id, int):
                running_jobs.append(job_id)
            elapsed = seconds_since(row.get("started_at"))
            if elapsed is not None:
                running_elapsed.append(elapsed)

        duration = _duration(row)
        if duration is not None and status in TERMINAL:
            durations.append(duration)

    total = len(rows)
    completed = counts.get(DONE, 0) + counts.get(SKIPPED, 0)
    remaining = counts.get(QUEUED, 0) + counts.get(RUNNING, 0)
    failed = counts.get(FAILED, 0)
    avg_duration = sum(durations) / len(durations) if durations else None
    max_parallel = _max_parallel_jobs(workflow, node_name)
    eta_seconds = None

    if avg_duration is not None and remaining > 0:
        slots = max(1, min(max_parallel, remaining))
        eta_seconds = remaining * avg_duration / slots

    progress = (completed / total * 100.0) if total else 0.0

    return {
        "node": node_name,
        "status": workflow.storage.get_node_status(node_name) or "missing",
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
        "eta_seconds": eta_seconds,
        "max_parallel_jobs": max_parallel,
        "running_jobs": sorted(running_jobs),
        "running_elapsed_seconds": running_elapsed,
    }


def workflow_snapshot(workflow, nodes: list[str] | None = None) -> dict[str, Any]:
    selected = list(nodes) if nodes is not None else list(workflow.graph_obj.nodes)
    run_state = workflow.storage.get_run_state()
    node_rows = [node_stats(workflow, node) for node in selected]

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

    return {
        "generated_at": now_iso(),
        "project_dir": str(workflow.storage.project_dir),
        "runner": workflow.runner,
        "run_state": run_state,
        "running_nodes": running_nodes,
        "totals": totals,
        "nodes": node_rows,
    }


def _fit(text: Any, width: int) -> str:
    value = str(text)
    if len(value) <= width:
        return value.ljust(width)
    if width <= 1:
        return value[:width]
    return (value[: width - 1] + "…").ljust(width)


def render_snapshot(snapshot: dict[str, Any]) -> str:
    run_state = snapshot.get("run_state") or {}
    totals = snapshot["totals"]
    lines: list[str] = []

    title = f"micro-workflow monitor | {snapshot['generated_at']} | runner={snapshot['runner']}"
    lines.append(title)
    lines.append("=" * len(title))

    if run_state:
        state_status = run_state.get("status", "unknown")
        command = run_state.get("command", "run")
        start_node = run_state.get("start_node", "?")
        elapsed = human_seconds(seconds_since(run_state.get("started_at")))
        selected = run_state.get("nodes") or []
        selected_text = ", ".join(selected) if selected else "all graph nodes"
        lines.append(
            f"active run: {command} {start_node} | status={state_status} | elapsed={elapsed} | nodes={selected_text}"
        )

    running_nodes = snapshot.get("running_nodes") or []
    running_text = ", ".join(running_nodes) if running_nodes else "none"
    eta_text = human_seconds(totals.get("rough_eta_seconds"))
    lines.append(
        "totals: "
        f"jobs={totals['jobs']} "
        f"done={totals['done']} "
        f"queued={totals['queued']} "
        f"running={totals['running']} "
        f"failed={totals['failed']} "
        f"left={totals['remaining']} "
        f"progress={totals['progress_percent']}% "
        f"rough_eta={eta_text}"
    )
    lines.append(f"running nodes: {running_text}")
    lines.append("")

    headers = [
        ("node", 18),
        ("status", 9),
        ("jobs", 6),
        ("Q", 5),
        ("R", 5),
        ("D", 5),
        ("F", 5),
        ("left", 6),
        ("avg", 7),
        ("eta", 7),
        ("running jobs", 18),
    ]
    lines.append(" ".join(_fit(name, width) for name, width in headers).rstrip())
    lines.append(" ".join("-" * width for _, width in headers).rstrip())

    for row in snapshot["nodes"]:
        running_jobs = row["running_jobs"]
        if len(running_jobs) > 5:
            running_text = ",".join(str(item) for item in running_jobs[:5]) + ",…"
        else:
            running_text = ",".join(str(item) for item in running_jobs)

        values = [
            row["node"],
            row["status"],
            row["total"],
            row["queued"],
            row["running"],
            row["done"],
            row["failed"],
            row["remaining"],
            human_seconds(row["avg_duration_seconds"]),
            human_seconds(row["eta_seconds"]),
            running_text or "-",
        ]
        lines.append(" ".join(_fit(value, width) for value, (_, width) in zip(values, headers)).rstrip())

    lines.append("")
    lines.append("ETA is a rough estimate from completed job durations; it is unknown until at least one job has finished.")
    return "\n".join(lines)


def print_snapshot(workflow, nodes: list[str] | None = None, *, json_output: bool = False):
    snapshot = workflow_snapshot(workflow, nodes=nodes)
    if json_output:
        print(json.dumps(snapshot, indent=2, ensure_ascii=False))
    else:
        print(render_snapshot(snapshot))


def monitor_loop(
    workflow,
    nodes: list[str] | None = None,
    *,
    interval: float = 2.0,
    once: bool = False,
    json_output: bool = False,
    no_clear: bool = False,
):
    while True:
        if not once and not json_output and not no_clear:
            print("\033[2J\033[H", end="")

        print_snapshot(workflow, nodes=nodes, json_output=json_output)

        if once:
            return

        try:
            time.sleep(interval)
        except KeyboardInterrupt:
            return


class InlineStatsReporter:
    def __init__(
        self,
        workflow,
        nodes: list[str] | None = None,
        *,
        enabled: bool = False,
        interval: float = 5.0,
    ):
        self.workflow = workflow
        self.nodes = nodes
        self.enabled = enabled
        self.interval = interval
        self.stop = threading.Event()
        self.thread: threading.Thread | None = None

    def __enter__(self):
        if not self.enabled:
            return self

        self.thread = threading.Thread(target=self._loop, name="mwf-stats", daemon=True)
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        if not self.enabled:
            return False

        self.stop.set()
        if self.thread is not None:
            self.thread.join(timeout=1.0)

        self._print_compact(prefix="final stats")
        return False

    def _loop(self):
        # Print immediately after the run starts, then periodically.
        while not self.stop.is_set():
            self._print_compact(prefix="stats")
            self.stop.wait(self.interval)

    def _print_compact(self, prefix: str):
        snapshot = workflow_snapshot(self.workflow, nodes=self.nodes)
        totals = snapshot["totals"]
        running_nodes = snapshot.get("running_nodes") or []
        running_text = ",".join(running_nodes) if running_nodes else "none"
        print(
            f"[{prefix}] "
            f"running_nodes={running_text} "
            f"jobs={totals['jobs']} "
            f"done={totals['done']} "
            f"queued={totals['queued']} "
            f"running={totals['running']} "
            f"failed={totals['failed']} "
            f"left={totals['remaining']} "
            f"progress={totals['progress_percent']}% "
            f"rough_eta={human_seconds(totals.get('rough_eta_seconds'))}",
            file=sys.stderr,
            flush=True,
        )
