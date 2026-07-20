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
    return workflow.effective_max_threads(node_name)


def node_stats(workflow, node_name: str) -> dict[str, Any]:
    # Fast path: use FileStorage's per-node job index. This makes monitor
    # snapshots O(number of nodes + running jobs), not O(all job folders/status
    # files). On large cyclic autostart runs, the old monitor could itself
    # compete with the runner by rereading 10k+ status files every refresh.
    summary = workflow.storage.node_job_summary(node_name)
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
    max_parallel = _max_parallel_jobs(workflow, node_name)
    eta_seconds = None

    if avg_duration is not None and remaining > 0:
        slots = max(1, min(max_parallel, remaining))
        eta_seconds = remaining * avg_duration / slots

    progress = (completed / total * 100.0) if total else 0.0

    stored_status = workflow.storage.get_node_status(node_name) or "missing"
    # Node-state files describe component lifecycle and can briefly be broader
    # than the work actually executing in this node. For monitor display, job
    # counts are the source of truth: queued work must not look running merely
    # because a sibling pump is active, and a real running job must not be shown
    # queued because of a concurrent component refresh.
    if failed > 0 or stored_status == FAILED:
        display_status = FAILED
    elif counts.get(RUNNING, 0) > 0:
        display_status = RUNNING
    elif counts.get(QUEUED, 0) > 0:
        display_status = QUEUED
    elif total > 0 and counts.get(CANCELLED, 0) > 0:
        display_status = CANCELLED
    elif total > 0 and completed == total:
        display_status = DONE
    elif total == 0 and stored_status == RUNNING:
        display_status = QUEUED
    else:
        display_status = stored_status

    return {
        "node": node_name,
        "status": display_status,
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
        "declared_max_threads": getattr(workflow.nodes.get(node_name), "max_threads", 1),
        "thread_override": workflow.thread_override(node_name),
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
    api_node_names = {
        node_name
        for node_name in selected
        if node_name in workflow.nodes
        and (workflow.nodes[node_name].runner_override or workflow.runner) == "api"
    }
    api_rows = [row for row in node_rows if row["node"] in api_node_names]
    api_runtime = {
        "mode": "cooperative",
        "aggregate_limit": None,
        "declared_capacity": sum(int(row["max_parallel_jobs"]) for row in api_rows),
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
        "run_state": run_state,
        "running_nodes": running_nodes,
        "totals": totals,
        "api_runtime": api_runtime,
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

    if run_state.get("status") == "running":
        command = run_state.get("command", "run")
        start_node = run_state.get("start_node", "?")
        elapsed = human_seconds(seconds_since(run_state.get("started_at")))
        selected = run_state.get("nodes") or []
        selected_text = ", ".join(selected) if selected else "all graph nodes"
        lines.append(
            f"active run: {command} {start_node} | status=running | elapsed={elapsed} | nodes={selected_text}"
        )
    else:
        lines.append("active run: none")
        if run_state:
            command = run_state.get("command", "run")
            start_node = run_state.get("start_node", "?")
            state_status = run_state.get("status", "unknown")
            finished_at = run_state.get("finished_at") or "?"
            lines.append(
                f"last run: {command} {start_node} | status={state_status} | finished={finished_at}"
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
    api = snapshot.get("api_runtime") or {}
    if api.get("nodes"):
        lines.append(
            "api fibers: "
            f"running={api.get('running', 0)} "
            f"queued={api.get('queued', 0)} "
            f"done_60s={api.get('completed_last_60_seconds', 0)} "
            f"declared_capacity={api.get('declared_capacity', 0)} "
            "aggregate_limit=none"
        )
    lines.append(f"running nodes: {running_text}")
    lines.append("")

    headers = [
        ("node", 18),
        ("status", 9),
        ("threads", 8),
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
            (
                f"{row['max_parallel_jobs']}*"
                if row.get("thread_override") is not None
                else str(row["max_parallel_jobs"])
            ),
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
    lines.append("threads marked with * use a runtime override from 'mwf threads'.")
    lines.append("API max_threads values are cooperative fiber counts; there is no workflow-wide aggregate API cap.")
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


class _InlineReporterBase:
    def __init__(
        self,
        workflow,
        nodes: list[str] | None = None,
        *,
        enabled: bool = False,
        interval: float = 5.0,
        thread_name: str,
    ):
        self.workflow = workflow
        self.nodes = nodes
        self.enabled = enabled
        self.interval = interval
        self.thread_name = thread_name
        self.stop = threading.Event()
        self.thread: threading.Thread | None = None

    def start(self):
        if not self.enabled or self.thread is not None:
            return self
        self.thread = threading.Thread(target=self._loop, name=self.thread_name, daemon=True)
        self.thread.start()
        return self

    def stop_periodic(self):
        if not self.enabled:
            return
        self.stop.set()
        if self.thread is not None:
            self.thread.join(timeout=max(1.0, min(5.0, self.interval + 0.5)))
            self.thread = None

    def print_final(self):
        if self.enabled:
            self._safe_print(final=True)

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, tb):
        self.stop_periodic()
        self.print_final()
        return False

    def _loop(self):
        # Print immediately after the active run is claimed, then periodically.
        try:
            while not self.stop.is_set():
                self._safe_print(final=False)
                self.stop.wait(self.interval)
        finally:
            storage = getattr(self.workflow, "storage", None)
            close = getattr(storage, "close_thread_connection", None)
            if close is not None:
                close()

    def _safe_print(self, *, final: bool):
        try:
            self._print(final=final)
        except Exception as error:
            # Diagnostics must never change workflow correctness. Report a
            # snapshot failure and allow the scheduler/run finalization to
            # continue; a later interval may succeed.
            print(
                f"[{self.thread_name} error] snapshot unavailable: {error!r}",
                file=sys.stderr,
                flush=True,
            )

    def _print(self, *, final: bool):
        raise NotImplementedError


class InlineStatsReporter(_InlineReporterBase):
    def __init__(
        self,
        workflow,
        nodes: list[str] | None = None,
        *,
        enabled: bool = False,
        interval: float = 5.0,
    ):
        super().__init__(
            workflow,
            nodes,
            enabled=enabled,
            interval=interval,
            thread_name="mwf-stats",
        )

    def _print(self, *, final: bool):
        snapshot = workflow_snapshot(self.workflow, nodes=self.nodes)
        totals = snapshot["totals"]
        running_nodes = snapshot.get("running_nodes") or []
        running_text = ",".join(running_nodes) if running_nodes else "none"
        prefix = "final stats" if final else "stats"
        print(
            f"[{prefix}] "
            f"time={snapshot['generated_at']} "
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


class InlineMonitorReporter(_InlineReporterBase):
    """Print timestamped full monitor snapshots beside an execution command.

    Inline monitoring never clears the terminal. Task output remains visible and
    every snapshot is retained as a chronological diagnostic record.
    """

    def __init__(
        self,
        workflow,
        nodes: list[str] | None = None,
        *,
        enabled: bool = False,
        interval: float = 2.0,
    ):
        super().__init__(
            workflow,
            nodes,
            enabled=enabled,
            interval=interval,
            thread_name="mwf-inline-monitor",
        )

    def _print(self, *, final: bool):
        snapshot = workflow_snapshot(self.workflow, nodes=self.nodes)
        label = "final monitor" if final else "monitor"
        print(f"\n--- mwf {label} snapshot ---", file=sys.stderr, flush=True)
        print(render_snapshot(snapshot), file=sys.stderr, flush=True)
