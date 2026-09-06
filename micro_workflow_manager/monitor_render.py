from __future__ import annotations

import json
from typing import Any

from .monitor_metrics import human_seconds, seconds_since


def _fit(text: Any, width: int) -> str:
    value = str(text)
    if len(value) <= width:
        return value.ljust(width)
    if width <= 1:
        return value[:width]
    return (value[: width - 1] + "…").ljust(width)


def render_session_lines(sessions: list[dict[str, Any]]) -> list[str]:
    if not sessions:
        return ["execution sessions: none"]
    lines = ["Execution sessions:"]
    for session in sessions:
        components = "; ".join(
            ",".join(component) for component in session["selected_components"]
        )
        line = (
            f"  session={session['session_id']} kind={session['session_kind']} "
            f"command={session['command']} status={session['status']} "
            f"parent={session['parent_session_id'] or '-'} "
            f"components=[{components}]"
        )
        if session["status"] == "running":
            line += f" elapsed={human_seconds(seconds_since(session['started_at']))}"
        else:
            line += f" outcome={session['outcome'] or '-'} finished={session['finished_at'] or '-'}"
        if session["selected_jobs"]:
            line += " jobs=" + ",".join(f"{node}/{job}" for node, job in session["selected_jobs"])
        lines.append(line)
        for failure in session["failures"]:
            lines.append(f"    failure={failure}")
    return lines


def render_snapshot(snapshot: dict[str, Any]) -> str:
    totals = snapshot["totals"]
    lines: list[str] = []

    title = f"micro-workflow monitor | {snapshot['generated_at']} | runner={snapshot['runner']}"
    lines.append(title)
    lines.append("=" * len(title))

    lines.extend(render_session_lines(snapshot["sessions"]))

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
            f"active_capacity={api.get('active_capacity', api.get('declared_capacity', 0))} "
            f"aggregate_limit={api.get('aggregate_limit') or 'none'}"
        )
    lines.append(f"running nodes: {running_text}")
    waiting_nodes = snapshot.get("waiting_nodes") or []
    waiting_text = ", ".join(waiting_nodes) if waiting_nodes else "none"
    lines.append(f"waiting nodes: {waiting_text}")
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
    lines.append("API max_threads values are cooperative fiber counts; 'mwf threads --api-total' sets an optional run-scoped aggregate budget.")
    return "\n".join(lines)

def print_snapshot(workflow, nodes: list[str] | None = None, *, json_output: bool = False):
    # Resolve through the public facade at call time so existing tests and
    # integrations can monkeypatch ``micro_workflow_manager.monitor``.
    from . import monitor as monitor_api

    snapshot = monitor_api.workflow_snapshot(workflow, nodes=nodes)
    if json_output:
        print(json.dumps(snapshot, indent=2, ensure_ascii=False))
    else:
        print(render_snapshot(snapshot))
