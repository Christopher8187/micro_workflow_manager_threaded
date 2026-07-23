from __future__ import annotations

import json
import math
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from threading import Event
from typing import Any

from micro_workflow_manager.models import CANCELLED, DONE, FAILED, QUEUED, RUNNING, SKIPPED


TERMINAL_EVENTS = {"done", "failed", "cancelled", "skipped"}


def _epoch(value: Any) -> float | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return parsed.timestamp()


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(percentile * len(ordered)) - 1))
    return ordered[index]


def _pid_snapshot(pid: Any) -> dict[str, Any]:
    try:
        normalized = int(pid)
    except (TypeError, ValueError):
        return {"pid": None, "alive": False}
    alive = True
    try:
        os.kill(normalized, 0)
    except OSError:
        alive = False
    result: dict[str, Any] = {"pid": normalized, "alive": alive}
    status_path = Path(f"/proc/{normalized}/status")
    if alive and status_path.is_file():
        try:
            for line in status_path.read_text(encoding="utf-8", errors="replace").splitlines():
                if line.startswith("VmRSS:"):
                    result["rss_kib"] = int(line.split()[1])
                elif line.startswith("Threads:"):
                    result["threads"] = int(line.split()[1])
        except (OSError, ValueError, IndexError):
            pass
    return result


def _database_snapshot(storage) -> dict[str, Any]:
    path = storage.state_database_path()
    def size(candidate: Path) -> int:
        try:
            return candidate.stat().st_size
        except OSError:
            return 0
    return {
        "path": str(path),
        "bytes": size(path),
        "wal_bytes": size(Path(str(path) + "-wal")),
        "shm_bytes": size(Path(str(path) + "-shm")),
        "latest_event_id": storage.latest_job_event_id(),
    }


def _event_rows(storage, nodes: list[str], limit: int) -> list[dict[str, Any]]:
    if not nodes:
        return []
    placeholders = ",".join("?" for _ in nodes)
    rows = storage.db_connection().execute(
        "SELECT e.event_id, e.node_name, e.job_id, e.time, e.event, "
        "e.data_json, j.created_at "
        "FROM job_events AS e "
        "LEFT JOIN jobs AS j ON j.node_name=e.node_name AND j.job_id=e.job_id "
        f"WHERE e.node_name IN ({placeholders}) "
        "ORDER BY e.event_id DESC LIMIT ?",
        [*nodes, limit],
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        try:
            data = json.loads(row["data_json"] or "{}")
        except json.JSONDecodeError:
            data = {}
        if not isinstance(data, dict):
            data = {}
        result.append({
            "event_id": int(row["event_id"]),
            "node_name": str(row["node_name"]),
            "job_id": int(row["job_id"]),
            "time": str(row["time"]),
            "event": str(row["event"]),
            "created_at": row["created_at"],
            **data,
        })
    return result


def _writer_snapshot(storage, run_state: dict[str, Any]) -> dict[str, Any]:
    local = storage.mutation_writer_diagnostics()
    try:
        active_pid = int(run_state.get("pid"))
    except (TypeError, ValueError):
        active_pid = None
    if active_pid is None or active_pid == os.getpid():
        return {**local, "source": "local", "age_seconds": 0.0}
    persisted = storage.persisted_mutation_writer_diagnostics()
    try:
        persisted_pid = int(persisted.get("pid"))
        updated_at = float(persisted.get("updated_at"))
    except (TypeError, ValueError):
        return {**local, "source": "unavailable", "age_seconds": None}
    if persisted_pid != active_pid:
        return {**local, "source": "stale", "age_seconds": None}
    return {
        **persisted,
        "source": "active-process",
        "age_seconds": max(0.0, time.time() - updated_at),
    }


def top_snapshot(
    workflow,
    nodes: list[str],
    *,
    window_seconds: float = 5.0,
    recent_events: int = 8,
) -> dict[str, Any]:
    now_epoch = time.time()
    storage = workflow.storage
    run_state = storage.get_run_state()
    normalized = list(dict.fromkeys(nodes))
    counts: dict[str, dict[str, int]] = {node: {} for node in normalized}
    oldest_queued: dict[str, float] = {}
    oldest_running: dict[str, float] = {}

    if normalized:
        placeholders = ",".join("?" for _ in normalized)
        rows = storage.db_connection().execute(
            "SELECT node_name, status, COUNT(*) AS count, "
            "MIN(CASE WHEN status='queued' THEN created_at END) AS oldest_queued, "
            "MIN(CASE WHEN status='running' THEN active_started_at END) AS oldest_running "
            f"FROM jobs WHERE node_name IN ({placeholders}) "
            "GROUP BY node_name, status",
            normalized,
        ).fetchall()
        for row in rows:
            node = str(row["node_name"])
            counts.setdefault(node, {})[str(row["status"])] = int(row["count"])
            queued_epoch = _epoch(row["oldest_queued"])
            running_epoch = _epoch(row["oldest_running"])
            if queued_epoch is not None:
                oldest_queued[node] = min(oldest_queued.get(node, queued_epoch), queued_epoch)
            if running_epoch is not None:
                oldest_running[node] = min(oldest_running.get(node, running_epoch), running_epoch)

    # A bounded tail is enough for live rates while avoiding an O(all events)
    # scan in long-running cyclic workflows.
    event_limit = max(2000, min(50000, recent_events * 1000))
    events = _event_rows(storage, normalized, event_limit)
    window_start = now_epoch - window_seconds
    recent_window = [event for event in events if (_epoch(event.get("time")) or 0) >= window_start]

    metrics: dict[str, dict[str, Any]] = {
        node: {
            "started": 0,
            "finished": 0,
            "failed_recent": 0,
            "queue_seconds": [],
            "duration_seconds": [],
            "terminal_lag_seconds": [],
        }
        for node in normalized
    }
    for event in recent_window:
        node = event["node_name"]
        item = metrics.setdefault(node, {
            "started": 0,
            "finished": 0,
            "failed_recent": 0,
            "queue_seconds": [],
            "duration_seconds": [],
            "terminal_lag_seconds": [],
        })
        name = event["event"]
        if name == "started":
            item["started"] += 1
            started = _epoch(event.get("started_at")) or _epoch(event.get("time"))
            created = _epoch(event.get("created_at"))
            if started is not None and created is not None and started >= created:
                item["queue_seconds"].append(started - created)
        if name in TERMINAL_EVENTS:
            item["finished"] += 1
            if name == "failed":
                item["failed_recent"] += 1
            duration = event.get("duration_seconds")
            if isinstance(duration, (int, float)) and not isinstance(duration, bool):
                item["duration_seconds"].append(float(duration))
            published = _epoch(event.get("time"))
            finished = _epoch(event.get("finished_at"))
            if published is not None and finished is not None and published >= finished:
                item["terminal_lag_seconds"].append(published - finished)

    node_rows = []
    all_terminal_lags: list[float] = []
    for node in normalized:
        mounted = workflow.nodes[node]
        node_counts = counts.get(node, {})
        item = metrics[node]
        all_terminal_lags.extend(item["terminal_lag_seconds"])
        runner = mounted.runner_override or workflow.runner
        row = {
            "node": node,
            "node_status": storage.get_node_status(node) or "missing",
            "runner": runner,
            "declared_limit": mounted.max_threads,
            "effective_limit": workflow.effective_max_threads(node),
            "queued": node_counts.get(QUEUED, 0),
            "running": node_counts.get(RUNNING, 0),
            "done": node_counts.get(DONE, 0),
            "failed": node_counts.get(FAILED, 0),
            "cancelled": node_counts.get(CANCELLED, 0),
            "skipped": node_counts.get(SKIPPED, 0),
            "starts_per_second": item["started"] / window_seconds,
            "finishes_per_second": item["finished"] / window_seconds,
            "recent_failures": item["failed_recent"],
            "queue_wait_p95_seconds": _percentile(item["queue_seconds"], 0.95),
            "duration_p95_seconds": _percentile(item["duration_seconds"], 0.95),
            "terminal_lag_p95_seconds": _percentile(item["terminal_lag_seconds"], 0.95),
            "oldest_queued_seconds": (
                max(0.0, now_epoch - oldest_queued[node]) if node in oldest_queued else None
            ),
            "oldest_running_seconds": (
                max(0.0, now_epoch - oldest_running[node]) if node in oldest_running else None
            ),
        }
        node_rows.append(row)

    totals = {
        status: sum(row[status] for row in node_rows)
        for status in (QUEUED, RUNNING, DONE, FAILED, CANCELLED, SKIPPED)
    }
    last_event_time = _epoch(events[0]["time"]) if events else None
    return {
        "generated_at": datetime.now().astimezone().isoformat(timespec="milliseconds"),
        "window_seconds": window_seconds,
        "event_driven": True,
        "active_run": run_state,
        "process": _pid_snapshot(run_state.get("pid")),
        "database": _database_snapshot(storage),
        "mutation_writer": _writer_snapshot(storage, run_state),
        "totals": totals,
        "event_rate_per_second": len(recent_window) / window_seconds,
        "last_event_age_seconds": (
            max(0.0, now_epoch - last_event_time) if last_event_time is not None else None
        ),
        "terminal_lag_p95_seconds": _percentile(all_terminal_lags, 0.95),
        "terminal_lag_max_seconds": max(all_terminal_lags) if all_terminal_lags else None,
        "nodes": node_rows,
        "recent_events": list(reversed(events[:recent_events])),
    }


def _seconds(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "-"
    if value < 1:
        return f"{value * 1000:.0f}ms"
    return f"{value:.1f}s"


def _bytes(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "-"
    units = ("B", "KiB", "MiB", "GiB")
    amount = float(value)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{amount:.1f}{unit}"
        amount /= 1024
    return f"{amount:.1f}GiB"


def render_top(snapshot: dict[str, Any]) -> str:
    width = shutil.get_terminal_size((140, 40)).columns
    run = snapshot["active_run"]
    process = snapshot["process"]
    totals = snapshot["totals"]
    db = snapshot["database"]
    writer = snapshot["mutation_writer"]
    lines = [
        f"mwf top | {snapshot['generated_at']} | event cursor={db['latest_event_id']} "
        f"rate={snapshot['event_rate_per_second']:.1f}/s last={_seconds(snapshot['last_event_age_seconds'])}",
        (
            f"run={run.get('status', 'none')} command={run.get('command', '-')} "
            f"pid={process.get('pid') or '-'} alive={process.get('alive', False)} "
            f"rss={_bytes((process.get('rss_kib') or 0) * 1024)} "
            f"threads={process.get('threads', '-')} strategy={run.get('api_startup_strategy', 'balanced')} "
            f"windows={run.get('api_startup_windows', 'auto:1-2')} burst={run.get('api_max_admission_burst', '512')} "
            f"rounds={run.get('api_admission_target_rounds', '4')} claim-tx={run.get('api_claim_transaction_rows', '192')}"
        ),
        (
            f"jobs queued={totals['queued']} running={totals['running']} done={totals['done']} "
            f"failed={totals['failed']} cancelled={totals['cancelled']} skipped={totals['skipped']} | "
            f"terminal p95={_seconds(snapshot['terminal_lag_p95_seconds'])} "
            f"max={_seconds(snapshot['terminal_lag_max_seconds'])} | "
            f"db={_bytes(db['bytes'])} wal={_bytes(db['wal_bytes'])}"
        ),
        (
            f"writer source={writer.get('source', '?')} age={_seconds(writer.get('age_seconds'))} "
            f"queued={writer.get('queued', 0)} urgent={writer.get('urgent', 0)} "
            f"durability-backlog={writer.get('durability_backlog', 0)} "
            f"active=p{writer.get('active_priority') if writer.get('active_priority') is not None else '-'}x{writer.get('active_batch_size', 0)} "
            f"last={_seconds(writer.get('last_batch_seconds'))}"
        ),
        "",
    ]
    header = (
        f"{'NODE':<24} {'STATE':<9} {'RUNNER':<8} {'LIMIT':>7} {'QUEUE':>7} "
        f"{'RUN':>6} {'DONE':>7} {'FAIL':>6} {'START/s':>8} {'FIN/s':>8} "
        f"{'Q95':>7} {'TERM95':>7} {'OLDEST-Q':>9}"
    )
    lines.append(header[:width])
    lines.append(("-" * min(width, len(header))))
    for row in snapshot["nodes"]:
        line = (
            f"{row['node']:<24.24} {row['node_status']:<9.9} {row['runner']:<8.8} "
            f"{row['effective_limit']:>7} {row['queued']:>7} {row['running']:>6} "
            f"{row['done']:>7} {row['failed']:>6} {row['starts_per_second']:>8.1f} "
            f"{row['finishes_per_second']:>8.1f} {_seconds(row['queue_wait_p95_seconds']):>7} "
            f"{_seconds(row['terminal_lag_p95_seconds']):>7} {_seconds(row['oldest_queued_seconds']):>9}"
        )
        lines.append(line[:width])
    lines.extend(["", "Recent lifecycle events:"])
    for event in snapshot["recent_events"]:
        details = ""
        if event.get("duration_seconds") is not None:
            details = f" duration={_seconds(event['duration_seconds'])}"
        lines.append(
            f"  {event['event_id']:>8} {event['time']} {event['node_name']}/{event['job_id']} "
            f"{event['event']}{details}"[:width]
        )
    lines.append("q/Ctrl-C: exit | event-driven durable job_events cursor; interval is only redraw/fallback cadence")
    return "\n".join(lines)


def top_command(
    workflow,
    nodes: list[str],
    *,
    interval: float = 0.5,
    once: bool = False,
    json_output: bool = False,
    no_clear: bool = False,
    window_seconds: float = 5.0,
    recent_events: int = 8,
) -> int:
    wake = Event()
    unsubscribe = workflow.storage.subscribe_state_changes(
        wake.set,
        local=False,
        cross_process=not once,
    )
    try:
        last_render = 0.0
        while True:
            snapshot = top_snapshot(
                workflow,
                nodes,
                window_seconds=window_seconds,
                recent_events=recent_events,
            )
            text = json.dumps(snapshot, ensure_ascii=False, default=str) if json_output else render_top(snapshot)
            if not once and not no_clear and not json_output and sys.stdout.isatty():
                print("\x1b[2J\x1b[H", end="")
            print(text, flush=True)
            if once:
                return 0
            last_render = time.monotonic()
            wake.clear()
            wake.wait(interval)
            # Coalesce terminal waves so htop-style rendering remains usable.
            remaining = 0.1 - (time.monotonic() - last_render)
            if remaining > 0:
                time.sleep(remaining)
    except KeyboardInterrupt:
        return 0
    finally:
        unsubscribe()
