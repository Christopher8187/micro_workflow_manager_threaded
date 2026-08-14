from __future__ import annotations

import socket
from datetime import datetime
from typing import Any

from micro_workflow_manager.processes import process_identity, process_is_alive


DEFAULT_HEARTBEAT_STALE_SECONDS = 15.0


def _storage(value):
    return getattr(value, "storage", value)


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def run_state_liveness(
    state: dict[str, Any],
    *,
    stale_after_seconds: float = DEFAULT_HEARTBEAT_STALE_SECONDS,
) -> dict[str, Any]:
    if not isinstance(state, dict) or state.get("status") != "running":
        return {"live": False, "reason": "no running sequence is recorded"}

    recorded_host = state.get("hostname")
    current_host = socket.gethostname()
    same_host = recorded_host in {None, "", current_host}
    pid = state.get("pid")
    pid_live = process_is_alive(pid) if same_host else None
    recorded_identity = state.get("process_identity")
    current_identity = (
        process_identity(pid)
        if same_host and pid_live and isinstance(recorded_identity, str)
        else None
    )
    identity_matches = (
        current_identity == recorded_identity
        if isinstance(recorded_identity, str)
        else None
    )

    heartbeat = _parse_time(state.get("heartbeat_at") or state.get("started_at"))
    heartbeat_age = None
    heartbeat_fresh = False
    if heartbeat is not None:
        now = datetime.now(heartbeat.tzinfo) if heartbeat.tzinfo is not None else datetime.now()
        heartbeat_age = max(0.0, (now - heartbeat).total_seconds())
        heartbeat_fresh = heartbeat_age <= stale_after_seconds

    if same_host and pid_live and identity_matches is True:
        return {
            "live": True,
            "reason": "the recorded process instance is alive",
            "same_host": True,
            "pid_live": True,
            "process_identity_matches": True,
            "heartbeat_age_seconds": heartbeat_age,
        }
    if (
        same_host
        and pid_live
        and recorded_identity is None
        and (heartbeat is None or heartbeat_fresh)
    ):
        return {
            "live": True,
            "reason": "the legacy run PID is alive and its heartbeat is not stale",
            "same_host": True,
            "pid_live": True,
            "process_identity_matches": None,
            "heartbeat_age_seconds": heartbeat_age,
        }
    if not same_host and heartbeat_fresh:
        return {
            "live": True,
            "reason": "the run belongs to another host and its heartbeat is fresh",
            "same_host": False,
            "pid_live": None,
            "heartbeat_age_seconds": heartbeat_age,
        }
    if same_host and pid_live and identity_matches is False:
        reason = "the recorded PID belongs to a different process instance"
    elif same_host and pid_live and recorded_identity is None and not heartbeat_fresh:
        reason = "the legacy run heartbeat is stale; PID existence alone is ambiguous"
    elif same_host and pid_live is False:
        reason = "the recorded process is no longer alive"
    elif not same_host:
        reason = "the other host heartbeat is stale"
    else:
        reason = "the run heartbeat is stale or missing"
    return {
        "live": False,
        "reason": reason,
        "same_host": same_host,
        "pid_live": pid_live,
        "process_identity_matches": identity_matches,
        "heartbeat_age_seconds": heartbeat_age,
    }


def live_active_run(storage_or_workflow) -> dict[str, Any] | None:
    storage = _storage(storage_or_workflow)
    state = storage.get_run_state()
    return state if run_state_liveness(state)["live"] else None


def refuse_competing_run(storage_or_workflow):
    active = live_active_run(storage_or_workflow)
    if active is None:
        return
    command = active.get("command", "workflow")
    run_id = active.get("run_id", "?")
    pid = active.get("pid", "?")
    raise RuntimeError(
        f"A {command} sequence is already active (run {run_id}, process {pid}). "
        "Do not start a competing run from a second terminal. To restart the "
        "running and failed work in one active component, use: mwf restart <node>. "
        "The explicit mwf restart <node> job <id> form is also available."
    )
