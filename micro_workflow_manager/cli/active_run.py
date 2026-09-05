from __future__ import annotations

from pathlib import Path
from typing import Any

from micro_workflow_manager.processes import process_identity, process_is_alive
from micro_workflow_manager.legacy_runs import read_legacy_run_records


from micro_workflow_manager.session_liveness import (
    DEFAULT_HEARTBEAT_STALE_SECONDS, execution_session_liveness,
)


def _storage(value):
    return getattr(value, "storage", value)


def run_state_liveness(
    state: dict[str, Any],
    *,
    stale_after_seconds: float = DEFAULT_HEARTBEAT_STALE_SECONDS,
) -> dict[str, Any]:
    return execution_session_liveness(
        state, stale_after_seconds=stale_after_seconds,
        pid_probe=process_is_alive, identity_probe=process_identity,
    )


def live_active_run(storage_or_workflow) -> dict[str, Any] | None:
    storage = _storage(storage_or_workflow)
    state = storage.get_run_state()
    return state if run_state_liveness(state)["live"] else None


def refuse_live_legacy_migration(root: Path) -> None:
    """Check both legacy run locations before layout or database writes."""
    records = read_legacy_run_records(root)
    if len(records) > 1:
        names = ", ".join(
            f"{path.relative_to(root).as_posix()} (run {state.get('run_id', '?')})"
            for path, state in records
        )
        raise RuntimeError(f"Cannot migrate two legacy run records: {names}. Both records were preserved.")
    for path, state in records:
        if run_state_liveness(state)["live"]:
            raise RuntimeError(
                f"Cannot perform migration while legacy run {state.get('run_id', '?')} "
                f"is alive ({path.relative_to(root).as_posix()}). "
                "Wait for that run to finish or become stale before migrating."
            )


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
