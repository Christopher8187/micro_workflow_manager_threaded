from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from micro_workflow_manager.models import CANCELLED, FAILED, QUEUED, RUNNING
from micro_workflow_manager.storage import FileStorage

from .active_run import live_active_run
from .layout import ensure_runtime_layout
from .files import find_root, safe_node_name
from .jobs import selected_job_ids_from_args


RETRYABLE_FINISHED_STATUSES = {FAILED, CANCELLED}


def _restart_mode(
    storage: FileStorage,
    node: str,
    job_id: int,
    *,
    active: dict | None,
) -> tuple[str, dict]:
    if active is None:
        raise RuntimeError(
            "mwf restart is a second-terminal control and requires a live "
            "mwf run, runfrom, resume, or resumefrom sequence. After a finished "
            "partial run, use mwf resume or mwf resumefrom; those commands "
            "requeue failed jobs automatically."
        )
    if not storage.job_exists(node, job_id):
        raise RuntimeError(f"Job does not exist: {node}/{job_id}")

    status = storage.get_job_status(node, job_id)
    control = storage.read_job_control(node, job_id)
    if status == RUNNING and control.get("active_execution_id"):
        return "running", control
    if status in RETRYABLE_FINISHED_STATUSES:
        return "failed", control
    if status == QUEUED:
        raise RuntimeError(
            f"Job {node}/{job_id} is queued, not running or failed. The active "
            "scheduler already owns queued work."
        )
    raise RuntimeError(
        f"Job {node}/{job_id} has status {status!r}. Restart accepts an active "
        "running job or a failed/cancelled job; it never resets done work."
    )


def restart_active_jobs(root: Path, node: str, job_ids: list[int], *, dry_run: bool = False) -> int:
    """Restart attempts inside the one active workflow sequence."""
    storage = FileStorage(root)
    active = live_active_run(storage)
    if active is None:
        raise RuntimeError(
            "mwf restart is only available from a second terminal while an MWF "
            "run sequence is active. Use mwf resume or mwf resumefrom after the "
            "original sequence has ended; failed jobs are reset automatically."
        )
    active_nodes = set(active.get("nodes") or [])

    modes: list[tuple[int, str, dict]] = []
    for job_id in job_ids:
        mode, control = _restart_mode(storage, node, job_id, active=active)
        if node not in active_nodes:
            raise RuntimeError(
                f"Node {node} is not part of active {active.get('command', 'workflow')} "
                f"run {active.get('run_id', '?')}."
            )
        modes.append((job_id, mode, control))

    if dry_run:
        print(
            f"Restart dry run inside active {active.get('command', 'workflow')} "
            f"run {active.get('run_id', '?')}:"
        )
        for job_id, mode, control in modes:
            action = "replace active generation" if mode == "running" else "requeue failed job"
            print(
                f"  would {action} {node}/{job_id} "
                f"from generation {control.get('generation', 0)}"
            )
        print("  no execution generation, status, output, or files were changed")
        return 0

    restarted = []
    for job_id, mode, _ in modes:
        if mode == "running":
            item = storage.request_active_job_restart(
                node,
                job_id,
                requested_by_pid=os.getpid(),
                reason=(
                    "second-terminal restart inside active "
                    f"{active.get('command', 'workflow')} run {active.get('run_id', '?')}"
                ),
            )
        else:
            item = storage.request_job_restart(
                node,
                job_id,
                requested_by_pid=os.getpid(),
                reason="manual retry of failed/cancelled job",
            )
            storage.set_node_status(node, RUNNING)
        item["mode"] = mode
        restarted.append(item)

    print(
        f"Restarted inside active {active.get('command', 'workflow')} "
        f"run {active.get('run_id', '?')}:"
    )
    for item in restarted:
        label = "active restart" if item["mode"] == "running" else "failed-job retry"
        print(
            f"  {node}/{item['job_id']} ({label}) "
            f"generation {item['previous_generation']} -> {item['generation']}"
        )
    print("The existing run remains in control; no second workflow was started.")
    return 0


def restart_cli(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="mwf restart",
        description=(
            "From a second terminal, replace a running attempt or requeue a "
            "failed/cancelled job inside the active workflow sequence."
        ),
    )
    parser.add_argument("node", help="Node containing the job.")
    parser.add_argument("job_mode", metavar="job", help="Literal 'job' or 'jobs'.")
    parser.add_argument(
        "job_specs",
        nargs="+",
        metavar="id|start-end",
        help="Job IDs and ranges, for example: 1 3 8-10.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and show restart targets without changing them.",
    )

    args = parser.parse_args(argv)

    try:
        root = find_root()
        ensure_runtime_layout(root)
        node = safe_node_name(args.node)
        job_ids = selected_job_ids_from_args(
            args.job_mode,
            args.job_specs,
            command="restart",
        )
        assert job_ids is not None
        return restart_active_jobs(root, node, job_ids, dry_run=args.dry_run)
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
