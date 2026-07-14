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
    if not storage.job_exists(node, job_id):
        if active is None:
            raise RuntimeError(
                "No live mwf run/runfrom sequence was found, and the selected "
                f"job does not exist: {node}/{job_id}."
            )
        raise RuntimeError(f"Job does not exist: {node}/{job_id}")

    status = storage.get_job_status(node, job_id)
    control = storage.read_job_control(node, job_id)
    if status == RUNNING and control.get("active_execution_id"):
        return "running", control
    if status in RETRYABLE_FINISHED_STATUSES:
        return "failed", control
    if status == QUEUED:
        if active is None:
            raise RuntimeError(
                "No live mwf run/runfrom sequence was found. The selected job "
                f"{node}/{job_id} is queued rather than failed."
            )
        raise RuntimeError(
            f"Job {node}/{job_id} is not currently running; its status is queued."
        )
    raise RuntimeError(
        f"Job {node}/{job_id} has status {status!r}. Restart accepts an active "
        "running job or a failed/cancelled job; it never resets done work."
    )


def restart_active_jobs(root: Path, node: str, job_ids: list[int], *, dry_run: bool = False) -> int:
    """Restart live attempts or requeue failed jobs without resetting done jobs."""
    storage = FileStorage(root)
    active = live_active_run(storage)
    active_nodes = set((active or {}).get("nodes") or [])

    modes: list[tuple[int, str, dict]] = []
    for job_id in job_ids:
        mode, control = _restart_mode(storage, node, job_id, active=active)
        if mode == "running" and active is None:
            raise RuntimeError(
                f"Job {node}/{job_id} is marked running, but no live MWF sequence owns it. "
                "Use 'mwf recover' after confirming the previous process is gone."
            )
        if active is not None and node not in active_nodes:
            raise RuntimeError(
                f"Node {node} is not part of active {active.get('command', 'workflow')} "
                f"run {active.get('run_id', '?')}."
            )
        modes.append((job_id, mode, control))

    if dry_run:
        if active is None:
            print("Restart dry run with no active workflow:")
        else:
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
            storage.set_node_status(node, RUNNING if active is not None else QUEUED)
        item["mode"] = mode
        restarted.append(item)

    if active is None:
        print("Requeued failed jobs:")
    else:
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
    if active is None:
        print(f"Run 'mwf resume {node}' (or the appropriate 'mwf resumefrom <node-name>') to execute the queued retry.")
    else:
        print("The existing run remains in control; no second workflow was started.")
    return 0


def restart_cli(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="mwf restart",
        description=(
            "Replace an active running attempt or requeue a failed/cancelled job "
            "without resetting completed jobs."
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
