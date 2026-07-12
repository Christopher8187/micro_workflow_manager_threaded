from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from micro_workflow_manager.storage import FileStorage

from .active_run import live_active_run
from .files import find_root, safe_node_name
from .jobs import selected_job_ids_from_args


def restart_active_jobs(root: Path, node: str, job_ids: list[int], *, dry_run: bool = False) -> int:
    """Restart running jobs without creating a second workflow scheduler.

    This command deliberately uses FileStorage directly instead of importing the
    graph and node modules. The execution-generation fence is therefore written
    as early as possible after argument parsing, before normal workflow loading
    or any output cleanup.
    """
    storage = FileStorage(root)
    active = live_active_run(storage)
    if active is None:
        raise RuntimeError(
            "No live mwf run/runfrom sequence was found. The restart command is "
            "only for replacing a currently running job from a second terminal."
        )

    active_nodes = set(active.get("nodes") or [])
    if node not in active_nodes:
        raise RuntimeError(
            f"Node {node} is not part of active {active.get('command', 'workflow')} "
            f"run {active.get('run_id', '?')}."
        )

    if dry_run:
        print(
            f"Restart dry run inside active {active.get('command', 'workflow')} "
            f"run {active.get('run_id', '?')}:"
        )
        for job_id in job_ids:
            if not storage.job_exists(node, job_id):
                raise RuntimeError(f"Job does not exist: {node}/{job_id}")
            status = storage.get_job_status(node, job_id)
            control = storage.read_job_control(node, job_id)
            if status != "running" or not control.get("active_execution_id"):
                raise RuntimeError(f"Job {node}/{job_id} is not currently running with an active execution lease")
            print(f"  would restart {node}/{job_id} generation {control.get('generation', 0)}")
        print("  no execution generation, status, output, or files were changed")
        return 0

    restarted = []
    for job_id in job_ids:
        restarted.append(
            storage.request_active_job_restart(
                node,
                job_id,
                requested_by_pid=os.getpid(),
                reason=(
                    "second-terminal restart inside active "
                    f"{active.get('command', 'workflow')} run {active.get('run_id', '?')}"
                ),
            )
        )

    print(
        f"Restarted inside active {active.get('command', 'workflow')} "
        f"run {active.get('run_id', '?')}:"
    )
    for item in restarted:
        print(
            f"  {node}/{item['job_id']} "
            f"generation {item['previous_generation']} -> {item['generation']}"
        )
    print("The existing run remains in control; no second workflow was started.")
    return 0


def restart_cli(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="mwf restart",
        description=(
            "Safely replace currently running jobs inside an existing mwf "
            "run/runfrom sequence without starting a second scheduler."
        ),
    )
    parser.add_argument("node", help="Node containing the currently running job.")
    parser.add_argument("job_mode", metavar="job", help="Literal 'job' or 'jobs'.")
    parser.add_argument(
        "job_specs",
        nargs="+",
        metavar="id|start-end",
        help="Running job IDs and ranges, for example: 1 3 8-10.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and show restart targets without fencing them.",
    )

    args = parser.parse_args(argv)

    try:
        root = find_root()
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
