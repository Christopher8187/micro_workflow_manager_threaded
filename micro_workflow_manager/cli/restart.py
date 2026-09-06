from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from micro_workflow_manager.storage import FileStorage

from .files import find_root, safe_node_name
from .jobs import selected_job_ids_from_args
from .layout import ensure_runtime_layout


def _restart_owned(root, node, *, job_ids=None, failed_only=False, dry_run=False):
    storage = FileStorage(root)
    try:
        component_plan = None
        if job_ids is None:
            component_plan = storage.plan_owned_component_restart(node, failed_only=failed_only)
            targets = component_plan['targets']
        else:
            targets = storage.plan_owned_job_restarts([(node, job_id) for job_id in job_ids])
        if dry_run:
            print('Restart dry run:')
            for target in targets:
                print(
                    f"  would restart {target['node']}/{target['job_id']} "
                    f"in session {target['owner']['session_id']} "
                    f"component {{{', '.join(target['owner']['component'])}}} "
                    f"from generation {target['generation']}"
                )
        else:
            restarted = storage.request_owned_job_restarts(
                targets, requested_by_pid=os.getpid(), component_plan=component_plan,
            )
            for item in restarted:
                print(
                    f"Restarted {item['node']}/{item['job_id']} in session {item['session_id']}: "
                    f"generation {item['previous_generation']} -> {item['generation']}"
                )
                for warning in item['warnings']:
                    print(warning, file=sys.stderr)
        if not targets and component_plan is not None:
            print(
                f"No matching jobs in session {component_plan['session_id']} "
                f"component {{{', '.join(component_plan['component'])}}}."
            )
        return 0
    finally:
        storage.close_database_connections()


def restart_active_scope(
    root: Path, node: str, *, failed_only: bool = False, dry_run: bool = False,
) -> int:
    """Restart eligible work in the node's persisted reserved component."""
    return _restart_owned(root, node, failed_only=failed_only, dry_run=dry_run)


def restart_active_jobs(
    root: Path, node: str, job_ids: list[int], *, dry_run: bool = False,
) -> int:
    """Restart explicit jobs through their exact native execution owner."""
    return _restart_owned(root, node, job_ids=job_ids, dry_run=dry_run)

def restart_cli(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="mwf restart",
        description=(
            "Restart running and failed jobs for a node's active Hoeflein "
            "component, or select explicit job IDs."
        ),
    )
    parser.add_argument("node", help="Node selecting the active component.")
    parser.add_argument(
        "mode",
        nargs="?",
        choices=("failed", "job", "jobs"),
        help="Use 'failed' for failed-only scope, or job/jobs for explicit IDs.",
    )
    parser.add_argument("job_specs", nargs="*", metavar="id|start-end")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    try:
        root = find_root()
        ensure_runtime_layout(root)
        node = safe_node_name(args.node)
        if args.mode in {"job", "jobs"}:
            job_ids = selected_job_ids_from_args(
                args.mode,
                args.job_specs,
                command="restart",
            )
            assert job_ids is not None
            return restart_active_jobs(root, node, job_ids, dry_run=args.dry_run)
        if args.job_specs:
            raise RuntimeError("Job IDs require the literal job or jobs mode.")
        return restart_active_scope(
            root,
            node,
            failed_only=args.mode == "failed",
            dry_run=args.dry_run,
        )
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        for note in getattr(error, '__notes__', ()):
            print(note, file=sys.stderr)
        return 1
