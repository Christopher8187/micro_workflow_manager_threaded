from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from micro_workflow_manager.models import CANCELLED, FAILED, QUEUED, RUNNING
from micro_workflow_manager.storage import FileStorage

from .active_run import live_active_run
from .files import find_root, safe_node_name
from .jobs import selected_job_ids_from_args
from .layout import ensure_runtime_layout


RETRYABLE_FINISHED_STATUSES = {FAILED, CANCELLED}


@dataclass(slots=True)
class RestartTarget:
    node: str
    job_id: int
    mode: str
    control: dict


def _require_active_run(storage: FileStorage) -> dict:
    active = live_active_run(storage)
    if active is None:
        raise RuntimeError(
            "mwf restart is only available from a second terminal while an MWF "
            "run sequence is active. Use mwf resume or mwf resumefrom after the "
            "original sequence has ended; resume first registers output-backed "
            "finished jobs before requeueing the remaining work."
        )
    return active


def _restart_mode(
    storage: FileStorage,
    node: str,
    job_id: int,
    *,
    active: dict | None,
) -> tuple[str, dict]:
    if active is None:
        _require_active_run(storage)
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


def _active_component_nodes(active: dict, node: str) -> list[str]:
    active_nodes = set(active.get("nodes") or [])
    if node not in active_nodes:
        raise RuntimeError(
            f"Node {node} is not part of active {active.get('command', 'workflow')} "
            f"run {active.get('run_id', '?')}."
        )

    components = active.get("components")
    if isinstance(components, dict):
        stored = components.get(node)
        if isinstance(stored, list) and stored:
            result = [str(name) for name in stored if str(name) in active_nodes]
            if node in result:
                return result
    # Compatibility with run records created before component membership was
    # persisted. Such records can safely provide singleton restart semantics.
    return [node]


def _scope_targets(
    storage: FileStorage,
    active: dict,
    node: str,
    *,
    failed_only: bool,
) -> tuple[list[str], list[RestartTarget]]:
    nodes = _active_component_nodes(active, node)
    targets: list[RestartTarget] = []
    for node_name in nodes:
        for job_id in storage.list_job_ids(node_name):
            status = storage.get_job_status(node_name, job_id)
            control = storage.read_job_control(node_name, job_id)
            if not failed_only and status == RUNNING and control.get("active_execution_id"):
                targets.append(RestartTarget(node_name, job_id, "running", control))
            elif status in RETRYABLE_FINISHED_STATUSES:
                targets.append(RestartTarget(node_name, job_id, "failed", control))
    return nodes, targets


def _apply_restart_targets(
    storage: FileStorage,
    active: dict,
    targets: list[RestartTarget],
) -> list[dict]:
    restarted = []
    touched_nodes: set[str] = set()
    for target in targets:
        if target.mode == "running":
            item = storage.request_active_job_restart(
                target.node,
                target.job_id,
                requested_by_pid=os.getpid(),
                reason=(
                    "second-terminal restart inside active "
                    f"{active.get('command', 'workflow')} run {active.get('run_id', '?')}"
                ),
            )
        else:
            item = storage.request_job_restart(
                target.node,
                target.job_id,
                requested_by_pid=os.getpid(),
                reason="manual retry of failed/cancelled job",
            )
        item["mode"] = target.mode
        restarted.append(item)
        touched_nodes.add(target.node)

    if touched_nodes:
        storage.set_node_statuses({node_name: RUNNING for node_name in touched_nodes})
        storage.notify_queue_change()
    return restarted


def _print_restart_result(active: dict, restarted: list[dict]) -> None:
    print(
        f"Restarted inside active {active.get('command', 'workflow')} "
        f"run {active.get('run_id', '?')}:"
    )
    for item in restarted:
        label = "active restart" if item["mode"] == "running" else "failed-job retry"
        print(
            f"  {item['node']}/{item['job_id']} ({label}) "
            f"generation {item['previous_generation']} -> {item['generation']}"
        )
    print("The existing run remains in control; no second workflow was started.")


def restart_active_scope(
    root: Path,
    node: str,
    *,
    failed_only: bool = False,
    dry_run: bool = False,
) -> int:
    """Restart component-wide running/failed work inside the active sequence."""
    storage = FileStorage(root)
    active = _require_active_run(storage)
    nodes, targets = _scope_targets(
        storage,
        active,
        node,
        failed_only=failed_only,
    )
    scope = "failed/cancelled jobs" if failed_only else "running and failed/cancelled jobs"

    if dry_run:
        print(
            f"Restart dry run inside active {active.get('command', 'workflow')} "
            f"run {active.get('run_id', '?')}:"
        )
        print(f"  component scope: {', '.join(nodes)}")
        print(f"  selection: {scope}")
        for target in targets:
            action = (
                "replace active generation"
                if target.mode == "running"
                else "requeue failed job"
            )
            print(
                f"  would {action} {target.node}/{target.job_id} "
                f"from generation {target.control.get('generation', 0)}"
            )
        if not targets:
            print("  no matching jobs")
        print("  no execution generation, status, output, or files were changed")
        return 0

    restarted = _apply_restart_targets(storage, active, targets)
    if not restarted:
        print(
            f"No {scope} were found in active component "
            f"{{{', '.join(nodes)}}}."
        )
        return 0
    _print_restart_result(active, restarted)
    return 0


def restart_active_jobs(
    root: Path,
    node: str,
    job_ids: list[int],
    *,
    dry_run: bool = False,
) -> int:
    """Compatibility path for explicitly selected active job IDs."""
    storage = FileStorage(root)
    active = _require_active_run(storage)
    active_nodes = set(active.get("nodes") or [])

    targets: list[RestartTarget] = []
    for job_id in job_ids:
        mode, control = _restart_mode(storage, node, job_id, active=active)
        if node not in active_nodes:
            raise RuntimeError(
                f"Node {node} is not part of active {active.get('command', 'workflow')} "
                f"run {active.get('run_id', '?')}."
            )
        targets.append(RestartTarget(node, job_id, mode, control))

    if dry_run:
        print(
            f"Restart dry run inside active {active.get('command', 'workflow')} "
            f"run {active.get('run_id', '?')}:"
        )
        for target in targets:
            action = (
                "replace active generation"
                if target.mode == "running"
                else "requeue failed job"
            )
            print(
                f"  would {action} {target.node}/{target.job_id} "
                f"from generation {target.control.get('generation', 0)}"
            )
        print("  no execution generation, status, output, or files were changed")
        return 0

    restarted = _apply_restart_targets(storage, active, targets)
    _print_restart_result(active, restarted)
    return 0


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
        return 1
