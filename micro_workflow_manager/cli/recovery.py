from __future__ import annotations

import socket
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from micro_workflow_manager.models import QUEUED, RUNNING
from micro_workflow_manager.storage import FileStorage

from .active_run import process_is_alive, run_state_liveness


def recover_stale_jobs(
    root: Path,
    workflow=None,
    *,
    quiet: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Recover jobs left running by a dead CLI sequence.

    This never touches done, skipped, failed, cancelled, or already queued jobs.
    Each recovered running job receives a new execution generation before being
    requeued, fencing any late process that might still hold stale state.
    """
    storage = workflow.storage if workflow is not None else FileStorage(root)
    state = storage.get_run_state()
    liveness = run_state_liveness(state)

    if liveness["live"]:
        raise RuntimeError(
            f"The recorded {state.get('command', 'workflow')} sequence is still active "
            f"(process {state.get('pid', '?')}). Recovery would compete with it."
        )

    candidate_nodes: list[str] = []
    if isinstance(state.get("nodes"), list):
        candidate_nodes.extend(str(item) for item in state["nodes"] if isinstance(item, str))
    if workflow is not None:
        candidate_nodes.extend(str(item) for item in workflow.graph_obj.nodes)

    node_root = root / "node"
    if node_root.exists():
        candidate_nodes.extend(path.name for path in node_root.iterdir() if path.is_dir())

    recovered: list[dict[str, Any]] = []
    for node in sorted(set(candidate_nodes)):
        jobs_root = node_root / node / "jobs"
        if not jobs_root.is_dir():
            continue
        for path in sorted(jobs_root.iterdir(), key=lambda item: int(item.name) if item.name.isdigit() else 10**18):
            if not path.is_dir() or not path.name.isdigit():
                continue
            job_id = int(path.name)
            if storage.get_job_status(node, job_id) != RUNNING:
                continue

            status_data = storage.read_json(storage.status_file(node, job_id), default={})
            pid = status_data.get("pid") if isinstance(status_data, dict) else None
            control = storage.read_job_control(node, job_id)
            active_pid = control.get("active_pid") or pid
            if type(active_pid) is int and process_is_alive(active_pid):
                # A job may belong to a programmatic run without .mwf_run.json.
                # Do not recover a demonstrably live owner.
                continue

            if dry_run:
                control = storage.read_job_control(node, job_id)
                recovered.append({
                    "node": node,
                    "job_id": job_id,
                    "previous_generation": int(control.get("generation", 0)),
                    "generation": int(control.get("generation", 0)) + 1,
                })
            else:
                recovered.append(
                    storage.request_job_restart(
                        node,
                        job_id,
                        reason="recover stale running job",
                    )
                )
                storage.set_node_status(node, QUEUED)

    if not dry_run and state.get("status") == "running" and not liveness["live"]:
        storage.update_run_state(
            status="recovered",
            recovered_at=datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
            recovered_by_host=socket.gethostname(),
            recovered_jobs=[f"{item['node']}/{item['job_id']}" for item in recovered],
            recovery_reason=liveness["reason"],
        )

    result = {
        "recovered": recovered,
        "previous_run": state,
        "liveness": liveness,
    }
    if not quiet:
        if recovered:
            print("Would recover running jobs:" if dry_run else "Recovered running jobs:")
            for item in recovered:
                print(f"  {item['node']}/{item['job_id']} -> generation {item['generation']}")
        else:
            print("No stale running jobs needed recovery.")
        if state.get("status") == "running":
            print(f"Recorded run state: {liveness['reason']}")
    return result


def recover_command(root: Path, workflow, *, dry_run: bool = False) -> int:
    recover_stale_jobs(root, workflow, dry_run=dry_run)
    return 0
