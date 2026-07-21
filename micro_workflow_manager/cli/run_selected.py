from __future__ import annotations

from pathlib import Path

from micro_workflow_manager.models import RUNNING
from micro_workflow_manager.system import MicroWorkflow

from .active_run import refuse_competing_run
from .cleanup import reset_job_for_run
from .run_session import active_workflow_run
from .validation import is_ready, print_not_ready


def run_selected_jobs(
    root: Path,
    workflow: MicroWorkflow,
    node: str,
    job_ids: list[int],
    *,
    stats: bool = False,
    stats_interval: float = 5.0,
    monitor: bool = False,
    monitor_interval: float = 2.0,
) -> int:
    refuse_competing_run(workflow)

    if not is_ready(workflow, node):
        print_not_ready(workflow, node)
        return 1

    for job_id in job_ids:
        if not workflow.storage.job_exists(node, job_id):
            raise RuntimeError(f"Job does not exist: {node}/{job_id}")

    previous_allowed_run_nodes = workflow.allowed_run_nodes
    previous_autostart_mode = workflow.autostart_mode
    previous_restart_enabled = workflow.active_job_restart_enabled
    workflow.allowed_run_nodes = {node}
    workflow.autostart_mode = "queue"
    workflow.active_job_restart_enabled = True

    try:
        with active_workflow_run(
            workflow,
            command="run jobs",
            start_node=node,
            nodes=[node],
            selected_jobs=job_ids,
            stats=stats,
            stats_interval=stats_interval,
            monitor=monitor,
            monitor_interval=monitor_interval,
        ) as finish_run:
            # The run slot is claimed before any selected-job artifacts are
            # reset, so a second run command cannot race with preparation.
            workflow.storage.set_node_status(node, RUNNING)
            for job_id in job_ids:
                reset_job_for_run(root, workflow, node, job_id, mark_queued=False)

            jobs = [workflow.storage.load_job(node, job_id) for job_id in job_ids]
            workflow.run_node_jobs(node, jobs, ignore_readiness=True)
            finish_run("done")
    finally:
        workflow.allowed_run_nodes = previous_allowed_run_nodes
        workflow.autostart_mode = previous_autostart_mode
        workflow.active_job_restart_enabled = previous_restart_enabled

    print(f"Ran jobs for {node}:")
    for job_id in job_ids:
        print(f"  {job_id}")

    return 0
