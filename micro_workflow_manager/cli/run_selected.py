from __future__ import annotations

from pathlib import Path
from typing import Callable

from micro_workflow_manager.models import RUNNING
from micro_workflow_manager.system import MicroWorkflow

from .active_run import refuse_competing_run
from .cleanup import reset_job_for_run
from .run_session import active_workflow_run
from .sampling import plan_sample, print_sample_plan
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
    keep_trace: bool = False,
) -> int:
    return _run_selected_jobs(
        root,
        workflow,
        node,
        list(job_ids),
        command="run jobs",
        check_readiness=True,
        stats=stats,
        stats_interval=stats_interval,
        monitor=monitor,
        monitor_interval=monitor_interval,
        keep_trace=keep_trace,
    )


def run_sampled_jobs(
    root: Path,
    workflow: MicroWorkflow,
    node: str,
    count: int,
    *,
    seed: str,
    statuses: tuple[str, ...] = (),
    expected_population: str | None = None,
    stats: bool = False,
    stats_interval: float = 5.0,
    monitor: bool = False,
    monitor_interval: float = 2.0,
    keep_trace: bool = False,
) -> int:
    selected_job_ids: list[int] = []

    def build_selection() -> dict:
        plan = plan_sample(
            workflow,
            node,
            count,
            seed=seed,
            statuses=statuses,
            expected_population=expected_population,
        )
        selected_job_ids.extend(plan.selected_job_ids)
        print_sample_plan(plan, executing=True)
        return plan.manifest()

    component = workflow.component_key(workflow.component_for(node))
    if len(component) > 1:
        print(
            f"Notice: {node} belongs to Hoeflein component "
            f"{{{', '.join(component)}}}. This is an isolated {node} sample; "
            "component circulation and downstream execution are disabled."
        )
    else:
        print(f"Notice: this is an isolated {node} sample; descendants are disabled.")

    return _run_selected_jobs(
        root,
        workflow,
        node,
        selected_job_ids,
        command="run sample",
        check_readiness=False,
        selection_builder=build_selection,
        stats=stats,
        stats_interval=stats_interval,
        monitor=monitor,
        monitor_interval=monitor_interval,
        keep_trace=keep_trace,
    )


def _run_selected_jobs(
    root: Path,
    workflow: MicroWorkflow,
    node: str,
    job_ids: list[int],
    *,
    command: str,
    check_readiness: bool,
    selection_builder: Callable[[], dict] | None = None,
    stats: bool = False,
    stats_interval: float = 5.0,
    monitor: bool = False,
    monitor_interval: float = 2.0,
    keep_trace: bool = False,
) -> int:
    refuse_competing_run(workflow)

    if check_readiness and not is_ready(workflow, node):
        print_not_ready(workflow, node)
        return 1

    # Preserve the established explicit-ID behavior: reject a typo before
    # publishing an active-run receipt. Sample IDs do not exist until their
    # population is selected under the active-run lock, so they are validated
    # again inside the run body instead.
    if selection_builder is None:
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
            command=command,
            start_node=node,
            nodes=[node],
            selected_jobs=job_ids,
            selection_builder=selection_builder,
            stats=stats,
            stats_interval=stats_interval,
            monitor=monitor,
            monitor_interval=monitor_interval,
        ) as finish_run:
            # The run slot is claimed before any selected-job artifacts are
            # reset, so a second run command cannot race with preparation.
            if not job_ids:
                raise RuntimeError(f"No jobs selected for {node}")
            for job_id in job_ids:
                if not workflow.storage.job_exists(node, job_id):
                    raise RuntimeError(f"Job does not exist: {node}/{job_id}")
            workflow.storage.set_node_status(node, RUNNING)
            for job_id in job_ids:
                reset_job_for_run(
                    root,
                    workflow,
                    node,
                    job_id,
                    mark_queued=False,
                    keep_trace=keep_trace,
                )

            jobs = [workflow.storage.load_job(node, job_id) for job_id in job_ids]
            workflow.run_node_jobs(node, jobs, ignore_readiness=True)
            finish_run("done")
    finally:
        workflow.allowed_run_nodes = previous_allowed_run_nodes
        workflow.autostart_mode = previous_autostart_mode
        workflow.active_job_restart_enabled = previous_restart_enabled

    label = "sample jobs" if command == "run sample" else "jobs"
    print(f"Ran {label} for {node}:")
    for job_id in job_ids:
        print(f"  {job_id}")

    return 0
