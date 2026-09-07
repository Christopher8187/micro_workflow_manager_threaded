from __future__ import annotations

from pathlib import Path

from micro_workflow_manager.system import MicroWorkflow
from micro_workflow_manager.workflow.preparation import observe_programmatic_fresh_preparation
from micro_workflow_manager.workflow.selected_preparation import prepare_selected_jobs, prepare_selected_addresses
from micro_workflow_manager.workflow.selected_execution_operation import run_selected_addresses
from micro_workflow_manager.workflow.sample_admission import SampleRequest
from micro_workflow_manager.storage.session_admission import NoSelectedJobs

from .active_run import refuse_competing_run
from .run_session import active_workflow_run
from .sampling import print_sample_selection
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
        stats=stats,
        stats_interval=stats_interval,
        monitor=monitor,
        monitor_interval=monitor_interval,
        keep_trace=keep_trace,
    )


def run_sampled_jobs(
    root, workflow, node, selectors, *, seed, statuses=(), expected_population=None,
    stats=False, stats_interval=5.0, monitor=False, monitor_interval=2.0, keep_trace=False,
):
    request = SampleRequest(tuple(selectors), seed, statuses, expected_population)
    try:
        return _run_selected_jobs(
            root, workflow, node, None, command='run sample', sample_request=request,
            stats=stats, stats_interval=stats_interval, monitor=monitor,
            monitor_interval=monitor_interval, keep_trace=keep_trace,
        )
    except NoSelectedJobs:
        print('No jobs selected; no work was started.')
        return 0


def _run_selected_jobs(
    root: Path,
    workflow: MicroWorkflow,
    node: str,
    job_ids: list[int] | None,
    *,
    command: str,
    sample_request: SampleRequest | None = None,
    stats: bool = False,
    stats_interval: float = 5.0,
    monitor: bool = False,
    monitor_interval: float = 2.0,
    keep_trace: bool = False,
) -> int:
    refuse_competing_run(workflow)

    if not is_ready(workflow, node):
        print_not_ready(workflow, node)
        return 1
    preparation = observe_programmatic_fresh_preparation(workflow, [node])

    if sample_request is None:
        for job_id in job_ids:
            if not workflow.storage.job_exists(node, job_id):
                raise RuntimeError(f"Job does not exist: {node}/{job_id}")

    with active_workflow_run(
        workflow,
        queue_autostarts=True,
        command=command,
        start_node=node,
        nodes=list(workflow.component_key(workflow.component_for(node))),
        selected_jobs=job_ids,
        sample_request=sample_request,
        stats=stats,
        stats_interval=stats_interval,
        monitor=monitor,
        monitor_interval=monitor_interval,
    ) as finish_run:
        execution_context = workflow.execution_session_context
        if sample_request is not None:
            admission = finish_run.sample_admission
            addresses = admission.addresses
            print_sample_selection(admission.selection, executing=True)
            prepare_selected_addresses(
                root, workflow, addresses, selection=preparation, keep_trace=keep_trace,
            )
            jobs = [workflow.storage.load_job(member, job_id) for member, job_id in addresses]
            run_selected_addresses(workflow, jobs, execution_context, finish_run)
        else:
            if not job_ids:
                raise RuntimeError(f"No jobs selected for {node}")
            addresses = tuple((node, job_id) for job_id in job_ids)
            prepare_selected_jobs(
                root, workflow, node, job_ids, selection=preparation, keep_trace=keep_trace,
            )
            jobs = [workflow.storage.load_job(node, job_id) for job_id in job_ids]
            workflow._run_node_jobs(
                node, jobs, ignore_readiness=True, execution_context=execution_context,
                _session_driver=finish_run,
            )
        finish_run("done")

    label = "sample jobs" if command == "run sample" else "jobs"
    print(f"Ran {label} for {node}:")
    for member, job_id in addresses:
        print(f"  {member}/{job_id}")

    return 0
