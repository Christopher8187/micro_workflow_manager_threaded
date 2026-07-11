from __future__ import annotations

import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Callable
from uuid import uuid4

from micro_workflow_manager.models import QUEUED, RUNNING
from micro_workflow_manager.monitor import InlineStatsReporter, now_iso
from micro_workflow_manager.system import MicroWorkflow

from .active_run import refuse_competing_run
from .autostart_scan import autostart_closure
from .cleanup import reset_job_for_run, reset_node_for_run
from .files import read_config
from .graph_utils import (
    component_topological_nodes,
    descendants_in_order,
    direct_incomplete_inputs,
    expand_to_components,
    ready_for_run_set,
    topo_subset,
)
from .validation import ask, is_ready, print_not_ready

@contextmanager
def active_workflow_run(
    workflow: MicroWorkflow,
    *,
    command: str,
    start_node: str,
    nodes: list[str],
    selected_jobs: list[int] | None = None,
):
    run_id = f"{int(time.time())}-{os.getpid()}-{uuid4().hex[:8]}"
    data = {
        "run_id": run_id,
        "status": "running",
        "command": command,
        "start_node": start_node,
        "nodes": list(nodes),
        "selected_jobs": list(selected_jobs or []),
        "started_at": now_iso(),
        "pid": os.getpid(),
    }

    # Claim the project run slot atomically. This prevents two terminals from
    # replacing .mwf_run.json at the same time. The restart command does not
    # claim this slot; it only controls a job already owned by this run.
    with workflow.storage.interprocess_lock("active-run-state"):
        refuse_competing_run(workflow)
        workflow.storage.write_run_state(data)

    finished = False

    def finish(status: str, error: str | None = None):
        nonlocal finished
        if finished:
            return

        with workflow.storage.interprocess_lock("active-run-state"):
            current = workflow.storage.get_run_state()
            # Never let a stale process overwrite a newer run record.
            if current.get("run_id") == run_id:
                updates = {
                    "status": status,
                    "finished_at": now_iso(),
                }
                if error is not None:
                    updates["error"] = error
                workflow.storage.update_run_state(**updates)
        finished = True

    try:
        yield finish
    except Exception as error:
        finish("failed", repr(error))
        raise
    finally:
        if not finished:
            finish("done")

def run_selected_jobs(
    root: Path,
    workflow: MicroWorkflow,
    node: str,
    job_ids: list[int],
    *,
    stats: bool = False,
    stats_interval: float = 5.0,
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
        ) as finish_run:
            # The run slot is claimed before any selected-job artifacts are
            # reset, so a second run command cannot race with preparation.
            workflow.storage.set_node_status(node, RUNNING)
            for job_id in job_ids:
                reset_job_for_run(root, workflow, node, job_id, mark_queued=False)

            with InlineStatsReporter(
                workflow,
                nodes=[node],
                enabled=stats,
                interval=stats_interval,
            ):
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

def run_node(root: Path, workflow: MicroWorkflow, node: str, *, stats: bool = False, stats_interval: float = 5.0) -> int:
    refuse_competing_run(workflow)

    if not is_ready(workflow, node):
        print_not_ready(workflow, node)
        return 1

    graph_file = (root / read_config(root)["graph_path"]).resolve()
    autostart_nodes = autostart_closure(workflow, graph_file, [node])
    nodes = topo_subset(workflow, expand_to_components(workflow, {node}))

    if autostart_nodes:
        print("Detected autostarts to:", ", ".join(autostart_nodes))
        if not ask("Run all detected nodes sequentially?"):
            print("Stopped without running.")
            return 1
        nodes = topo_subset(workflow, expand_to_components(workflow, {node, *autostart_nodes}))

    blockers = (
        direct_incomplete_inputs(workflow, set(nodes))
        - workflow.component_predecessors(workflow.component_for(node))
    )
    ignore_external = False

    if blockers:
        print("Detected incomplete nodes:", ", ".join(sorted(blockers)))
        print("These nodes directly lead into the requested run set.")
        if not ask("Run anyway?"):
            print("Stopped without running.")
            return 1
        ignore_external = True

    def prepare():
        # The active run slot is claimed before this destructive preparation.
        workflow.storage.set_node_status(node, RUNNING)
        reset_node_for_run(root, workflow, node, mark_queued=False)
        for item in nodes:
            if item != node:
                reset_node_for_run(root, workflow, item, remove_parented_jobs=True)

    return run_nodes(
        workflow,
        nodes,
        node,
        ignore_external=ignore_external,
        command="run",
        stats=stats,
        stats_interval=stats_interval,
        prepare=prepare,
    )

def run_from(root: Path, workflow: MicroWorkflow, node: str, *, stats: bool = False, stats_interval: float = 5.0) -> int:
    refuse_competing_run(workflow)

    nodes = component_topological_nodes(
        workflow,
        expand_to_components(workflow, {node, *descendants_in_order(workflow, node)}),
    )
    graph_file = (root / read_config(root)["graph_path"]).resolve()
    autostart_nodes = autostart_closure(workflow, graph_file, nodes)
    extra_autostart_nodes = [item for item in autostart_nodes if item not in nodes]

    if extra_autostart_nodes:
        print("Detected autostarts outside the runfrom set:", ", ".join(extra_autostart_nodes))
        if not ask("Include these nodes and run all selected nodes sequentially?"):
            print("Stopped without running.")
            return 1
        nodes = topo_subset(
            workflow,
            expand_to_components(workflow, {node, *nodes, *extra_autostart_nodes}),
        )

    blockers = direct_incomplete_inputs(workflow, set(nodes))
    ignore_external = False

    if blockers:
        print("Detected incomplete nodes:", ", ".join(sorted(blockers)))
        print("These nodes directly lead into the runfrom node set.")
        if not ask("Run anyway?"):
            print("Stopped without running.")
            return 1
        ignore_external = True

    def prepare():
        workflow.storage.set_node_status(node, RUNNING)
        reset_node_for_run(root, workflow, node, mark_queued=False)
        for child in nodes:
            if child != node:
                reset_node_for_run(root, workflow, child, remove_parented_jobs=True)

    return run_nodes(
        workflow,
        nodes,
        node,
        ignore_external=ignore_external,
        command="runfrom",
        stats=stats,
        stats_interval=stats_interval,
        prepare=prepare,
    )

def run_nodes(
    workflow: MicroWorkflow,
    nodes: list[str],
    start_node: str,
    ignore_external: bool = False,
    *,
    command: str = "run",
    stats: bool = False,
    stats_interval: float = 5.0,
    prepare: Callable[[], None] | None = None,
) -> int:
    run_set = set(nodes)
    previous_allowed_run_nodes = workflow.allowed_run_nodes
    previous_autostart_mode = workflow.autostart_mode
    previous_restart_enabled = workflow.active_job_restart_enabled

    workflow.allowed_run_nodes = run_set
    workflow.autostart_mode = "queue"
    workflow.active_job_restart_enabled = True

    try:
        with active_workflow_run(
            workflow,
            command=command,
            start_node=start_node,
            nodes=nodes,
        ) as finish_run:
            if prepare is not None:
                prepare()

            if not workflow.storage.has_queued_jobs(start_node):
                workflow.storage.set_node_status(start_node, QUEUED)
                print(
                    f"No queued jobs for {start_node}. "
                    f"Create default jobs in node_behavior/{start_node}.py with "
                    "router.create_job(number=..., params={...})."
                )
                finish_run("done")
                return 0

            with InlineStatsReporter(
                workflow,
                nodes=nodes,
                enabled=stats,
                interval=stats_interval,
            ):
                if workflow.runner in {"threaded", "process"}:
                    ran = workflow.run_concurrently(
                        nodes=nodes,
                        ready_check=lambda item: ready_for_run_set(
                            workflow,
                            item,
                            run_set,
                            ignore_external,
                        ),
                    )
                else:
                    ran = []
                    units = workflow.execution_components(nodes)

                    while True:
                        ready_units = [
                            unit
                            for unit in units
                            if any(workflow.storage.has_queued_jobs(node) for node in unit)
                            and all(
                                ready_for_run_set(workflow, node, run_set, ignore_external)
                                for node in unit
                            )
                        ]

                        if not ready_units:
                            break

                        for unit in ready_units:
                            ran.extend(workflow.run_component(set(unit), ignore_readiness=True))

            workflow.finalize_ready_nodes()

            blocked = [node for node in nodes if workflow.storage.has_queued_jobs(node)]

            if blocked:
                finish_run("blocked")
                print("Stopped before these queued nodes became ready:")
                for node in blocked:
                    status = workflow.storage.get_node_status(node) or "missing"
                    print(f"  {node}: {status}")
                return 1

            unfinished = [node for node in nodes if not workflow.node_complete(node)]

            if unfinished:
                finish_run("incomplete")
                print("These nodes did not complete:")
                for node in unfinished:
                    status = workflow.storage.get_node_status(node) or "missing"
                    job_count = len(workflow.storage.list_jobs(node))
                    queued_count = len(workflow.storage.queued_job_ids(node))
                    print(f"  {node}: {status}, jobs={job_count}, queued={queued_count}")
                print("This usually means an upstream task did not create the expected downstream jobs.")
                return 1

            finish_run("done")
            print("Ran:")
            for node in ran:
                print(f"  {node}")

            return 0

    finally:
        workflow.allowed_run_nodes = previous_allowed_run_nodes
        workflow.autostart_mode = previous_autostart_mode
        workflow.active_job_restart_enabled = previous_restart_enabled
