from __future__ import annotations

from pathlib import Path

from .autostart_scan import autostart_closure
from .files import read_config
from .graph_utils import (
    component_topological_nodes,
    descendants_in_order,
    direct_incomplete_inputs,
    expand_to_components,
    topo_subset,
)
from .project import resolve_configured_graph_path


def _selection(root: Path, workflow, command: str, node: str) -> tuple[list[str], list[str], set[str]]:
    graph_file = resolve_configured_graph_path(root, read_config(root))
    if command in {"runfrom", "resumefrom"}:
        nodes = component_topological_nodes(
            workflow,
            expand_to_components(workflow, {node, *descendants_in_order(workflow, node)}),
        )
        autostarts = autostart_closure(workflow, graph_file, nodes)
        extra = [item for item in autostarts if item not in nodes]
        if extra:
            nodes = topo_subset(workflow, expand_to_components(workflow, {*nodes, *extra}))
        blockers = direct_incomplete_inputs(workflow, set(nodes))
        return nodes, extra, blockers

    autostarts = autostart_closure(workflow, graph_file, [node])
    nodes = topo_subset(workflow, expand_to_components(workflow, {node, *autostarts}))
    blockers = direct_incomplete_inputs(workflow, set(nodes)) - workflow.component_predecessors(
        workflow.component_for(node)
    )
    return nodes, autostarts, blockers


def print_run_plan(
    root: Path,
    workflow,
    *,
    command: str,
    node: str,
    selected_jobs: list[int] | None = None,
) -> int:
    if selected_jobs is not None:
        nodes = [node]
        autostarts: list[str] = []
        blockers = set(workflow.graph_obj.predecessors(node)) if not workflow.node_ready(node) else set()
    else:
        nodes, autostarts, blockers = _selection(root, workflow, command, node)

    fresh = command in {"run", "runfrom"}
    print(f"Plan for: mwf {command} {node}")
    print("  mode: " + (
        "fresh reset of selected work before execution"
        if fresh
        else "preserve done/skipped jobs and continue queued or unsuccessful work"
    ))
    if selected_jobs is not None:
        print("  selected jobs:")
        for job_id in selected_jobs:
            if workflow.storage.job_exists(node, job_id):
                status = workflow.storage.get_job_status(node, job_id)
                print(f"    {node}/{job_id}: {status}")
            else:
                print(f"    {node}/{job_id}: missing")
    print("  selected nodes:")
    for item in nodes:
        summary = workflow.storage.node_job_summary(item)
        counts = ", ".join(
            f"{name}={count}" for name, count in sorted(summary["counts"].items()) if count
        ) or "no jobs"
        print(f"    {item}: node_status={workflow.storage.get_node_status(item) or 'missing'}, {counts}")
    print("  detected static autostarts: " + (", ".join(autostarts) if autostarts else "(none)"))
    print("  incomplete external inputs: " + (", ".join(sorted(blockers)) if blockers else "(none)"))
    print("  dynamic downstream jobs: determined when task functions run")
    print("  no state, jobs, inputs, outputs, or node folders were changed")
    return 0
