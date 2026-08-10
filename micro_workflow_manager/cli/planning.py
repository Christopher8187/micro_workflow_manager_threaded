from __future__ import annotations

from pathlib import Path

from .graph_utils import direct_incomplete_inputs


def _selection(root: Path, workflow, command: str, node: str) -> tuple[list[str], list[str], set[str]]:
    start_component = workflow.component_for(node)
    companions = [name for name in workflow.component_key(start_component) if name != node]

    if command in {"runfrom", "resumefrom"}:
        components = [start_component, *[set(item) for item in workflow.component_descendants(start_component)]]
        nodes = [name for component in components for name in workflow.component_key(component)]
        blockers = {
            previous
            for previous in workflow.component_predecessors(start_component)
            if not workflow.node_complete(previous)
        }
        return nodes, companions, blockers

    nodes = list(workflow.component_key(start_component))
    blockers = {
        previous
        for previous in workflow.component_predecessors(start_component)
        if not workflow.node_complete(previous)
    }
    return nodes, companions, blockers


def print_run_plan(
    root: Path,
    workflow,
    *,
    command: str,
    node: str,
    selected_jobs: list[int] | None = None,
    keep_trace: bool = False,
    refuse_after_node: str | None = None,
) -> int:
    if selected_jobs is not None:
        nodes = [node]
        companions: list[str] = []
        blockers = set(workflow.graph_obj.predecessors(node)) if not workflow.node_ready(node) else set()
    else:
        nodes, companions, blockers = _selection(root, workflow, command, node)

    fresh = command in {"run", "runfrom"}
    print(f"Plan for: mwf {command} {node}")
    print("  mode: " + (
        "fresh rerun; fully reset the start component and rebuild selected-producer work in descendants"
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
    print("  same Hoeflein component: " + (", ".join(companions) if companions else "(none)"))
    if refuse_after_node is not None:
        if refuse_after_node not in nodes:
            raise RuntimeError(
                f"refuseafter node {refuse_after_node!r} is not in the {command} "
                f"selection starting at {node!r}"
            )
        boundary = workflow.component_key(workflow.component_for(refuse_after_node))
        print(
            "  refusal boundary: stop admitting new components after "
            f"{{{', '.join(boundary)}}} terminates"
        )
        if command == "runfrom":
            print("  reset scope: unchanged; every selected runfrom component is still freshened")
        else:
            print("  resume scope: unchanged; later selected work remains queued for a future resume")
    if command == "resume":
        trace_mode = "preserve the current component trace journal"
    elif command == "resumefrom" and not keep_trace:
        trace_mode = "preserve the start component trace; clear descendant traces"
    elif keep_trace:
        trace_mode = "preserve all affected trace journals"
    else:
        trace_mode = "clear affected trace journals before reexecution"
    print(f"  trace mode: {trace_mode}")
    print("  incomplete start-component inputs: " + (", ".join(sorted(blockers)) if blockers else "(none)"))
    if command in {"runfrom", "resumefrom"}:
        external = direct_incomplete_inputs(workflow, set(nodes)) - blockers
        print("  incomplete external inputs on descendants: " + (", ".join(sorted(external)) if external else "(none; partial branches are otherwise preserved)"))
    print("  dynamic downstream jobs: determined when task functions run")
    print("  no state, jobs, inputs, outputs, or node folders were changed")
    return 0
