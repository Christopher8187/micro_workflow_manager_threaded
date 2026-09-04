from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from micro_workflow_manager.system import MicroWorkflow

from .active_run import refuse_competing_run
from .cleanup import prepare_fresh_components, reset_job_for_run
from .jobs import selected_job_ids_from_args
from .validation import require_node


@dataclass(frozen=True)
class DestructiveSelection:
    components: tuple[frozenset[str], ...]
    nodes: tuple[str, ...]
    selected_jobs: tuple[int, ...] | None = None
    refuse_after_node: str | None = None


def _all_components(workflow: MicroWorkflow) -> list[set[str]]:
    return [set(component) for component in workflow.execution_components()]


def _components_from(
    workflow: MicroWorkflow,
    node: str,
    *,
    descendants: bool,
) -> list[set[str]]:
    if node == "*":
        return _all_components(workflow)
    require_node(workflow, node)
    start = workflow.component_for(node)
    if not descendants:
        return [set(start)]
    return [
        set(start),
        *[set(component) for component in workflow.component_descendants(start)],
    ]


def _selection(
    workflow: MicroWorkflow,
    *,
    command: str,
    node: str,
    job_mode: str | None = None,
    job_specs: list[str] | None = None,
    refuse_after_node: str | None = None,
) -> DestructiveSelection:
    descendants = command.endswith("from")
    if job_mode is not None or job_specs:
        if command != "reset":
            raise RuntimeError("Explicit job selection is available only for mwf reset.")
        if node == "*":
            raise RuntimeError("Explicit job selection cannot be combined with '*'.")
        require_node(workflow, node)
        selected = selected_job_ids_from_args(job_mode, job_specs or [])
        assert selected is not None
        return DestructiveSelection(
            components=(frozenset({node}),),
            nodes=(node,),
            selected_jobs=tuple(selected),
        )

    components = _components_from(workflow, node, descendants=descendants)
    nodes = tuple(
        name
        for component in components
        for name in workflow.component_key(component)
    )
    if refuse_after_node is not None:
        if command != "resetfrom":
            raise RuntimeError("refuseafter is available only for mwf resetfrom.")
        require_node(workflow, refuse_after_node)
        selected_component_ids = {
            workflow.component_id(component) for component in components
        }
        if workflow.component_id(refuse_after_node) not in selected_component_ids:
            raise RuntimeError(
                f"refuseafter node {refuse_after_node!r} is not in the resetfrom "
                f"selection starting at {node!r}"
            )
    return DestructiveSelection(
        components=tuple(frozenset(component) for component in components),
        nodes=nodes,
        refuse_after_node=refuse_after_node,
    )


def _component_text(selection: DestructiveSelection) -> str:
    return ", ".join(
        "{" + ", ".join(sorted(component)) + "}"
        for component in selection.components
    )


def _print_completion(
    workflow: MicroWorkflow,
    selection: DestructiveSelection,
) -> None:
    all_nodes = tuple(
        name
        for component in _all_components(workflow)
        for name in workflow.component_key(component)
    )
    if set(selection.nodes) == set(all_nodes):
        print("Reset all nodes: " + ", ".join(selection.nodes))
    elif len(selection.components) == 1 and len(selection.components[0]) == 1:
        node = selection.nodes[0]
        print(f"Reset DAG node {node}: {node}")
    else:
        print(
            f"Reset Hoeflein component(s) {_component_text(selection)}: "
            + ", ".join(selection.nodes)
        )


def _danger_text() -> str:
    return (
        "clears generated output, requeues retained jobs, and may delete "
        "downstream jobs produced by the selected Hoeflein components"
    )


def _confirm(command: str, selection: DestructiveSelection, *, assume_yes: bool) -> bool:
    if assume_yes:
        return True
    print(f"DANGER: mwf {command} {_danger_text()}.")
    print("Selected nodes: " + ", ".join(selection.nodes))
    if selection.selected_jobs is not None:
        print(
            "Selected jobs: "
            + ", ".join(f"{selection.nodes[0]}/{job_id}" for job_id in selection.selected_jobs)
        )
    if selection.refuse_after_node is not None:
        print(
            "Note: refuseafter has no execution boundary here; resetfrom still "
            "freshens the full descendant selection."
        )
    try:
        answer = input(f"Type {command!r} to continue: ").strip()
    except EOFError:
        answer = ""
    if answer != command:
        print(f"Aborted mwf {command}; requested {command} was not applied. CLI bootstrap and router mounting may already have updated framework state.")
        return False
    return True


def _print_plan(
    workflow: MicroWorkflow,
    command: str,
    selection: DestructiveSelection,
    *,
    keep_trace: bool,
) -> None:
    print(f"Dry run for mwf {command}:")
    print("  selected Hoeflein components: " + _component_text(selection))
    print("  selected nodes: " + ", ".join(selection.nodes))
    if selection.selected_jobs is not None:
        print(
            "  selected jobs: "
            + ", ".join(
                f"{selection.nodes[0]}/{job_id}" for job_id in selection.selected_jobs
            )
        )
    for node in selection.nodes:
        summary = workflow.storage.node_job_summary(node)
        counts = ", ".join(
            f"{status}={count}"
            for status, count in sorted(summary["counts"].items())
            if count
        ) or "no jobs"
        print(f"  {node}: would preserve jobs/input; {counts}; would {_danger_text()}")
    print(
        "  trace journals: "
        + ("would be preserved" if keep_trace else "would be cleared")
    )
    print(f"  requested {command} was not applied; bootstrap and router mounting may already have updated framework state")


def execute_destructive_command(
    root: Path,
    workflow: MicroWorkflow,
    args,
) -> int:
    command = str(args.command)
    if command not in {"reset", "resetfrom"}:
        raise RuntimeError(f"Unknown reset command: {command}")
    node = getattr(args, "node", None)
    if node is None:
        raise RuntimeError("No node specified")
    refuse_node = getattr(args, "refuse_node", None)
    selection = _selection(
        workflow,
        command=command,
        node=node,
        job_mode=getattr(args, "job_mode", None),
        job_specs=getattr(args, "job_specs", None),
        refuse_after_node=refuse_node,
    )

    if getattr(args, "dry_run", False):
        _print_plan(
            workflow,
            command,
            selection,
            keep_trace=bool(getattr(args, "keeptrace", False)),
        )
        return 0

    refuse_competing_run(workflow)
    if not _confirm(command, selection, assume_yes=bool(getattr(args, "yes", False))):
        return 1

    keep_trace = bool(getattr(args, "keeptrace", False))
    if command == "reset" and selection.selected_jobs is not None:
        for job_id in selection.selected_jobs:
            reset_job_for_run(
                root,
                workflow,
                selection.nodes[0],
                job_id,
                keep_trace=keep_trace,
            )
    else:
        prepare_fresh_components(
            root,
            workflow,
            [set(component) for component in selection.components],
            keep_trace=keep_trace,
        )
    _print_completion(workflow, selection)
    print(f"Completed mwf {command} for: " + ", ".join(selection.nodes))
    if selection.selected_jobs is not None:
        print("Reset jobs: " + ", ".join(map(str, selection.selected_jobs)))
    return 0
