"""Read-only adapter and rendering for ordinary interrupt policy."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from micro_workflow_manager.workflow.graph_command_selection import (
    GraphCommandSelection,
    select_graph_command,
)
from micro_workflow_manager.workflow.interrupt_preflight import (
    InterruptPreflight,
    resolve_interrupt_preflight,
)

from .files import safe_node_name
from .interrupt_scan import InterruptDeclarations, scan_interrupt_declarations
from .preview import load_preview
from micro_workflow_manager.storage.interrupt_admission import read_interrupt_admission_conflicts


@dataclass(frozen=True, slots=True)
class ObservedInterruptCommand:
    shape_json: str
    source_directory: Path | None
    declarations: InterruptDeclarations
    selection: GraphCommandSelection
    preflight: InterruptPreflight


def selection_for_interrupt_preflight(workflow, args):
    node = safe_node_name(args.node)
    end_node = getattr(args, "end_node", None)
    if end_node is not None:
        end_node = safe_node_name(end_node)
    return select_graph_command(workflow.topology, args.command, node, end_node)


def observe_interrupt_preflight(workflow, selection, args):
    return resolve_interrupt_preflight(
        workflow.topology,
        selection,
        workflow.interrupt_declarations.interrupt_nodes,
        policy=getattr(args, "interrupt_policy", None),
        raw_choices=tuple(getattr(args, "interrupt_choice", ())),
        explicit_start=bool(getattr(args, "interrupt", False)),
    )


def require_interrupt_admission_available(workflow, selection, preflight):
    component = preflight.explicit_start_component
    if component is None:
        return
    read_interrupt_admission_conflicts(
        workflow.storage.connection, start_component=component,
        selected_components=selection.components,
        direct_predecessors=sorted(workflow.topology.component_predecessor_components(set(component))),
        expected_shape=workflow.topology.graph_shape(),
    )


def read_interrupt_command_preflight(root, args):
    """Resolve policy before startup recovery or any user-code import."""
    workflow = load_preview(root)
    try:
        selection = selection_for_interrupt_preflight(workflow, args)
        preflight = observe_interrupt_preflight(workflow, selection, args)
        require_interrupt_admission_available(workflow, selection, preflight)
        return ObservedInterruptCommand(
            shape_json=workflow.topology.graph_shape(),
            source_directory=workflow.interrupt_source_directory,
            declarations=workflow.interrupt_declarations,
            selection=selection,
            preflight=preflight,
        )
    finally:
        workflow.storage.close()


def render_interrupt_preflight(preflight) -> str:
    lines = [f"interrupt policy: {preflight.policy}"]
    for decision in preflight.decisions:
        lines.extend((
            f"interrupt component {{{', '.join(decision.members)}}}",
            f"  canonical replay key: {decision.canonical_key}",
            f"  action: {decision.action}",
        ))
    for choice in preflight.unused_choices:
        lines.append(
            f"unused interrupt choice {choice.key}={choice.action}: "
            f"component {{{', '.join(choice.members)}}} is {choice.reason}"
        )
    return "\n".join(lines)


def require_interrupt_source_unchanged(observed) -> None:
    if observed.source_directory is None:
        current = type(observed.declarations)(())
    else:
        current = scan_interrupt_declarations(observed.source_directory)
    if current != observed.declarations:
        raise RuntimeError(
            "Interrupt declaration source changed after preflight; try the command again"
        )


def require_interrupt_preflight_unchanged(root, observed) -> None:
    """Recheck source and synchronized topology before any startup mutation."""
    require_interrupt_source_unchanged(observed)
    workflow = load_preview(root)
    try:
        if workflow.topology.graph_shape() != observed.shape_json:
            raise RuntimeError(
                "Workflow topology changed after interrupt preflight; try the command again"
            )
        if (workflow.interrupt_source_directory != observed.source_directory
                or workflow.interrupt_declarations != observed.declarations):
            raise RuntimeError(
                "Interrupt declaration source changed after preflight; try the command again"
            )
        require_interrupt_admission_available(workflow, observed.selection, observed.preflight)
    finally:
        workflow.storage.close()


def validate_loaded_interrupt_declarations(workflow, observed) -> None:
    """Reject source/runtime drift before session admission or preparation."""
    actual = frozenset(
        name for name, node in workflow.nodes.items() if node.interrupt
    )
    if workflow.topology.graph_shape() != observed.shape_json:
        raise RuntimeError(
            "Workflow topology changed after interrupt preflight; try the command again"
        )
    if actual != observed.declarations.interrupt_nodes:
        raise RuntimeError(
            "Interrupt declarations changed after read-only preflight; try the command again"
        )
