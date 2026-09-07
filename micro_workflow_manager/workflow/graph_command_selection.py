"""Pure quotient selection for the nine graph commands."""

from __future__ import annotations

from dataclasses import dataclass

from micro_workflow_manager.errors import InvalidGraphError


@dataclass(frozen=True)
class GraphCommandSpec:
    operation: str
    scope: str
    preview_attribute: str


_COMMANDS = {
    "run": GraphCommandSpec("run", "one", "plan"),
    "runfrom": GraphCommandSpec("run", "from", "plan"),
    "runbetween": GraphCommandSpec("run", "between", "plan"),
    "resume": GraphCommandSpec("resume", "one", "plan"),
    "resumefrom": GraphCommandSpec("resume", "from", "plan"),
    "resumebetween": GraphCommandSpec("resume", "between", "plan"),
    "reset": GraphCommandSpec("reset", "one", "dry_run"),
    "resetfrom": GraphCommandSpec("reset", "from", "dry_run"),
    "resetbetween": GraphCommandSpec("reset", "between", "dry_run"),
}


@dataclass(frozen=True)
class GraphCommandSelection:
    command: str
    operation: str
    scope: str
    start_component: tuple[str, ...] | None
    components: tuple[tuple[str, ...], ...]
    nodes: tuple[str, ...]
    excluded_end_component: tuple[str, ...] | None
    entering_edges: tuple[tuple[str, str], ...]
    leaving_edges: tuple[tuple[str, str], ...]

    @property
    def node_set(self) -> frozenset[str]:
        return frozenset(self.nodes)


def select_graph_command(topology, command: str, start: str, end: str | None = None):
    """Return the one, descendant, or half-open quotient selection."""
    try:
        spec = _COMMANDS[command]
    except KeyError as error:
        raise ValueError(f"Unknown graph command: {command}") from error
    operation, scope = spec.operation, spec.scope
    if command in {"reset", "resetfrom"} and start == "*":
        if end is not None:
            raise ValueError(f"{command} '*' does not accept an end node")
        components = tuple(topology.execution_components())
        selected = {node for component in components for node in component}
        return GraphCommandSelection(
            command=command,
            operation=operation,
            scope="all",
            start_component=None,
            components=components,
            nodes=tuple(node for component in components for node in component),
            excluded_end_component=None,
            entering_edges=(),
            leaving_edges=tuple(sorted(
                (source, target) for source, target in topology.graph_obj.edges
                if source in selected and target not in selected
            )),
        )
    if start not in topology.graph_obj:
        raise InvalidGraphError(f"Unknown start node {start!r}")
    if scope == "between":
        if end is None:
            raise ValueError(f"{command} requires an end node")
        components = tuple(topology.component_interval(start, end))
        excluded_end = topology.component_id(end)
    else:
        if end is not None:
            raise ValueError(f"{command} does not accept an end node")
        first = topology.component_id(start)
        components = (first,)
        if scope == "from":
            components += tuple(topology.component_descendants(first))
        excluded_end = None
    start_component = topology.component_id(start)
    selected = {node for component in components for node in component}
    entering = tuple(sorted(
        (source, target) for source, target in topology.graph_obj.edges
        if source not in selected and target in selected
    ))
    leaving = tuple(sorted(
        (source, target) for source, target in topology.graph_obj.edges
        if source in selected and target not in selected
    ))
    return GraphCommandSelection(
        command=command,
        operation=operation,
        scope=scope,
        start_component=start_component,
        components=components,
        nodes=tuple(node for component in components for node in component),
        excluded_end_component=excluded_end,
        entering_edges=entering,
        leaving_edges=leaving,
    )


def graph_command_spec(command: str) -> GraphCommandSpec | None:
    return _COMMANDS.get(command)
