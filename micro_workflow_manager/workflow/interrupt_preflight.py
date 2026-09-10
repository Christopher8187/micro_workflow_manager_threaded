"""Pure ordinary-interrupt policy resolution over a fixed graph selection."""

from __future__ import annotations

from dataclasses import dataclass

import networkx as nx


INTERRUPT_POLICIES = ("run-all", "stop-all", "individual")
INTERRUPT_ACTIONS = ("run", "stop")


class InterruptPolicyRequired(RuntimeError):
    pass


class InterruptChoiceRequired(RuntimeError):
    def __init__(self, component: tuple[str, ...]):
        self.component = component
        canonical = component[0]
        super().__init__(
            f"Individual interrupt policy requires {canonical}=run or "
            f"{canonical}=stop for component {{{', '.join(component)}}}"
        )


@dataclass(frozen=True, slots=True)
class InterruptDecision:
    canonical_key: str
    members: tuple[str, ...]
    action: str

    def record(self) -> dict:
        return {
            "canonical_key": self.canonical_key,
            "members": list(self.members),
            "action": self.action,
        }


@dataclass(frozen=True, slots=True)
class UnusedInterruptChoice:
    key: str
    canonical_key: str
    members: tuple[str, ...]
    action: str
    reason: str


@dataclass(frozen=True, slots=True)
class InterruptPreflight:
    policy: str
    decisions: tuple[InterruptDecision, ...]
    stopped_components: tuple[tuple[str, ...], ...]
    blocked_components: tuple[tuple[str, ...], ...]
    unused_choices: tuple[UnusedInterruptChoice, ...]
    explicit_start_component: tuple[str, ...] | None = None

    def record(self) -> dict:
        return {
            "policy": self.policy,
            "decisions": [decision.record() for decision in self.decisions],
            "stopped_components": [list(component) for component in self.stopped_components],
            "blocked_components": [list(component) for component in self.blocked_components],
        }


def _interrupt_components(topology, interrupt_nodes) -> frozenset[tuple[str, ...]]:
    graph_nodes = set(topology.graph_obj.nodes)
    names = set(interrupt_nodes)
    unknown = sorted(names - graph_nodes)
    if unknown:
        raise RuntimeError(
            "Interrupt declarations name unknown raw nodes: " + ", ".join(unknown)
        )
    return frozenset(topology.component_id(name) for name in names)


def _parse_choice(text: str) -> tuple[str, str]:
    if not isinstance(text, str) or text.count("=") != 1:
        raise ValueError("Interrupt choices must use RAW_NODE=run or RAW_NODE=stop")
    key, action = (value.strip() for value in text.split("=", 1))
    if not key or action not in INTERRUPT_ACTIONS:
        raise ValueError("Interrupt choices must use RAW_NODE=run or RAW_NODE=stop")
    return key, action


def _validated_choices(topology, classified, policy, raw_choices):
    if raw_choices and policy != "individual":
        raise ValueError(
            "Interrupt choices contradict the selected policy; choices require individual"
        )
    by_component: dict[tuple[str, ...], str] = {}
    supplied: list[tuple[str, tuple[str, ...], str]] = []
    for text in raw_choices:
        key, action = _parse_choice(text)
        if key not in topology.graph_obj:
            raise ValueError(f"Unknown interrupt-choice raw node {key!r}")
        component = topology.component_id(key)
        if component not in classified:
            raise ValueError(f"Raw node {key!r} is not interrupt-classified")
        previous = by_component.get(component)
        if previous is not None and previous != action:
            members = ", ".join(component)
            raise ValueError(
                f"Conflicting interrupt choices for component {{{members}}}"
            )
        by_component[component] = action
        supplied.append((key, component, action))
    return by_component, tuple(supplied)


def _reachable(dag, selected, roots, stopped):
    remaining = set(selected) - set(stopped)
    reachable: set[tuple[str, ...]] = set()
    for root in roots:
        if root not in remaining:
            continue
        reachable.add(root)
        reachable.update(nx.descendants(dag.subgraph(remaining), root))
    return reachable


def resolve_interrupt_preflight(
    topology,
    selection,
    interrupt_nodes,
    *,
    policy: str | None,
    raw_choices=(),
    explicit_start: bool = False,
) -> InterruptPreflight:
    """Resolve every reachable ordinary-interrupt decision without side effects."""
    if policy is not None and policy not in INTERRUPT_POLICIES:
        raise ValueError(
            "interrupt policy must be run-all, stop-all, or individual"
        )
    classified = _interrupt_components(topology, interrupt_nodes)
    if explicit_start and selection.start_component not in classified:
        raise ValueError("The explicit starting component is not interrupt-classified")
    choices, supplied = _validated_choices(
        topology, classified, policy, tuple(raw_choices),
    )
    selected = tuple(selection.components)
    selected_set = set(selected)
    dag = topology.component_dag().subgraph(selected_set).copy()
    if selection.start_component is not None:
        roots = (selection.start_component,)
    else:
        roots = tuple(component for component in selected if dag.in_degree(component) == 0)
    excluded = selection.start_component if explicit_start else None
    stopped: list[tuple[str, ...]] = []
    decisions: list[InterruptDecision] = []
    decided_components: set[tuple[str, ...]] = set()

    for component in selected:
        reachable = _reachable(dag, selected_set, roots, stopped)
        if component not in reachable or component not in classified or component == excluded:
            continue
        canonical = component[0]
        if policy is None:
            raise InterruptPolicyRequired(
                "Interrupt policy is required before mutation; choose run-all, "
                "stop-all, or individual with "
                f"{canonical}=run or {canonical}=stop"
            )
        if policy == "individual":
            try:
                action = choices[component]
            except KeyError as error:
                raise InterruptChoiceRequired(component) from error
        else:
            action = "run" if policy == "run-all" else "stop"
        decisions.append(InterruptDecision(canonical, component, action))
        decided_components.add(component)
        if action == "stop":
            stopped.append(component)

    final_reachable = _reachable(dag, selected_set, roots, stopped)
    blocked = tuple(component for component in selected if component not in final_reachable)
    unused: list[UnusedInterruptChoice] = []
    seen_unused: set[tuple[str, tuple[str, ...], str]] = set()
    for key, component, action in supplied:
        if component in decided_components:
            continue
        identity = (key, component, action)
        if identity in seen_unused:
            continue
        seen_unused.add(identity)
        reason = (
            "explicit starting component"
            if component == excluded
            else "unreachable or outside the selection"
        )
        unused.append(UnusedInterruptChoice(
            key, component[0], component, action, reason,
        ))
    return InterruptPreflight(
        policy=policy or "run-all",
        decisions=tuple(decisions),
        stopped_components=tuple(stopped),
        blocked_components=blocked,
        unused_choices=tuple(unused),
        explicit_start_component=selection.start_component if explicit_start else None,
    )
