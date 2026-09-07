"""Compose exact native observations for graph-command previews."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from micro_workflow_manager.storage.preparation_footprint import PreparationFootprint
from micro_workflow_manager.workflow.graph_command_selection import GraphCommandSelection


@dataclass(frozen=True)
class ComponentPlanState:
    component: tuple[str, ...]
    lifecycle: str | None
    stability: str | None = None
    instability_origin: str | None = None
    misaligned: bool | None = None
    alignment_generation: int | None = None

    @classmethod
    def from_native(cls, component, state):
        if state is None:
            return cls(tuple(component), None)
        return cls(
            tuple(component), state["lifecycle"], state["stability"],
            state["instability_origin"], state["misaligned"],
            state["alignment_generation"],
        )


@dataclass(frozen=True)
class PreparationEffect:
    receiver: str
    selected_receiver: bool
    clears_output: bool
    input_paths: tuple[str, ...]
    deleted_jobs: tuple[int, ...]
    reset_jobs: tuple[int, ...]
    cleared_traces: tuple[int, ...]


@dataclass(frozen=True)
class ResumeJobEffect:
    node: str
    job_id: int
    status: str
    generation: int


@dataclass(frozen=True)
class ResumePlanEffects:
    jobs: tuple[ResumeJobEffect, ...]
    successful_results: tuple[
        tuple[tuple[str, ...], tuple[str, str | None, str | None] | None], ...
    ]


@dataclass(frozen=True)
class LiveSessionPlanState:
    session_id: str
    session_kind: str
    status: str
    outcome: str | None


@dataclass(frozen=True)
class NodeJobPlanState:
    node: str
    counts: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class GraphCommandObservation:
    shape_json: str
    selection: GraphCommandSelection
    selected_states: tuple[ComponentPlanState, ...]
    prerequisite_states: tuple[ComponentPlanState, ...]
    node_job_counts: tuple[NodeJobPlanState, ...]
    live_sessions: tuple[LiveSessionPlanState, ...]
    preparation_footprint: PreparationFootprint | None
    preparation_effects: tuple[PreparationEffect, ...]
    resume_jobs: tuple[ResumeJobEffect, ...]
    declared_receivers: tuple[str, ...]
    refusals: tuple[str, ...]


class GraphPlanningReader(Protocol):
    """Connection/root observer implemented by extracted native readers."""

    @property
    def shape_json(self) -> str: ...

    def read_component_states(self, components, *, allow_missing: bool): ...

    def list_live_execution_sessions(self) -> tuple[LiveSessionPlanState, ...]: ...

    def read_component_activity(self, components) -> tuple[str, ...]: ...

    def read_node_job_counts(self, nodes) -> tuple[NodeJobPlanState, ...]: ...

    def read_receiver_activity(
        self, receivers, *, include_active_jobs: bool,
    ) -> tuple[str, ...]: ...

    def read_preparation_footprint(self, components, *, keep_trace: bool): ...

    def read_resume_plan(self, components, states) -> ResumePlanEffects: ...


def _external_predecessors(topology, selection):
    selected = set(selection.components)
    return tuple(sorted({
        predecessor
        for component in selection.components
        for predecessor in topology.component_predecessor_components(set(component))
        if predecessor not in selected
    }))


def _preparation_effects(footprint, selected_nodes, *, keep_trace):
    effects = {}
    for unit in footprint.units:
        for item in unit.inputs:
            current = effects.setdefault(item.receiver, [False, set(), set(), set(), set()])
            current[1].add(item.relative)
        for plan in unit.jobs:
            current = effects.setdefault(plan.node, [False, set(), set(), set(), set()])
            current[0] = current[0] or plan.clear_output
            current[2].update(plan.delete_ids)
            current[3].update(plan.reset_ids)
            if not keep_trace:
                current[4].update(plan.delete_ids)
                current[4].update(plan.reset_ids)
                current[4].update(plan.orphan_ids)
    return tuple(
        PreparationEffect(
            receiver,
            receiver in selected_nodes,
            values[0],
            tuple(sorted(values[1])),
            tuple(sorted(values[2])),
            tuple(sorted(values[3])),
            tuple(sorted(values[4])),
        )
        for receiver, values in sorted(effects.items())
    )


def observe_graph_command(
    reader: GraphPlanningReader, topology, selection, *, keep_trace=False,
    static_receivers_by_node=None,
):
    """Read one coherent preview snapshot without admitting or preparing work."""
    if reader.shape_json != topology.graph_shape():
        raise RuntimeError("Stored native shape differs from the preview topology")
    external = _external_predecessors(topology, selection)
    observed = reader.read_component_states(
        (*selection.components, *external), allow_missing=True,
    )
    selected_states = tuple(
        ComponentPlanState.from_native(component, observed[component])
        for component in selection.components
    )
    prerequisite_states = tuple(
        ComponentPlanState.from_native(component, observed[component])
        for component in external
    )
    node_job_counts = reader.read_node_job_counts(selection.nodes)
    footprint = None
    preparation_effects = ()
    resume_jobs = ()
    component_activity = reader.read_component_activity(selection.components)
    refusals = list(component_activity)
    receiver_map = {} if static_receivers_by_node is None else static_receivers_by_node
    declared_receivers = tuple(sorted({
        receiver
        for node in selection.nodes
        for receiver in receiver_map.get(node, ())
        if receiver not in selection.node_set
    }))
    if selection.operation in {"run", "reset"}:
        footprint = reader.read_preparation_footprint(
            selection.components, keep_trace=keep_trace,
        )
        preparation_effects = _preparation_effects(
            footprint, selection.node_set, keep_trace=keep_trace,
        )
        effect_receivers = tuple(effect.receiver for effect in preparation_effects)
        excluded_receivers = tuple(
            effect.receiver for effect in preparation_effects
            if not effect.selected_receiver
        )
        refusals.extend(reader.read_receiver_activity(
            effect_receivers, include_active_jobs=False,
        ))
        refusals.extend(reader.read_receiver_activity(
            excluded_receivers, include_active_jobs=True,
        ))
        if selection.operation == "run":
            from micro_workflow_manager.component_readiness import calculate_component_readiness

            selected = set(selection.components)
            start = selection.start_component
            parents = (
                set() if start is None
                else topology.component_predecessor_components(set(start)) - selected
            )
            inputs = [observed[parent] for parent in sorted(parents)]
            if any(state is None or state["lifecycle"] != "done" for state in inputs) \
                    or calculate_component_readiness(
                        (state["lifecycle"], state["stability"], state["instability_origin"])
                        for state in inputs if state is not None
                    ) is None:
                refusals.append(
                    f"{selection.command} would refuse incomplete or incompatible parents "
                    f"for start component {start}"
                )
    else:
        blocked = bool(component_activity)
        for component in selection.components:
            state = observed[component]
            if state is None:
                refusals.append(f"{selection.command} would refuse uninitialized component {component}")
                blocked = True
            elif state["misaligned"]:
                refusals.append(f"{selection.command} would refuse misaligned component {component}")
                blocked = True
            elif state["lifecycle"] == "running":
                refusals.append(f"{selection.command} would require recovery of running component {component}")
                blocked = True
        if not blocked:
            resume_plan = reader.read_resume_plan(
                selection.components,
                {component: observed[component] for component in selection.components},
            )
            from micro_workflow_manager.errors import InvalidGraphError
            from micro_workflow_manager.workflow.resume_preparation import predict_resume_lineages

            all_parents = {
                component: topology.component_predecessor_components(set(component))
                for component in selection.components
            }
            try:
                predict_resume_lineages(
                    selection.components,
                    all_parents,
                    observed,
                    dict(resume_plan.successful_results),
                )
            except InvalidGraphError as error:
                refusals.append(str(error))
            resume_jobs = resume_plan.jobs
    return GraphCommandObservation(
        shape_json=reader.shape_json,
        selection=selection,
        selected_states=selected_states,
        prerequisite_states=prerequisite_states,
        node_job_counts=node_job_counts,
        live_sessions=reader.list_live_execution_sessions(),
        preparation_footprint=footprint,
        preparation_effects=preparation_effects,
        resume_jobs=resume_jobs,
        declared_receivers=declared_receivers,
        refusals=tuple(dict.fromkeys(refusals)),
    )
