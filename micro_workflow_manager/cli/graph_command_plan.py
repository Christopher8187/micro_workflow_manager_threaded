"""Render a graph-command preview from immutable selection and observations."""

from __future__ import annotations

from micro_workflow_manager.storage.membership_footprint import MembershipPreparationFootprint
from micro_workflow_manager.storage.membership_observation import membership_repair_lines


def _component(component):
    return "{" + ", ".join(component) + "}"


def _edges(edges):
    return ", ".join(f"{source} -> {target}" for source, target in edges) or "(none)"


def _state_text(state):
    if state.lifecycle is None:
        return "uninitialized"
    result = state.lifecycle
    if state.stability == "stable":
        result += ", stable"
    elif state.stability == "unstable":
        result += f", unstable({state.instability_origin})"
    if state.misaligned:
        result += ", misaligned"
    return result


def render_graph_command_plan(observation, *, command_line=None, blocked_components=()) -> str:
    selection = observation.selection
    blocked_components = frozenset(blocked_components)
    disposition = "requested" if selection.operation == "reset" else "planned"
    lines = [
        f"Plan for: mwf {command_line or selection.command}",
        "  selected Hoeflein components: "
        + ", ".join(_component(component) for component in selection.components),
        "  selected nodes: " + ", ".join(selection.nodes),
    ]
    if observation.interrupt_start_component is not None:
        lines.append('  explicit interrupt start component ' + _component(observation.interrupt_start_component))
        lines.append('  predecessor readiness may be overridden for this start; descendants use normal readiness')
    if selection.excluded_end_component is not None:
        lines.append("  excluded end component: " + _component(selection.excluded_end_component))
    lines.extend((
        "  entering edges: " + _edges(selection.entering_edges),
        "  leaving edges: " + _edges(selection.leaving_edges),
        "  selected component state:",
    ))
    membership = (observation.preparation_footprint.change
                  if isinstance(observation.preparation_footprint, MembershipPreparationFootprint) else None)
    floors = {} if membership is None else {item.members: item.generation_floor for item in membership.preparation}
    for line in membership_repair_lines(membership):
        lines.append('  ' + line)
    for state in observation.selected_states:
        lines.append(f"    {_component(state.component)}: {_state_text(state)}")
        if state.component in blocked_components:
            lines.append(f"      preparation stopped before component {_component(state.component)}")
        elif selection.operation in {"run", "reset"} and state.lifecycle is not None:
            lines.append(
                "      fresh preparation would queue alignment generation "
                + str(max(state.alignment_generation, floors.get(state.component) or 0) + 1)
            )
    lines.append("  existing jobs:")
    for jobs in observation.node_job_counts:
        summary = (
            ", ".join(f"{status}={count}" for status, count in jobs.counts)
            if jobs.counts else "no jobs"
        )
        lines.append(f"    {jobs.node}: {summary}")
    lines.extend((
        "  prerequisite state:",
    ))
    if observation.prerequisite_states:
        for state in observation.prerequisite_states:
            text = _state_text(state)
            for node in state.component:
                lines.append(f"    {node}: {text}")
    else:
        lines.append("    (none)")
    boundary_receivers = {target for _, target in selection.leaving_edges}
    effect_receivers = {
        effect.receiver for effect in observation.preparation_effects
        if not effect.selected_receiver
    }
    receivers = sorted(
        boundary_receivers | effect_receivers | set(observation.declared_receivers)
    )
    lines.append("  unselected receivers: " + (", ".join(receivers) or "(none)"))
    if observation.declared_receivers:
        lines.append(
            "  statically declared receivers: "
            + ", ".join(observation.declared_receivers)
        )
    for effect in observation.preparation_effects:
        if effect.clears_output:
            lines.append(f"    {effect.receiver}: generated output would be cleared")
        for relative in effect.input_paths:
            lines.append(f"    {effect.receiver}/{relative}: managed input would be removed")
        for job_id in effect.deleted_jobs:
            lines.append(f"    {effect.receiver}/{job_id}: produced job would be removed")
        for job_id in effect.reset_jobs:
            lines.append(f"    {effect.receiver}/{job_id}: job would be requeued")
        for job_id in effect.cleared_traces:
            lines.append(f"    {effect.receiver}/{job_id}: trace would be cleared")
    for job in observation.resume_jobs:
        lines.append(
            f"    {job.node}/{job.job_id}: {job.status}; generation={job.generation}; would resume"
        )
    for refusal in observation.refusals:
        lines.append("  would refuse: " + refusal)
    if selection.operation == "reset" and observation.live_sessions:
        names = ", ".join(session.session_id for session in observation.live_sessions)
        lines.append("  reset refusal: live execution session(s) " + names)
    lines.extend((
        "  preview uses synchronized raw edges, AST-read autostart declarations, and persisted native state; user code was not loaded",
        f"  {disposition} {selection.command} was not applied; no project state was changed",
    ))
    return "\n".join(lines)
