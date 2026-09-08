"""Observe entirely removed components that have no reusable work."""

from .component_membership import ActiveComponentPartition, HistoricalMembership, MembershipChange


def observe_removed_memberships(active, snapshot, historical_memberships):
    from .component_definitions import component_snapshot_from_shape

    if not isinstance(active, ActiveComponentPartition):
        raise ValueError('Expected the active component partition')
    if snapshot != component_snapshot_from_shape(snapshot.shape_json):
        raise ValueError('Removed membership observation requires a canonical target shape')
    history = tuple(historical_memberships)
    if any(not isinstance(item, HistoricalMembership) for item in history):
        raise ValueError('Expected historical membership observations')
    history = tuple(sorted(history, key=lambda item: (
        item.shape_id, item.members, item.alignment_generation,
    )))
    identities = [(item.members, item.shape_id, item.alignment_generation) for item in history]
    if len(set(identities)) != len(identities):
        raise ValueError('Historical membership observations contain duplicate identities')
    current_nodes = {node for component in snapshot.components for node in component}
    sources = tuple(item.members for item in active.components
                    if not current_nodes.intersection(item.members)
                    and not any(old.has_reusable_work and set(old.members).intersection(item.members)
                                for old in history))
    return MembershipChange(
        source=active, target_shape_json=snapshot.shape_json, requested_components=(),
        source_components=sources, target_components=(), preparation=(),
        historical_memberships=history,
        removed_members=tuple(sorted(node for component in sources for node in component)),
    )
