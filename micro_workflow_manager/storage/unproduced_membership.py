"""Project automatic membership changes before their registration writer runs."""

from .component_membership import read_active_component_partition, observe_membership_change


def read_unproduced_component_states(connection, snapshot, *, new_components=()):
    from .component_states import read_component_state_snapshot
    from .membership_history import read_membership_history

    active = read_active_component_partition(connection)
    if active is None:
        return {}
    history = read_membership_history(connection)
    established = {item.members for item in active.components}
    newly_created = set(new_components)
    projected = {}
    for component in snapshot.components:
        change = observe_membership_change(active, snapshot, (component,), history)
        if (not change.changes_active_membership
                or any(item.has_reusable_work for item in change.historical_memberships)):
            continue
        for target in change.target_components:
            if target in established or target in projected:
                continue
            state = read_component_state_snapshot(connection, target)
            if state is None or target in newly_created:
                generation = 0
            else:
                floor = max((item.alignment_generation for item in change.historical_memberships
                             if set(target).intersection(item.members)), default=0)
                generation = max(state['alignment_generation'], floor) + 1
            projected[target] = {
                'members': target, 'shape_json': snapshot.shape_json, 'lifecycle': 'queued',
                'stability': None, 'instability_origin': None, 'misaligned': False,
                'alignment_generation': generation,
            }
    return projected


def read_execution_component_states(connection, components, *, expected_shape, allow_missing=False):
    from .component_definitions import component_snapshot_from_shape
    from .component_states import read_component_state_snapshot

    snapshot = component_snapshot_from_shape(expected_shape)
    components = tuple(tuple(component) for component in components)
    if any(component not in snapshot.components for component in components):
        raise RuntimeError('Component observations require current graph membership')
    connection.execute('SAVEPOINT mwf_execution_component_observation')
    try:
        projected = read_unproduced_component_states(connection, snapshot)
        active = read_active_component_partition(connection)
        established = set() if active is None else {item.members for item in active.components}
        states = {}
        for component in components:
            state = read_component_state_snapshot(connection, component)
            states[component] = projected.get(component, state if component in established else None)
        if not allow_missing and any(state is None for state in states.values()):
            raise RuntimeError('Component observations require initialized current membership')
        return states
    finally:
        connection.execute('RELEASE SAVEPOINT mwf_execution_component_observation')
