"""Reconcile regions with no reusable work during native topology registration."""

from . import component_membership
from .membership_history import read_membership_history
from .unproduced_membership import read_unproduced_component_states
from micro_workflow_manager.component_identity import encode_component_key
from .membership_activity import require_idle_membership_region


def reconcile_unproduced_memberships(connection, snapshot, *, new_components=()):
    active = component_membership.read_active_component_partition(connection)
    if active is None:
        raise RuntimeError('Membership reconciliation requires an initialized partition')
    history = read_membership_history(connection)
    projected = read_unproduced_component_states(connection, snapshot, new_components=new_components)
    changed = False
    for component in snapshot.components:
        change = component_membership.observe_membership_change(active, snapshot, (component,), history)
        if not change.changes_active_membership:
            continue
        if any(item.has_reusable_work for item in change.historical_memberships):
            continue
        require_idle_membership_region(
            connection, (*change.source_components, *change.target_components),
        )
        for target in change.target_components:
            state = projected.get(target)
            if state is None:
                continue
            shape = connection.execute('SELECT shape_id FROM graph_shapes WHERE shape_json=?',
                                       (snapshot.shape_json,)).fetchone()
            if shape is None:
                raise RuntimeError('Automatic membership target shape is not registered')
            changed_state = connection.execute(
                "UPDATE component_states SET shape_id=?, lifecycle='queued', stability=NULL, "
                'instability_origin=NULL, misaligned=0, alignment_generation=?, '
                'retained_result_shape_id=NULL, retained_result_alignment_generation=NULL WHERE component_key=?',
                (shape['shape_id'], state['alignment_generation'], encode_component_key(target)),
            ).rowcount
            if changed_state != 1:
                raise RuntimeError('Automatic membership target state is missing')
        active = component_membership._replace_active_component_closure(connection, change)
        changed = True
    from .membership_retirement import observe_removed_memberships

    retirement = observe_removed_memberships(active, snapshot, read_membership_history(connection))
    if retirement.changes_active_membership:
        require_idle_membership_region(connection, retirement.source_components)
        component_membership._replace_active_component_closure(connection, retirement)
        changed = True
    return changed
