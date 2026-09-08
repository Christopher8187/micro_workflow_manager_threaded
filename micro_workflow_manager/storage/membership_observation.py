"""Observe membership before admission or full preparation changes project state."""

from .component_membership import read_active_component_partition, observe_membership_change
from .membership_history import read_membership_history


def read_membership_change(connection, snapshot, requested_components):
    connection.execute('SAVEPOINT mwf_membership_observation')
    try:
        active = read_active_component_partition(connection)
        history = read_membership_history(connection)
        if active is None:
            return None
        return observe_membership_change(active, snapshot, requested_components, history)
    finally:
        connection.execute('RELEASE SAVEPOINT mwf_membership_observation')


def membership_needs_fresh_preparation(change):
    if change is None:
        return False
    current = set(change.target_components)
    reusable = tuple(item for item in change.historical_memberships if item.has_reusable_work)
    return bool(reusable) and (
        change.changes_active_membership or any(item.members not in current for item in reusable)
    )


def require_reusable_membership_match(change):
    if membership_needs_fresh_preparation(change):
        from shlex import join
        node = change.requested_components[0][0]
        reset, fresh = join(['mwf', 'reset', node]), join(['mwf', 'run', node])
        raise RuntimeError(
            'Component membership changed while reusable work exists; '
            f'use {reset} or {fresh} for full fresh preparation before resume or selected execution'
        )


def membership_repair_lines(change):
    if not membership_needs_fresh_preparation(change):
        return ()
    render = lambda components: ', '.join('{' + ', '.join(component) + '}' for component in components)
    return (
        'membership repair preparation: ' + render(change.preparation_components),
        'historical membership overlap: ' + render(change.source_components),
    ) + (('removed historical members: ' + ', '.join(change.removed_members),)
         if change.removed_members else ())
