"""Observe and atomically replace independently established memberships."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from micro_workflow_manager.component_identity import (
    component_key,
    decode_component_key,
    encode_component_key,
)
from .base import FileStorageBase

if TYPE_CHECKING:
    from collections.abc import Iterable
    from micro_workflow_manager.topology import ComponentTopologySnapshot


ComponentMembers = tuple[str, ...]


def _members(value, label: str) -> ComponentMembers:
    if isinstance(value, (str, bytes)):
        raise ValueError(f'{label} must contain raw-node names')
    try:
        supplied = tuple(value)
    except TypeError as error:
        raise ValueError(f'{label} must contain raw-node names') from error
    if not supplied or any(not isinstance(node, str) for node in supplied):
        raise ValueError(f'{label} must contain raw-node names')
    for node in supplied:
        FileStorageBase.validate_node_name(node)
    normalized = component_key(supplied)
    if len(normalized) != len(supplied):
        raise ValueError(f'{label} contains duplicate raw-node names')
    return normalized


def _ordered_components(values, label: str) -> tuple[ComponentMembers, ...]:
    result = tuple(_members(value, label) for value in values)
    if len(set(result)) != len(result):
        raise ValueError(f'{label} contains duplicate components')
    return result


@dataclass(frozen=True)
class ActiveComponent:
    members: ComponentMembers
    established_revision: int

    def __post_init__(self):
        object.__setattr__(self, 'members', _members(self.members, 'Active component'))
        if type(self.established_revision) is not int or self.established_revision < 0:
            raise ValueError('Established revision must be a nonnegative integer')


@dataclass(frozen=True)
class ActiveComponentPartition:
    revision: int
    components: tuple[ActiveComponent, ...]

    def __post_init__(self):
        if type(self.revision) is not int or self.revision < 0:
            raise ValueError('Membership revision must be a nonnegative integer')
        if any(not isinstance(item, ActiveComponent) for item in self.components):
            raise ValueError('Active partition must contain active components')
        components = tuple(sorted(self.components, key=lambda item: item.members))
        if len({item.members for item in components}) != len(components):
            raise ValueError('Active partition contains a duplicate component')
        claimed = set()
        for item in components:
            if item.established_revision > self.revision:
                raise ValueError('Active component was established after the partition revision')
            if claimed.intersection(item.members):
                raise ValueError('Active components overlap')
            claimed.update(item.members)
        object.__setattr__(self, 'components', components)


@dataclass(frozen=True)
class HistoricalMembership:
    """One reliable membership/generation identity from the root history reader."""

    members: ComponentMembers
    shape_id: int
    alignment_generation: int
    has_reusable_work: bool

    def __post_init__(self):
        object.__setattr__(self, 'members', _members(self.members, 'Historical component'))
        if type(self.shape_id) is not int or self.shape_id < 1:
            raise ValueError('Historical shape ID must be a positive integer')
        if type(self.alignment_generation) is not int or self.alignment_generation < 0:
            raise ValueError('Historical alignment generation must be a nonnegative integer')
        if type(self.has_reusable_work) is not bool:
            raise ValueError('Historical reusable-work marker must be Boolean')


@dataclass(frozen=True)
class ComponentPreparationScope:
    members: ComponentMembers
    generation_floor: int | None

    def __post_init__(self):
        object.__setattr__(self, 'members', _members(self.members, 'Preparation component'))
        if (self.generation_floor is not None
                and (type(self.generation_floor) is not int or self.generation_floor < 0)):
            raise ValueError('Preparation generation floor must be a nonnegative integer or None')


@dataclass(frozen=True)
class MembershipChange:
    source: ActiveComponentPartition
    target_shape_json: str
    requested_components: tuple[ComponentMembers, ...]
    source_components: tuple[ComponentMembers, ...]
    target_components: tuple[ComponentMembers, ...]
    preparation: tuple[ComponentPreparationScope, ...]
    historical_memberships: tuple[HistoricalMembership, ...]
    removed_members: tuple[str, ...]

    @property
    def preparation_components(self) -> tuple[ComponentMembers, ...]:
        return tuple(item.members for item in self.preparation)

    @property
    def changes_active_membership(self) -> bool:
        return set(self.source_components) != set(self.target_components)


def _read_partition_revision(connection) -> int | None:
    rows = connection.execute(
        'SELECT singleton, revision FROM component_partition_state',
    ).fetchall()
    if not rows:
        active = connection.execute('SELECT 1 FROM active_components LIMIT 1').fetchone()
        members = connection.execute('SELECT 1 FROM active_component_members LIMIT 1').fetchone()
        if active is not None or members is not None:
            raise RuntimeError('Active membership rows have no partition state')
        return None
    if len(rows) != 1 or rows[0]['singleton'] != 1:
        raise RuntimeError('Invalid component partition state')
    revision = rows[0]['revision']
    if type(revision) is not int or revision < 0:
        raise RuntimeError('Invalid component partition revision')
    return revision


def _decode_active_component(connection, row, revision: int) -> ActiveComponent:
    try:
        members = _members(decode_component_key(row['component_key']), 'Stored active component')
    except (TypeError, ValueError) as error:
        raise RuntimeError('Invalid stored active component') from error
    if encode_component_key(members) != row['component_key']:
        raise RuntimeError('Stored active component key is not canonical')
    recorded = tuple(
        member['node_name']
        for member in connection.execute(
            'SELECT node_name FROM active_component_members WHERE component_key=? ORDER BY node_name',
            (row['component_key'],),
        )
    )
    if recorded != members:
        raise RuntimeError('Active component disagrees with its member rows')
    try:
        result = ActiveComponent(members, row['established_revision'])
    except ValueError as error:
        raise RuntimeError('Invalid active component revision') from error
    if result.established_revision > revision:
        raise RuntimeError('Active component was established after the partition revision')
    return result


def read_active_component_partition(connection) -> ActiveComponentPartition | None:
    """Read and validate the complete active regional membership."""
    revision = _read_partition_revision(connection)
    if revision is None:
        return None
    rows = connection.execute(
        'SELECT component_key, established_revision FROM active_components ORDER BY component_key',
    ).fetchall()
    components = tuple(_decode_active_component(connection, row, revision) for row in rows)
    member_count = connection.execute(
        'SELECT COUNT(*) AS count FROM active_component_members',
    ).fetchone()['count']
    if member_count != sum(len(item.members) for item in components):
        raise RuntimeError('Active membership contains an orphan member row')
    try:
        return ActiveComponentPartition(revision, components)
    except ValueError as error:
        raise RuntimeError('Invalid active component partition') from error


def read_active_component_for_node(connection, node_name: str) -> ComponentMembers | None:
    """Resolve one receiver without scanning unrelated historical definitions."""
    FileStorageBase.validate_node_name(node_name)
    revision = _read_partition_revision(connection)
    if revision is None:
        return None
    rows = connection.execute(
        'SELECT component.component_key, component.established_revision '
        'FROM active_component_members AS member '
        'JOIN active_components AS component USING(component_key) '
        'WHERE member.node_name=?',
        (node_name,),
    ).fetchall()
    if not rows:
        raise RuntimeError('Raw node has no active component membership')
    if len(rows) != 1:
        raise RuntimeError('Raw node belongs to more than one active component')
    component = _decode_active_component(connection, rows[0], revision)
    if node_name not in component.members:
        raise RuntimeError('Active component lookup returned the wrong raw node')
    return component.members


def initialize_active_component_partition(
    connection, components: Iterable[Iterable[str]],
) -> ActiveComponentPartition:
    """Initialize active membership once, inside the caller's writer transaction."""
    if not connection.in_transaction:
        raise RuntimeError('Active membership initialization requires a writer transaction')
    if read_active_component_partition(connection) is not None:
        raise RuntimeError('Active component membership is already initialized')
    normalized = tuple(sorted(_ordered_components(components, 'Initial membership')))
    ActiveComponentPartition(0, tuple(ActiveComponent(item, 0) for item in normalized))
    if connection.execute(
        'INSERT INTO component_partition_state(singleton, revision) VALUES(1, 0)',
    ).rowcount != 1:
        raise RuntimeError('Active component partition was not initialized')
    inserted = 0 if not normalized else connection.executemany(
        'INSERT INTO active_components(component_key, established_revision) VALUES(?, 0)',
        [(encode_component_key(item),) for item in normalized],
    ).rowcount
    if inserted != len(normalized):
        raise RuntimeError('Initial active components were not recorded')
    member_rows = [
        (node, encode_component_key(item))
        for item in normalized
        for node in item
    ]
    inserted = 0 if not member_rows else connection.executemany(
        'INSERT INTO active_component_members(node_name, component_key) VALUES(?, ?)',
        member_rows,
    ).rowcount
    if inserted != len(member_rows):
        raise RuntimeError('Initial active component members were not recorded')
    result = read_active_component_partition(connection)
    if result is None:
        raise RuntimeError('Active component partition disappeared during initialization')
    return result


def observe_membership_change(
    active: ActiveComponentPartition,
    target_snapshot: ComponentTopologySnapshot,
    requested_components: Iterable[Iterable[str]],
    historical_memberships: Iterable[HistoricalMembership],
) -> MembershipChange:
    """Calculate regional reconciliation and preparation as separate scopes."""
    from micro_workflow_manager.topology import ComponentTopologySnapshot

    if not isinstance(active, ActiveComponentPartition):
        raise ValueError('Expected the active component partition')
    if not isinstance(target_snapshot, ComponentTopologySnapshot):
        raise ValueError('Expected the target component topology snapshot')
    if not isinstance(target_snapshot.shape_json, str) or not target_snapshot.shape_json:
        raise ValueError('Target graph shape must be nonempty text')
    target = _ordered_components(target_snapshot.components, 'Target membership')
    if target != tuple(sorted(target)):
        raise ValueError('Target components must use canonical snapshot order')
    requested = _ordered_components(requested_components, 'Requested membership')
    target_set = set(target)
    if not requested or any(item not in target_set for item in requested):
        raise ValueError('Requested components must belong to the target graph')
    history = tuple(historical_memberships)
    if any(not isinstance(item, HistoricalMembership) for item in history):
        raise ValueError('Expected historical membership observations')
    history = tuple(sorted(
        history,
        key=lambda item: (item.shape_id, item.members, item.alignment_generation),
    ))
    identities = [(item.members, item.shape_id, item.alignment_generation) for item in history]
    if len(set(identities)) != len(identities):
        raise ValueError('Historical membership observations contain duplicate identities')

    selected_target = set(requested)
    selected_active: set[ComponentMembers] = set()
    selected_reusable: set[HistoricalMembership] = set()
    while True:
        before = len(selected_target), len(selected_active), len(selected_reusable)
        reached = {
            node
            for members in selected_target | selected_active
            for node in members
        }
        reached.update(
            node for item in selected_reusable for node in item.members
        )
        selected_active.update(
            item.members for item in active.components if reached.intersection(item.members)
        )
        reached.update(node for members in selected_active for node in members)
        selected_reusable.update(
            item for item in history
            if item.has_reusable_work and reached.intersection(item.members)
        )
        reached.update(node for item in selected_reusable for node in item.members)
        selected_target.update(
            item for item in target if reached.intersection(item)
        )
        after = len(selected_target), len(selected_active), len(selected_reusable)
        if after == before:
            break

    affected_nodes = {
        node
        for members in selected_target | selected_active
        for node in members
    }
    affected_nodes.update(node for item in selected_reusable for node in item.members)
    relevant_history = tuple(
        item for item in history if affected_nodes.intersection(item.members)
    )
    reusable_nodes = {node for item in selected_reusable for node in item.members}
    preparation_components = set(requested)
    preparation_components.update(
        item for item in selected_target if reusable_nodes.intersection(item)
    )
    preparation = []
    for members in target:
        if members not in preparation_components:
            continue
        generations = [
            item.alignment_generation
            for item in relevant_history
            if set(members).intersection(item.members)
        ]
        preparation.append(ComponentPreparationScope(
            members,
            max(generations) if generations else None,
        ))
    target_nodes = {node for members in target for node in members}
    removed = tuple(sorted(affected_nodes - target_nodes))
    return MembershipChange(
        source=active,
        target_shape_json=target_snapshot.shape_json,
        requested_components=requested,
        source_components=tuple(sorted(selected_active)),
        target_components=tuple(item for item in target if item in selected_target),
        preparation=tuple(preparation),
        historical_memberships=relevant_history,
        removed_members=removed,
    )


def validate_membership_change(connection, change: MembershipChange) -> None:
    """Recheck the change shape and exact source partition in one DB snapshot."""
    if not isinstance(change, MembershipChange):
        raise ValueError('Expected a membership change')
    current = read_active_component_partition(connection)
    if current != change.source:
        raise RuntimeError('Active component membership changed after observation')
    from .component_definitions import component_snapshot_from_shape

    try:
        target = component_snapshot_from_shape(change.target_shape_json)
        if change.requested_components:
            expected = observe_membership_change(
                change.source, target, change.requested_components, change.historical_memberships,
            )
        else:
            from .membership_retirement import observe_removed_memberships
            expected = observe_removed_memberships(change.source, target, change.historical_memberships)
    except ValueError as error:
        raise RuntimeError('Invalid observed membership change') from error
    if expected != change:
        raise RuntimeError('Membership change does not match its observations')


def _replace_active_component_closure(
    connection, change: MembershipChange,
) -> ActiveComponentPartition:
    """Replace a closure after the caller validates receipts and history in this transaction."""
    if not connection.in_transaction:
        raise RuntimeError('Active membership replacement requires a writer transaction')
    validate_membership_change(connection, change)
    current = change.source
    source = set(change.source_components)
    target = set(change.target_components)
    known = {item.members for item in current.components}
    if not source.issubset(known):
        raise RuntimeError('Membership replacement source is not active')
    unaffected = known - source
    unaffected_nodes = {node for members in unaffected for node in members}
    target_nodes = {node for members in target for node in members}
    if unaffected_nodes.intersection(target_nodes):
        raise RuntimeError('Membership replacement overlaps an unaffected active component')
    if source == target:
        return current

    source_member_count = sum(len(item) for item in source)
    deleted = 0 if not source else connection.executemany(
        'DELETE FROM active_component_members WHERE component_key=?',
        [(encode_component_key(item),) for item in sorted(source)],
    ).rowcount
    if deleted != source_member_count:
        raise RuntimeError('Active component members changed before replacement')
    deleted = 0 if not source else connection.executemany(
        'DELETE FROM active_components WHERE component_key=?',
        [(encode_component_key(item),) for item in sorted(source)],
    ).rowcount
    if deleted != len(source):
        raise RuntimeError('Active components changed before replacement')

    revision = current.revision + 1
    inserted = 0 if not target else connection.executemany(
        'INSERT INTO active_components(component_key, established_revision) VALUES(?, ?)',
        [(encode_component_key(item), revision) for item in sorted(target)],
    ).rowcount
    if inserted != len(target):
        raise RuntimeError('Replacement active components were not recorded')
    member_rows = [
        (node, encode_component_key(item))
        for item in sorted(target)
        for node in item
    ]
    inserted = 0 if not member_rows else connection.executemany(
        'INSERT INTO active_component_members(node_name, component_key) VALUES(?, ?)',
        member_rows,
    ).rowcount
    if inserted != len(member_rows):
        raise RuntimeError('Replacement active component members were not recorded')
    changed = connection.execute(
        'UPDATE component_partition_state SET revision=? WHERE singleton=1 AND revision=?',
        (revision, current.revision),
    ).rowcount
    if changed != 1:
        raise RuntimeError('Component partition revision changed before replacement')

    result = read_active_component_partition(connection)
    expected = unaffected | target
    if result is None or result.revision != revision or {
        item.members for item in result.components
    } != expected:
        raise RuntimeError('Active membership replacement did not persist exactly')
    return result
