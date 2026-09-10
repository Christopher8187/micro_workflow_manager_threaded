"""Validate current component state, admitted shape, and retained results."""

from __future__ import annotations

from dataclasses import dataclass

from micro_workflow_manager.component_identity import component_key, decode_component_key, encode_component_key
from .base import FileStorageBase
from .component_definitions import component_snapshot_from_shape


@dataclass(frozen=True)
class ComponentGenerationIdentity:
    shape_id: int
    alignment_generation: int

    def __post_init__(self):
        if type(self.shape_id) is not int or self.shape_id < 1:
            raise ValueError('Component shape ID must be a positive integer')
        if type(self.alignment_generation) is not int or self.alignment_generation < 0:
            raise ValueError('Component alignment generation must be a nonnegative integer')

    @property
    def values(self):
        return self.shape_id, self.alignment_generation


@dataclass(frozen=True)
class SuccessfulResultObservation:
    identity: ComponentGenerationIdentity
    lifecycle: str
    stability: str
    instability_origin: str | None

    def __post_init__(self):
        if not isinstance(self.identity, ComponentGenerationIdentity):
            raise ValueError('Successful result requires its component generation identity')
        valid_lineage = (
            self.stability == 'stable' and self.instability_origin is None
        ) or (
            self.stability == 'unstable'
            and isinstance(self.instability_origin, str)
            and bool(self.instability_origin.strip())
        )
        if self.lifecycle not in ('sampled', 'done') or not valid_lineage:
            raise ValueError('Invalid successful component result observation')

    @property
    def result(self):
        return self.lifecycle, self.stability, self.instability_origin

    @property
    def lineage(self):
        return self.stability, self.instability_origin


@dataclass(frozen=True)
class ComponentStateRecord:
    members: tuple[str, ...]
    identity: ComponentGenerationIdentity
    shape_json: str
    lifecycle: str
    stability: str | None
    instability_origin: str | None
    misaligned: bool
    retained_result_identity: ComponentGenerationIdentity | None

    @property
    def snapshot(self):
        return {
            'members': self.members,
            'shape_json': self.shape_json,
            'lifecycle': self.lifecycle,
            'stability': self.stability,
            'instability_origin': self.instability_origin,
            'misaligned': self.misaligned,
            'alignment_generation': self.identity.alignment_generation,
        }


@dataclass(frozen=True)
class AdmittedComponentShape:
    shape_id: int
    shape_json: str
    partition_revision: int


def _component(value) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise ValueError('A component requires raw-node names')
    try:
        supplied = tuple(value)
    except TypeError as error:
        raise ValueError('A component requires raw-node names') from error
    if not supplied:
        raise ValueError('A component requires raw-node names')
    for node in supplied:
        FileStorageBase.validate_node_name(node)
    normalized = component_key(supplied)
    if len(normalized) != len(supplied):
        raise ValueError('A component cannot contain duplicate raw-node names')
    return normalized


def _lineage_is_valid(stability, origin, origin_kind, origin_scope_admitted):
    return (
        stability == 'stable'
        and origin is None
        and origin_kind is None
        and origin_scope_admitted is None
    ) or (
        stability == 'unstable'
        and isinstance(origin, str)
        and bool(origin.strip())
        and origin_kind == 'interrupt'
        and type(origin_scope_admitted) is int
        and origin_scope_admitted == 1
    )


def read_successful_result(connection, component, identity):
    members = _component(component)
    if not isinstance(identity, ComponentGenerationIdentity):
        identity = ComponentGenerationIdentity(*identity)
    row = connection.execute(
        'SELECT result.*, definition.component_key AS definition_key, shape.shape_json, '
        'origin.session_kind AS origin_kind, origin.scope_admitted AS origin_scope_admitted '
        'FROM component_successful_results AS result '
        'LEFT JOIN component_definitions AS definition '
        'ON definition.component_key=result.component_key AND definition.shape_id=result.shape_id '
        'LEFT JOIN graph_shapes AS shape ON shape.shape_id=result.shape_id '
        'LEFT JOIN execution_sessions AS origin ON origin.session_id=result.instability_origin '
        'WHERE result.component_key=? AND result.shape_id=? AND result.alignment_generation=?',
        (encode_component_key(members), *identity.values),
    ).fetchone()
    if row is None:
        return None
    if row['definition_key'] != encode_component_key(members) or row['shape_json'] is None:
        raise RuntimeError('Retained successful result lost its historical component definition')
    try:
        producing = component_snapshot_from_shape(row['shape_json'])
    except ValueError as error:
        raise RuntimeError('Invalid retained successful result graph shape') from error
    if members not in producing.components:
        raise RuntimeError('Retained successful result differs from its historical component')
    if row['lifecycle'] not in ('sampled', 'done') or not _lineage_is_valid(
        row['stability'], row['instability_origin'], row['origin_kind'], row['origin_scope_admitted'],
    ):
        raise RuntimeError('Invalid retained successful component result')
    return SuccessfulResultObservation(
        identity, row['lifecycle'], row['stability'], row['instability_origin'],
    )


def _validate_active_membership(connection, members):
    from .component_membership import read_active_component_for_node

    for node in members:
        if read_active_component_for_node(connection, node) != members:
            raise RuntimeError('Component differs from the active membership: ' + encode_component_key(members))


def read_component_state_record(connection, component, *, require_active=True):
    members = _component(component)
    key = encode_component_key(members)
    row = connection.execute(
        'SELECT state.*, definition.component_key AS definition_key, shape.shape_json, '
        'origin.session_kind AS origin_kind, origin.scope_admitted AS origin_scope_admitted '
        'FROM component_states AS state '
        'LEFT JOIN component_definitions AS definition '
        'ON definition.component_key=state.component_key AND definition.shape_id=state.shape_id '
        'LEFT JOIN graph_shapes AS shape ON shape.shape_id=state.shape_id '
        'LEFT JOIN execution_sessions AS origin ON origin.session_id=state.instability_origin '
        'WHERE state.component_key=?',
        (key,),
    ).fetchone()
    if row is None:
        if connection.execute(
            'SELECT 1 FROM component_definitions WHERE component_key=? LIMIT 1', (key,),
        ).fetchone() is not None:
            raise RuntimeError('Component definition has no current state: ' + key)
        return None
    if row['definition_key'] != key or row['shape_json'] is None:
        raise RuntimeError('Component state has no historical definition: ' + key)
    try:
        producing = component_snapshot_from_shape(row['shape_json'])
    except ValueError as error:
        raise RuntimeError('Invalid component state producing shape') from error
    if members not in producing.components:
        raise RuntimeError('Component state differs from its historical definition: ' + key)
    if require_active:
        _validate_active_membership(connection, members)
    if type(row['alignment_generation']) is not int or row['alignment_generation'] < 0:
        raise RuntimeError('Invalid component alignment generation: ' + key)
    identity = ComponentGenerationIdentity(row['shape_id'], row['alignment_generation'])
    retained_values = row['retained_result_shape_id'], row['retained_result_alignment_generation']
    if (retained_values[0] is None) != (retained_values[1] is None):
        raise RuntimeError('Incomplete retained component result identity: ' + key)
    retained_identity = None if retained_values[0] is None else ComponentGenerationIdentity(*retained_values)
    if (retained_identity is not None
            and retained_identity.alignment_generation != identity.alignment_generation):
        raise RuntimeError('Retained component result has a different alignment generation: ' + key)
    retained = None if retained_identity is None else read_successful_result(
        connection, members, retained_identity,
    )
    if retained_identity is not None and retained is None:
        raise RuntimeError('Component lost its retained successful result: ' + key)
    no_lineage = row['stability'] is None and row['instability_origin'] is None
    result_lineage = _lineage_is_valid(
        row['stability'], row['instability_origin'], row['origin_kind'], row['origin_scope_admitted'],
    )
    lifecycle = row['lifecycle']
    valid_lifecycle = (
        (lifecycle == 'queued' and no_lineage and row['misaligned'] == 0)
        or (lifecycle == 'running' and (no_lineage or result_lineage))
        or (lifecycle in ('sampled', 'done') and result_lineage)
        or (lifecycle == 'failed' and no_lineage)
    )
    if not valid_lifecycle or type(row['misaligned']) is not int or row['misaligned'] not in (0, 1):
        raise RuntimeError('Invalid component state for ' + key)
    if lifecycle == 'queued' and retained is not None:
        raise RuntimeError('Queued component unexpectedly retains successful history: ' + key)
    if lifecycle in ('sampled', 'done'):
        if retained is None or retained.result != (
            lifecycle, row['stability'], row['instability_origin'],
        ):
            raise RuntimeError('Component state differs from its retained successful result: ' + key)
    if lifecycle == 'running':
        if ((no_lineage and retained is not None)
                or (not no_lineage and (
                    retained is None
                    or retained.lineage != (row['stability'], row['instability_origin'])
                ))):
            raise RuntimeError('Running component differs from its retained successful result: ' + key)
    return ComponentStateRecord(
        members, identity, row['shape_json'], lifecycle, row['stability'],
        row['instability_origin'], bool(row['misaligned']), retained_identity,
    )


def read_retained_successful_result(connection, state):
    if not isinstance(state, ComponentStateRecord):
        raise ValueError('Expected a component state record')
    if state.retained_result_identity is None:
        return None
    result = read_successful_result(connection, state.members, state.retained_result_identity)
    if result is None:
        raise RuntimeError('Component lost its retained successful result')
    return result


def observe_current_success(connection, state):
    if not isinstance(state, ComponentStateRecord):
        raise ValueError('Expected a component state record')
    retained = read_retained_successful_result(connection, state)
    if retained is not None:
        return retained
    if state.lifecycle in ('sampled', 'done'):
        raise RuntimeError('Component lost its retained successful result')
    return None


def read_admitted_component_shape(connection, session_id, component, expected_shape):
    members = _component(component)
    key = encode_component_key(members)
    row = connection.execute(
        'SELECT session.status, session.admitted_shape_id, session.partition_revision, '
        'selected.component_key AS selected_key, reservation.session_id AS reservation_owner '
        'FROM execution_sessions AS session '
        'LEFT JOIN session_components AS selected '
        'ON selected.session_id=session.session_id AND selected.component_key=? '
        'LEFT JOIN component_reservations AS reservation ON reservation.component_key=? '
        'WHERE session.session_id=?',
        (key, key, session_id),
    ).fetchone()
    if row is None or row['status'] != 'running' or row['selected_key'] != key:
        raise RuntimeError('Component start requires its selected running session: ' + key)
    if row['reservation_owner'] != session_id:
        raise RuntimeError('Component start requires its exact reservation: ' + key)
    if type(row['admitted_shape_id']) is not int or row['admitted_shape_id'] < 1:
        raise RuntimeError('Component session has no admitted graph shape: ' + key)
    revision = row['partition_revision']
    if type(revision) is not int or revision < 0:
        raise RuntimeError('Component session has no admitted membership revision: ' + key)
    from .component_membership import read_active_component_partition
    from .session_shapes import validate_session_shape_snapshot

    selected = []
    for item in connection.execute(
        'SELECT component_key FROM session_components WHERE session_id=? ORDER BY position',
        (session_id,),
    ):
        try:
            decoded = tuple(decode_component_key(item['component_key']))
        except (TypeError, ValueError) as error:
            raise RuntimeError('Session has an invalid selected component') from error
        if encode_component_key(decoded) != item['component_key']:
            raise RuntimeError('Session has a noncanonical selected component')
        selected.append(decoded)
    shape_json = validate_session_shape_snapshot(connection, {
        'admitted_shape_id': row['admitted_shape_id'],
        'partition_revision': revision,
        'selected_components': selected,
    })
    if shape_json != expected_shape:
        raise RuntimeError('Component start differs from its admitted graph shape: ' + key)

    active = read_active_component_partition(connection)
    if active is None or active.revision < revision:
        raise RuntimeError('Active component membership predates session admission')
    established = next((item for item in active.components if item.members == members), None)
    if established is None or established.established_revision > revision:
        raise RuntimeError('Selected component membership changed after session admission')
    _validate_active_membership(connection, members)
    if members not in selected:
        raise RuntimeError('Component is outside its admitted session selection: ' + key)
    return AdmittedComponentShape(row['admitted_shape_id'], expected_shape, revision)
