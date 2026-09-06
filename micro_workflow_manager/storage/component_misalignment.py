from __future__ import annotations

import os

from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.file_helpers import _relative_file_parts
from .component_definitions import component_snapshot_from_shape


class ComponentMisalignmentStorageMixin:
    """Keep the first managed arrival at each receiver's established result."""

    def read_component_misalignment_causes(self, component, *, expected_shape=None):
        self._require_execution_session_storage()
        members = self._session_component(component)
        if expected_shape is not None:
            self._session_text(expected_shape, 'expected_shape')
        connection = self.db_connection()
        connection.execute('SAVEPOINT mwf_misalignment_observation')
        try:
            state = self._read_component_state(connection, members)
            if expected_shape is not None and (state is None or state['shape_json'] != expected_shape):
                raise RuntimeError('Misalignment causes require the expected component shape')
            return [] if state is None else self._read_component_arrival_causes(connection, state)
        finally:
            connection.execute('RELEASE SAVEPOINT mwf_misalignment_observation')

    def _read_component_arrival_causes(self, connection, state):
        rows = connection.execute(
            'SELECT * FROM component_misalignment_causes WHERE component_key=? AND alignment_generation=? '
            'ORDER BY receiver_node',
            (encode_component_key(state['members']), state['alignment_generation']),
        ).fetchall()
        causes = [self._decode_component_arrival_cause(connection, row, state) for row in rows]
        if bool(causes) != state['misaligned']:
            raise RuntimeError('Component misalignment and current first causes disagree')
        return causes

    def _decode_component_arrival_cause(self, connection, row, state):
        shape = connection.execute('SELECT shape_json FROM graph_shapes WHERE shape_id=?',
                                   (row['shape_id'],)).fetchone()
        if (row['component_key'] != encode_component_key(state['members'])
                or row['receiver_node'] not in state['members']
                or type(row['alignment_generation']) is not int
                or row['alignment_generation'] != state['alignment_generation']
                or shape is None or shape['shape_json'] != state['shape_json']
                or row['arrival_kind'] != 'managed-input'):
            raise RuntimeError('Damaged component misalignment cause')
        owner = self._read_execution_owner(connection, row['producer_execution_id'])
        try:
            parts = _relative_file_parts(row['relative_path'])
        except (TypeError, ValueError) as error:
            raise RuntimeError('Damaged component misalignment path') from error
        if (owner is None or owner['shape_id'] != row['shape_id'] or len(parts) < 2
                or parts[0] != owner['node_name'] or '/'.join(parts) != row['relative_path']):
            raise RuntimeError('Damaged component misalignment producer or path')
        self._validate_input_edge(connection, owner, row['receiver_node'])
        return {
            'receiver_node': row['receiver_node'], 'alignment_generation': row['alignment_generation'],
            'producer_node': owner['node_name'], 'producer_job_id': owner['job_id'],
            'arrival_kind': row['arrival_kind'], 'path': row['relative_path'],
        }

    def _mark_component_input_arrival(self, connection, receiver, owner, relative):
        shape = connection.execute('SELECT shape_json FROM graph_shapes WHERE shape_id=?',
                                   (owner['shape_id'],)).fetchone()
        if shape is None:
            raise RuntimeError('Managed input producing graph shape is missing')
        snapshot = component_snapshot_from_shape(shape['shape_json'])
        members, = (component for component in snapshot.components if receiver in component)
        state = self._read_component_state(connection, members)
        if state is None or state['shape_json'] != shape['shape_json']:
            raise RuntimeError('Managed input receiver requires its matching component shape')
        identity = (os.getpid(), owner['shape_id'], members, state['alignment_generation'])
        if state['misaligned'] and self._component_arrival_latches.get(receiver) == identity:
            return identity
        causes = self._read_component_arrival_causes(connection, state)
        if state['lifecycle'] in ('queued', 'running'):
            self._component_arrival_latches.pop(receiver, None)
            return None
        if any(cause['receiver_node'] == receiver for cause in causes):
            return identity
        if not state['misaligned']:
            changed = connection.execute(
                "UPDATE component_states SET misaligned=1 WHERE component_key=? AND misaligned=0 "
                "AND alignment_generation=? AND lifecycle IN ('done','sampled','failed')",
                (encode_component_key(members), state['alignment_generation']),
            ).rowcount
            if changed != 1:
                raise RuntimeError('Managed input receiver changed before publication')
        connection.execute(
            'INSERT INTO component_misalignment_causes VALUES(?,?,?,?,?,?,?) '
            'ON CONFLICT(receiver_node,alignment_generation) DO NOTHING',
            (receiver, state['alignment_generation'], encode_component_key(members), owner['shape_id'],
             owner['execution_id'], 'managed-input', relative),
        )
        recorded = connection.execute(
            'SELECT * FROM component_misalignment_causes WHERE receiver_node=? AND alignment_generation=?',
            (receiver, state['alignment_generation']),
        ).fetchone()
        if recorded is None:
            raise RuntimeError('Managed input receiver lost its first cause')
        self._decode_component_arrival_cause(connection, recorded, state)
        return identity
