from __future__ import annotations

import os
import json

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key
from micro_workflow_manager.file_helpers import _relative_file_parts
from .component_definitions import component_snapshot_from_shape
from .component_membership import read_active_component_for_node
from .membership_receipts import validate_membership_receipt_producer


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
            if expected_shape is not None:
                from .component_states import read_component_states_snapshot
                read_component_states_snapshot(connection, (members,), expected_shape=expected_shape)
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
                or shape is None):
            raise RuntimeError('Damaged component misalignment cause')
        if state['members'] not in component_snapshot_from_shape(shape['shape_json']).components:
            raise RuntimeError('Misalignment cause has a different historical receiver component')
        if row['preparation_kind'] is not None:
            return self._decode_component_preparation_cause(connection, row, state)
        if (row['arrival_kind'] not in ('managed-input', 'managed-job')
                or any(row[key] is not None for key in ('operation', 'action', 'affected_kind', 'preparation_id'))):
            raise RuntimeError('Damaged component misalignment arrival')
        if row['arrival_kind'] == 'managed-job':
            return self._decode_component_job_cause(connection, row)
        owner = self._read_execution_owner(connection, row['producer_execution_id'])
        try:
            parts = _relative_file_parts(row['relative_path'])
        except (TypeError, ValueError) as error:
            raise RuntimeError('Damaged component misalignment path') from error
        if (owner is None or len(parts) < 2
                or row['receiver_job_id'] is not None or row['receiver_job_instance_id'] is not None
                or parts[0] != owner['node_name'] or '/'.join(parts) != row['relative_path']):
            raise RuntimeError('Damaged component misalignment producer or path')
        self._validate_input_edge(connection, owner, row['receiver_node'])
        return {
            'receiver_node': row['receiver_node'], 'alignment_generation': row['alignment_generation'],
            'producer_node': owner['node_name'], 'producer_job_id': owner['job_id'],
            'arrival_kind': row['arrival_kind'], 'path': row['relative_path'],
        }

    def _decode_component_job_cause(self, connection, row):
        instance = row['receiver_job_instance_id']
        if (row['relative_path'] is not None or type(row['receiver_job_id']) is not int
                or row['receiver_job_id'] < 1 or type(instance) is not str or len(instance) != 32
                or any(character not in '0123456789abcdef' for character in instance)):
            raise RuntimeError('Damaged component misalignment receiving job')
        owner = None
        if row['producer_execution_id'] is not None:
            owner = self._read_execution_owner(connection, row['producer_execution_id'])
            if owner is None:
                raise RuntimeError('Damaged component misalignment job producer')
        current = connection.execute(
            'SELECT instance_id, created_by_execution_id FROM job_instances WHERE node_name=? AND job_id=?',
            (row['receiver_node'], row['receiver_job_id']),
        ).fetchone()
        if (current is not None and current['instance_id'] == instance
                and current['created_by_execution_id'] != row['producer_execution_id']):
            raise RuntimeError('Component misalignment job creator disagrees with its instance')
        return {
            'receiver_node': row['receiver_node'], 'alignment_generation': row['alignment_generation'],
            'producer_node': None if owner is None else owner['node_name'],
            'producer_job_id': None if owner is None else owner['job_id'],
            'arrival_kind': 'managed-job', 'job_id': row['receiver_job_id'],
        }

    def _decode_component_preparation_cause(self, connection, row, state):
        owner = self._read_execution_owner(connection, row['producer_execution_id'])
        receipt = connection.execute('SELECT * FROM preparation_receipts WHERE operation_id=?',
                                     (row['preparation_id'],)).fetchone()
        if (row['arrival_kind'] is not None or row['preparation_kind'] != 'preparation-removal'
                or row['action'] != 'delete' or row['affected_kind'] not in ('managed-input', 'managed-job')
                or owner is None or receipt is None or receipt['state'] != 'committed'
                or receipt['operation'] != row['operation']):
            raise RuntimeError('Damaged component preparation cause')
        validate_membership_receipt_producer(connection, receipt, owner)
        effect = {'receiver': row['receiver_node'], 'kind': row['affected_kind'],
                  'producer_execution_id': row['producer_execution_id']}
        result = {
            'component': state['members'], 'receiver_node': row['receiver_node'],
            'alignment_generation': row['alignment_generation'],
            'producer_node': owner['node_name'], 'producer_job_id': owner['job_id'],
            'preparation_kind': row['preparation_kind'], 'operation': row['operation'],
            'action': row['action'], 'affected_kind': row['affected_kind'],
        }
        if row['affected_kind'] == 'managed-input':
            try:
                parts = _relative_file_parts(row['relative_path'])
            except (TypeError, ValueError) as error:
                raise RuntimeError('Damaged preparation input path') from error
            if (len(parts) < 2 or parts[0] != owner['node_name'] or '/'.join(parts) != row['relative_path']
                    or row['receiver_job_id'] is not None or row['receiver_job_instance_id'] is not None):
                raise RuntimeError('Damaged preparation input producer or path')
            self._validate_input_edge(connection, owner, row['receiver_node'])
            effect['path'] = result['path'] = row['relative_path']
        else:
            instance = row['receiver_job_instance_id']
            if (row['relative_path'] is not None or type(row['receiver_job_id']) is not int
                    or row['receiver_job_id'] < 1 or type(instance) is not str or len(instance) != 32
                    or any(character not in '0123456789abcdef' for character in instance)):
                raise RuntimeError('Damaged preparation receiving job')
            effect['job_id'] = result['job_id'] = row['receiver_job_id']
            effect['job_instance_id'] = result['job_instance_id'] = instance
        try:
            effects = json.loads(receipt['manifest_json'])['effects']
        except (ValueError, TypeError, KeyError) as error:
            raise RuntimeError('Damaged preparation receipt effects') from error
        if not isinstance(effects, list) or effect not in effects:
            raise RuntimeError('Preparation cause disagrees with its recorded mutation')
        return result

    def _mark_component_preparation_change(self, connection, effect, receipt):
        receiver = effect['receiver']
        observed = self._read_job_receiver_state(connection, receiver, None)
        if observed is None:
            return None
        shape_id, state = observed
        return self._mark_component_arrival(
            connection, receiver, shape_id, state, effect['producer_execution_id'], None,
            relative=effect.get('path'), job_id=effect.get('job_id'), job_instance_id=effect.get('job_instance_id'),
            preparation=('preparation-removal', receipt.operation, 'delete', effect['kind'], receipt.operation_id),
        )

    def validate_job_receiver_shape(self, receiver, *, expected_shape=None):
        receiver = self.validate_node_name(receiver)
        connection = self.db_connection()
        connection.execute('SAVEPOINT mwf_job_receiver_observation')
        try:
            self._read_job_receiver_state(connection, receiver, expected_shape)
        finally:
            connection.execute('RELEASE SAVEPOINT mwf_job_receiver_observation')

    def _read_job_receiver_state(self, connection, receiver, expected_shape):
        members = read_active_component_for_node(connection, receiver)
        if members is None:
            return None
        if expected_shape is not None:
            expected = component_snapshot_from_shape(expected_shape)
            if members not in expected.components or receiver not in members:
                raise RuntimeError('Managed receiver membership requires fresh preparation')
        state = self._read_component_state(connection, members)
        if state is None:
            raise RuntimeError('Managed receiver component state is missing')
        snapshot = component_snapshot_from_shape(state['shape_json'])
        if members not in snapshot.components:
            raise RuntimeError('Managed receiver does not match its producing shape')
        shape_id, _ = self._read_component_producing_identity(connection, members)
        return shape_id, state

    def _mark_component_job_arrival(self, connection, receiver, job_id, *, expected_shape=None):
        instance = connection.execute(
            'SELECT instance_id, created_by_execution_id FROM job_instances WHERE node_name=? AND job_id=?',
            (receiver, job_id),
        ).fetchone()
        if instance is None:
            raise RuntimeError('Managed job arrival requires its inserted instance')
        observed = self._read_job_receiver_state(connection, receiver, expected_shape)
        if observed is None:
            return None
        shape_id, state = observed
        creator = instance['created_by_execution_id']
        if creator is not None:
            owner = self._read_execution_owner(connection, creator)
            if owner is None:
                raise RuntimeError('Managed job producer has no historical execution identity')
        return self._mark_component_arrival(
            connection, receiver, shape_id, state, creator, 'managed-job',
            job_id=job_id, job_instance_id=instance['instance_id'],
        )

    def _mark_component_input_arrival(self, connection, receiver, owner, relative):
        shape = connection.execute('SELECT shape_json FROM graph_shapes WHERE shape_id=?',
                                   (owner['shape_id'],)).fetchone()
        if shape is None:
            raise RuntimeError('Managed input producing graph shape is missing')
        observed = self._read_job_receiver_state(connection, receiver, shape['shape_json'])
        if observed is None:
            raise RuntimeError('Managed input receiver has no established membership')
        receiver_shape_id, state = observed
        return self._mark_component_arrival(
            connection, receiver, receiver_shape_id, state, owner['execution_id'], 'managed-input',
            relative=relative,
        )

    def _mark_component_arrival(self, connection, receiver, shape_id, state, creator, kind,
                                *, relative=None, job_id=None, job_instance_id=None, preparation=None):
        members = state['members']
        identity = (os.getpid(), shape_id, members, state['alignment_generation'])
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
                raise RuntimeError('Managed arrival receiver changed before publication')
        connection.execute(
            'INSERT INTO component_misalignment_causes VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) '
            'ON CONFLICT(receiver_node,alignment_generation) DO NOTHING',
            (receiver, state['alignment_generation'], encode_component_key(members), shape_id,
             creator, kind, relative, job_id, job_instance_id, *(preparation or (None,) * 5)),
        )
        recorded = connection.execute(
            'SELECT * FROM component_misalignment_causes WHERE receiver_node=? AND alignment_generation=?',
            (receiver, state['alignment_generation']),
        ).fetchone()
        if recorded is None:
            raise RuntimeError('Managed arrival receiver lost its first cause')
        self._decode_component_arrival_cause(connection, recorded, state)
        return identity
