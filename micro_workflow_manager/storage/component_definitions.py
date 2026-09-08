from __future__ import annotations

import json
from functools import lru_cache
from typing import TYPE_CHECKING

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key
from .base import FileStorageBase

if TYPE_CHECKING:
    from micro_workflow_manager.topology import ComponentTopologySnapshot


@lru_cache(maxsize=128)
def component_snapshot_from_shape(shape_json: str):
    """Decode a canonical stored shape without consulting current definitions."""
    import networkx as nx
    from micro_workflow_manager.topology import ComponentTopology

    try:
        shape = json.loads(shape_json)
        if not isinstance(shape, dict) or set(shape) != {'nodes', 'edges', 'autostart_edges'}:
            raise ValueError('Invalid graph shape collections')
        if any(not isinstance(shape[name], list) for name in shape):
            raise ValueError('Graph shape collections must be lists')
        for node in shape['nodes']:
            FileStorageBase.validate_node_name(node)
        nodes = set(shape['nodes'])
        for name in ('edges', 'autostart_edges'):
            for edge in shape[name]:
                if (not isinstance(edge, list) or len(edge) != 2
                        or any(not isinstance(node, str) or node not in nodes for node in edge)):
                    raise ValueError('Invalid graph shape edge')
        graph = nx.DiGraph()
        graph.add_nodes_from(nodes)
        graph.add_edges_from(shape['edges'])
        snapshot = ComponentTopology(graph, shape['autostart_edges']).snapshot()
        if snapshot.shape_json != shape_json:
            raise ValueError('Graph shape must be canonical')
        return snapshot
    except (TypeError, ValueError) as error:
        raise ValueError('Invalid producing graph shape') from error


class ComponentDefinitionStorageMixin:
    """Store exact component identities and their producing topology."""

    def register_component_topology(self, snapshot: ComponentTopologySnapshot) -> bool:
        self._require_execution_session_storage()
        self._validate_component_snapshot(snapshot)
        components = [encode_component_key(component) for component in snapshot.components]

        def register(connection):
            from .component_membership import (
                initialize_active_component_partition, read_active_component_partition,
            )

            active = read_active_component_partition(connection)
            has_definitions = connection.execute(
                'SELECT 1 FROM component_definitions LIMIT 1',
            ).fetchone() is not None
            if active is None and has_definitions:
                raise RuntimeError('Stored components have no active membership partition')
            shape = connection.execute(
                'SELECT shape_id FROM graph_shapes WHERE shape_json=?', (snapshot.shape_json,),
            ).fetchone()
            shape_id = None if shape is None else shape['shape_id']
            if shape_id is not None:
                registered = {
                    row['component_key'] for row in connection.execute(
                        'SELECT component_key FROM component_definitions WHERE shape_id=?',
                        (shape_id,),
                    )
                }
                if registered != set(components):
                    raise RuntimeError('Incomplete registered component topology')
            existing_components = set()
            for component, key in zip(snapshot.components, components):
                historical = connection.execute(
                    'SELECT 1 FROM component_definitions WHERE component_key=? LIMIT 1',
                    (key,),
                ).fetchone()
                state = self._read_component_state(connection, component)
                if historical is not None and state is None:
                    raise RuntimeError('Incomplete component state for ' + key)
                if state is not None:
                    existing_components.add(key)
                    try:
                        producing = component_snapshot_from_shape(state['shape_json'])
                    except ValueError as error:
                        raise RuntimeError('Invalid producing component shape') from error
                    if component not in producing.components:
                        raise RuntimeError('Stored component is outside its producing shape')
            changed = connection.execute(
                'INSERT INTO graph_shapes(shape_json) VALUES(?) ON CONFLICT DO NOTHING',
                (snapshot.shape_json,),
            ).rowcount
            if shape_id is None:
                shape_id = connection.execute(
                    'SELECT shape_id FROM graph_shapes WHERE shape_json=?', (snapshot.shape_json,),
                ).fetchone()['shape_id']
            changed += connection.executemany(
                'INSERT INTO component_definitions(component_key, shape_id) VALUES(?, ?) '
                'ON CONFLICT(component_key, shape_id) DO NOTHING',
                [(component, shape_id) for component in components],
            ).rowcount
            connection.executemany(
                'INSERT INTO component_states(component_key, shape_id) VALUES(?, ?)',
                [(component, shape_id) for component in components if component not in existing_components],
            )
            if active is None:
                initialize_active_component_partition(connection, snapshot.components)
            from .membership_registration import reconcile_unproduced_memberships

            reconciled = reconcile_unproduced_memberships(
                connection, snapshot,
                new_components=[component for component in snapshot.components
                                if encode_component_key(component) not in existing_components],
            )
            return changed > 0 or reconciled

        return self.submit_db_mutation(register, wait=True, priority=0)

    def _validate_component_snapshot(self, snapshot: ComponentTopologySnapshot) -> None:
        from micro_workflow_manager.topology import ComponentTopologySnapshot

        if not isinstance(snapshot, ComponentTopologySnapshot):
            raise ValueError('Expected a component topology snapshot')
        expected = component_snapshot_from_shape(snapshot.shape_json)
        if snapshot != expected:
            raise ValueError('Component snapshot must match its canonical graph shape and exact components')

    def _read_component_producing_identity(self, connection, component):
        state = self._read_component_state(connection, component)
        if state is None:
            raise RuntimeError('Missing producing component state')
        try:
            snapshot = component_snapshot_from_shape(state['shape_json'])
        except ValueError as error:
            raise RuntimeError('Invalid producing component shape') from error
        if component not in snapshot.components:
            raise RuntimeError('Producing component does not belong to its recorded shape')
        shape = connection.execute(
            'SELECT shape_id FROM component_states WHERE component_key=?',
            (encode_component_key(component),),
        ).fetchone()
        return shape['shape_id'], state['alignment_generation']

    def get_component_definition(self, component) -> dict | None:
        self._require_execution_session_storage()
        members = self._session_component(component)
        row = self.db_connection().execute(
            "SELECT state.component_key, shape.shape_json FROM component_states AS state "
            "LEFT JOIN component_definitions AS definition "
            "ON definition.component_key=state.component_key AND definition.shape_id=state.shape_id "
            "LEFT JOIN graph_shapes AS shape ON shape.shape_id=definition.shape_id "
            "WHERE state.component_key=?",
            (encode_component_key(members),),
        ).fetchone()
        if row is None:
            return None
        if row['shape_json'] is None:
            raise RuntimeError('Incomplete component definition or producing shape')
        return {'members': decode_component_key(row['component_key']), 'shape_json': row['shape_json']}
