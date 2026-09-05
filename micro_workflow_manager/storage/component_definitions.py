from __future__ import annotations

import json
from typing import TYPE_CHECKING

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key

if TYPE_CHECKING:
    from micro_workflow_manager.topology import ComponentTopologySnapshot


class ComponentDefinitionStorageMixin:
    """Store exact component identities and their producing topology."""

    def register_component_topology(self, snapshot: ComponentTopologySnapshot) -> bool:
        self._require_execution_session_storage()
        self._validate_component_snapshot(snapshot)
        components = [encode_component_key(component) for component in snapshot.components]

        def register(connection):
            shape = connection.execute(
                'SELECT shape_id FROM graph_shapes WHERE shape_json=?', (snapshot.shape_json,),
            ).fetchone()
            shape_id = shape['shape_id'] if shape is not None else None
            conflicts = []
            for offset in range(0, len(components), 500):
                selected = components[offset:offset + 500]
                placeholders = ','.join('?' for _ in selected)
                rows = connection.execute(
                    'SELECT component_key, shape_id FROM component_definitions '
                    f'WHERE component_key IN ({placeholders})', selected,
                )
                conflicts.extend(row['component_key'] for row in rows if row['shape_id'] != shape_id)
            if conflicts:
                raise RuntimeError('Components belong to a different graph shape: ' + ', '.join(sorted(conflicts)))
            changed = connection.execute(
                "INSERT INTO graph_shapes(shape_json) VALUES(?) ON CONFLICT DO NOTHING",
                (snapshot.shape_json,),
            ).rowcount
            if shape_id is None:
                shape_id = connection.execute(
                    'SELECT shape_id FROM graph_shapes WHERE shape_json=?', (snapshot.shape_json,),
                ).fetchone()['shape_id']
            changed += connection.executemany(
                "INSERT INTO component_definitions(component_key, shape_id) VALUES(?, ?) "
                "ON CONFLICT(component_key) DO NOTHING",
                [(component, shape_id) for component in components],
            ).rowcount
            return changed > 0

        return self.submit_db_mutation(register, wait=True, priority=0)

    def _validate_component_snapshot(self, snapshot: ComponentTopologySnapshot) -> None:
        import networkx as nx
        from micro_workflow_manager.topology import ComponentTopology, ComponentTopologySnapshot

        if not isinstance(snapshot, ComponentTopologySnapshot):
            raise ValueError('Expected a component topology snapshot')
        try:
            shape = json.loads(snapshot.shape_json)
        except (TypeError, ValueError) as error:
            raise ValueError('Invalid graph shape in component snapshot') from error
        if not isinstance(shape, dict) or set(shape) != {'nodes', 'edges', 'autostart_edges'}:
            raise ValueError('Invalid graph shape in component snapshot')
        if any(not isinstance(shape[name], list) for name in shape):
            raise ValueError('Component snapshot graph collections must be lists')
        nodes = {self.validate_node_name(node) for node in shape['nodes']}
        for name in ('edges', 'autostart_edges'):
            for edge in shape[name]:
                if not isinstance(edge, list) or len(edge) != 2:
                    raise ValueError('Component snapshot edges need two node names')
                if any(self.validate_node_name(node) not in nodes for node in edge):
                    raise ValueError('Component snapshot edges must join known nodes')
        graph = nx.DiGraph()
        graph.add_nodes_from(nodes)
        graph.add_edges_from(shape['edges'])
        expected = ComponentTopology(graph, shape['autostart_edges']).snapshot()
        if snapshot != expected:
            raise ValueError('Component snapshot must match its canonical graph shape and exact components')

    def get_component_definition(self, component) -> dict | None:
        self._require_execution_session_storage()
        members = self._session_component(component)
        row = self.db_connection().execute(
            "SELECT component_key, shape_json FROM component_definitions "
            "JOIN graph_shapes USING(shape_id) WHERE component_key=?",
            (encode_component_key(members),),
        ).fetchone()
        if row is None:
            return None
        return {'members': decode_component_key(row['component_key']), 'shape_json': row['shape_json']}
