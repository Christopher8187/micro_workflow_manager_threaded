"""Derive raw-node lifecycle observations from native component results."""

from .base import FileStorageBase
from .component_membership import read_active_component_for_node
from .component_result_identity import read_component_state_record


def read_node_lifecycles(connection, node_names):
    names = sorted({FileStorageBase.validate_node_name(name) for name in node_names})
    connection.execute('SAVEPOINT mwf_node_lifecycles')
    try:
        mounted = set()
        memberships = {}
        for offset in range(0, len(names), 500):
            chunk = names[offset:offset + 500]
            placeholders = ','.join('?' for _ in chunk)
            mounted.update(row[0] for row in connection.execute(
                f'SELECT node_name FROM nodes WHERE node_name IN ({placeholders})', chunk,
            ))
            memberships.update(connection.execute(
                'SELECT node_name, component_key FROM active_component_members '
                f'WHERE node_name IN ({placeholders})', chunk,
            ).fetchall())
        result, states = {}, {}
        for node in sorted(mounted):
            if node not in memberships:
                # Router mounting can precede first membership admission. Such
                # a node has no established component result yet.
                result[node] = 'queued'
                continue
            key = memberships[node]
            if key not in states:
                component = read_active_component_for_node(connection, node)
                state = read_component_state_record(connection, component)
                if state is None:
                    raise RuntimeError('Active component has no lifecycle record: ' + key)
                states[key] = state
            state = states[key]
            if node not in state.members:
                raise RuntimeError('Node lifecycle membership changed: ' + node)
            result[node] = state.lifecycle
        return result
    finally:
        connection.execute('RELEASE SAVEPOINT mwf_node_lifecycles')
