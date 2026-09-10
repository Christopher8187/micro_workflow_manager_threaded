from __future__ import annotations

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key
from .preparation_guards import refuse_receiver_mutation
from .session_scope import require_admitted_reservation_scope


class ComponentReservationConflict(RuntimeError):
    def __init__(self, conflicts):
        self.conflicts = tuple(conflicts)
        names = ', '.join(f'{members!r} owned by {owner}' for members, owner in self.conflicts)
        super().__init__('Component reservations conflict: ' + names)


class ComponentReservationStorageMixin:
    """Reserve the exact selected component scope retained by a session."""

    def reserve_execution_components(
        self, session_id: str, *, expected_shape: str, _wait: bool = True,
    ):
        self._require_execution_session_storage()
        self._session_text(session_id, 'session_id')

        return self.submit_db_mutation(
            lambda connection: self._reserve_execution_components(connection, session_id, expected_shape),
            wait=_wait, priority=0,
        )

    def _reserve_execution_components(self, connection, session_id, expected_shape):
        session = connection.execute(
            'SELECT status, admitted_shape_id, partition_revision, scope_admitted '
            'FROM execution_sessions WHERE session_id=?', (session_id,),
        ).fetchone()
        if session is None or session['status'] != 'running':
            raise RuntimeError('Component reservations require an existing running session: ' + session_id)
        rows = connection.execute(
            'SELECT selected.component_key, shape.shape_json '
            'FROM session_components AS selected '
            'LEFT JOIN component_definitions AS definition '
            'ON definition.component_key=selected.component_key AND definition.shape_id=? '
            'LEFT JOIN graph_shapes AS shape ON shape.shape_id=definition.shape_id '
            'WHERE selected.session_id=? ORDER BY selected.position', (session['admitted_shape_id'], session_id),
        ).fetchall()
        if not rows or any(row['shape_json'] != expected_shape for row in rows):
            raise RuntimeError('Session scope does not match registered components in the expected graph shape')
        from .component_membership import read_active_component_partition

        active = read_active_component_partition(connection)
        if active is None or active.revision != session['partition_revision']:
            raise RuntimeError('Active component membership changed during admission')
        selected_nodes = {node for row in rows for node in decode_component_key(row['component_key'])}
        for node in sorted(selected_nodes):
            refuse_receiver_mutation(connection, node)
        reservations = connection.execute(
            'SELECT component_key, session_id FROM component_reservations ORDER BY component_key',
        ).fetchall()
        conflicts = [(decode_component_key(row['component_key']), row['session_id'])
                     for row in reservations if row['session_id'] != session_id
                     and selected_nodes.intersection(decode_component_key(row['component_key']))]
        if conflicts:
            raise ComponentReservationConflict(conflicts)
        owned_keys = {row['component_key'] for row in reservations if row['session_id'] == session_id}
        expected_keys = {row['component_key'] for row in rows}
        if owned_keys == expected_keys:
            if session['scope_admitted'] != 1:
                raise RuntimeError('Session reservation admission marker is missing: ' + session_id)
            self._bind_pending_thread_overrides(connection, session_id, selected_nodes)
            require_admitted_reservation_scope(connection, session_id)
            return False
        if owned_keys:
            raise RuntimeError('Session has a damaged partial reservation: ' + session_id)
        if session['scope_admitted'] != 0:
            raise RuntimeError('Admitted session lost its component reservations: ' + session_id)
        inserted = connection.executemany(
            'INSERT INTO component_reservations(component_key, session_id) VALUES(?, ?)',
            [(row['component_key'], session_id) for row in rows],
        ).rowcount
        if inserted != len(rows):
            raise RuntimeError('Component reservation was not recorded completely')
        self._bind_pending_thread_overrides(connection, session_id, selected_nodes)
        if connection.execute(
            "UPDATE execution_sessions SET scope_admitted=1 "
            "WHERE session_id=? AND status='running' AND scope_admitted=0",
            (session_id,),
        ).rowcount != 1:
            raise RuntimeError('Session reservation admission was not retained')
        if not require_admitted_reservation_scope(connection, session_id):
            raise RuntimeError('Session reservation admission marker changed')
        return True

    def get_component_reservation(self, component) -> dict | None:
        self._require_execution_session_storage()
        members = self._session_component(component)
        row = self.db_connection().execute(
            'SELECT session_id FROM component_reservations WHERE component_key=?',
            (encode_component_key(members),),
        ).fetchone()
        return None if row is None else {'members': members, 'session_id': row['session_id']}

    def release_execution_components(self, session_id: str) -> int:
        self._require_execution_session_storage()
        self._session_text(session_id, 'session_id')

        def release(connection):
            return connection.execute(
                'DELETE FROM component_reservations WHERE session_id=?', (session_id,),
            ).rowcount

        return self.submit_db_mutation(release, wait=True, priority=0)
