from __future__ import annotations

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key


class ComponentReservationConflict(RuntimeError):
    def __init__(self, conflicts):
        self.conflicts = tuple(conflicts)
        names = ', '.join(f'{members!r} owned by {owner}' for members, owner in self.conflicts)
        super().__init__('Component reservations conflict: ' + names)


class ComponentReservationStorageMixin:
    """Reserve the exact selected component scope retained by a session."""

    def reserve_execution_components(self, session_id: str, *, expected_shape: str) -> bool:
        self._require_execution_session_storage()
        self._session_text(session_id, 'session_id')

        def reserve(connection):
            session = connection.execute(
                'SELECT status FROM execution_sessions WHERE session_id=?', (session_id,),
            ).fetchone()
            if session is None or session['status'] != 'running':
                raise RuntimeError('Component reservations require an existing running session: ' + session_id)
            rows = connection.execute(
                'SELECT selected.component_key, shape.shape_json '
                'FROM session_components AS selected '
                'LEFT JOIN component_definitions AS definition USING(component_key) '
                'LEFT JOIN graph_shapes AS shape USING(shape_id) '
                'WHERE selected.session_id=? ORDER BY selected.position', (session_id,),
            ).fetchall()
            if not rows or any(row['shape_json'] != expected_shape for row in rows):
                raise RuntimeError('Session scope does not match registered components in the expected graph shape')
            selected_nodes = {node for row in rows for node in decode_component_key(row['component_key'])}
            reservations = connection.execute(
                'SELECT component_key, session_id FROM component_reservations ORDER BY component_key',
            ).fetchall()
            conflicts = [(decode_component_key(row['component_key']), row['session_id'])
                         for row in reservations if row['session_id'] != session_id
                         and selected_nodes.intersection(decode_component_key(row['component_key']))]
            if conflicts:
                raise ComponentReservationConflict(conflicts)
            owned_keys = {row['component_key'] for row in reservations if row['session_id'] == session_id}
            if owned_keys == {row['component_key'] for row in rows}:
                return False
            if owned_keys:
                raise RuntimeError('Session has a damaged partial reservation: ' + session_id)
            connection.executemany(
                'INSERT INTO component_reservations(component_key, session_id) VALUES(?, ?)',
                [(row['component_key'], session_id) for row in rows],
            )
            return True

        return self.submit_db_mutation(reserve, wait=True, priority=0)

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
