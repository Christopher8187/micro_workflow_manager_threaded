from __future__ import annotations

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key


class ComponentHoldStorageMixin:
    """Retain counted component holds and their exact session association."""

    def acquire_component_holds(self, session_id: str, components) -> dict:
        self._require_execution_session_storage()
        self._session_text(session_id, 'session_id')
        keys = [encode_component_key(self._session_component(component)) for component in components]
        if len(keys) != len(set(keys)):
            raise ValueError('A hold batch cannot contain duplicate components')

        def acquire(connection):
            session = connection.execute(
                'SELECT status FROM execution_sessions WHERE session_id=?', (session_id,),
            ).fetchone()
            if session is None or session['status'] != 'running':
                raise RuntimeError('Component holds require an existing running session: ' + session_id)
            connection.executemany(
                'INSERT INTO component_holds(session_id, component_key, hold_count) VALUES(?, ?, 1) '
                'ON CONFLICT(session_id, component_key) DO UPDATE SET hold_count=hold_count+1',
                [(session_id, key) for key in keys],
            )
            requested = set(keys)
            return {decode_component_key(row['component_key']): row['hold_count']
                    for row in connection.execute(
                        'SELECT component_key, hold_count FROM component_holds WHERE session_id=?',
                        (session_id,),
                    ) if row['component_key'] in requested}

        return self.submit_db_mutation(acquire, wait=True, priority=0)

    def get_component_holds(self, component) -> list[dict]:
        self._require_execution_session_storage()
        members = self._session_component(component)
        rows = self.db_connection().execute(
            'SELECT holds.session_id, holds.hold_count AS count, session.heartbeat_at '
            'FROM component_holds AS holds '
            'JOIN execution_sessions AS session USING(session_id) '
            'WHERE holds.component_key=? ORDER BY holds.session_id',
            (encode_component_key(members),),
        )
        return [dict(row) for row in rows]

    def release_component_holds(self, session_id: str, components) -> dict:
        self._require_execution_session_storage()
        self._session_text(session_id, 'session_id')
        keys = [encode_component_key(self._session_component(component)) for component in components]
        if len(keys) != len(set(keys)):
            raise ValueError('A hold batch cannot contain duplicate components')

        def release(connection):
            parameters = [(session_id, key) for key in keys]
            connection.executemany(
                'DELETE FROM component_holds WHERE session_id=? AND component_key=? AND hold_count=1',
                parameters,
            )
            connection.executemany(
                'UPDATE component_holds SET hold_count=hold_count-1 '
                'WHERE session_id=? AND component_key=? AND hold_count>1', parameters,
            )
            remaining = {row['component_key']: row['hold_count'] for row in connection.execute(
                'SELECT component_key, hold_count FROM component_holds WHERE session_id=?', (session_id,),
            )}
            return {decode_component_key(key): remaining.get(key, 0) for key in keys}

        return self.submit_db_mutation(release, wait=True, priority=0)
