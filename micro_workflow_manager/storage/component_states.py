from __future__ import annotations

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key


class ComponentStateStorageMixin:
    """Persist private component lifecycle records at their producing shape."""

    def begin_sampled_component_resume(
        self, session_id: str, component, *, expected_alignment_generation: int,
    ) -> bool:
        self._require_execution_session_storage()
        self._session_text(session_id, 'session_id')
        key = encode_component_key(self._session_component(component))
        if type(expected_alignment_generation) is not int or expected_alignment_generation < 0:
            raise ValueError('Expected alignment generation must be a nonnegative integer')

        def resume(connection):
            owner = connection.execute(
                'SELECT session.status, reservation.session_id AS owner, selected.component_key AS selected_key '
                'FROM execution_sessions AS session '
                'LEFT JOIN component_reservations AS reservation ON reservation.component_key=? '
                'LEFT JOIN session_components AS selected '
                'ON selected.session_id=session.session_id AND selected.component_key=? '
                'WHERE session.session_id=?', (key, key, session_id),
            ).fetchone()
            if owner is None or owner['status'] != 'running':
                raise RuntimeError('Sampled resume requires an existing running session: ' + session_id)
            if owner['owner'] != session_id:
                raise RuntimeError('Sampled resume requires the exact component reservation: ' + key)
            if owner['selected_key'] is None:
                raise RuntimeError('Component is outside the session selected scope: ' + key)
            state = connection.execute(
                'SELECT d.component_key, g.shape_json, s.component_key AS state_key, '
                's.lifecycle, s.stability, s.instability_origin, s.misaligned, s.alignment_generation, '
                'origin.session_kind AS origin_kind '
                'FROM component_definitions d '
                'LEFT JOIN graph_shapes g USING(shape_id) '
                'LEFT JOIN component_states s USING(component_key) '
                'LEFT JOIN execution_sessions origin ON origin.session_id=s.instability_origin '
                'WHERE d.component_key=?',
                (key,),
            ).fetchone()
            if state is None:
                raise RuntimeError('Unknown component: ' + key)
            if state['state_key'] is None or state['shape_json'] is None:
                raise RuntimeError('Incomplete component state or producing graph shape')
            self._validate_component_state_row(state)
            if (state['misaligned'] != 0
                    or state['alignment_generation'] != expected_alignment_generation
                    or state['lifecycle'] not in ('sampled', 'running')):
                raise RuntimeError('Resume requires an aligned sampled component at the expected generation')
            if state['lifecycle'] == 'running':
                return False
            changed = connection.execute(
                "UPDATE component_states SET lifecycle='running' "
                "WHERE component_key=? AND lifecycle='sampled' AND misaligned=0 AND alignment_generation=? "
                "AND stability IS ? AND instability_origin IS ?",
                (key, expected_alignment_generation, state['stability'], state['instability_origin']),
            ).rowcount
            if changed != 1:
                raise RuntimeError('Sampled component changed before resume: ' + key)
            return True

        return self.submit_db_mutation(resume, wait=True, priority=0)

    def get_component_state(self, component) -> dict | None:
        self._require_execution_session_storage()
        members = self._session_component(component)
        row = self.db_connection().execute(
            "SELECT d.component_key, g.shape_json, s.component_key AS state_key, "
            "s.lifecycle, s.stability, s.instability_origin, s.misaligned, s.alignment_generation, "
            "origin.session_kind AS origin_kind "
            "FROM component_definitions d "
            "LEFT JOIN graph_shapes g USING(shape_id) "
            "LEFT JOIN component_states s USING(component_key) "
            "LEFT JOIN execution_sessions origin ON origin.session_id=s.instability_origin "
            "WHERE d.component_key=?",
            (encode_component_key(members),),
        ).fetchone()
        if row is None:
            return None
        if row['state_key'] is None or row['shape_json'] is None:
            raise RuntimeError('Incomplete component state or producing graph shape')
        self._validate_component_state_row(row)
        return {
            'members': decode_component_key(row['component_key']),
            'shape_json': row['shape_json'],
            'lifecycle': row['lifecycle'],
            'stability': row['stability'],
            'instability_origin': row['instability_origin'],
            'misaligned': bool(row['misaligned']),
            'alignment_generation': row['alignment_generation'],
        }

    @staticmethod
    def _validate_component_state_row(row) -> None:
        lifecycle = row['lifecycle']
        stability = row['stability']
        origin = row['instability_origin']
        misaligned = row['misaligned']
        generation = row['alignment_generation']
        no_lineage = stability is None and origin is None
        result_lineage = (
            (stability == 'stable' and origin is None)
            or (stability == 'unstable' and isinstance(origin, str) and bool(origin.strip())
                and row['origin_kind'] == 'interrupt')
        )
        valid_lifecycle = (
            (lifecycle == 'queued' and no_lineage and misaligned == 0)
            or (lifecycle == 'running' and (no_lineage or result_lineage) and misaligned == 0)
            or (lifecycle in ('sampled', 'done') and result_lineage)
            or (lifecycle == 'failed' and no_lineage)
        )
        if not (
            valid_lifecycle
            and type(misaligned) is int and misaligned in (0, 1)
            and type(generation) is int and generation >= 0
        ):
            raise RuntimeError('Invalid component state for ' + row['component_key'])
