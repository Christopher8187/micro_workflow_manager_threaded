from micro_workflow_manager.component_identity import encode_component_key
from .job_preparation import apply_job_preparation


class ComponentTransitionStorageMixin:
    def refuse_live_sessions_for_reset(self) -> None:
        self._require_execution_session_storage()
        self._refuse_live_sessions_for_reset(self.db_connection())

    @staticmethod
    def _refuse_live_sessions_for_reset(connection) -> None:
        live = connection.execute(
            "SELECT session_id FROM execution_sessions WHERE status='running' ORDER BY session_id",
        ).fetchall()
        if live:
            raise RuntimeError('Reset requires no live execution sessions: ' + ', '.join(row['session_id'] for row in live))

    def read_component_fresh_preparation(self, session_id: str, component, *, expected_shape: str) -> dict:
        self._session_text(session_id, 'session_id')
        return self._observe_component_preparation(session_id, component, expected_shape)

    def read_component_reset_preparation(self, component, *, expected_shape: str) -> dict:
        return self._observe_component_preparation(None, component, expected_shape)

    def _observe_component_preparation(self, session_id, component, expected_shape):
        self._require_execution_session_storage()
        self._session_text(expected_shape, 'expected_shape')
        members = self._session_component(component)
        connection = self.db_connection()
        connection.execute('SAVEPOINT mwf_component_preparation')
        try:
            return self._read_component_preparation(connection, session_id, members, expected_shape)
        finally:
            connection.execute('RELEASE SAVEPOINT mwf_component_preparation')

    def _read_component_preparation(self, connection, session_id, members, expected_shape):
        key = encode_component_key(members)
        reservation = connection.execute(
            'SELECT session_id FROM component_reservations WHERE component_key=?', (key,),
        ).fetchone()
        if session_id is None:
            self._refuse_live_sessions_for_reset(connection)
            if reservation is not None:
                raise RuntimeError('Reset component retains a reservation: ' + key)
        else:
            owner = connection.execute(
                'SELECT session.status, selected.component_key FROM execution_sessions AS session '
                'LEFT JOIN session_components AS selected '
                'ON selected.session_id=session.session_id AND selected.component_key=? '
                'WHERE session.session_id=?', (key, session_id),
            ).fetchone()
            if owner is None or owner['status'] != 'running' or owner['component_key'] is None:
                raise RuntimeError('Full preparation requires a running selected component owner: ' + key)
            if reservation is None or reservation['session_id'] != session_id:
                raise RuntimeError('Full preparation requires the exact component reservation: ' + key)
            if self._read_session_job_roots(connection, session_id):
                raise RuntimeError('Selected-job preparation cannot realign a full component')
        if connection.execute(
            'SELECT 1 FROM component_holds WHERE component_key=? LIMIT 1', (key,),
        ).fetchone() or connection.execute(
            'SELECT 1 FROM pending_component_executions WHERE component_key=? LIMIT 1', (key,),
        ).fetchone():
            raise RuntimeError('Full preparation cannot change a held or pending component: ' + key)
        for node in members:
            if connection.execute(
                "SELECT 1 FROM jobs WHERE node_name=? AND (active_execution_id IS NOT NULL OR status='running' "
                'OR active_pid IS NOT NULL OR active_thread_id IS NOT NULL OR active_started_at IS NOT NULL) LIMIT 1',
                (node,),
            ).fetchone():
                raise RuntimeError('Full preparation cannot change active component jobs: ' + key)
        state = self._read_component_state(connection, members)
        if state is None or state['shape_json'] != expected_shape:
            raise RuntimeError('Full preparation requires the expected producing graph shape')
        if state['lifecycle'] == 'running':
            raise RuntimeError('Full preparation cannot change a running component: ' + key)
        return state

    def complete_component_fresh_preparation(
        self, session_id: str, component, *, expected_state: dict, job_preparation=(), keep_trace: bool = False,
    ) -> int:
        self._session_text(session_id, 'session_id')
        return self._complete_component_preparation(session_id, component, expected_state, job_preparation, keep_trace)

    def complete_component_reset_preparation(
        self, component, *, expected_state: dict, job_preparation=(), keep_trace: bool = False,
    ) -> int:
        return self._complete_component_preparation(None, component, expected_state, job_preparation, keep_trace)

    def _complete_component_preparation(self, session_id, component, expected_state, job_preparation, keep_trace,
                                        *, connection=None):
        self._require_execution_session_storage()
        members = self._session_component(component)
        expected = dict(expected_state)
        fields = {'members', 'shape_json', 'lifecycle', 'stability', 'instability_origin',
                  'misaligned', 'alignment_generation'}
        if (set(expected) != fields or type(expected.get('alignment_generation')) is not int
                or expected['alignment_generation'] < 0 or type(expected.get('misaligned')) is not bool
                or type(expected.get('members')) is not tuple or expected['members'] != members):
            raise ValueError('Full preparation requires an exact captured component observation')
        expected_shape = self._session_text(expected.get('shape_json'), 'expected_shape')
        key = encode_component_key(members)
        preparations = tuple(job_preparation)
        if preparations and (len(preparations) != len(members) or {plan.node for plan in preparations} != set(members)):
            raise ValueError('Job preparation must cover the exact full component')

        def complete(connection):
            state = self._read_component_preparation(connection, session_id, members, expected_shape)
            if state != expected:
                raise RuntimeError('Component changed during full preparation: ' + key)
            apply_job_preparation(connection, preparations, keep_trace=keep_trace)
            changed = connection.execute(
                "UPDATE component_states SET lifecycle='queued', stability=NULL, instability_origin=NULL, "
                'misaligned=0, alignment_generation=alignment_generation+1 '
                'WHERE component_key=? AND lifecycle=? AND stability IS ? AND instability_origin IS ? '
                'AND misaligned=? AND alignment_generation=?',
                (key, expected['lifecycle'], expected['stability'], expected['instability_origin'],
                 int(expected['misaligned']), expected['alignment_generation']),
            ).rowcount
            if changed != 1:
                raise RuntimeError('Component changed during full preparation: ' + key)
            return expected['alignment_generation'] + 1

        return complete(connection) if connection is not None else self.submit_db_mutation(complete, wait=True, priority=0)

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

    def finish_sampled_component_resume(
        self, session_id: str, component, *, expected_shape: str,
        expected_alignment_generation: int,
    ) -> bool:
        self._require_execution_session_storage()
        self._session_text(session_id, 'session_id')
        key = encode_component_key(self._session_component(component))
        if type(expected_alignment_generation) is not int or expected_alignment_generation < 0:
            raise ValueError('Expected alignment generation must be a nonnegative integer')

        def finish(connection):
            owner = connection.execute(
                'SELECT session.status, reservation.session_id AS owner, selected.component_key AS selected_key '
                'FROM execution_sessions AS session '
                'LEFT JOIN component_reservations AS reservation ON reservation.component_key=? '
                'LEFT JOIN session_components AS selected '
                'ON selected.session_id=session.session_id AND selected.component_key=? '
                'WHERE session.session_id=?', (key, key, session_id),
            ).fetchone()
            if owner is None or owner['status'] != 'running':
                raise RuntimeError('Sampled completion requires an existing running session: ' + session_id)
            if owner['owner'] != session_id:
                raise RuntimeError('Sampled completion requires the exact component reservation: ' + key)
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
                'WHERE d.component_key=?', (key,),
            ).fetchone()
            if state is None:
                raise RuntimeError('Unknown component: ' + key)
            if state['state_key'] is None or state['shape_json'] is None:
                raise RuntimeError('Incomplete component state or producing graph shape')
            if state['shape_json'] != expected_shape:
                raise RuntimeError('Sampled completion requires the expected producing graph shape')
            self._validate_component_state_row(state)
            if (state['lifecycle'] != 'running' or state['stability'] is None
                    or state['misaligned'] != 0
                    or state['alignment_generation'] != expected_alignment_generation):
                raise RuntimeError('Completion requires an aligned running component with retained lineage '
                                   'at the expected generation')
            changed = connection.execute(
                "UPDATE component_states SET lifecycle='done' "
                "WHERE component_key=? AND lifecycle='running' AND misaligned=0 AND alignment_generation=? "
                "AND stability IS ? AND instability_origin IS ?",
                (key, expected_alignment_generation, state['stability'], state['instability_origin']),
            ).rowcount
            if changed != 1:
                raise RuntimeError('Sampled component changed before completion: ' + key)
            return True

        return self.submit_db_mutation(finish, wait=True, priority=0)

    def fail_running_component(
        self, session_id: str, component, *, expected_shape: str,
        expected_alignment_generation: int,
    ) -> bool:
        self._require_execution_session_storage()
        self._session_text(session_id, 'session_id')
        key = encode_component_key(self._session_component(component))
        if type(expected_alignment_generation) is not int or expected_alignment_generation < 0:
            raise ValueError('Expected alignment generation must be a nonnegative integer')

        def fail(connection):
            owner = connection.execute(
                'SELECT session.status, reservation.session_id AS owner, selected.component_key AS selected_key '
                'FROM execution_sessions AS session '
                'LEFT JOIN component_reservations AS reservation ON reservation.component_key=? '
                'LEFT JOIN session_components AS selected '
                'ON selected.session_id=session.session_id AND selected.component_key=? '
                'WHERE session.session_id=?', (key, key, session_id),
            ).fetchone()
            if owner is None or owner['status'] != 'running':
                raise RuntimeError('Component failure requires an existing running session: ' + session_id)
            if owner['owner'] != session_id:
                raise RuntimeError('Component failure requires the exact component reservation: ' + key)
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
                'WHERE d.component_key=?', (key,),
            ).fetchone()
            if state is None:
                raise RuntimeError('Unknown component: ' + key)
            if state['state_key'] is None or state['shape_json'] is None:
                raise RuntimeError('Incomplete component state or producing graph shape')
            if state['shape_json'] != expected_shape:
                raise RuntimeError('Component failure requires the expected producing graph shape')
            self._validate_component_state_row(state)
            if (state['lifecycle'] != 'running' or state['misaligned'] != 0
                    or state['alignment_generation'] != expected_alignment_generation):
                raise RuntimeError('Failure requires an aligned running component at the expected generation')
            changed = connection.execute(
                "UPDATE component_states SET lifecycle='failed', stability=NULL, instability_origin=NULL "
                "WHERE component_key=? AND lifecycle='running' AND misaligned=0 AND alignment_generation=? "
                "AND stability IS ? AND instability_origin IS ?",
                (key, expected_alignment_generation, state['stability'], state['instability_origin']),
            ).rowcount
            if changed != 1:
                raise RuntimeError('Component changed before failure: ' + key)
            return True

        return self.submit_db_mutation(fail, wait=True, priority=0)
