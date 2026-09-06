from __future__ import annotations

from dataclasses import dataclass, replace

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key


@dataclass(frozen=True)
class ComponentTerminalOutcome:
    """A calculated result guarded by the component shape and generation used."""

    component: tuple[str, ...]
    expected_shape: str
    expected_alignment_generation: int
    lifecycle: str
    stability: str | None
    instability_origin: str | None


class ComponentStateStorageMixin:
    """Persist private component lifecycle records at their producing shape."""

    def _normalize_component_terminal_outcomes(self, outcomes):
        normalized = []
        seen = set()
        for outcome in outcomes:
            if not isinstance(outcome, ComponentTerminalOutcome):
                raise ValueError('Expected a component terminal outcome')
            members = self._session_component(outcome.component)
            if members in seen:
                raise ValueError('A component may have only one terminal outcome')
            seen.add(members)
            self._session_text(outcome.expected_shape, 'expected_shape')
            generation = outcome.expected_alignment_generation
            if type(generation) is not int or generation < 0:
                raise ValueError('Expected alignment generation must be a nonnegative integer')
            if outcome.lifecycle not in ('done', 'failed'):
                raise ValueError('A terminal component outcome must be done or failed')
            if outcome.lifecycle == 'failed':
                valid = outcome.stability is None and outcome.instability_origin is None
            else:
                valid = (
                    (outcome.stability == 'stable' and outcome.instability_origin is None)
                    or (outcome.stability == 'unstable'
                        and isinstance(outcome.instability_origin, str)
                        and bool(outcome.instability_origin.strip()))
                )
            if not valid:
                raise ValueError('Invalid terminal component lineage')
            normalized.append(replace(outcome, component=members))
        return tuple(normalized)

    def _validate_component_terminal_outcomes(self, connection, session_id, outcomes):
        for outcome in outcomes:
            key = encode_component_key(outcome.component)
            owner = connection.execute(
                'SELECT session.status, reservation.session_id AS owner, selected.component_key AS selected_key '
                'FROM execution_sessions AS session '
                'LEFT JOIN component_reservations AS reservation ON reservation.component_key=? '
                'LEFT JOIN session_components AS selected '
                'ON selected.session_id=session.session_id AND selected.component_key=? '
                'WHERE session.session_id=?', (key, key, session_id),
            ).fetchone()
            if (owner is None or owner['status'] != 'running' or owner['owner'] != session_id
                    or owner['selected_key'] is None):
                raise RuntimeError('Component settlement requires its selected running owner and reservation: ' + key)
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
            if state is None or state['state_key'] is None or state['shape_json'] is None:
                raise RuntimeError('Incomplete component state or producing graph shape')
            self._validate_component_state_row(state)
            if (state['shape_json'] != outcome.expected_shape or state['lifecycle'] != 'running'
                    or state['misaligned'] != 0
                    or state['alignment_generation'] != outcome.expected_alignment_generation):
                raise RuntimeError('Component settlement requires the expected aligned running state: ' + key)
            if outcome.lifecycle == 'done':
                if state['stability'] is not None and (
                    state['stability'], state['instability_origin']
                ) != (outcome.stability, outcome.instability_origin):
                    raise RuntimeError('Component settlement cannot replace retained successful lineage')
                if outcome.instability_origin is not None:
                    origin = connection.execute(
                        'SELECT session_kind FROM execution_sessions WHERE session_id=?',
                        (outcome.instability_origin,),
                    ).fetchone()
                    if origin is None or origin['session_kind'] != 'interrupt':
                        raise RuntimeError('An unstable component requires an exact interrupt origin')

    def _publish_component_terminal_outcomes(self, connection, outcomes):
        from .execution_sessions import ExecutionSessionHasActiveJobs

        for outcome in outcomes:
            for node in outcome.component:
                for job in connection.execute(
                    'SELECT job_id, status, active_execution_id FROM jobs WHERE node_name=?', (node,),
                ):
                    if job['status'] == 'running' or job['active_execution_id'] is not None:
                        raise ExecutionSessionHasActiveJobs(
                            f'Component settlement still has active job {node}/{job["job_id"]}'
                        )
                    if outcome.lifecycle == 'done' and job['status'] not in ('done', 'skipped'):
                        raise RuntimeError(f'Component completion has unfinished job {node}/{job["job_id"]}')
            changed = connection.execute(
                'UPDATE component_states SET lifecycle=?, stability=?, instability_origin=? '
                "WHERE component_key=? AND lifecycle='running' AND misaligned=0 AND alignment_generation=?",
                (outcome.lifecycle, outcome.stability, outcome.instability_origin,
                 encode_component_key(outcome.component), outcome.expected_alignment_generation),
            ).rowcount
            if changed != 1:
                raise RuntimeError('Component changed before settlement')

    def begin_queued_component_execution(
        self, session_id: str, component, *, expected_shape: str,
        expected_alignment_generation: int,
    ) -> bool:
        """Start one aligned queued component while its selected session owns it."""
        self._require_execution_session_storage()
        self._session_text(session_id, 'session_id')
        self._session_text(expected_shape, 'expected_shape')
        key = encode_component_key(self._session_component(component))
        if type(expected_alignment_generation) is not int or expected_alignment_generation < 0:
            raise ValueError('Expected alignment generation must be a nonnegative integer')

        def begin(connection):
            owner = connection.execute(
                'SELECT session.status, reservation.session_id AS owner, selected.component_key AS selected_key '
                'FROM execution_sessions AS session '
                'LEFT JOIN component_reservations AS reservation ON reservation.component_key=? '
                'LEFT JOIN session_components AS selected '
                'ON selected.session_id=session.session_id AND selected.component_key=? '
                'WHERE session.session_id=?', (key, key, session_id),
            ).fetchone()
            if owner is None or owner['status'] != 'running':
                raise RuntimeError('Component start requires an existing running session: ' + session_id)
            if owner['owner'] != session_id:
                raise RuntimeError('Component start requires the exact component reservation: ' + key)
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
            self._validate_component_state_row(state)
            if state['shape_json'] != expected_shape:
                raise RuntimeError('Component start requires the expected producing graph shape')
            if (state['lifecycle'] != 'queued' or state['misaligned'] != 0
                    or state['alignment_generation'] != expected_alignment_generation):
                raise RuntimeError('Start requires an aligned queued component at the expected generation')
            changed = connection.execute(
                "UPDATE component_states SET lifecycle='running' "
                "WHERE component_key=? AND lifecycle='queued' AND misaligned=0 AND alignment_generation=? "
                "AND stability IS NULL AND instability_origin IS NULL",
                (key, expected_alignment_generation),
            ).rowcount
            if changed != 1:
                raise RuntimeError('Component changed before start: ' + key)
            return True

        return self.submit_db_mutation(begin, wait=True, priority=0)

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
