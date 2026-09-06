from __future__ import annotations

from dataclasses import dataclass, replace

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key
from .component_transitions import ComponentTransitionStorageMixin


@dataclass(frozen=True)
class ComponentTerminalOutcome:
    """A calculated result guarded by the component shape and generation used."""

    component: tuple[str, ...]
    expected_shape: str
    expected_alignment_generation: int
    lifecycle: str
    stability: str | None
    instability_origin: str | None


@dataclass(frozen=True)
class ComponentTaskParent:
    node_name: str
    job_id: int
    generation: int
    execution_id: str
    session_id: str
    component: tuple[str, ...]


class ComponentExecutionIncomplete(RuntimeError):
    """Successful tasks left unfinished work in their begun component."""


class ComponentStateStorageMixin(ComponentTransitionStorageMixin):
    """Persist private component lifecycle records at their producing shape."""

    def _component_claim_error(self, connection, batch, checked):
        if batch.session_id is None:
            return None
        try:
            pending = connection.execute(
                'SELECT 1 FROM pending_component_executions WHERE session_id=? AND component_key=?',
                (batch.session_id, encode_component_key(batch.component)),
            ).fetchone()
            if pending is None:
                return None
            if all(self._read_owned_restart(
                connection, batch.node_name, job_id, batch.session_id, batch.component,
            ) is not None for job_id in batch.job_ids):
                # Accepted repairs precede terminal proposal validation. A
                # damaged pending result must still refuse publication later.
                return None
            key = batch.session_id, batch.component
            if key not in checked:
                checked[key] = self._read_pending_component_work(connection, *key)
            unfinished = checked[key]
            if unfinished:
                node, job_id = next(iter(unfinished))
                return ComponentExecutionIncomplete(
                    f'Ordinary admission waits for unfinished component job {node}/{job_id}'
                )
        except Exception as error:
            # Keep this batch's damaged state from rolling back another
            # component's claims or an exact accepted replacement batch.
            return error
        return None

    def _read_pending_component_work(self, connection, session_id, component):
        pending = connection.execute(
            'SELECT shape.shape_json, pending.alignment_generation, '
            'pending.completion_ready, pending.stability, pending.instability_origin '
            'FROM pending_component_executions AS pending LEFT JOIN graph_shapes AS shape USING(shape_id) '
            'WHERE pending.session_id=? AND pending.component_key=?',
            (session_id, encode_component_key(component)),
        ).fetchone()
        if pending is None:
            return {}
        proposal = ComponentTerminalOutcome(
            component, pending['shape_json'], pending['alignment_generation'],
            'done', pending['stability'], pending['instability_origin'],
        )
        self._validate_component_terminal_outcomes(connection, session_id, (proposal,))
        unfinished = {}
        for node in component:
            for job in connection.execute(
                "SELECT job_id, status FROM jobs WHERE node_name=? AND "
                "(status IN ('failed','cancelled') OR restart_requested_at IS NOT NULL "
                "OR (generation>0 AND (active_execution_id IS NOT NULL OR status='queued')))",
                (node,),
            ).fetchall():
                key = node, job['job_id']
                observed = self._read_job_owner_observation(connection, *key)
                owner = observed['owner']
                if owner is None or owner['session_id'] != session_id or owner['component'] != component:
                    continue
                if job['status'] in ('queued', 'running') and self._is_claimed_component_restart(connection, observed):
                    unfinished[key] = None
                    continue
                if job['status'] == 'running':
                    continue
                if observed['active_execution_id'] is not None:
                    raise RuntimeError('Unfinished component work still has an active execution')
                if job['status'] == 'queued':
                    expected = self._read_owned_restart(connection, *key, session_id, component)
                    if expected is not None:
                        unfinished[key] = expected
                else:
                    unfinished[key] = None
        return unfinished

    def finish_successful_component_execution(
        self, session_id: str, outcome: ComponentTerminalOutcome, *, task_parent=None,
    ) -> bool:
        """Publish one successful component while retaining its session scope."""
        self._require_execution_session_storage()
        self._session_text(session_id, 'session_id')
        outcomes = self._normalize_component_terminal_outcomes([outcome])
        if outcomes[0].lifecycle != 'done':
            raise ValueError('Intermediate component completion requires a successful result')

        def finish(connection):
            self._validate_component_task_parent(connection, session_id, task_parent)
            self._validate_component_terminal_outcomes(connection, session_id, outcomes)
            if task_parent is not None and task_parent.component == outcomes[0].component:
                ready = connection.execute(
                    'UPDATE pending_component_executions SET completion_ready=1 '
                    'WHERE session_id=? AND component_key=?',
                    (session_id, encode_component_key(outcome.component)),
                ).rowcount
                if ready != 1:
                    raise RuntimeError('Deferred component completion was not recorded')
                return False
            self._publish_component_terminal_outcomes(connection, session_id, outcomes)
            return True

        return self.submit_db_mutation(finish, wait=True, priority=0)

    def _validate_component_task_parent(self, connection, session_id, task_parent):
        if task_parent is None:
            return
        if not isinstance(task_parent, ComponentTaskParent):
            raise ValueError('Invalid component task parent')
        observed = self._read_job_owner_observation(connection, task_parent.node_name, task_parent.job_id)
        owner = None if observed is None else observed['owner']
        reservation = connection.execute(
            'SELECT session_id FROM component_reservations WHERE component_key=?',
            (encode_component_key(task_parent.component),),
        ).fetchone()
        if (owner is None or task_parent.session_id != session_id or owner['session_id'] != session_id
                or owner['component'] != task_parent.component
                or observed['generation'] != task_parent.generation
                or observed['active_execution_id'] != task_parent.execution_id
                or owner['execution_id'] != task_parent.execution_id or observed['status'] != 'running'
                or reservation is None or reservation['session_id'] != session_id):
            raise RuntimeError('Component task parent is no longer the active owner')

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

    def _validate_pending_component_row(self, connection, row):
        ready = row['completion_ready']
        stability, origin = row['stability'], row['instability_origin']
        if type(ready) is not int or ready not in (0, 1):
            raise RuntimeError('Invalid pending component completion metadata')
        valid = ((stability == 'stable' and origin is None)
                 or (stability == 'unstable' and isinstance(origin, str) and bool(origin.strip())))
        if not valid:
            raise RuntimeError('Invalid pending component completion lineage')
        if origin is not None:
            recorded_origin = connection.execute(
                'SELECT session_kind FROM execution_sessions WHERE session_id=?', (origin,),
            ).fetchone()
            if recorded_origin is None or recorded_origin['session_kind'] != 'interrupt':
                raise RuntimeError('A pending component requires an exact interrupt origin')

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
            pending = connection.execute(
                'SELECT shape.shape_json, pending.alignment_generation, '
                'pending.completion_ready, pending.stability, pending.instability_origin '
                'FROM pending_component_executions AS pending JOIN graph_shapes AS shape USING(shape_id) '
                'WHERE pending.session_id=? AND pending.component_key=?', (session_id, key),
            ).fetchone()
            if (pending is None or pending['shape_json'] != outcome.expected_shape
                    or pending['alignment_generation'] != outcome.expected_alignment_generation):
                raise RuntimeError('Component settlement requires its matching pending execution: ' + key)
            self._validate_pending_component_row(connection, pending)
            if (outcome.lifecycle == 'done'
                    and (pending['stability'], pending['instability_origin'])
                    != (outcome.stability, outcome.instability_origin)):
                raise RuntimeError('Component completion cannot replace recorded successful lineage')
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

    def _publish_component_terminal_outcomes(self, connection, session_id, outcomes):
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
                        raise ComponentExecutionIncomplete(
                            f'Component completion has unfinished job {node}/{job["job_id"]}'
                        )
            changed = connection.execute(
                'UPDATE component_states SET lifecycle=?, stability=?, instability_origin=? '
                "WHERE component_key=? AND lifecycle='running' AND misaligned=0 AND alignment_generation=?",
                (outcome.lifecycle, outcome.stability, outcome.instability_origin,
                 encode_component_key(outcome.component), outcome.expected_alignment_generation),
            ).rowcount
            if changed != 1:
                raise RuntimeError('Component changed before settlement')
            removed = connection.execute(
                'DELETE FROM pending_component_executions WHERE session_id=? AND component_key=?',
                (session_id, encode_component_key(outcome.component)),
            ).rowcount
            if removed != 1:
                raise RuntimeError('Component pending execution was not removed')

    def begin_queued_component_execution(
        self, session_id: str, component, *, expected_shape: str,
        expected_alignment_generation: int, successful_lineage, expected_parent_states=None, task_parent=None,
    ) -> bool:
        """Start one aligned queued component while its selected session owns it."""
        self._require_execution_session_storage()
        self._session_text(session_id, 'session_id')
        self._session_text(expected_shape, 'expected_shape')
        key = encode_component_key(self._session_component(component))
        if type(expected_alignment_generation) is not int or expected_alignment_generation < 0:
            raise ValueError('Expected alignment generation must be a nonnegative integer')
        proposal = None
        if successful_lineage is not None:
            if not isinstance(successful_lineage, tuple) or len(successful_lineage) != 2:
                raise ValueError('Component start requires its calculated successful lineage')
            proposal, = self._normalize_component_terminal_outcomes([ComponentTerminalOutcome(
                self._session_component(component), expected_shape, expected_alignment_generation,
                'done', *successful_lineage,
            )])
        parents = {self._session_component(parent): dict(state)
                   for parent, state in (expected_parent_states or {}).items()}

        def begin(connection):
            self._validate_component_task_parent(connection, session_id, task_parent)
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
            if (state['lifecycle'] == 'running' and task_parent is not None
                    and encode_component_key(task_parent.component) == key):
                pending = connection.execute(
                    'SELECT shape.shape_json, pending.alignment_generation, '
                    'pending.completion_ready, pending.stability, pending.instability_origin '
                    'FROM pending_component_executions AS pending JOIN graph_shapes AS shape USING(shape_id) '
                    'WHERE pending.session_id=? AND pending.component_key=?', (session_id, key),
                ).fetchone()
                if (pending is None or pending['shape_json'] != expected_shape
                        or pending['alignment_generation'] != state['alignment_generation']
                        or state['alignment_generation'] != expected_alignment_generation):
                    raise RuntimeError('Nested component execution has no matching recorded start')
                self._validate_pending_component_row(connection, pending)
                return False
            if (state['lifecycle'] != 'queued' or state['misaligned'] != 0
                    or state['alignment_generation'] != expected_alignment_generation):
                raise RuntimeError('Start requires an aligned queued component at the expected generation')
            if proposal is None:
                raise ValueError('Component start requires its calculated successful lineage')
            self._validate_pending_component_row(connection, {
                'completion_ready': 0, 'stability': proposal.stability,
                'instability_origin': proposal.instability_origin,
            })
            for parent, observed in parents.items():
                if self._read_component_state(connection, parent) != observed:
                    raise RuntimeError('Component parent changed before start: ' + encode_component_key(parent))
            changed = connection.execute(
                "UPDATE component_states SET lifecycle='running' "
                "WHERE component_key=? AND lifecycle='queued' AND misaligned=0 AND alignment_generation=? "
                "AND stability IS NULL AND instability_origin IS NULL",
                (key, expected_alignment_generation),
            ).rowcount
            if changed != 1:
                raise RuntimeError('Component changed before start: ' + key)
            recorded = connection.execute(
                'INSERT INTO pending_component_executions '
                '(session_id, component_key, shape_id, alignment_generation, stability, instability_origin) '
                'SELECT ?, component_key, shape_id, ?, ?, ? FROM component_definitions WHERE component_key=?',
                (session_id, expected_alignment_generation, proposal.stability, proposal.instability_origin, key),
            ).rowcount
            if recorded != 1:
                raise RuntimeError('Component start was not recorded: ' + key)
            return True

        return self.submit_db_mutation(begin, wait=True, priority=0)

    def read_component_states(self, components, *, expected_shape: str, allow_missing: bool = False) -> dict:
        """Read one ordered snapshot, optionally retaining absent definitions as None."""
        self._require_execution_session_storage()
        self._session_text(expected_shape, 'expected_shape')
        members = tuple(self._session_component(component) for component in components)
        connection = self.db_connection()
        connection.execute('SAVEPOINT mwf_component_observation')
        try:
            observed = {component: self._read_component_state(connection, component) for component in members}
            if any((not allow_missing if state is None else state['shape_json'] != expected_shape)
                   for state in observed.values()):
                raise RuntimeError('Component observations require the expected producing shape')
            return observed
        finally:
            connection.execute('RELEASE SAVEPOINT mwf_component_observation')

    def get_component_state(self, component) -> dict | None:
        self._require_execution_session_storage()
        members = self._session_component(component)
        return self._read_component_state(self.db_connection(), members)

    def _read_component_state(self, connection, members):
        row = connection.execute(
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
