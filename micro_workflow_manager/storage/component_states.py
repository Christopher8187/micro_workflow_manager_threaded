from __future__ import annotations

from dataclasses import dataclass

from micro_workflow_manager.component_identity import component_key, encode_component_key
from .base import FileStorageBase
from .component_transitions import ComponentTransitionStorageMixin
from .component_settlement import ComponentSettlementStorageMixin
from .selected_component_lifecycle import SelectedComponentLifecycleStorageMixin
from .component_result_identity import (
    ComponentGenerationIdentity,
    read_admitted_component_shape,
    read_component_state_record,
    observe_current_success,
)


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


def _observation_component(component) -> tuple[str, ...]:
    if not isinstance(component, (tuple, list, set, frozenset)) or not component:
        raise ValueError('A selected component needs member node names')
    return component_key(FileStorageBase.validate_node_name(node) for node in component)


def validate_component_state_snapshot(row) -> None:
    lifecycle = row['lifecycle']
    stability = row['stability']
    origin = row['instability_origin']
    misaligned = row['misaligned']
    generation = row['alignment_generation']
    no_lineage = stability is None and origin is None
    result_lineage = (
        (stability == 'stable' and origin is None)
        or (stability == 'unstable' and isinstance(origin, str) and bool(origin.strip())
            and row['origin_kind'] == 'interrupt'
            and type(row['origin_scope_admitted']) is int
            and row['origin_scope_admitted'] == 1)
    )
    valid_lifecycle = (
        (lifecycle == 'queued' and no_lineage and misaligned == 0)
        or (lifecycle == 'running' and (no_lineage or result_lineage))
        or (lifecycle in ('sampled', 'done') and result_lineage)
        or (lifecycle == 'failed' and no_lineage)
    )
    if not (
        valid_lifecycle
        and type(misaligned) is int and misaligned in (0, 1)
        and type(generation) is int and generation >= 0
    ):
        raise RuntimeError('Invalid component state for ' + row['component_key'])


def read_component_state_snapshot(connection, component) -> dict | None:
    state = read_component_state_record(
        connection, _observation_component(component), require_active=False,
    )
    return None if state is None else state.snapshot


def read_component_states_snapshot(
    connection, components, *, expected_shape: str, allow_missing: bool = False,
) -> dict[tuple[str, ...], dict | None]:
    if not isinstance(expected_shape, str) or not expected_shape.strip():
        raise ValueError('expected_shape must be nonempty text')
    members = tuple(_observation_component(component) for component in components)
    connection.execute('SAVEPOINT mwf_component_observation')
    try:
        from .component_definitions import component_snapshot_from_shape

        try:
            snapshot = component_snapshot_from_shape(expected_shape)
        except ValueError as error:
            raise RuntimeError('Component observations require a valid current graph shape') from error
        if any(component not in snapshot.components for component in members):
            raise RuntimeError('Component observations require current graph membership')
        observed = {}
        for component in members:
            state = read_component_state_record(connection, component)
            observed[component] = None if state is None else state.snapshot
        if any(not allow_missing and state is None for state in observed.values()):
            raise RuntimeError('Component observations require initialized current state')
        return observed
    finally:
        connection.execute('RELEASE SAVEPOINT mwf_component_observation')


class ComponentStateStorageMixin(
    SelectedComponentLifecycleStorageMixin, ComponentSettlementStorageMixin,
    ComponentTransitionStorageMixin,
):
    """Persist private component lifecycle records at their producing shape."""

    def _component_claim_error(self, connection, batch, checked):
        if batch.session_id is None:
            return None
        try:
            self._require_interrupt_fence_authority(
                connection, batch.session_id, encode_component_key(batch.component),
            )
            pending = connection.execute(
                'SELECT 1 FROM pending_component_executions WHERE session_id=? AND component_key=?',
                (batch.session_id, encode_component_key(batch.component)),
            ).fetchone()
            if pending is None:
                return None
            restarts = [self._read_owned_restart(
                connection, batch.node_name, job_id, batch.session_id, batch.component,
            ) for job_id in batch.job_ids]
            if all(restart is not None for restart in restarts):
                identity = self._read_component_producing_identity(connection, batch.component)
                if any((restart['owner']['shape_id'], restart['owner']['alignment_generation']) != identity
                       for restart in restarts):
                    raise RuntimeError('Restart producing component changed before claim')
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
            'SELECT pending.component_key, pending.shape_id, pending.starting_shape_id, shape.shape_json, '
            'pending.alignment_generation, '
            'pending.completion_ready, pending.stability, pending.instability_origin, '
            'pending.execution_kind, pending.starting_lifecycle, pending.starting_misaligned '
            'FROM pending_component_executions AS pending '
            'LEFT JOIN graph_shapes AS shape ON shape.shape_id=pending.shape_id '
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

    def begin_queued_component_execution(
        self, session_id: str, component, *, expected_shape: str,
        expected_alignment_generation: int, successful_lineage, expected_parent_states=None, task_parent=None,
    ) -> bool:
        """Start one aligned queued component while its selected session owns it."""
        return self._begin_component_execution(
            session_id, component, expected_shape=expected_shape,
            expected_alignment_generation=expected_alignment_generation,
            successful_lineage=successful_lineage, expected_parent_states=expected_parent_states,
            task_parent=task_parent, starting_lifecycle='queued',
        )

    def begin_sampled_component_execution(
        self, session_id: str, component, *, expected_shape: str,
        expected_alignment_generation: int, successful_lineage, expected_parent_states=None, task_parent=None,
    ) -> bool:
        """Resume one aligned sampled component while retaining its successful lineage."""
        return self._begin_component_execution(
            session_id, component, expected_shape=expected_shape,
            expected_alignment_generation=expected_alignment_generation,
            successful_lineage=successful_lineage, expected_parent_states=expected_parent_states,
            task_parent=task_parent, starting_lifecycle='sampled',
        )

    def _begin_component_execution(
        self, session_id: str, component, *, expected_shape: str,
        expected_alignment_generation: int, successful_lineage, expected_parent_states, task_parent,
        starting_lifecycle: str,
    ) -> bool:
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
            self._require_interrupt_component_admission(connection, session_id, component)
            self._validate_component_task_parent(connection, session_id, task_parent)
            admitted = read_admitted_component_shape(
                connection, session_id, proposal.component if proposal is not None else component,
                expected_shape,
            )
            owner = connection.execute(
                'SELECT command FROM execution_sessions WHERE session_id=?', (session_id,),
            ).fetchone()
            if (starting_lifecycle == 'sampled'
                    and owner['command'] not in ('resume', 'resumefrom', 'resumebetween')):
                raise RuntimeError('Sampled component execution requires a resume session')
            state_record = read_component_state_record(connection, component)
            if state_record is None:
                raise RuntimeError('Unknown component: ' + key)
            state = state_record.snapshot
            if (state['lifecycle'] == 'running' and task_parent is not None
                    and encode_component_key(task_parent.component) == key):
                pending = connection.execute(
                    'SELECT pending.component_key, pending.shape_id, pending.starting_shape_id, shape.shape_json, '
                    'pending.alignment_generation, '
                    'pending.completion_ready, pending.stability, pending.instability_origin, '
                    'pending.execution_kind, pending.starting_lifecycle, pending.starting_misaligned '
                    'FROM pending_component_executions AS pending '
                    'JOIN graph_shapes AS shape ON shape.shape_id=pending.shape_id '
                    'WHERE pending.session_id=? AND pending.component_key=?', (session_id, key),
                ).fetchone()
                if (pending is None or pending['shape_json'] != expected_shape
                        or pending['shape_id'] != admitted.shape_id
                        or pending['alignment_generation'] != state['alignment_generation']
                        or state_record.identity != ComponentGenerationIdentity(
                            admitted.shape_id, expected_alignment_generation,
                        )):
                    raise RuntimeError('Nested component execution has no matching recorded start')
                self._validate_pending_component_row(connection, pending)
                return False
            if (state['lifecycle'] != starting_lifecycle or state['misaligned'] != 0
                    or state['alignment_generation'] != expected_alignment_generation):
                raise RuntimeError(
                    f'Start requires an aligned {starting_lifecycle} component at the expected generation'
                )
            if proposal is None:
                raise ValueError('Component start requires its calculated successful lineage')
            self._validate_interrupt_component_start(
                connection, session_id, proposal.component,
                (proposal.stability, proposal.instability_origin),
            )
            retained_lineage = (state['stability'], state['instability_origin'])
            if (starting_lifecycle == 'queued' and retained_lineage != (None, None)):
                raise RuntimeError('Queued component start cannot retain successful lineage')
            retained = observe_current_success(connection, state_record)
            if starting_lifecycle == 'sampled':
                if (retained is None or retained.result != ('sampled', *retained_lineage)
                        or retained_lineage != (proposal.stability, proposal.instability_origin)):
                    raise RuntimeError('Sampled resume cannot replace retained successful result')
            retained_pointer = state_record.retained_result_identity
            execution_kind = 'resume' if starting_lifecycle == 'sampled' else 'full'
            self._validate_pending_component_row(connection, {
                'completion_ready': 0, 'stability': proposal.stability,
                'instability_origin': proposal.instability_origin,
                'execution_kind': execution_kind, 'starting_lifecycle': starting_lifecycle,
                'starting_misaligned': 0, 'shape_id': admitted.shape_id,
                'starting_shape_id': state_record.identity.shape_id, 'component_key': key,
            })
            for parent, observed in parents.items():
                if self._read_component_state(connection, parent) != observed:
                    raise RuntimeError('Component parent changed before start: ' + encode_component_key(parent))
            changed = connection.execute(
                "UPDATE component_states SET shape_id=?, lifecycle='running', "
                "retained_result_shape_id=?, retained_result_alignment_generation=? "
                "WHERE component_key=? AND shape_id=? AND lifecycle=? AND misaligned=0 "
                "AND alignment_generation=? AND stability IS ? AND instability_origin IS ? "
                "AND retained_result_shape_id IS ? AND retained_result_alignment_generation IS ?",
                (admitted.shape_id,
                 None if retained_pointer is None else retained_pointer.shape_id,
                 None if retained_pointer is None else retained_pointer.alignment_generation,
                 key, state_record.identity.shape_id, starting_lifecycle,
                 expected_alignment_generation, *retained_lineage,
                 None if retained_pointer is None else retained_pointer.shape_id,
                 None if retained_pointer is None else retained_pointer.alignment_generation),
            ).rowcount
            if changed != 1:
                raise RuntimeError('Component changed before start: ' + key)
            recorded = connection.execute(
                'INSERT INTO pending_component_executions '
                '(session_id, component_key, shape_id, starting_shape_id, alignment_generation, '
                'stability, instability_origin, execution_kind, starting_lifecycle, starting_misaligned) '
                'VALUES(?,?,?,?,?,?,?,?,?,0)',
                (session_id, key, admitted.shape_id, state_record.identity.shape_id,
                 expected_alignment_generation, proposal.stability, proposal.instability_origin,
                 execution_kind, starting_lifecycle),
            ).rowcount
            if recorded != 1:
                raise RuntimeError('Component start was not recorded: ' + key)
            return True

        return self.submit_db_mutation(begin, wait=True, priority=0)

    def read_component_states(self, components, *, expected_shape: str, allow_missing: bool = False) -> dict:
        """Read one ordered snapshot, optionally retaining absent definitions as None."""
        self._require_execution_session_storage()
        return read_component_states_snapshot(
            self.db_connection(), components,
            expected_shape=expected_shape, allow_missing=allow_missing,
        )

    def get_component_state(self, component) -> dict | None:
        self._require_execution_session_storage()
        return read_component_state_snapshot(self.db_connection(), component)

    def _read_component_state(self, connection, members):
        return read_component_state_snapshot(connection, members)

    @staticmethod
    def _validate_component_state_row(row) -> None:
        validate_component_state_snapshot(row)
