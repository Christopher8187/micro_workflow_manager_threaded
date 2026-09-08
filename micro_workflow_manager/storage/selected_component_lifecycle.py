from __future__ import annotations

import json

from micro_workflow_manager.component_identity import encode_component_key
from .selected_execution import read_selected_execution_jobs
from .component_result_identity import (
    ComponentGenerationIdentity,
    read_admitted_component_shape,
    read_component_state_record,
    observe_current_success,
    read_retained_successful_result,
    read_successful_result,
)


class SelectedComponentLifecycleStorageMixin:
    """Begin selected attempts and retain successful results across failed repairs."""

    def _read_component_successful_result(self, connection, component, identity):
        result = read_successful_result(connection, component, identity)
        return None if result is None else result.result

    def _record_component_successful_result(self, connection, component, identity, result):
        if isinstance(identity, ComponentGenerationIdentity):
            identity = identity.values
        changed = connection.execute(
            'INSERT INTO component_successful_results '
            '(component_key, shape_id, alignment_generation, lifecycle, stability, instability_origin) '
            'VALUES(?,?,?,?,?,?) ON CONFLICT(component_key, shape_id, alignment_generation) DO UPDATE '
            'SET lifecycle=excluded.lifecycle, stability=excluded.stability, '
            'instability_origin=excluded.instability_origin',
            (encode_component_key(component), *identity, *result),
        ).rowcount
        if changed != 1:
            raise RuntimeError('Successful component result was not recorded')

    def _match_component_successful_result(
        self, connection, component, identity, current, *, record_missing=False,
        require_present=False,
    ):
        """Match current success to its exact retained row, optionally recording it."""
        prior = self._read_component_successful_result(connection, component, identity)
        if prior is None:
            if require_present:
                raise RuntimeError('Component lost its retained successful result')
            if record_missing:
                self._record_component_successful_result(connection, component, identity, current)
                return current
            return None
        if prior != current:
            raise RuntimeError('Current and retained successful component results disagree')
        return prior

    def begin_selected_component_execution(
        self, context, roots, *, expected_identity, expected_state, expected_parent_states, successful_lineage,
    ):
        """Capture selected lifecycle, lineage, and producing identity in one writer."""
        from .component_states import ComponentTerminalOutcome

        self._require_execution_session_storage()
        session_id, ownership, shape = context
        component = ownership[roots[0][0]]
        key = encode_component_key(component)
        expected = dict(expected_state)
        parents = {parent: dict(state) for parent, state in expected_parent_states.items()}
        proposal, = self._normalize_component_terminal_outcomes([ComponentTerminalOutcome(
            component, shape, expected['alignment_generation'], 'done', *successful_lineage,
        )])

        def begin(connection):
            observed_identity = ComponentGenerationIdentity(*expected_identity)
            admitted = read_admitted_component_shape(connection, session_id, component, shape)
            read_selected_execution_jobs(self, context, roots, expected_identity, connection=connection)
            state_record = read_component_state_record(connection, component)
            if (state_record is None or state_record.snapshot != expected
                    or state_record.identity != observed_identity or state_record.lifecycle == 'running'):
                raise RuntimeError('Selected component changed before its captured start')
            if connection.execute(
                'SELECT 1 FROM pending_component_executions WHERE component_key=?', (key,),
            ).fetchone():
                raise RuntimeError('Selected component already has a pending execution')
            for parent, observed in parents.items():
                if self._read_component_state(connection, parent) != observed:
                    raise RuntimeError('Selected component parent changed before start')
            prior = observe_current_success(connection, state_record)
            if state_record.lifecycle == 'queued' and prior is not None:
                raise RuntimeError('Queued component unexpectedly retains successful history')
            if prior is not None and prior.lineage != successful_lineage:
                raise RuntimeError('Selected execution cannot combine incompatible successful lineage')
            self._validate_pending_component_row(connection, {
                'completion_ready': 0, 'stability': proposal.stability,
                'instability_origin': proposal.instability_origin, 'execution_kind': 'jobs',
                'starting_lifecycle': state_record.lifecycle,
                'starting_misaligned': int(state_record.misaligned),
                'shape_id': admitted.shape_id,
                'starting_shape_id': state_record.identity.shape_id,
                'component_key': key,
            })
            retained_lineage = (None, None) if prior is None else prior.lineage
            pointer = state_record.retained_result_identity
            changed = connection.execute(
                "UPDATE component_states SET shape_id=?, lifecycle='running', "
                'stability=?, instability_origin=?, retained_result_shape_id=?, '
                'retained_result_alignment_generation=? '
                'WHERE component_key=? AND shape_id=? AND lifecycle=? AND misaligned=? '
                'AND alignment_generation=? AND retained_result_shape_id IS ? '
                'AND retained_result_alignment_generation IS ?',
                (admitted.shape_id, *retained_lineage,
                 None if pointer is None else pointer.shape_id,
                 None if pointer is None else pointer.alignment_generation,
                 key, state_record.identity.shape_id,
                 state_record.lifecycle, int(state_record.misaligned),
                 state_record.identity.alignment_generation,
                 None if pointer is None else pointer.shape_id,
                 None if pointer is None else pointer.alignment_generation),
            ).rowcount
            if changed != 1:
                raise RuntimeError('Selected component changed before start')
            recorded = connection.execute(
                'INSERT INTO pending_component_executions '
                '(session_id, component_key, shape_id, starting_shape_id, alignment_generation, '
                'stability, instability_origin, execution_kind, starting_lifecycle, starting_misaligned) '
                'VALUES(?,?,?,?,?,?,?,?,?,?)',
                (session_id, key, admitted.shape_id, state_record.identity.shape_id,
                 state_record.identity.alignment_generation, *successful_lineage, 'jobs',
                 state_record.lifecycle, int(state_record.misaligned)),
            ).rowcount
            if recorded != 1:
                raise RuntimeError('Selected component start was not recorded')
            return proposal, (admitted.shape_id, state_record.identity.alignment_generation)

        return self.submit_db_mutation(begin, wait=True, priority=0)

    def mark_selected_component_execution_ready(self, session_id, proposal):
        def ready(connection):
            self._validate_component_terminal_outcomes(connection, session_id, (proposal,))
            row = connection.execute(
                'SELECT pending.*, shape.shape_json FROM pending_component_executions AS pending '
                'JOIN graph_shapes AS shape USING(shape_id) WHERE session_id=? AND component_key=?',
                (session_id, encode_component_key(proposal.component)),
            ).fetchone()
            if row['execution_kind'] != 'jobs':
                raise RuntimeError('Selected completion requires its selected pending attempt')
            self._selected_component_result(connection, session_id, row, successful=True)
            changed = connection.execute(
                'UPDATE pending_component_executions SET completion_ready=1 '
                'WHERE session_id=? AND component_key=?',
                (session_id, encode_component_key(proposal.component)),
            ).rowcount
            if changed != 1:
                raise RuntimeError('Selected completion was not recorded')

        self.submit_db_mutation(ready, wait=True, priority=0)

    def _selected_component_result(self, connection, session_id, pending, *, successful):
        from .component_states import ComponentExecutionIncomplete, ComponentTerminalOutcome

        component = self._stored_session_component(pending['component_key'])
        identity = pending['shape_id'], pending['alignment_generation']
        roots = tuple(self._read_session_job_roots(connection, session_id))
        context = session_id, {node: component for node in component}, pending['shape_json']
        frontier = read_selected_execution_jobs(self, context, roots, identity, connection=connection)
        if successful and any(status not in ('done', 'skipped') for _, _, status in frontier):
            raise ComponentExecutionIncomplete('Selected completion has unfinished causal work')
        state = read_component_state_record(connection, component)
        if state is None or state.identity != ComponentGenerationIdentity(*identity):
            raise RuntimeError('Selected execution lost its admitted component identity')
        prior = read_retained_successful_result(connection, state)
        if pending['starting_lifecycle'] == 'queued' and prior is not None:
            raise RuntimeError('Queued selected execution cannot have a retained successful result')
        if pending['starting_lifecycle'] in ('sampled', 'done') and (
            prior is None or prior.lifecycle != pending['starting_lifecycle']
        ):
            raise RuntimeError('Selected execution lost its retained successful result')
        if prior is not None and prior.lineage != (pending['stability'], pending['instability_origin']):
            raise RuntimeError('Selected pending lineage differs from its retained success')
        session = connection.execute(
            'SELECT command, details_json FROM execution_sessions WHERE session_id=?', (session_id,),
        ).fetchone()
        if session is None:
            raise RuntimeError('Selected execution lost its session')
        try:
            details = json.loads(session['details_json'])
        except (TypeError, json.JSONDecodeError) as error:
            raise RuntimeError('Selected execution has damaged session details') from error
        if not isinstance(details, dict):
            raise RuntimeError('Selected execution has damaged session details')
        if session['command'] == 'run sample' or 'selection' in details:
            from .sample_history import read_sample_admission_history, sample_has_no_unprocessed_work

            history = read_sample_admission_history(
                self, connection, session_id, component=component, expected_shape=pending['shape_json'],
            )
            full_coverage = sample_has_no_unprocessed_work(self, connection, history)
            lifecycle = 'done' if full_coverage else 'sampled'
        else:
            full_coverage = all(
                job['status'] in ('done', 'skipped') and job['active_execution_id'] is None
                for node in component for job in connection.execute(
                    'SELECT status, active_execution_id FROM jobs WHERE node_name=?', (node,),
                )
            )
            lifecycle = 'done' if full_coverage or (prior is not None and prior.lifecycle == 'done') else 'sampled'
        return ComponentTerminalOutcome(
            component, pending['shape_json'], identity[1], lifecycle if successful else 'failed',
            pending['stability'] if successful else None,
            pending['instability_origin'] if successful else None,
        )
