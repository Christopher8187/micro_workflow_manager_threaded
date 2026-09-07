from __future__ import annotations

from dataclasses import replace

from micro_workflow_manager.component_identity import encode_component_key


class ComponentSettlementStorageMixin:
    """Validate and publish component outcomes in the session writer."""

    def _normalize_component_terminal_outcomes(self, outcomes):
        from .component_states import ComponentTerminalOutcome

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
            if outcome.lifecycle not in ('sampled', 'done', 'failed'):
                raise ValueError('A terminal component outcome must be sampled, done, or failed')
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
        if (row['execution_kind'] not in ('full', 'jobs')
                or row['starting_lifecycle'] not in ('queued', 'sampled', 'done', 'failed')
                or type(row['starting_misaligned']) is not int or row['starting_misaligned'] not in (0, 1)
                or (row['execution_kind'] == 'full'
                    and (row['starting_lifecycle'] != 'queued' or row['starting_misaligned'] != 0))
                or (row['starting_lifecycle'] == 'queued' and row['starting_misaligned'] != 0)):
            raise RuntimeError('Invalid pending component execution kind or starting state')
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
                'pending.completion_ready, pending.stability, pending.instability_origin, '
                'pending.execution_kind, pending.starting_lifecycle, pending.starting_misaligned '
                'FROM pending_component_executions AS pending JOIN graph_shapes AS shape USING(shape_id) '
                'WHERE pending.session_id=? AND pending.component_key=?', (session_id, key),
            ).fetchone()
            if (pending is None or pending['shape_json'] != outcome.expected_shape
                    or pending['alignment_generation'] != outcome.expected_alignment_generation):
                raise RuntimeError('Component settlement requires its matching pending execution: ' + key)
            self._validate_pending_component_row(connection, pending)
            if outcome.lifecycle == 'sampled' and pending['execution_kind'] != 'jobs':
                raise RuntimeError('Only selected execution may settle as sampled')
            if (outcome.lifecycle != 'failed'
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
                    or state['misaligned'] != pending['starting_misaligned']
                    or state['alignment_generation'] != outcome.expected_alignment_generation):
                raise RuntimeError('Component settlement requires the expected aligned running state: ' + key)
            if outcome.lifecycle != 'failed':
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
        from .component_states import ComponentExecutionIncomplete

        for outcome in outcomes:
            pending = connection.execute(
                'SELECT pending.*, shape.shape_json FROM pending_component_executions AS pending '
                'JOIN graph_shapes AS shape USING(shape_id) WHERE session_id=? AND component_key=?',
                (session_id, encode_component_key(outcome.component)),
            ).fetchone()
            selected = pending['execution_kind'] == 'jobs'
            if selected:
                calculated = self._selected_component_result(
                    connection, session_id, pending, successful=outcome.lifecycle != 'failed',
                )
                if calculated != outcome or (outcome.lifecycle != 'failed' and not pending['completion_ready']):
                    raise RuntimeError('Selected outcome does not match its joined causal coverage')
            for node in outcome.component:
                for job in connection.execute(
                    'SELECT job_id, status, active_execution_id FROM jobs WHERE node_name=?', (node,),
                ):
                    if job['status'] == 'running' or job['active_execution_id'] is not None:
                        raise ExecutionSessionHasActiveJobs(
                            f'Component settlement still has active job {node}/{job["job_id"]}'
                        )
                    if not selected and outcome.lifecycle == 'done' and job['status'] not in ('done', 'skipped'):
                        raise ComponentExecutionIncomplete(
                            f'Component completion has unfinished job {node}/{job["job_id"]}'
                        )
            changed = connection.execute(
                'UPDATE component_states SET lifecycle=?, stability=?, instability_origin=? '
                "WHERE component_key=? AND lifecycle='running' AND misaligned=? AND alignment_generation=?",
                (outcome.lifecycle, outcome.stability, outcome.instability_origin,
                 encode_component_key(outcome.component), pending['starting_misaligned'],
                 outcome.expected_alignment_generation),
            ).rowcount
            if changed != 1:
                raise RuntimeError('Component changed before settlement')
            if outcome.lifecycle != 'failed':
                self._record_component_successful_result(
                    connection, outcome.component, (pending['shape_id'], outcome.expected_alignment_generation),
                    (outcome.lifecycle, outcome.stability, outcome.instability_origin),
                )
            removed = connection.execute(
                'DELETE FROM pending_component_executions WHERE session_id=? AND component_key=?',
                (session_id, encode_component_key(outcome.component)),
            ).rowcount
            if removed != 1:
                raise RuntimeError('Component pending execution was not removed')
