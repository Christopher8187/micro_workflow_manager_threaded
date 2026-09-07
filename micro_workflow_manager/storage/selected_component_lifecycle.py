from __future__ import annotations

from micro_workflow_manager.component_identity import encode_component_key
from .selected_execution import read_selected_execution_jobs


class SelectedComponentLifecycleStorageMixin:
    """Begin selected attempts and retain successful results across failed repairs."""

    def _read_component_successful_result(self, connection, component, identity):
        row = connection.execute(
            'SELECT result.*, origin.session_kind AS origin_kind FROM component_successful_results AS result '
            'LEFT JOIN execution_sessions AS origin ON origin.session_id=result.instability_origin '
            'WHERE component_key=? AND shape_id=? AND alignment_generation=?',
            (encode_component_key(component), *identity),
        ).fetchone()
        if row is None:
            return None
        if (row['lifecycle'] not in ('sampled', 'done')
                or not ((row['stability'] == 'stable' and row['instability_origin'] is None)
                        or (row['stability'] == 'unstable' and isinstance(row['instability_origin'], str)
                            and bool(row['instability_origin'].strip())
                            and row['origin_kind'] == 'interrupt'))):
            raise RuntimeError('Invalid retained successful component result')
        return tuple(row[name] for name in ('lifecycle', 'stability', 'instability_origin'))

    def _record_component_successful_result(self, connection, component, identity, result):
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
            identity = self._read_component_producing_identity(connection, component)
            read_selected_execution_jobs(self, context, roots, expected_identity, connection=connection)
            state = self._read_component_state(connection, component)
            if state != expected or state['lifecycle'] == 'running':
                raise RuntimeError('Selected component changed before its captured start')
            if connection.execute(
                'SELECT 1 FROM pending_component_executions WHERE component_key=?', (key,),
            ).fetchone():
                raise RuntimeError('Selected component already has a pending execution')
            for parent, observed in parents.items():
                if self._read_component_state(connection, parent) != observed:
                    raise RuntimeError('Selected component parent changed before start')
            prior = self._read_component_successful_result(connection, component, identity)
            if state['lifecycle'] in ('sampled', 'done'):
                current = tuple(state[name] for name in ('lifecycle', 'stability', 'instability_origin'))
                if prior is not None and prior != current:
                    raise RuntimeError('Current and retained successful component results disagree')
                prior = current
                self._record_component_successful_result(connection, component, identity, prior)
            elif state['lifecycle'] == 'queued' and prior is not None:
                raise RuntimeError('Queued component unexpectedly retains success at its current alignment')
            if prior is not None and prior[1:] != successful_lineage:
                raise RuntimeError('Selected execution cannot combine incompatible successful lineage')
            self._validate_pending_component_row(connection, {
                'completion_ready': 0, 'stability': proposal.stability,
                'instability_origin': proposal.instability_origin, 'execution_kind': 'jobs',
                'starting_lifecycle': state['lifecycle'], 'starting_misaligned': int(state['misaligned']),
            })
            retained_lineage = (None, None) if prior is None else prior[1:]
            changed = connection.execute(
                "UPDATE component_states SET lifecycle='running', stability=?, instability_origin=? "
                'WHERE component_key=? AND lifecycle=? AND misaligned=? AND alignment_generation=?',
                (*retained_lineage, key, state['lifecycle'], int(state['misaligned']), identity[1]),
            ).rowcount
            if changed != 1:
                raise RuntimeError('Selected component changed before start')
            recorded = connection.execute(
                'INSERT INTO pending_component_executions '
                '(session_id, component_key, shape_id, alignment_generation, stability, instability_origin, '
                'execution_kind, starting_lifecycle, starting_misaligned) VALUES(?,?,?,?,?,?,?,?,?)',
                (session_id, key, *identity, *successful_lineage, 'jobs', state['lifecycle'], int(state['misaligned'])),
            ).rowcount
            if recorded != 1:
                raise RuntimeError('Selected component start was not recorded')
            return proposal, identity

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
        prior = self._read_component_successful_result(connection, component, identity)
        if pending['starting_lifecycle'] == 'queued' and prior is not None:
            raise RuntimeError('Queued selected execution cannot have a retained successful result')
        if pending['starting_lifecycle'] in ('sampled', 'done') and (
            prior is None or prior[0] != pending['starting_lifecycle']
        ):
            raise RuntimeError('Selected execution lost its retained successful result')
        if prior is not None and prior[1:] != (pending['stability'], pending['instability_origin']):
            raise RuntimeError('Selected pending lineage differs from its retained success')
        full_coverage = all(
            job['status'] in ('done', 'skipped') and job['active_execution_id'] is None
            for node in component for job in connection.execute(
                'SELECT status, active_execution_id FROM jobs WHERE node_name=?', (node,),
            )
        )
        lifecycle = ('done' if full_coverage or (prior is not None and prior[0] == 'done') else 'sampled')
        return ComponentTerminalOutcome(
            component, pending['shape_json'], identity[1], lifecycle if successful else 'failed',
            pending['stability'] if successful else None,
            pending['instability_origin'] if successful else None,
        )
