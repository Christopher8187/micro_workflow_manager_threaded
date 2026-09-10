"""Validate and finish every unit before publishing a regional membership repair."""

import json
from dataclasses import replace

from .job_preparation import _read_jobs
from .input_publication_files import checked_input_path

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key
from .component_definitions import component_snapshot_from_shape
from . import component_membership
from .membership_footprint import selected_membership_owners
from .membership_history import read_membership_history
from .preparation_guards import validate_held_guards
from .preparation_receipts import submit_preparation_decision
from .membership_completion import MembershipCompletion
from .job_preparation import validate_job_preparation
from .preparation_footprint import validate_prepared_inputs


class MembershipPreparation:
    def __init__(
        self, storage, change, footprint, session_id, operation, *,
        admitted_components=None, interrupt_preflight_record=None,
    ):
        self.storage = storage
        self.change = change
        self.footprint = footprint
        self.session_id = session_id
        self.operation = operation
        self.admitted_components = tuple(
            change.requested_components if admitted_components is None else admitted_components
        )
        self.interrupt_preflight_record = interrupt_preflight_record
        try:
            self.interrupt_preflight_identity = None if interrupt_preflight_record is None else json.dumps(
                interrupt_preflight_record, sort_keys=True, separators=(',', ':'),
            )
        except (TypeError, ValueError) as error:
            raise ValueError('Interrupt preflight record must be JSON data') from error
        self.revision = change.source.revision + int(set(change.source_components) != set(change.target_components))
        self.completion = None
        self.positions = {unit.component: position for position, unit in enumerate(footprint.units)}
        connection = storage.db_connection()
        component_membership.validate_membership_change(connection, change)
        shape = connection.execute(
            'SELECT shape_id FROM graph_shapes WHERE shape_json=?', (change.target_shape_json,),
        ).fetchone()
        if shape is None:
            raise RuntimeError('Membership preparation requires its registered target shape')
        self.shape_id = shape['shape_id']
        observed_history = read_membership_history(connection)
        identities = {(item.members, item.shape_id, item.alignment_generation): item for item in observed_history}
        if any(identities.get((item.members, item.shape_id, item.alignment_generation)) != item
               for item in change.historical_memberships):
            raise RuntimeError('Membership history changed before preparation')
        self.states = {}
        for component in set(change.source_components) | set(change.target_components):
            row = connection.execute('SELECT * FROM component_states WHERE component_key=?',
                                     (encode_component_key(component),)).fetchone()
            if row is None:
                raise RuntimeError('Membership preparation target state is missing')
            storage._read_component_state(connection, component)
            self.states[component] = dict(row)
        self.post_states = {}
        floors = {item.members: item.generation_floor for item in change.preparation}
        for unit in footprint.units:
            before = self.states[unit.component]
            floor = floors[unit.component]
            generation = max(before['alignment_generation'], 0 if floor is None else floor) + 1
            self.post_states[unit.component] = dict(
                before, shape_id=self.shape_id, retained_result_shape_id=None,
                retained_result_alignment_generation=None, lifecycle='queued', stability=None,
                instability_origin=None, misaligned=0, alignment_generation=generation,
            )
        keys = {encode_component_key(item.members) for item in change.historical_memberships}
        self.result_rows = tuple(tuple(row) for row in connection.execute(
            'SELECT * FROM component_successful_results ORDER BY component_key, shape_id, alignment_generation',
        ) if row['component_key'] in keys)
        self.result_keys = keys
        self.source_metadata = {
            'revision': change.source.revision,
            'components': [{'members': list(item.members), 'established_revision': item.established_revision}
                           for item in change.source.components],
        }

    def bind_attempt(self, connection, operation_id):
        self.completion = MembershipCompletion(connection, operation_id, self.revision, self.session_id)

    def _validate_session(self, connection):
        sessions = connection.execute("SELECT * FROM execution_sessions WHERE status='running'").fetchall()
        if self.session_id is None:
            if sessions:
                raise RuntimeError('Reset requires no live execution sessions')
        else:
            session = next((row for row in sessions if row['session_id'] == self.session_id), None)
            if session is None:
                raise RuntimeError('Membership preparation requires its admitted running session')
            selected = tuple(self.storage._read_session_components(connection, self.session_id))
            try:
                details = json.loads(session['details_json'])
                if not isinstance(details, dict):
                    raise ValueError('Session details must be an object')
                if self.interrupt_preflight_identity is None:
                    matching_preflight = 'interrupt_preflight' not in details
                else:
                    matching_preflight = self.interrupt_preflight_identity == json.dumps(
                        details['interrupt_preflight'], sort_keys=True, separators=(',', ':'),
                    )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                raise RuntimeError('Membership preparation has damaged admitted interrupt scope') from error
            if (selected != self.admitted_components
                    or not matching_preflight
                    or session['start_component'] != encode_component_key(self.footprint.start_component)
                    or session['admitted_shape_id'] != self.shape_id
                    or session['partition_revision'] != self.change.source.revision):
                raise RuntimeError('Membership preparation differs from its admitted scope')
            reservations = {
                row['component_key'] for row in connection.execute(
                    'SELECT component_key FROM component_reservations WHERE session_id=?', (self.session_id,),
                )
            }
            if reservations != {encode_component_key(item) for item in selected}:
                raise RuntimeError('Membership preparation lost its exact selected reservations')

    def _validate_source(self, connection):
        component_membership.validate_membership_change(connection, self.change)
        self._validate_session(connection)
        if selected_membership_owners(connection, self.change) != self.footprint.owners:
            raise RuntimeError('Historical executions changed during membership preparation')
        result_rows = tuple(tuple(row) for row in connection.execute(
            'SELECT * FROM component_successful_results ORDER BY component_key, shape_id, alignment_generation',
        ) if row['component_key'] in self.result_keys)
        if result_rows != self.result_rows:
            raise RuntimeError('Historical results changed during membership preparation')
        nodes = {node for component in self.states for node in component}
        for row in connection.execute('SELECT component_key FROM component_holds UNION SELECT component_key FROM pending_component_executions'):
            if nodes.intersection(decode_component_key(row['component_key'])):
                raise RuntimeError('Membership preparation cannot change held or pending components')
        for row in connection.execute('SELECT component_key, session_id FROM component_reservations'):
            if row['session_id'] != self.session_id and nodes.intersection(decode_component_key(row['component_key'])):
                raise RuntimeError('Membership preparation conflicts with another reservation')
        for node in nodes:
            if connection.execute(
                "SELECT 1 FROM jobs WHERE node_name=? AND (status='running' OR active_execution_id IS NOT NULL "
                'OR active_pid IS NOT NULL OR active_thread_id IS NOT NULL OR active_started_at IS NOT NULL) LIMIT 1',
                (node,),
            ).fetchone():
                raise RuntimeError('Membership preparation cannot change active jobs')

    def _validate_states(self, connection, completed):
        for component, before in self.states.items():
            expected = self.post_states[component] if component in completed else before
            row = connection.execute('SELECT * FROM component_states WHERE component_key=?',
                                     (encode_component_key(component),)).fetchone()
            if row is None or dict(row) != expected or row['lifecycle'] == 'running':
                raise RuntimeError('Component changed during membership preparation: ' + repr(component))

    def validate_initial(self, connection):
        self._validate_source(connection)
        self._validate_states(connection, set())
        current = read_membership_history(connection)
        observed = component_membership.observe_membership_change(
            self.change.source,
            component_snapshot_from_shape(self.change.target_shape_json),
            self.change.requested_components, current,
        )
        if observed != self.change:
            raise RuntimeError('Reusable membership work changed before preparation')
        for unit in self.footprint.units:
            validate_job_preparation(connection, unit.jobs)
            validate_prepared_inputs(self.storage, connection, unit.inputs)

    def receipt_metadata(self, unit):
        return {
            'source_partition': self.source_metadata,
            'start_component': list(self.footprint.start_component),
            'target_shape_id': self.shape_id,
            'target_shape_json': self.change.target_shape_json,
            'requested_components': [list(item) for item in self.change.requested_components],
            'preparation_components': [list(item.component) for item in self.footprint.units],
            'unit_position': self.positions[unit.component],
            'before_state': self.states[unit.component],
            'after_state': self.post_states[unit.component],
            'historical_memberships': [dict(members=list(item.members), shape_id=item.shape_id,
                                           alignment_generation=item.alignment_generation,
                                           has_reusable_work=item.has_reusable_work)
                                      for item in self.change.historical_memberships],
            'historical_owners': [dict(owner, component=list(owner['component'])) for owner in self.footprint.owners],
        }

    def _committed_units(self, connection, guard_id, *, before_position=None):
        from .preparation_execution import _effects

        completed = set()
        seen = set()
        for row in connection.execute('SELECT * FROM preparation_receipts WHERE guard_id=?', (guard_id,)):
            try:
                manifest = json.loads(row['manifest_json'])
                metadata = manifest['membership']
                position = metadata['unit_position']
                if type(position) is not int or not 0 <= position < len(self.footprint.units):
                    raise ValueError('Invalid membership preparation position')
                unit = self.footprint.units[position]
                if (position in seen or metadata != self.receipt_metadata(unit)
                        or manifest['effects'] != _effects(unit)
                        or row['component_key'] != encode_component_key(unit.component)
                        or row['session_id'] != self.session_id or row['operation'] != self.operation):
                    raise ValueError('Membership receipt differs from its exact unit')
                seen.add(position)
            except (KeyError, TypeError, ValueError) as error:
                raise RuntimeError('Damaged membership preparation receipt') from error
            if before_position is not None and position >= before_position:
                continue
            if row['state'] != 'committed' or unit.component in completed:
                raise RuntimeError('Membership preparation lacks exactly one committed receipt per unit')
            completed.add(unit.component)
        expected = {unit.component for position, unit in enumerate(self.footprint.units)
                    if before_position is None or position < before_position}
        if completed != expected:
            raise RuntimeError('Membership preparation has incomplete current-attempt receipts')
        return completed

    def validate_unit(self, connection, unit, guard_id):
        self._validate_source(connection)
        completed = self._committed_units(connection, guard_id, before_position=self.positions[unit.component])
        self._validate_states(connection, completed)
        validate_held_guards(connection, self.footprint.excluded_nodes, guard_id)

    def complete_unit(self, connection, unit):
        before, after = self.states[unit.component], self.post_states[unit.component]
        changed = connection.execute(
            "UPDATE component_states SET shape_id=?, retained_result_shape_id=NULL, "
            "retained_result_alignment_generation=NULL, lifecycle='queued', stability=NULL, "
            "instability_origin=NULL, misaligned=0, alignment_generation=? "
            'WHERE component_key=? AND shape_id=? AND alignment_generation=?',
            (after['shape_id'], after['alignment_generation'], before['component_key'],
             before['shape_id'], before['alignment_generation']),
        ).rowcount
        if changed != 1:
            raise RuntimeError('Membership preparation lost its exact target state')

    def _validate_final_effects(self, connection):
        expected = {}
        deletions = {}
        resets = {}
        for unit in self.footprint.units:
            for plan in unit.jobs:
                if plan.node in expected and expected[plan.node] != plan.jobs:
                    raise RuntimeError('Membership preparation has inconsistent receiver observations')
                expected[plan.node] = plan.jobs
                deletions.setdefault(plan.node, set()).update(plan.delete_ids)
                resets.setdefault(plan.node, set()).update(plan.reset_ids)
            for item in unit.inputs:
                if connection.execute(
                    'SELECT 1 FROM managed_input_files WHERE receiver_node=? AND relative_path=?',
                    (item.receiver, item.relative),
                ).fetchone() is not None or checked_input_path(
                    self.storage, item.receiver, item.relative,
                ).exists():
                    raise RuntimeError('Prepared managed input remains before membership replacement')
        for node, jobs in expected.items():
            after = tuple(
                replace(job, status='queued', last_execution_id=None, active_execution_id=None,
                        active_pid=None, active_thread_id=None, active_started_at=None)
                if job.job_id in resets[node] else job
                for job in jobs if job.job_id not in deletions[node]
            )
            if _read_jobs(connection, node) != after:
                raise RuntimeError('Prepared jobs changed before membership replacement: ' + node)

    def finish(self, guard_id):
        if self.completion is None or self.completion.expected['operation_id'] != guard_id:
            raise RuntimeError('Membership preparation lacks its bound attempt')

        def finish(connection):
            self.completion.require_prepared(connection)
            self._validate_source(connection)
            completed = self._committed_units(connection, guard_id)
            self._validate_states(connection, completed)
            validate_held_guards(connection, self.footprint.excluded_nodes, guard_id)
            self._validate_final_effects(connection)
            active = component_membership._replace_active_component_closure(connection, self.change)
            session = None
            if self.session_id is not None:
                session = dict(connection.execute('SELECT * FROM execution_sessions WHERE session_id=?',
                                                  (self.session_id,)).fetchone())
                changed = connection.execute(
                    'UPDATE execution_sessions SET partition_revision=? WHERE session_id=? '
                    "AND status='running' AND admitted_shape_id=? AND partition_revision=?",
                    (active.revision, self.session_id, self.shape_id, self.change.source.revision),
                ).rowcount
                if changed != 1:
                    raise RuntimeError('Membership repair lost its admitted session')
            self.completion.commit(connection, active.revision)
            if component_membership.read_active_component_partition(connection) != active:
                raise RuntimeError('Active membership changed during completion')
            self._validate_states(connection, completed)
            self._validate_final_effects(connection)
            if session is not None:
                current = connection.execute('SELECT * FROM execution_sessions WHERE session_id=?',
                                             (self.session_id,)).fetchone()
                if current is None or dict(current) != dict(session, partition_revision=active.revision):
                    raise RuntimeError('Admitted session changed during membership completion')
        submit_preparation_decision(self.storage, finish)
