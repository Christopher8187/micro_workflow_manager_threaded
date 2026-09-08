"""Commit one component and its excluded receiver changes with one receipt."""

from __future__ import annotations

from contextlib import ExitStack

from .job_preparation import apply_job_preparation, validate_job_preparation, queue_prepared_nodes
from .preparation_files import stage_preparation_files
from .preparation_footprint import validate_prepared_inputs, validate_preparation_footprint
from .preparation_guards import validate_held_guards, refuse_unfinished_preparation
from .preparation_receipts import PreparationReceipt, submit_preparation_decision


def _effects(unit):
    effects = []
    for item in unit.inputs:
        effects.append({'receiver': item.receiver, 'kind': 'managed-input', 'path': item.relative,
                        'producer_execution_id': item.producer['execution_id']})
    for plan in unit.jobs:
        for job in plan.jobs:
            if job.job_id in plan.delete_ids:
                effects.append({'receiver': plan.node, 'kind': 'managed-job', 'job_id': job.job_id,
                                'job_instance_id': job.instance_id,
                                'producer_execution_id': job.created_by_execution_id})
    return effects


def prepare_component_unit(storage, root, unit, expected, session_id, guard_id, operation, *, keep_trace,
                           selected_footprint=None, membership_preparation=None, target_shape=None):
    roots = None if selected_footprint is None else selected_footprint.roots
    if selected_footprint is not None and (not roots or selected_footprint.units != (unit,)):
        raise ValueError('Selected preparation requires one complete exact footprint')
    effects = _effects(unit)
    receivers = sorted({plan.node for plan in unit.jobs} | {item.receiver for item in unit.inputs})

    def validate(connection):
        validate_held_guards(connection, unit.excluded_nodes, guard_id)
        if membership_preparation is not None:
            membership_preparation.validate_unit(connection, unit, guard_id)
        else:
            observed = storage._read_component_preparation(
                connection, session_id, unit.component, expected['shape_json'], selected_roots=roots,
            )
            if observed != expected:
                raise RuntimeError('Component changed during full preparation: ' + repr(unit.component))
        validate_job_preparation(connection, unit.jobs)
        validate_prepared_inputs(storage, connection, unit.inputs)
        if selected_footprint is not None:
            validate_preparation_footprint(storage, connection, selected_footprint)

    def validate_initial(connection):
        for receiver in receivers:
            refuse_unfinished_preparation(connection, receiver)
        validate(connection)

    receipt = PreparationReceipt(
        storage, guard_id, operation, unit.component, session_id, validate_initial, effects,
        membership=None if membership_preparation is None else membership_preparation.receipt_metadata(unit),
    )
    identities = {}

    def commit(connection):
        validate(connection)
        selected = tuple(plan for plan in unit.jobs if plan.node in unit.component)
        outside = tuple(plan for plan in unit.jobs if plan.node not in unit.component)
        if membership_preparation is not None:
            apply_job_preparation(connection, selected, keep_trace=keep_trace)
            membership_preparation.complete_unit(connection, unit)
        elif selected_footprint is None:
            storage._complete_component_preparation(session_id, unit.component, expected, selected, keep_trace,
                                                    connection=connection, target_shape=target_shape)
        else:
            apply_job_preparation(connection, selected, keep_trace=keep_trace)
        apply_job_preparation(connection, outside, keep_trace=keep_trace)
        if roots is not None and session_id is None and operation == 'reset':
            queue_prepared_nodes(connection, sorted({node for node, _, _ in roots}))
        for item in unit.inputs:
            changed = connection.execute('DELETE FROM managed_input_files WHERE receiver_node=? AND relative_path=?',
                                         (item.receiver, item.relative)).rowcount
            if changed != 1:
                raise RuntimeError('Prepared input deletion was not applied: ' + item.receiver + '/' + item.relative)
        receipt.commit(connection)
        for effect in effects:
            receiver = effect['receiver']
            if receiver not in unit.excluded_nodes or receiver in identities:
                continue
            identities[receiver] = storage._mark_component_preparation_change(connection, effect, receipt)

    with ExitStack() as locks:
        for receiver in receivers:
            locks.enter_context(storage.interprocess_lock(f'node-{receiver}-input'))
            locks.enter_context(storage.interprocess_lock(f'node-{receiver}-jobs'))
        with stage_preparation_files(root, unit.jobs, inputs=unit.inputs, receipt=receipt):
            submit_preparation_decision(storage, commit)
    for receiver, identity in identities.items():
        if identity is not None:
            storage._component_arrival_latches[receiver] = identity
    storage.notify_queue_changes(receivers)
    return {plan.node: len(plan.delete_ids) for plan in unit.jobs if plan.delete_ids}
