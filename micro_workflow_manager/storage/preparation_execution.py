"""Commit one component and its excluded receiver changes with one receipt."""

from __future__ import annotations

from contextlib import ExitStack

from .job_preparation import apply_job_preparation, validate_job_preparation
from .preparation_files import stage_preparation_files
from .preparation_footprint import validate_prepared_inputs
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


def prepare_component_unit(storage, root, unit, expected, session_id, guard_id, operation, *, keep_trace):
    effects = _effects(unit)
    receivers = sorted({plan.node for plan in unit.jobs} | {item.receiver for item in unit.inputs})

    def validate(connection):
        validate_held_guards(connection, unit.excluded_nodes, guard_id)
        observed = storage._read_component_preparation(connection, session_id, unit.component, expected['shape_json'])
        if observed != expected:
            raise RuntimeError('Component changed during full preparation: ' + repr(unit.component))
        validate_job_preparation(connection, unit.jobs)
        validate_prepared_inputs(storage, connection, unit.inputs)

    def validate_initial(connection):
        for receiver in receivers:
            refuse_unfinished_preparation(connection, receiver)
        validate(connection)

    receipt = PreparationReceipt(storage, guard_id, operation, unit.component, session_id, validate_initial, effects)
    identities = {}

    def commit(connection):
        validate(connection)
        selected = tuple(plan for plan in unit.jobs if plan.node in unit.component)
        outside = tuple(plan for plan in unit.jobs if plan.node not in unit.component)
        storage._complete_component_preparation(session_id, unit.component, expected, selected, keep_trace,
                                                connection=connection)
        apply_job_preparation(connection, outside, keep_trace=keep_trace)
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
