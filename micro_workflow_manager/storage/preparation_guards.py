"""Protect the complete excluded receiver footprint during preparation."""

from __future__ import annotations


from contextlib import contextmanager

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key
from .preparation_footprint import validate_preparation_footprint
from .preparation_attempts import hold_preparation_attempt
from .file_operation_receipts import validate_file_receipt


def refuse_unfinished_preparation(connection, receiver):
    rows = connection.execute(
        "SELECT * FROM preparation_receipts WHERE EXISTS "
        "(SELECT 1 FROM json_each(manifest_json, '$.receivers') WHERE value=?)", (receiver,),
    )
    for row in rows:
        validate_file_receipt('preparation_receipts', row)
        if row['state'] == 'prepared':
            raise RuntimeError(f'Receiver {receiver} has unfinished preparation {row["operation_id"]} requiring recovery')


def refuse_receiver_mutation(connection, receiver):
    guard = connection.execute('SELECT * FROM receiver_mutation_guards WHERE receiver_node=?', (receiver,)).fetchone()
    if guard is not None:
        owner = guard['session_id'] or f'process {guard["owner_pid"]} operation {guard["operation_id"]}'
        raise RuntimeError(f'Receiver {receiver} is guarded by {owner}')
    refuse_unfinished_preparation(connection, receiver)


def validate_held_guards(connection, receivers, operation_id):
    for receiver in receivers:
        guard = connection.execute('SELECT operation_id FROM receiver_mutation_guards WHERE receiver_node=?',
                                   (receiver,)).fetchone()
        if guard is None or guard['operation_id'] != operation_id:
            raise RuntimeError(f'Preparation lost receiver {receiver} guard {operation_id}')


@contextmanager
def hold_preparation_guards(storage, footprint, session_id, observations, *, operation, membership_preparation=None):
    receivers = footprint.excluded_nodes
    nodes = {plan.node for unit in footprint.units for plan in unit.jobs}
    nodes.update(item.receiver for unit in footprint.units for item in unit.inputs)

    def acquire(connection):
        if membership_preparation is not None:
            membership_preparation.validate_initial(connection)
        else:
            for unit in footprint.units:
                expected = observations[unit.component]
                observed = storage._read_component_preparation(
                    connection, session_id, unit.component, expected['shape_json'],
                    selected_roots=footprint.roots or None,
                )
                if observed != expected:
                    raise RuntimeError('Component changed during complete preparation preflight: ' + repr(unit.component))
            validate_preparation_footprint(storage, connection, footprint)
        for node in sorted(nodes):
            refuse_receiver_mutation(connection, node)
        reservations = connection.execute('SELECT component_key, session_id FROM component_reservations').fetchall()
        for receiver in receivers:
            storage._require_settled_input_publications(connection, receiver)
            for reservation in reservations:
                if receiver in decode_component_key(reservation['component_key']):
                    raise RuntimeError(f'Receiver {receiver} is owned by session {reservation["session_id"]}')
            observed = storage._read_job_receiver_state(connection, receiver, None)
            members = (receiver,) if observed is None else observed[1]['members']
            for node in members:
                active = connection.execute(
                    "SELECT job_id, active_execution_id FROM jobs WHERE node_name=? AND (status='running' "
                    'OR active_execution_id IS NOT NULL OR active_pid IS NOT NULL OR active_thread_id IS NOT NULL '
                    'OR active_started_at IS NOT NULL) LIMIT 1', (node,),
                ).fetchone()
                if active is not None:
                    raise RuntimeError(f'Receiver {receiver} has active owner {node}/{active["job_id"]} '
                                       f'execution {active["active_execution_id"]}')
            if observed is not None and observed[1]['lifecycle'] == 'running':
                raise RuntimeError(f'Receiver {receiver} has running component {members}')

    def after_acquire(connection, operation_id):
        if membership_preparation is not None:
            membership_preparation.validate_initial(connection)
            validate_held_guards(connection, receivers, operation_id)
            membership_preparation.bind_attempt(connection, operation_id)

    intended = [{'operation': operation, 'component_key': encode_component_key(unit.component)}
                for unit in footprint.units]
    with hold_preparation_attempt(storage, session_id, receivers, nodes, intended, acquire,
                                  after_acquire=after_acquire, membership_revision=(
                                      None if membership_preparation is None else membership_preparation.revision)) as operation_id:
        yield operation_id
