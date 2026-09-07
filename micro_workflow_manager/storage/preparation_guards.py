"""Protect the complete excluded receiver footprint during preparation."""

from __future__ import annotations

import os
import socket
from contextlib import contextmanager
from uuid import uuid4

from micro_workflow_manager.component_identity import decode_component_key
from micro_workflow_manager.processes import process_identity
from .preparation_footprint import validate_preparation_footprint
from .preparation_receipts import submit_preparation_decision


def refuse_unfinished_preparation(connection, receiver):
    row = connection.execute(
        "SELECT operation_id FROM preparation_receipts WHERE state='prepared' AND EXISTS "
        "(SELECT 1 FROM json_each(manifest_json, '$.receivers') WHERE value=?) LIMIT 1", (receiver,),
    ).fetchone()
    if row is not None:
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
def hold_preparation_guards(storage, footprint, session_id, observations):
    operation_id = uuid4().hex
    receivers = footprint.excluded_nodes

    def acquire(connection):
        for unit in footprint.units:
            expected = observations[unit.component]
            observed = storage._read_component_preparation(
                connection, session_id, unit.component, expected['shape_json'],
                selected_roots=footprint.roots or None,
            )
            if observed != expected:
                raise RuntimeError('Component changed during complete preparation preflight: ' + repr(unit.component))
        validate_preparation_footprint(storage, connection, footprint)
        nodes = {plan.node for unit in footprint.units for plan in unit.jobs}
        nodes.update(item.receiver for unit in footprint.units for item in unit.inputs)
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
        identity = process_identity(os.getpid())
        if receivers and not identity:
            raise RuntimeError('Receiver preparation requires the current process identity')
        connection.executemany('INSERT INTO receiver_mutation_guards VALUES(?,?,?,?,?,?)',
                               [(node, operation_id, session_id, os.getpid(), identity, socket.gethostname())
                                for node in receivers])

    def release():
        def release(connection):
            unfinished = connection.execute(
                "SELECT operation_id FROM preparation_receipts WHERE guard_id=? AND state='prepared' LIMIT 1",
                (operation_id,),
            ).fetchone()
            if unfinished is not None:
                raise RuntimeError('Preparation guards retained for recovery: ' + unfinished['operation_id'])
            connection.execute('DELETE FROM receiver_mutation_guards WHERE operation_id=?', (operation_id,))
        submit_preparation_decision(storage, release)

    try:
        submit_preparation_decision(storage, acquire)
        yield operation_id
    except BaseException as error:
        try:
            release()
        except BaseException as release_error:
            error.__notes__ = [*getattr(error, '__notes__', ()), f'Receiver guard release failed: {release_error}']
        raise
    else:
        release()
