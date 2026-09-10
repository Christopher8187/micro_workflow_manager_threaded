"""Recover native sessions before loading any user graph or node module."""

from __future__ import annotations

import sys

from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.native_recovery import recover_observed_sessions, recover_observed_cleanup
from micro_workflow_manager.storage.native_cleanup_observation import observe_native_cleanup
from micro_workflow_manager.storage.native_recovery_observation import observe_native_recovery
from micro_workflow_manager.storage.native_recovery_replay import observe_recovery_receipts, recover_receipts
from micro_workflow_manager.storage.sqlite.preview_snapshot import open_preview_snapshot
from micro_workflow_manager.storage.clipboard_recovery import (
    observe_clipboard_cleanup,
    recover_clipboard_cleanup,
)


def recover_native_project(root, *, quiet=False):
    clipboard_connection = open_preview_snapshot(root / '.mwf' / 'state.sqlite3')
    try:
        clipboard_observation = observe_clipboard_cleanup(clipboard_connection, root)
    finally:
        clipboard_connection.close()
    clipboard = {'cleanup': [], 'errors': list(clipboard_observation.errors),
                 'live_operations': ()}
    if clipboard_observation.plans:
        storage = FileStorage(root)
        try:
            clipboard = recover_clipboard_cleanup(storage, clipboard_observation)
        finally:
            storage.db_mutation_barrier()
            storage._connection_finalizer()
    cleanup_connection = open_preview_snapshot(root / '.mwf' / 'state.sqlite3')
    try:
        cleanup_observation = observe_native_cleanup(cleanup_connection, root)
    finally:
        cleanup_connection.close()
    cleanup = {'cleanup': [], 'errors': list(cleanup_observation.errors),
               'live_operations': cleanup_observation.live_operations}
    if cleanup_observation.plans:
        storage = FileStorage(root)
        try:
            cleanup = recover_observed_cleanup(storage, cleanup_observation)
        finally:
            storage.db_mutation_barrier()
            storage._connection_finalizer()
    receipt_connection = open_preview_snapshot(root / '.mwf' / 'state.sqlite3')
    try:
        receipts = observe_recovery_receipts(receipt_connection, root)
    finally:
        receipt_connection.close()
    replay = {'cleanup': [], 'errors': list(receipts['errors'])}
    if receipts['rows']:
        storage = FileStorage(root)
        try:
            replay = recover_receipts(storage, receipts)
        finally:
            storage.db_mutation_barrier()
            storage._connection_finalizer()
    cleanup['cleanup'][:0] = clipboard['cleanup']
    cleanup['cleanup'].extend(replay['cleanup'])
    cleanup['errors'][:0] = clipboard['errors']
    cleanup['errors'].extend(replay['errors'])
    cleanup['live_operations'] = tuple(sorted(set(
        cleanup['live_operations'] + clipboard['live_operations']
    )))
    for operation in cleanup['cleanup']:
        print(f"Recovered file operation {operation['operation_id']}: {operation['state']}.")
    connection = open_preview_snapshot(root / '.mwf' / 'state.sqlite3')
    try:
        observation = observe_native_recovery(connection, root)
    finally:
        connection.close()
    result = {
        'recovered': [], 'live_sessions': observation.live_sessions,
        'errors': [*observation.errors, *(session_id + ': ' + '; '.join(errors)
                   for session_id, errors in observation.refusals)],
    }
    if observation.sessions:
        storage = FileStorage(root)
        try:
            result = recover_observed_sessions(storage, observation)
        finally:
            storage.db_mutation_barrier()
            storage._connection_finalizer()
    result['errors'] = [*cleanup['errors'], *result['errors']]
    result['cleanup'] = cleanup['cleanup']
    for recovered in result['recovered']:
        print(f"Recovered session {recovered['session_id']}: "
              f"{recovered['requeued']} requeued, {recovered['terminal']} terminal outputs retained.")
    if not quiet:
        for path in cleanup_observation.retained_material:
            print('Unrecorded private material retained: ' + path)
        for path in clipboard_observation.retained_material:
            print('Unrecorded private material retained: ' + path)
        for operation_id in cleanup['live_operations']:
            print('Live file operation retained: ' + operation_id)
        for session_id in result['live_sessions']:
            print('Live session retained: ' + session_id)
        if not result['recovered'] and not result['cleanup'] and not result['errors']:
            print('No stale native sessions needed recovery.')
    for error in result['errors']:
        print('Recovery refused: ' + error, file=sys.stderr)
    return result


def recover_command(root):
    return int(bool(recover_native_project(root)['errors']))
