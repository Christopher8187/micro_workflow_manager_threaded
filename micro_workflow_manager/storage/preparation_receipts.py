"""Resolve queued preparation decisions before restoring visible files."""

from __future__ import annotations

import json
from uuid import uuid4

from micro_workflow_manager.component_identity import encode_component_key
from .file_operation_receipts import (
    prepared_receipt, insert_file_receipt, read_file_receipt, require_prepared_file_receipt,
    finish_file_receipt, preparation_decision,
)


def submit_preparation_decision(storage, operation):
    future = storage.submit_db_mutation(operation, wait=False, priority=0)
    try:
        return future.result()
    except BaseException:
        # An interrupted caller cannot restore or release protection while
        # this exact writer decision can still commit.
        while not future.done():
            try:
                future.result()
            except BaseException:
                pass
        raise


class PreparationReceipt:
    def __init__(self, storage, guard_id, operation, component, session_id, validate, effects, *, membership=None):
        self.storage = storage
        self.operation_id = uuid4().hex
        self.guard_id = guard_id
        self.operation = operation
        self.component = component
        self.session_id = session_id
        self.validate = validate
        self.effects = effects
        self.membership = membership
        self.expected = None
        self.files = None

    def state(self):
        connection = self.storage._new_db_connection()
        try:
            row = read_file_receipt(connection, 'preparation_receipts', self.operation_id)
            if row is not None and (self.expected is None or row['intent_digest'] != self.expected['intent_digest']):
                raise RuntimeError('Preparation receipt no longer matches its intent: ' + self.operation_id)
            return None if row is None else row['state']
        finally:
            connection.close()

    def prepare(self, manifest):
        manifest = dict(manifest, effects=self.effects)
        if self.membership is not None:
            manifest["membership"] = self.membership

        self.expected = prepared_receipt(
            'preparation_receipts', operation_id=self.operation_id, guard_id=self.guard_id,
            operation=self.operation, component_key=encode_component_key(self.component), session_id=self.session_id,
            manifest_json=json.dumps(manifest, separators=(',', ':')),
        )

        def prepare(connection):
            self.validate(connection)
            insert_file_receipt(connection, 'preparation_receipts', self.expected)
        submit_preparation_decision(self.storage, prepare)

    def require_prepared(self, connection):
        return require_prepared_file_receipt(connection, 'preparation_receipts', self.expected)

    def commit(self, connection):
        finish_file_receipt(connection, 'preparation_receipts', self.expected, 'committed',
                            preparation_decision(connection, json.loads(self.expected['manifest_json'])))

    def abort(self):
        def abort(connection):
            self.require_prepared(connection)
            self.files.require_restored()
            finish_file_receipt(connection, 'preparation_receipts', self.expected, 'aborted', {'restored': True})
        submit_preparation_decision(self.storage, abort)
