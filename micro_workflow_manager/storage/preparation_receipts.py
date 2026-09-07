"""Resolve queued preparation decisions before restoring visible files."""

from __future__ import annotations

import json
from uuid import uuid4

from micro_workflow_manager.component_identity import encode_component_key


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
    def __init__(self, storage, guard_id, operation, component, session_id, validate, effects):
        self.storage = storage
        self.operation_id = uuid4().hex
        self.guard_id = guard_id
        self.operation = operation
        self.component = component
        self.session_id = session_id
        self.validate = validate
        self.effects = effects

    def state(self):
        connection = self.storage._new_db_connection()
        try:
            row = connection.execute('SELECT state FROM preparation_receipts WHERE operation_id=?',
                                     (self.operation_id,)).fetchone()
            return None if row is None else row['state']
        finally:
            connection.close()

    def prepare(self, manifest):
        def prepare(connection):
            self.validate(connection)
            connection.execute(
                "INSERT INTO preparation_receipts VALUES(?,?,?,?,?,'prepared',?)",
                (self.operation_id, self.guard_id, self.operation, encode_component_key(self.component),
                 self.session_id, json.dumps(dict(manifest, effects=self.effects), separators=(',', ':'))),
            )
        submit_preparation_decision(self.storage, prepare)

    def commit(self, connection):
        changed = connection.execute(
            "UPDATE preparation_receipts SET state='committed' WHERE operation_id=? AND state='prepared'",
            (self.operation_id,),
        ).rowcount
        if changed != 1:
            raise RuntimeError('Preparation lost its prepared receipt: ' + self.operation_id)

    def abort(self):
        def abort(connection):
            changed = connection.execute(
                "UPDATE preparation_receipts SET state='aborted' WHERE operation_id=? AND state='prepared'",
                (self.operation_id,),
            ).rowcount
            if changed != 1:
                raise RuntimeError('Preparation restoration lost its prepared receipt: ' + self.operation_id)
        submit_preparation_decision(self.storage, abort)
