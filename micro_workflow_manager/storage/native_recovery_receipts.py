"""Persist exact recovery intent and settle it with the business decision."""

from __future__ import annotations

import os
import socket
from datetime import datetime, timezone
from uuid import uuid4

from micro_workflow_manager.processes import process_identity

from .native_recovery_observation import require_session_recovery_unchanged
from .native_recovery_records import canonical, finish_receipt, intent_digest, read_receipt
from .preparation_receipts import submit_preparation_decision


class RecoveryReceipt:
    def __init__(self, storage, plan):
        self.storage, self.plan = storage, plan
        self.operation_id = uuid4().hex
        self.prepared_at = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
        self.expected = None

    def prepare(self, manifest):
        identity = process_identity(os.getpid())
        if identity is None:
            raise RuntimeError('Recovery cannot record this process instance')
        self.expected = {
            'operation_id': self.operation_id, 'session_id': self.plan.session_id, 'state': 'prepared',
            'owner_hostname': socket.gethostname(), 'owner_pid': os.getpid(), 'owner_identity': identity,
            'prepared_at': self.prepared_at, 'observation_json': canonical(self.plan.manifest()),
            'manifest_json': canonical(manifest), 'decision_json': None, 'decision_digest': None,
        }
        self.expected['intent_digest'] = intent_digest(self.expected)

        def prepare(connection):
            require_session_recovery_unchanged(connection, self.plan)
            names = tuple(self.expected)
            changed = connection.execute('INSERT INTO recovery_receipts (' + ','.join(names) + ') VALUES ('
                                         + ','.join('?' for _ in names) + ')',
                                         tuple(self.expected[name] for name in names)).rowcount
            if changed != 1:
                raise RuntimeError('Recovery did not record its prepared receipt: ' + self.operation_id)
            self.require_prepared(connection)
        submit_preparation_decision(self.storage, prepare)

    def require_prepared(self, connection):
        if read_receipt(connection, self.operation_id) != self.expected:
            raise RuntimeError('Recovery prepared receipt changed: ' + self.operation_id)

    def state(self):
        connection = self.storage._new_db_connection()
        try:
            row = read_receipt(connection, self.operation_id)
            if row is not None and row['intent_digest'] != self.expected['intent_digest']:
                raise RuntimeError('Recovery receipt belongs to another observation')
            return None if row is None else row['state']
        finally:
            connection.close()

    def commit(self, connection):
        self.require_prepared(connection)
        finish_receipt(connection, self.expected, self.plan, 'committed')

    def abort(self):
        def abort(connection):
            self.require_prepared(connection)
            self.files.require_restored()
            finish_receipt(connection, self.expected, self.plan, 'aborted')
        submit_preparation_decision(self.storage, abort)
