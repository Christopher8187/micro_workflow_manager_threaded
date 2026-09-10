"""Durable outcomes for native abandoned-session recovery."""


RECOVERY_TABLES = frozenset({'recovery_receipts'})


def create_recovery_tables(connection):
    connection.execute('''
        CREATE TABLE recovery_receipts (
            operation_id TEXT PRIMARY KEY
                CHECK(typeof(operation_id)='text' AND length(operation_id)=32
                      AND operation_id NOT GLOB '*[^0-9a-f]*'),
            session_id TEXT NOT NULL REFERENCES execution_sessions(session_id),
            state TEXT NOT NULL CHECK(state IN ('prepared','committed','aborted')),
            owner_hostname TEXT NOT NULL CHECK(length(owner_hostname)>0),
            owner_pid INTEGER NOT NULL CHECK(typeof(owner_pid)='integer' AND owner_pid>0),
            owner_identity TEXT NOT NULL CHECK(length(owner_identity)>0),
            prepared_at TEXT NOT NULL CHECK(length(prepared_at)>0),
            observation_json TEXT NOT NULL,
            manifest_json TEXT NOT NULL,
            intent_digest TEXT NOT NULL,
            decision_json TEXT,
            decision_digest TEXT
        )
    ''')
