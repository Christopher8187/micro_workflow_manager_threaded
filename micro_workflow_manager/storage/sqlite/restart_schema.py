"""Durable intent and outcomes for native manual restart output moves."""


RESTART_TABLES = frozenset({'restart_receipts'})


def create_restart_tables(connection):
    connection.execute('''
        CREATE TABLE restart_receipts (
            operation_id TEXT PRIMARY KEY
                CHECK(typeof(operation_id)='text' AND length(operation_id)=32
                      AND operation_id NOT GLOB '*[^0-9a-f]*'),
            session_id TEXT REFERENCES execution_sessions(session_id),
            state TEXT NOT NULL CHECK(state IN ('prepared','committed','aborted')),
            manifest_json TEXT NOT NULL,
            intent_digest TEXT NOT NULL,
            decision_json TEXT,
            decision_digest TEXT
        )
    ''')
