"""Durable preparation outcomes and short-lived receiver mutation guards."""


PREPARATION_TABLES = frozenset({'preparation_receipts', 'receiver_mutation_guards'})


def create_preparation_tables(connection):
    connection.execute('''
        CREATE TABLE preparation_receipts (
            operation_id TEXT PRIMARY KEY CHECK(length(operation_id)=32 AND operation_id NOT GLOB '*[^0-9a-f]*'),
            guard_id TEXT NOT NULL,
            operation TEXT NOT NULL CHECK(length(operation)>0),
            component_key TEXT NOT NULL REFERENCES component_definitions(component_key),
            session_id TEXT REFERENCES execution_sessions(session_id),
            state TEXT NOT NULL CHECK(state IN ('prepared','committed','aborted')),
            manifest_json TEXT NOT NULL
        )
    ''')
    connection.execute('''
        CREATE TABLE receiver_mutation_guards (
            receiver_node TEXT PRIMARY KEY,
            operation_id TEXT NOT NULL CHECK(length(operation_id)=32 AND operation_id NOT GLOB '*[^0-9a-f]*'),
            session_id TEXT REFERENCES execution_sessions(session_id),
            owner_pid INTEGER NOT NULL CHECK(typeof(owner_pid)='integer' AND owner_pid>0),
            process_identity TEXT NOT NULL,
            hostname TEXT NOT NULL
        )
    ''')
