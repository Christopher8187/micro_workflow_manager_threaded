"""Durable preparation outcomes and short-lived receiver mutation guards."""


PREPARATION_TABLES = frozenset({'preparation_attempts', 'preparation_receipts', 'receiver_mutation_guards'})


def create_preparation_tables(connection):
    connection.execute("""
        CREATE TABLE preparation_attempts (
            operation_id TEXT PRIMARY KEY CHECK(length(operation_id)=32 AND operation_id NOT GLOB '*[^0-9a-f]*'),
            state TEXT NOT NULL CHECK(state IN ('preparing','interrupted','committed','aborted')),
            session_id TEXT REFERENCES execution_sessions(session_id),
            owner_pid INTEGER NOT NULL CHECK(typeof(owner_pid)='integer' AND owner_pid>0),
            process_identity TEXT NOT NULL CHECK(length(process_identity)>0),
            hostname TEXT NOT NULL CHECK(length(hostname)>0),
            started_at TEXT NOT NULL,
            heartbeat_at TEXT NOT NULL,
            finished_at TEXT,
            receivers_json TEXT NOT NULL,
            affected_nodes_json TEXT NOT NULL,
            expected_receipts_json TEXT NOT NULL,
            membership_revision INTEGER CHECK(membership_revision IS NULL OR
                (typeof(membership_revision)='integer' AND membership_revision>=0)),
            completed_membership_revision INTEGER CHECK(completed_membership_revision IS NULL OR
                (membership_revision IS NOT NULL AND typeof(completed_membership_revision)='integer'
                 AND completed_membership_revision=membership_revision))
        )
    """)
    connection.execute('''
        CREATE TABLE preparation_receipts (
            operation_id TEXT PRIMARY KEY CHECK(length(operation_id)=32 AND operation_id NOT GLOB '*[^0-9a-f]*'),
            guard_id TEXT NOT NULL REFERENCES preparation_attempts(operation_id),
            operation TEXT NOT NULL CHECK(length(operation)>0),
            component_key TEXT NOT NULL REFERENCES component_states(component_key),
            session_id TEXT REFERENCES execution_sessions(session_id),
            state TEXT NOT NULL CHECK(state IN ('prepared','committed','aborted')),
            manifest_json TEXT NOT NULL,
            intent_digest TEXT NOT NULL,
            decision_json TEXT,
            decision_digest TEXT
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
