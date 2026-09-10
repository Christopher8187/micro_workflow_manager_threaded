"""Native managed-input ownership and publication receipts."""


INPUT_TABLES = frozenset({
    'managed_input_files', 'managed_input_producers', 'input_publications',
})


def create_input_tables(connection):
    connection.execute('''
        CREATE TABLE managed_input_files (
            receiver_node TEXT NOT NULL,
            relative_path TEXT NOT NULL,
            unowned_predecessor INTEGER NOT NULL
                CHECK(typeof(unowned_predecessor)='integer' AND unowned_predecessor IN (0,1)),
            PRIMARY KEY(receiver_node, relative_path)
        )
    ''')
    connection.execute('''
        CREATE TABLE managed_input_producers (
            receiver_node TEXT NOT NULL,
            relative_path TEXT NOT NULL,
            execution_id TEXT NOT NULL REFERENCES job_execution_owners(execution_id),
            PRIMARY KEY(receiver_node, relative_path, execution_id),
            FOREIGN KEY(receiver_node, relative_path)
                REFERENCES managed_input_files(receiver_node, relative_path) ON DELETE CASCADE
        )
    ''')
    connection.execute('''
        CREATE TABLE input_publications (
            operation_id TEXT PRIMARY KEY
                CHECK(typeof(operation_id)='text' AND length(operation_id)=32
                      AND operation_id NOT GLOB '*[^0-9a-f]*'),
            execution_id TEXT NOT NULL REFERENCES job_execution_owners(execution_id),
            receiver_node TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('prepared','committed','aborted')),
            changes_json TEXT NOT NULL,
            intent_digest TEXT NOT NULL,
            decision_json TEXT,
            decision_digest TEXT
        )
    ''')
