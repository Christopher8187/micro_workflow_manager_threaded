"""Durable native clipboard attempts and file decisions."""


CLIPBOARD_TABLES = frozenset({"clipboard_attempts", "clipboard_receipts"})


def create_clipboard_tables(connection):
    connection.execute("""
        CREATE TABLE clipboard_attempts (
            operation_id TEXT PRIMARY KEY
                CHECK(typeof(operation_id)='text' AND length(operation_id)=32
                      AND operation_id NOT GLOB '*[^0-9a-f]*'),
            operation TEXT NOT NULL CHECK(operation IN ('copy','paste')),
            node_name TEXT NOT NULL CHECK(length(node_name)>0),
            project_id TEXT NOT NULL
                CHECK(typeof(project_id)='text' AND length(project_id)=32
                      AND project_id NOT GLOB '*[^0-9a-f]*'),
            state TEXT NOT NULL
                CHECK(state IN ('allocating','prepared','committed','aborted')),
            owner_pid INTEGER NOT NULL CHECK(typeof(owner_pid)='integer' AND owner_pid>0),
            process_identity TEXT NOT NULL CHECK(length(process_identity)>0),
            hostname TEXT NOT NULL CHECK(length(hostname)>0),
            started_at TEXT NOT NULL,
            finished_at TEXT,
            observation_json TEXT NOT NULL,
            observation_digest TEXT NOT NULL,
            manifest_json TEXT,
            CHECK((state='allocating' AND manifest_json IS NOT NULL AND finished_at IS NULL)
                  OR (state='prepared' AND manifest_json IS NOT NULL AND finished_at IS NULL)
                  OR (state='committed' AND manifest_json IS NOT NULL AND finished_at IS NOT NULL)
                  OR (state='aborted' AND finished_at IS NOT NULL))
        )
    """)
    connection.execute("""
        CREATE TABLE clipboard_receipts (
            operation_id TEXT PRIMARY KEY REFERENCES clipboard_attempts(operation_id),
            operation TEXT NOT NULL CHECK(operation IN ('copy','paste')),
            node_name TEXT NOT NULL CHECK(length(node_name)>0),
            manifest_json TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('prepared','committed','aborted')),
            intent_digest TEXT NOT NULL,
            decision_json TEXT,
            decision_digest TEXT
        )
    """)
