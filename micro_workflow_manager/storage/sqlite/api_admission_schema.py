"""Durable project-wide API execution permits."""


API_ADMISSION_TABLES = frozenset({"api_execution_permits"})


def create_api_admission_tables(connection):
    connection.execute("""
        CREATE TABLE api_execution_permits (
            execution_id TEXT PRIMARY KEY
                REFERENCES job_execution_owners(execution_id)
                CHECK(typeof(execution_id)='text' AND length(execution_id)=32
                      AND length(CAST(execution_id AS BLOB))=32
                      AND execution_id NOT GLOB '*[^0-9a-f]*'),
            session_id TEXT NOT NULL REFERENCES execution_sessions(session_id)
                CHECK(typeof(session_id)='text' AND length(trim(session_id))>0),
            node_name TEXT NOT NULL REFERENCES nodes(node_name)
                CHECK(typeof(node_name)='text' AND length(trim(node_name))>0),
            job_id INTEGER NOT NULL
                CHECK(typeof(job_id)='integer' AND job_id>0),
            generation INTEGER NOT NULL
                CHECK(typeof(generation)='integer' AND generation>=0),
            acquired_at TEXT NOT NULL
                CHECK(typeof(acquired_at)='text' AND length(trim(acquired_at))>0),
            UNIQUE(node_name, job_id, generation)
        )
    """)
