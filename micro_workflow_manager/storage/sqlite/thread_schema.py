"""Native per-node runtime limits and the retained project API limit."""


THREAD_TABLES = frozenset({
    "pending_node_thread_overrides",
    "node_thread_overrides",
    "api_thread_limit",
})


def create_thread_tables(connection):
    connection.execute("""
        CREATE TABLE pending_node_thread_overrides (
            node_name TEXT PRIMARY KEY REFERENCES nodes(node_name),
            value INTEGER NOT NULL
                CHECK(typeof(value)='integer' AND value>0)
        )
    """)
    connection.execute("""
        CREATE TABLE node_thread_overrides (
            node_name TEXT NOT NULL REFERENCES nodes(node_name),
            session_id TEXT NOT NULL REFERENCES execution_sessions(session_id)
                CHECK(typeof(session_id)='text' AND length(trim(session_id))>0),
            value INTEGER NOT NULL
                CHECK(typeof(value)='integer' AND value>0),
            PRIMARY KEY(node_name, session_id)
        )
    """)
    connection.execute("""
        CREATE TABLE api_thread_limit (
            singleton INTEGER PRIMARY KEY
                CHECK(typeof(singleton)='integer' AND singleton=1),
            value INTEGER NOT NULL
                CHECK(typeof(value)='integer' AND value>0)
        )
    """)
