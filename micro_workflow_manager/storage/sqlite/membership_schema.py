"""SQLite tables for independently established component memberships."""


MEMBERSHIP_TABLES = frozenset({
    'component_partition_state',
    'active_components',
    'active_component_members',
})


def create_membership_tables(connection) -> None:
    """Create the schema-6 active-membership tables on a new database."""
    connection.execute("""
        CREATE TABLE component_partition_state (
            singleton INTEGER PRIMARY KEY
                CHECK(typeof(singleton)='integer' AND singleton=1),
            revision INTEGER NOT NULL
                CHECK(typeof(revision)='integer' AND revision>=0)
        )
    """)
    connection.execute("""
        CREATE TABLE active_components (
            component_key TEXT PRIMARY KEY CHECK(length(component_key)>0),
            established_revision INTEGER NOT NULL
                CHECK(typeof(established_revision)='integer' AND established_revision>=0)
        )
    """)
    connection.execute("""
        CREATE TABLE active_component_members (
            node_name TEXT PRIMARY KEY CHECK(length(node_name)>0),
            component_key TEXT NOT NULL REFERENCES active_components(component_key)
                ON DELETE CASCADE
        )
    """)
