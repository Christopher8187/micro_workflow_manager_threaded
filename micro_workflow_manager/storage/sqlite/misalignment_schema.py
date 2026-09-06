"""Native first causes for managed arrivals at established results."""


MISALIGNMENT_TABLES = frozenset({'component_misalignment_causes'})


def create_misalignment_tables(connection):
    connection.execute('''
        CREATE TABLE component_misalignment_causes (
            receiver_node TEXT NOT NULL,
            alignment_generation INTEGER NOT NULL
                CHECK(typeof(alignment_generation)='integer' AND alignment_generation>=0),
            component_key TEXT NOT NULL REFERENCES component_definitions(component_key),
            shape_id INTEGER NOT NULL REFERENCES graph_shapes(shape_id),
            producer_execution_id TEXT NOT NULL REFERENCES job_execution_owners(execution_id),
            arrival_kind TEXT NOT NULL CHECK(arrival_kind='managed-input'),
            relative_path TEXT NOT NULL CHECK(length(relative_path)>0),
            PRIMARY KEY(receiver_node, alignment_generation)
        )
    ''')
