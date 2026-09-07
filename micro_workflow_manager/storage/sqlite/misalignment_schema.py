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
            producer_execution_id TEXT REFERENCES job_execution_owners(execution_id),
            arrival_kind TEXT NOT NULL CHECK(arrival_kind IN ('managed-input','managed-job')),
            relative_path TEXT,
            receiver_job_id INTEGER,
            receiver_job_instance_id TEXT,
            CHECK((arrival_kind='managed-input' AND producer_execution_id IS NOT NULL
                   AND relative_path IS NOT NULL AND length(relative_path)>0
                   AND receiver_job_id IS NULL AND receiver_job_instance_id IS NULL)
                  OR (arrival_kind='managed-job' AND relative_path IS NULL
                      AND typeof(receiver_job_id)='integer' AND receiver_job_id>0
                      AND typeof(receiver_job_instance_id)='text'
                      AND length(receiver_job_instance_id)=32
                      AND length(CAST(receiver_job_instance_id AS BLOB))=32
                      AND receiver_job_instance_id NOT GLOB '*[^0-9a-f]*')),
            PRIMARY KEY(receiver_node, alignment_generation)
        )
    ''')
