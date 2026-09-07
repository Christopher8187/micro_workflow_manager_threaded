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
            arrival_kind TEXT CHECK(arrival_kind IN ('managed-input','managed-job')),
            relative_path TEXT,
            receiver_job_id INTEGER,
            receiver_job_instance_id TEXT,
            preparation_kind TEXT CHECK(preparation_kind IN ('preparation-removal','preparation-change')),
            operation TEXT,
            action TEXT,
            affected_kind TEXT CHECK(affected_kind IN ('managed-input','managed-job')),
            preparation_id TEXT REFERENCES preparation_receipts(operation_id),
            CHECK((arrival_kind IS NOT NULL AND preparation_kind IS NULL AND operation IS NULL
                   AND action IS NULL AND affected_kind IS NULL AND preparation_id IS NULL)
                  OR (arrival_kind IS NULL AND preparation_kind IS NOT NULL
                      AND typeof(operation)='text' AND length(operation)>0
                      AND typeof(action)='text' AND length(action)>0
                      AND affected_kind IS NOT NULL AND preparation_id IS NOT NULL
                      AND producer_execution_id IS NOT NULL)),
            CHECK((COALESCE(arrival_kind,affected_kind)='managed-input' AND producer_execution_id IS NOT NULL
                   AND relative_path IS NOT NULL AND length(relative_path)>0
                   AND receiver_job_id IS NULL AND receiver_job_instance_id IS NULL)
                  OR (COALESCE(arrival_kind,affected_kind)='managed-job' AND relative_path IS NULL
                      AND typeof(receiver_job_id)='integer' AND receiver_job_id>0
                      AND typeof(receiver_job_instance_id)='text'
                      AND length(receiver_job_instance_id)=32
                      AND length(CAST(receiver_job_instance_id AS BLOB))=32
                      AND receiver_job_instance_id NOT GLOB '*[^0-9a-f]*')),
            PRIMARY KEY(receiver_node, alignment_generation)
        )
    ''')
