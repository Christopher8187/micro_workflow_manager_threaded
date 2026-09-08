"""Native historical component identities and current execution state."""


def create_component_tables(connection):
    connection.execute("""
        CREATE TABLE graph_shapes (
            shape_id INTEGER PRIMARY KEY,
            shape_json TEXT NOT NULL UNIQUE
        )
    """)
    connection.execute("""
        CREATE TRIGGER prevent_graph_shape_update BEFORE UPDATE ON graph_shapes
        BEGIN
            SELECT RAISE(ABORT, 'Producing graph shapes are immutable');
        END
    """)
    connection.execute("""
        CREATE TABLE component_definitions (
            component_key TEXT NOT NULL,
            shape_id INTEGER NOT NULL REFERENCES graph_shapes(shape_id),
            PRIMARY KEY(component_key, shape_id)
        )
    """)
    connection.execute("""
        CREATE TABLE component_states (
            component_key TEXT PRIMARY KEY,
            shape_id INTEGER NOT NULL,
            retained_result_shape_id INTEGER,
            retained_result_alignment_generation INTEGER,
            lifecycle TEXT NOT NULL DEFAULT 'queued'
                CHECK(lifecycle IN ('queued','running','sampled','done','failed')),
            stability TEXT CHECK(stability IN ('stable','unstable')),
            instability_origin TEXT REFERENCES execution_sessions(session_id),
            misaligned INTEGER NOT NULL DEFAULT 0
                CHECK(typeof(misaligned)='integer' AND misaligned IN (0,1)),
            alignment_generation INTEGER NOT NULL DEFAULT 0
                CHECK(typeof(alignment_generation)='integer' AND alignment_generation>=0),
            FOREIGN KEY(component_key, shape_id) REFERENCES component_definitions(component_key, shape_id),
            FOREIGN KEY(component_key, retained_result_shape_id, retained_result_alignment_generation)
                REFERENCES component_successful_results(component_key, shape_id, alignment_generation),
            CHECK((retained_result_shape_id IS NULL) = (retained_result_alignment_generation IS NULL)),
            CHECK(retained_result_alignment_generation IS NULL
                  OR retained_result_alignment_generation=alignment_generation),
            CHECK((
                (
                    (lifecycle IN ('queued','running','failed')
                        AND stability IS NULL AND instability_origin IS NULL)
                    OR (lifecycle IN ('running','sampled','done') AND (
                        (stability='stable' AND instability_origin IS NULL)
                        OR (stability='unstable' AND instability_origin IS NOT NULL
                            AND length(instability_origin)>0)
                    ))
                )
                AND (lifecycle<>'queued' OR misaligned=0)
            ) IS 1)
        )
    """)
    connection.execute("""
        CREATE TABLE component_reservations (
            component_key TEXT PRIMARY KEY REFERENCES component_states(component_key),
            session_id TEXT NOT NULL REFERENCES execution_sessions(session_id)
        )
    """)
    connection.execute("""
        CREATE TABLE pending_component_executions (
            session_id TEXT NOT NULL,
            component_key TEXT NOT NULL,
            shape_id INTEGER NOT NULL REFERENCES graph_shapes(shape_id),
            starting_shape_id INTEGER NOT NULL REFERENCES graph_shapes(shape_id),
            alignment_generation INTEGER NOT NULL
                CHECK(typeof(alignment_generation)='integer' AND alignment_generation>=0),
            completion_ready INTEGER NOT NULL DEFAULT 0
                CHECK(typeof(completion_ready)='integer' AND completion_ready IN (0,1)),
            execution_kind TEXT NOT NULL DEFAULT 'full' CHECK(execution_kind IN ('full','jobs','resume')),
            starting_lifecycle TEXT NOT NULL DEFAULT 'queued'
                CHECK(starting_lifecycle IN ('queued','sampled','done','failed')),
            starting_misaligned INTEGER NOT NULL DEFAULT 0
                CHECK(typeof(starting_misaligned)='integer' AND starting_misaligned IN (0,1)),
            stability TEXT NOT NULL CHECK(stability IN ('stable','unstable')),
            instability_origin TEXT REFERENCES execution_sessions(session_id),
            PRIMARY KEY(session_id, component_key),
            FOREIGN KEY(session_id, component_key) REFERENCES session_components(session_id, component_key),
            FOREIGN KEY(component_key, shape_id) REFERENCES component_definitions(component_key, shape_id),
            FOREIGN KEY(component_key, starting_shape_id) REFERENCES component_definitions(component_key, shape_id),
            CHECK((
                (stability='stable' AND instability_origin IS NULL)
                OR (stability='unstable' AND instability_origin IS NOT NULL AND length(instability_origin)>0)
            ) IS 1)
        )
    """)
    connection.execute("""
        CREATE TABLE component_successful_results (
            component_key TEXT NOT NULL,
            shape_id INTEGER NOT NULL REFERENCES graph_shapes(shape_id),
            alignment_generation INTEGER NOT NULL
                CHECK(typeof(alignment_generation)='integer' AND alignment_generation>=0),
            lifecycle TEXT NOT NULL CHECK(lifecycle IN ('sampled','done')),
            stability TEXT NOT NULL CHECK(stability IN ('stable','unstable')),
            instability_origin TEXT REFERENCES execution_sessions(session_id),
            PRIMARY KEY(component_key, shape_id, alignment_generation),
            FOREIGN KEY(component_key, shape_id) REFERENCES component_definitions(component_key, shape_id),
            CHECK(((stability='stable' AND instability_origin IS NULL)
                OR (stability='unstable' AND instability_origin IS NOT NULL
                    AND length(instability_origin)>0)) IS 1)
        )
    """)
    connection.execute("""
        CREATE TABLE component_holds (
            session_id TEXT NOT NULL REFERENCES execution_sessions(session_id),
            component_key TEXT NOT NULL REFERENCES component_states(component_key),
            hold_count INTEGER NOT NULL CHECK(typeof(hold_count)='integer' AND hold_count>0),
            PRIMARY KEY(session_id, component_key)
        )
    """)
    connection.execute("""
        CREATE TABLE job_execution_owners (
            execution_id TEXT PRIMARY KEY,
            node_name TEXT NOT NULL,
            job_id INTEGER NOT NULL,
            job_instance_id TEXT NOT NULL
                CHECK(typeof(job_instance_id)='text' AND length(job_instance_id)=32
                      AND length(CAST(job_instance_id AS BLOB))=32
                      AND job_instance_id NOT GLOB '*[^0-9a-f]*'),
            generation INTEGER NOT NULL,
            session_id TEXT NOT NULL REFERENCES execution_sessions(session_id),
            component_key TEXT NOT NULL,
            shape_id INTEGER NOT NULL REFERENCES graph_shapes(shape_id),
            alignment_generation INTEGER NOT NULL
                CHECK(typeof(alignment_generation)='integer' AND alignment_generation>=0),
            created_by_execution_id TEXT REFERENCES job_execution_owners(execution_id),
            FOREIGN KEY(component_key, shape_id) REFERENCES component_definitions(component_key, shape_id)
        )
    """)
