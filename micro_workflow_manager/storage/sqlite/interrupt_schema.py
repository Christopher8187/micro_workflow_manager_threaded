"""Relational ownership and boundaries for explicit interrupt execution."""


INTERRUPT_TABLES = frozenset({
    'execution_session_parents',
    'interrupt_admissions',
    'interrupt_scope_transfers',
    'interrupt_pause_requests',
    'interrupt_paused_executions',
    'interrupt_frozen_parent_states',
    'post_interrupt_fences',
    'session_fence_authorizations',
})


def create_interrupt_tables(connection):
    connection.execute('''
        CREATE TABLE execution_session_parents (
            child_session_id TEXT NOT NULL REFERENCES execution_sessions(session_id),
            parent_session_id TEXT NOT NULL REFERENCES execution_sessions(session_id),
            created_at TEXT NOT NULL,
            PRIMARY KEY(child_session_id, parent_session_id),
            CHECK(child_session_id<>parent_session_id)
        )
    ''')
    connection.execute('''
        CREATE TABLE interrupt_admissions (
            session_id TEXT PRIMARY KEY REFERENCES execution_sessions(session_id),
            target_component_key TEXT NOT NULL,
            readiness_override_requested INTEGER NOT NULL
                CHECK(typeof(readiness_override_requested)='integer'
                      AND readiness_override_requested IN (0,1)),
            readiness_overridden INTEGER
                CHECK(readiness_overridden IS NULL
                      OR (typeof(readiness_overridden)='integer' AND readiness_overridden IN (0,1))),
            state TEXT NOT NULL CHECK(state IN ('admitted','frozen','settled')),
            created_at TEXT NOT NULL,
            frozen_shape_id INTEGER,
            frozen_alignment_generation INTEGER,
            frozen_stability TEXT CHECK(frozen_stability IS NULL OR frozen_stability IN ('stable','unstable')),
            frozen_instability_origin TEXT REFERENCES execution_sessions(session_id),
            frozen_at TEXT,
            settled_at TEXT,
            FOREIGN KEY(session_id, target_component_key)
                REFERENCES session_components(session_id, component_key),
            CHECK((state='admitted' AND frozen_shape_id IS NULL
                    AND frozen_alignment_generation IS NULL
                    AND frozen_stability IS NULL AND frozen_instability_origin IS NULL
                    AND readiness_overridden IS NULL
                    AND frozen_at IS NULL AND settled_at IS NULL)
               OR (state='frozen' AND frozen_shape_id IS NOT NULL
                    AND frozen_alignment_generation IS NOT NULL
                    AND readiness_overridden IS NOT NULL
                    AND frozen_stability IS NOT NULL
                    AND frozen_at IS NOT NULL AND settled_at IS NULL)
               OR (state='settled' AND settled_at IS NOT NULL))
        )
    ''')
    connection.execute('''
        CREATE TABLE interrupt_scope_transfers (
            child_session_id TEXT NOT NULL REFERENCES interrupt_admissions(session_id),
            source_session_id TEXT NOT NULL REFERENCES execution_sessions(session_id),
            component_key TEXT NOT NULL,
            transfer_kind TEXT NOT NULL CHECK(transfer_kind IN ('direct-parent','future-admission')),
            state TEXT NOT NULL CHECK(state IN ('active','returned','released')),
            created_at TEXT NOT NULL,
            finished_at TEXT,
            PRIMARY KEY(child_session_id, component_key),
            FOREIGN KEY(child_session_id, component_key)
                REFERENCES session_components(session_id, component_key),
            CHECK(child_session_id<>source_session_id),
            CHECK((state='active' AND finished_at IS NULL)
               OR (state IN ('returned','released') AND finished_at IS NOT NULL))
        )
    ''')
    connection.execute('''
        CREATE TABLE interrupt_pause_requests (
            child_session_id TEXT NOT NULL REFERENCES interrupt_admissions(session_id),
            owner_session_id TEXT NOT NULL REFERENCES execution_sessions(session_id),
            component_key TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('requested','acknowledged','released')),
            created_at TEXT NOT NULL,
            released_at TEXT,
            PRIMARY KEY(child_session_id, owner_session_id, component_key),
            FOREIGN KEY(owner_session_id, component_key)
                REFERENCES session_components(session_id, component_key),
            CHECK(child_session_id<>owner_session_id),
            CHECK((state IN ('requested','acknowledged') AND released_at IS NULL)
               OR (state='released' AND released_at IS NOT NULL))
        )
    ''')
    connection.execute('''
        CREATE TABLE interrupt_paused_executions (
            child_session_id TEXT NOT NULL,
            owner_session_id TEXT NOT NULL,
            component_key TEXT NOT NULL,
            execution_id TEXT NOT NULL REFERENCES job_execution_owners(execution_id),
            node_name TEXT NOT NULL,
            job_id INTEGER NOT NULL,
            generation INTEGER NOT NULL
                CHECK(typeof(generation)='integer' AND generation>=0),
            acknowledged_at TEXT,
            PRIMARY KEY(child_session_id, execution_id),
            FOREIGN KEY(child_session_id, owner_session_id, component_key)
                REFERENCES interrupt_pause_requests(
                    child_session_id, owner_session_id, component_key
                )
        )
    ''')
    connection.execute('''
        CREATE TABLE interrupt_frozen_parent_states (
            child_session_id TEXT NOT NULL REFERENCES interrupt_admissions(session_id),
            component_key TEXT NOT NULL,
            shape_id INTEGER NOT NULL,
            lifecycle TEXT NOT NULL CHECK(lifecycle IN ('queued','running','done','failed','sampled')),
            stability TEXT CHECK(stability IS NULL OR stability IN ('stable','unstable')),
            instability_origin TEXT REFERENCES execution_sessions(session_id),
            misaligned INTEGER NOT NULL CHECK(typeof(misaligned)='integer' AND misaligned IN (0,1)),
            alignment_generation INTEGER NOT NULL
                CHECK(typeof(alignment_generation)='integer' AND alignment_generation>=0),
            PRIMARY KEY(child_session_id, component_key),
            FOREIGN KEY(component_key, shape_id)
                REFERENCES component_definitions(component_key, shape_id)
        )
    ''')
    connection.execute('''
        CREATE TABLE post_interrupt_fences (
            component_key TEXT NOT NULL,
            shape_id INTEGER NOT NULL,
            alignment_generation INTEGER NOT NULL
                CHECK(typeof(alignment_generation)='integer' AND alignment_generation>=0),
            source_session_id TEXT NOT NULL REFERENCES interrupt_admissions(session_id),
            created_at TEXT NOT NULL,
            PRIMARY KEY(component_key, shape_id, alignment_generation, source_session_id),
            FOREIGN KEY(component_key, shape_id, alignment_generation)
                REFERENCES component_successful_results(
                    component_key, shape_id, alignment_generation
                )
        )
    ''')
    connection.execute('''
        CREATE TABLE session_fence_authorizations (
            session_id TEXT NOT NULL,
            component_key TEXT NOT NULL,
            shape_id INTEGER NOT NULL,
            alignment_generation INTEGER NOT NULL,
            source_session_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(session_id, component_key, shape_id, alignment_generation, source_session_id),
            FOREIGN KEY(session_id) REFERENCES execution_sessions(session_id),
            FOREIGN KEY(component_key, shape_id, alignment_generation, source_session_id)
                REFERENCES post_interrupt_fences(
                    component_key, shape_id, alignment_generation, source_session_id
                ),
            FOREIGN KEY(source_session_id) REFERENCES interrupt_admissions(session_id)
        )
    ''')
