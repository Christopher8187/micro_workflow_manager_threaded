from __future__ import annotations

import json
from collections.abc import Sequence

from micro_workflow_manager.component_identity import encode_component_key
from .session_shapes import read_admission_shape, validate_session_shape_snapshot


class NoSelectedJobs(RuntimeError):
    """A reserved selection found no work and admission was rolled back."""


class SessionAdmissionStorageMixin:
    """Record exact session scope and optional selection after reservation."""

    def create_execution_session(
        self,
        session_id: str,
        *,
        session_kind: str,
        command: str,
        start_component,
        selected_components,
        selected_jobs=(),
        parent_session_ids=(),
        started_at: str,
        hostname: str,
        pid: int,
        process_identity: str | None,
        details: dict | None = None,
        _reserved_sample_builder=None,
        sample_id: str | None = None,
        expected_shape: str,
        _wait: bool = True,
    ):
        self._require_execution_session_storage()
        self._session_text(session_id, "session_id")
        self._session_text(command, "command")
        self._session_text(expected_shape, "expected_shape")
        self._session_text(hostname, "hostname")
        self._session_time(started_at, "started_at")
        if session_kind not in {"main", "interrupt"}:
            raise ValueError("session_kind must be main or interrupt")
        if type(pid) is not int or pid < 1:
            raise ValueError("pid must be a positive integer")
        if process_identity is not None:
            self._session_text(process_identity, "process_identity")
        if (not isinstance(parent_session_ids, Sequence)
                or isinstance(parent_session_ids, (str, bytes))):
            raise ValueError("Session parents must be an ordered sequence")
        parent_session_ids = tuple(sorted(
            self._session_text(parent, "parent_session_id") for parent in parent_session_ids
        ))
        if len(set(parent_session_ids)) != len(parent_session_ids):
            raise ValueError("Session parents must be distinct")
        if parent_session_ids and (
            session_kind != "interrupt" or session_id in parent_session_ids
        ):
            raise ValueError("Only an interrupt session may have distinct parent sessions")
        if details is not None and not isinstance(details, dict):
            raise ValueError("Session details must be an object")
        if not isinstance(selected_components, Sequence) or isinstance(selected_components, (str, bytes)):
            raise ValueError("Selected components must be an ordered sequence")
        if not isinstance(selected_jobs, Sequence) or isinstance(selected_jobs, (str, bytes)):
            raise ValueError("Selected jobs must be an ordered sequence")
        start_component = self._session_component(start_component)
        component_keys = [self._session_component(component) for component in selected_components]
        if start_component not in component_keys:
            raise ValueError("The starting component must be selected")
        nodes = set()
        for component in component_keys:
            if nodes.intersection(component):
                raise ValueError("Selected components must not overlap")
            nodes.update(component)
        jobs = []
        for node, job_id in selected_jobs:
            node = self.validate_node_name(node)
            job_id = self.validate_job_id(job_id)
            if node not in nodes:
                raise ValueError("Selected jobs must belong to selected components")
            jobs.append((node, job_id))
        components = [encode_component_key(component) for component in component_keys]
        if _reserved_sample_builder is not None:
            if jobs or expected_shape is None or component_keys != [start_component]:
                raise ValueError(
                    'Reserved sample admission requires exactly its starting component and no chosen jobs'
                )
            if not callable(_reserved_sample_builder):
                raise ValueError('Reserved sample admission requires its internal plan reader')
            self._session_text(expected_shape, 'expected_shape')
            self._session_text(sample_id, 'sample_id')
        elif sample_id is not None:
            raise ValueError('Sample admission metadata requires its internal reserved plan reader')

        def create(connection):
            shape_id, revision = read_admission_shape(connection, expected_shape, component_keys)
            for parent in parent_session_ids:
                row = connection.execute(
                    "SELECT status FROM execution_sessions WHERE session_id=?", (parent,),
                ).fetchone()
                if row is None or row['status'] != 'running':
                    raise RuntimeError("Session parent must be an existing running session: " + parent)
            job_roots = []
            for position, (node, job_id) in enumerate(jobs):
                observed = self._read_job_owner_observation(connection, node, job_id)
                if observed is None:
                    raise RuntimeError(f'Selected job does not exist at session admission: {node}/{job_id}')
                job_roots.append((session_id, position, node, job_id, observed['job_instance_id']))
            selection_kind = 'jobs' if job_roots else 'components'
            connection.execute(
                "INSERT INTO execution_sessions("
                "session_id, session_kind, command, selection_kind, start_component, "
                "status, started_at, heartbeat_at, hostname, pid, process_identity, details_json, "
                "admitted_shape_id, partition_revision) "
                "VALUES(?, ?, ?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?)",
                (session_id, session_kind, command, selection_kind,
                 encode_component_key(start_component), started_at, started_at,
                 hostname, pid, process_identity, json.dumps(details or {}), shape_id, revision),
            )
            connection.executemany(
                "INSERT INTO session_components(session_id, position, component_key) VALUES(?, ?, ?)",
                [(session_id, position, component) for position, component in enumerate(components)],
            )
            if parent_session_ids:
                inserted = connection.executemany(
                    "INSERT INTO execution_session_parents("
                    "child_session_id, parent_session_id, created_at) VALUES(?, ?, ?)",
                    [(session_id, parent, started_at) for parent in parent_session_ids],
                ).rowcount
                if inserted != len(parent_session_ids):
                    raise RuntimeError("Session parents were not recorded completely")
            if _reserved_sample_builder is not None:
                # Internal use only: the caller holds every component input/jobs
                # advisory lock. This callback may read only through this
                # connection and must not call a waiting storage API.
                self._reserve_execution_components(connection, session_id, expected_shape)
                from .sample_planning import SamplePlan

                plan = _reserved_sample_builder(connection)
                if not isinstance(plan, SamplePlan):
                    raise ValueError('Reserved sample reader must return a SamplePlan')
                selected_roots, selection = plan.admission_record(
                    sample_id=sample_id, component=start_component, expected_shape=expected_shape,
                )
                if not selected_roots:
                    raise NoSelectedJobs('Sample selected no jobs; no work was started')
                addresses = [(node, job_id) for node, job_id, _ in selected_roots]
                if len(set(addresses)) != len(addresses):
                    raise ValueError('Reserved selection contains duplicate jobs')
                for position, (node, job_id, expected_instance) in enumerate(selected_roots):
                    node, job_id = self.validate_node_name(node), self.validate_job_id(job_id)
                    if node not in start_component:
                        raise ValueError('Reserved selection must stay within its starting component')
                    observed = self._read_job_owner_observation(connection, node, job_id)
                    if observed is None:
                        raise RuntimeError(f'Selected job is missing after reservation: {node}/{job_id}')
                    if observed['job_instance_id'] != expected_instance:
                        raise RuntimeError(f'Selected job instance changed after observation: {node}/{job_id}')
                    job_roots.append((session_id, position, node, job_id, expected_instance))
                changed = connection.execute(
                    "UPDATE execution_sessions SET selection_kind='jobs', details_json=? "
                    "WHERE session_id=? AND status='running' AND selection_kind='components'",
                    (json.dumps({**(details or {}), 'selection': selection}), session_id),
                ).rowcount
                if changed != 1:
                    raise RuntimeError('Reserved selection was not retained by its running session')
            recorded = connection.executemany(
                "INSERT INTO session_jobs(session_id, position, node_name, job_id, job_instance_id) "
                "VALUES(?, ?, ?, ?, ?)", job_roots,
            ).rowcount
            if recorded != len(job_roots):
                raise RuntimeError('Session admission did not record every selected job instance')
            self._authorize_interrupt_fences(
                connection, session_id, component_keys, started_at,
            )
            row = connection.execute(
                'SELECT * FROM execution_sessions WHERE session_id=?', (session_id,),
            ).fetchone()
            return self._execution_session_from_row(connection, row)

        return self.submit_db_mutation(create, wait=_wait, priority=0)

    def _read_execution_session_admission(
        self,
        session_id,
        *,
        session_kind,
        command,
        start_component,
        selected_components,
        selected_jobs,
        started_at,
        hostname,
        pid,
        process_identity,
        details,
        reserved,
        expected_shape,
        expected_job_instances=None,
    ):
        """Read and validate one possibly committed admission in one snapshot."""
        self._require_execution_session_storage()
        start_component = self._session_component(start_component)
        selected_components = [self._session_component(value) for value in selected_components]
        selected_jobs = [
            (self.validate_node_name(node), self.validate_job_id(job_id))
            for node, job_id in selected_jobs
        ]
        if expected_job_instances is not None:
            expected_job_instances = tuple(expected_job_instances)
        connection = self._new_db_connection()
        try:
            connection.execute('BEGIN')
            row = connection.execute(
                'SELECT * FROM execution_sessions WHERE session_id=?', (session_id,),
            ).fetchone()
            if row is None:
                orphaned = sum(
                    connection.execute(statement, (session_id,)).fetchone()[0]
                    for statement in (
                        'SELECT COUNT(*) FROM session_components WHERE session_id=?',
                        'SELECT COUNT(*) FROM session_jobs WHERE session_id=?',
                        'SELECT COUNT(*) FROM component_reservations WHERE session_id=?',
                        'SELECT COUNT(*) FROM execution_session_parents WHERE child_session_id=?',
                        'SELECT COUNT(*) FROM session_fence_authorizations WHERE session_id=?',
                    )
                )
                if orphaned:
                    raise RuntimeError('Admission rollback left rows without its execution session')
                return None
            session = self._execution_session_from_row(connection, row)
            scope_admitted = row['scope_admitted']
            if validate_session_shape_snapshot(connection, session) != expected_shape:
                raise RuntimeError("Committed admission has a different graph shape")
            roots = tuple(self._read_session_job_roots(
                connection, session_id, components=session['selected_components'],
            ))
            expected_selection_kind = 'jobs' if selected_jobs else 'components'
            if (
                session['session_kind'] != session_kind
                or session['parent_session_ids']
                or session['command'] != command
                or session['selection_kind'] != expected_selection_kind
                or session['start_component'] != start_component
                or session['status'] != 'running'
                or session['outcome'] is not None
                or session['finished_at'] is not None
                or session['started_at'] != started_at
                or session['heartbeat_at'] != started_at
                or session['hostname'] != hostname
                or session['pid'] != pid
                or session['process_identity'] != process_identity
                or session['details'] != details
                or session['failures'] != []
                or session['selected_components'] != selected_components
                or [(node, job_id) for node, job_id, _ in roots] != selected_jobs
                or (expected_job_instances is not None and roots != expected_job_instances)
            ):
                raise RuntimeError('Committed execution-session admission differs from its request')
            reserved_keys = {
                reservation['component_key'] for reservation in connection.execute(
                    'SELECT component_key FROM component_reservations WHERE session_id=?', (session_id,),
                )
            }
            expected_keys = (
                {encode_component_key(component) for component in selected_components}
                if reserved else set()
            )
            if reserved and not reserved_keys:
                # Ordinary reservation is a second writer decision. Its
                # rollback leaves the already committed session intact.
                if scope_admitted != 0:
                    raise RuntimeError(
                        'Committed execution-session reservation lost its admitted scope'
                    )
                return None
            if scope_admitted != int(reserved) or reserved_keys != expected_keys:
                raise RuntimeError('Committed execution-session reservation differs from its request')
            if reserved:
                from .session_scope import require_admitted_reservation_scope
                if not require_admitted_reservation_scope(connection, session_id):
                    raise RuntimeError('Committed execution-session reservation lost its admission marker')
            return session
        finally:
            connection.close()
