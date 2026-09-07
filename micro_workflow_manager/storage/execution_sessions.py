from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime

from micro_workflow_manager.session_liveness import execution_session_liveness
from .session_selection import SessionSelectionStorageMixin
from micro_workflow_manager.component_identity import component_key, decode_component_key, encode_component_key


class ExecutionSessionHasActiveJobs(RuntimeError):
    """Joined work still has unsettled active claims in its owned scope."""


class ExecutionSessionStorageMixin(SessionSelectionStorageMixin):
    """Persist exact execution-session records in SQLite."""

    def _require_execution_session_storage(self) -> None:
        if self._metadata_value("database_schema_version") != "5":
            raise RuntimeError("Execution-session storage requires a session-capable database")

    @staticmethod
    def _session_text(value, field: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field} must be nonempty text")
        return value

    @classmethod
    def _session_time(cls, value, field: str) -> str:
        cls._session_text(value, field)
        try:
            datetime.fromisoformat(value)
        except ValueError as error:
            raise ValueError(f"{field} must be an ISO timestamp") from error
        return value

    def _session_component(self, component) -> tuple[str, ...]:
        if not isinstance(component, (tuple, list, set, frozenset)) or not component:
            raise ValueError("A selected component needs member node names")
        return component_key(self.validate_node_name(node) for node in component)

    def create_execution_session(
        self,
        session_id: str,
        *,
        session_kind: str,
        command: str,
        start_component,
        selected_components,
        selected_jobs=(),
        parent_session_id: str | None = None,
        started_at: str,
        hostname: str,
        pid: int,
        process_identity: str | None,
        details: dict | None = None,
    ) -> dict:
        self._require_execution_session_storage()
        self._session_text(session_id, "session_id")
        self._session_text(command, "command")
        self._session_text(hostname, "hostname")
        self._session_time(started_at, "started_at")
        if session_kind not in {"main", "interrupt"}:
            raise ValueError("session_kind must be main or interrupt")
        if type(pid) is not int or pid < 1:
            raise ValueError("pid must be a positive integer")
        if process_identity is not None:
            self._session_text(process_identity, "process_identity")
        if parent_session_id is not None:
            self._session_text(parent_session_id, "parent_session_id")
            if session_kind != "interrupt" or parent_session_id == session_id:
                raise ValueError("An interrupt session must have a distinct parent")
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

        def create(connection):
            job_roots = []
            for position, (node, job_id) in enumerate(jobs):
                observed = self._read_job_owner_observation(connection, node, job_id)
                if observed is None:
                    raise RuntimeError(f'Selected job does not exist at session admission: {node}/{job_id}')
                job_roots.append((session_id, position, node, job_id, observed['job_instance_id']))
            selection_kind = 'jobs' if job_roots else 'components'
            connection.execute(
                "INSERT INTO execution_sessions("
                "session_id, session_kind, parent_session_id, command, selection_kind, start_component, "
                "status, started_at, heartbeat_at, hostname, pid, process_identity, details_json) "
                "VALUES(?, ?, ?, ?, ?, ?, 'running', ?, ?, ?, ?, ?, ?)",
                (session_id, session_kind, parent_session_id, command, selection_kind,
                 encode_component_key(start_component), started_at, started_at,
                 hostname, pid, process_identity, json.dumps(details or {})),
            )
            connection.executemany(
                "INSERT INTO session_components(session_id, position, component_key) VALUES(?, ?, ?)",
                [(session_id, position, component) for position, component in enumerate(components)],
            )
            recorded = connection.executemany(
                "INSERT INTO session_jobs(session_id, position, node_name, job_id, job_instance_id) "
                "VALUES(?, ?, ?, ?, ?)", job_roots,
            ).rowcount
            if recorded != len(job_roots):
                raise RuntimeError('Session admission did not record every selected job instance')
            row = connection.execute(
                'SELECT * FROM execution_sessions WHERE session_id=?', (session_id,),
            ).fetchone()
            return self._execution_session_from_row(connection, row)

        return self.submit_db_mutation(create, wait=True, priority=0)

    def heartbeat_execution_session(self, session_id: str, heartbeat_at: str) -> bool:
        self._require_execution_session_storage()
        self._session_text(session_id, "session_id")
        self._session_time(heartbeat_at, "heartbeat_at")

        def heartbeat(connection):
            return connection.execute(
                "UPDATE execution_sessions SET heartbeat_at=? WHERE session_id=? AND status='running'",
                (heartbeat_at, session_id),
            ).rowcount == 1

        return self.submit_db_mutation(heartbeat, wait=True, priority=0)

    def finish_execution_session(
        self, session_id: str, *, outcome: str, finished_at: str, failures: list | None = None,
    ) -> bool:
        self._require_execution_session_storage()
        self._session_text(session_id, "session_id")
        self._session_time(finished_at, "finished_at")
        if not isinstance(outcome, str) or not outcome.strip():
            raise ValueError("A terminal session needs a nonempty outcome")
        if failures is not None and not isinstance(failures, list):
            raise ValueError("Session failures must be an ordered list")
        failure_data = json.dumps(failures if failures is not None else [])

        def finish(connection):
            return connection.execute(
                "UPDATE execution_sessions SET status='terminal', outcome=?, finished_at=?, failures_json=? "
                "WHERE session_id=? AND status='running'",
                (outcome, finished_at, failure_data, session_id),
            ).rowcount == 1

        return self.submit_db_mutation(finish, wait=True, priority=0)

    def decide_execution_session_exit(
        self, session_id: str, *, outcome: str, finished_at: str,
        failures: list | None = None, restart_attempts=(), rejected_restarts=None,
        exhausted_restarts=None, failed_nodes=(), component_outcomes=(),
    ) -> dict:
        """Continue accepted successors or end the session in one writer decision."""
        self._require_execution_session_storage()
        self._session_text(session_id, 'session_id')
        self._session_text(outcome, 'outcome')
        self._session_time(finished_at, 'finished_at')
        if failures is not None and not isinstance(failures, list):
            raise ValueError('Session failures must be an ordered list')
        failure_data = json.dumps(failures if failures is not None else [])
        attempts = tuple(restart_attempts)
        rejected = dict(rejected_restarts or {})
        exhausted = dict(exhausted_restarts or {})
        failed_nodes = tuple(self.validate_node_name(node) for node in failed_nodes)
        if failed_nodes and outcome != 'failed':
            raise ValueError('Failed node publication requires a failed session outcome')
        component_outcomes = self._normalize_component_terminal_outcomes(component_outcomes)
        if any(component.lifecycle == 'failed' for component in component_outcomes) and outcome != 'failed':
            raise ValueError('Failed component publication requires a failed session outcome')

        def decide(connection):
            session = connection.execute(
                'SELECT status FROM execution_sessions WHERE session_id=?', (session_id,),
            ).fetchone()
            if session is None or session['status'] != 'running':
                raise RuntimeError(f'Execution session {session_id} is missing or already terminal')
            components = self._read_session_components(connection, session_id)
            roots = set(self._read_session_job_roots(connection, session_id, components=components))
            reserved_keys = {row['component_key'] for row in connection.execute(
                'SELECT component_key FROM component_reservations WHERE session_id=?', (session_id,),
            )}
            owned_nodes = {node for component in components if encode_component_key(component) in reserved_keys
                           for node in component}
            selected_nodes = {node for component in components for node in component}
            # Joined execution has no active work left. Include reserved nodes
            # even when their active-owner pointer is damaged or contradictory.
            # Previously selected work can remain active under another owner,
            # but only with a valid current claim and that owner's reservation.
            active = None
            for row in connection.execute(
                'SELECT job.node_name, job.job_id, job.generation, job.active_execution_id, '
                'owner.session_id FROM jobs AS job '
                'LEFT JOIN job_execution_owners AS owner ON owner.execution_id=job.active_execution_id '
                'WHERE job.active_execution_id IS NOT NULL',
            ):
                unsettled = row['session_id'] == session_id or row['node_name'] in owned_nodes
                if not unsettled and row['node_name'] in selected_nodes:
                    try:
                        observed = self._read_job_owner_observation(connection, row['node_name'], row['job_id'])
                        owner = observed['owner']
                        reservation = connection.execute(
                            'SELECT session_id FROM component_reservations WHERE component_key=?',
                            (encode_component_key(owner['component']),),
                        ).fetchone()
                        unsettled = (reservation is None or reservation['session_id'] != owner['session_id']
                                     or observed['session']['status'] != 'running')
                    except (RuntimeError, ValueError):
                        unsettled = True
                if unsettled:
                    active = row
                    break
            if active is not None:
                raise ExecutionSessionHasActiveJobs(
                    f'Execution session {session_id} still owns active job '
                    f'{active["node_name"]}/{active["job_id"]}, generation {active["generation"]}, '
                    f'execution {active["active_execution_id"]}; recovery is required'
                )
            candidates = list(attempts)
            if roots:
                # A root can catch a nested child's failure. Its accepted
                # successor remains owned work even when no Python failure
                # escapes the root or reaches the session driver.
                candidates.extend(tuple(row) for row in connection.execute(
                    'SELECT job.node_name, job.job_id, owner.generation, owner.execution_id '
                    'FROM jobs AS job JOIN job_instances AS instance USING(node_name, job_id) '
                    'JOIN job_execution_owners AS owner ON owner.execution_id=instance.last_execution_id '
                    "WHERE owner.session_id=? AND job.status='queued' "
                    'AND job.active_execution_id IS NULL AND job.restart_requested_at IS NOT NULL '
                    'ORDER BY job.node_name, job.job_id',
                    (session_id,),
                ))
            replacements = {}
            known_attempts = set()
            for node, job_id, generation, execution_id in dict.fromkeys(candidates):
                observed = self._read_job_owner_observation(connection, node, job_id)
                owner = None if observed is None else observed['owner']
                if (owner is None or owner['session_id'] != session_id
                        or owner['execution_id'] != execution_id
                        or owner['generation'] != generation
                        or observed['generation'] <= generation
                        or observed['status'] != 'queued'
                        or observed['active_execution_id'] is not None):
                    continue
                if roots:
                    producing_identity = self._read_component_producing_identity(connection, owner['component'])
                    if not self._job_descends_from_selected_root(
                        connection, execution_id, roots, session_id, owner['component'], producing_identity,
                    ):
                        raise RuntimeError('Owned restart is outside selected job causal scope')
                    if self._read_owned_restart(connection, node, job_id, session_id, owner['component']) is None:
                        raise RuntimeError('Selected job restart lost its accepted owner')
                known_attempts.add((node, job_id))
                reservation = connection.execute(
                    'SELECT session_id FROM component_reservations WHERE component_key=?',
                    (encode_component_key(owner['component']),),
                ).fetchone()
                if reservation is None or reservation['session_id'] != session_id:
                    raise RuntimeError('Restart owner no longer holds its component reservation')
                marker = connection.execute(
                    'SELECT restart_requested_at FROM jobs WHERE node_name=? AND job_id=?',
                    (node, job_id),
                ).fetchone()['restart_requested_at']
                previous = rejected.get((node, job_id)) or exhausted.get((node, job_id))
                if previous is not None:
                    if (observed['job_instance_id'] != previous['job_instance_id']
                            or owner != previous['owner']
                            or observed['generation'] <= previous['generation']):
                        continue
                    # A rejected claim or exhausted setup retry needs a later
                    # accepted request. The event, generation, owner, and marker
                    # must still agree in this same writer decision.
                    event = connection.execute(
                        "SELECT time, data_json FROM job_events WHERE node_name=? AND job_id=? "
                        "AND event='restart_requested' ORDER BY event_id DESC LIMIT 1",
                        (node, job_id),
                    ).fetchone()
                    data = {} if event is None else json.loads(event['data_json'])
                    if (event is None or event['time'] != marker
                            or data.get('generation') != observed['generation']
                            or data.get('job_instance_id') != observed['job_instance_id']
                            or data.get('execution_id') != execution_id
                            or data.get('session_id') != session_id
                            or data.get('component') != list(owner['component'])):
                        continue
                if marker is not None:
                    replacements[(node, job_id)] = {
                        'job_instance_id': observed['job_instance_id'],
                        'generation': observed['generation'], 'owner': owner,
                        'restart_requested_at': marker,
                    }
            nested_components = {}
            from .component_states import ComponentTerminalOutcome

            for pending_row in connection.execute(
                'SELECT pending.component_key, shape.shape_json, pending.alignment_generation, '
                'pending.completion_ready, pending.stability, pending.instability_origin, '
                'pending.execution_kind, pending.starting_lifecycle, pending.starting_misaligned '
                'FROM pending_component_executions AS pending JOIN graph_shapes AS shape USING(shape_id) '
                'JOIN component_reservations AS reservation USING(component_key) '
                'WHERE pending.session_id=? AND reservation.session_id=?', (session_id, session_id),
            ).fetchall():
                component = decode_component_key(pending_row['component_key'])
                for node in component:
                    for job in connection.execute(
                        "SELECT job_id, restart_requested_at FROM jobs WHERE node_name=? AND status='queued' "
                        'AND active_execution_id IS NULL AND restart_requested_at IS NOT NULL', (node,),
                    ).fetchall():
                        key = node, job['job_id']
                        accepted = bool(roots) and key in replacements
                        if key in known_attempts and not accepted:
                            continue
                        expected = replacements[key] if accepted else self._read_owned_restart(
                            connection, *key, session_id, component,
                        )
                        if expected is None or encode_component_key(expected['owner']['component']) != pending_row['component_key']:
                            continue
                        previous = rejected.get(key) or exhausted.get(key)
                        if previous is not None and (
                            expected['job_instance_id'] != previous['job_instance_id']
                            or expected['owner'] != previous['owner'] or expected['generation'] <= previous['generation']
                        ):
                            continue
                        proposal = ComponentTerminalOutcome(
                            component, pending_row['shape_json'], pending_row['alignment_generation'],
                            'done', pending_row['stability'], pending_row['instability_origin'],
                        )
                        try:
                            self._validate_component_terminal_outcomes(connection, session_id, (proposal,))
                        except RuntimeError:
                            if not accepted:
                                raise
                            # Repair the already accepted job first. The next
                            # exit still refuses this damaged pending result.
                            continue
                        replacements[key] = expected
                        nested_components[key] = proposal
            if replacements:
                decision = {'restarts': replacements, 'released': 0}
                if nested_components:
                    decision['component_restarts'] = nested_components
                return decision

            pending = connection.execute(
                'SELECT pending.component_key, shape.shape_json, pending.alignment_generation, '
                'pending.completion_ready, pending.stability, pending.instability_origin, pending.shape_id, '
                'pending.execution_kind, pending.starting_lifecycle, pending.starting_misaligned '
                'FROM pending_component_executions AS pending '
                'JOIN graph_shapes AS shape USING(shape_id) '
                'JOIN component_reservations AS reservation USING(component_key) '
                'WHERE pending.session_id=? AND reservation.session_id=?', (session_id, session_id),
            ).fetchall()
            settled = {result.component: result for result in component_outcomes}
            for row in pending:
                self._validate_pending_component_row(connection, row)
                component = decode_component_key(row['component_key'])
                ready = row['completion_ready'] == 1
                if component not in settled and outcome != 'failed' and not ready:
                    raise RuntimeError('Session completion omitted a running component')
                if row['execution_kind'] == 'jobs':
                    recorded = self._selected_component_result(
                        connection, session_id, row, successful=ready,
                    )
                    supplied = settled.get(component)
                    if supplied is not None and (
                        supplied.expected_shape != recorded.expected_shape
                        or supplied.expected_alignment_generation != recorded.expected_alignment_generation
                    ):
                        raise RuntimeError('Selected outcome differs from its recorded start')
                    settled[component] = recorded
                    continue
                successful = ready and all(
                    job['status'] in ('done', 'skipped') and job['active_execution_id'] is None
                    for node in component for job in connection.execute(
                        'SELECT status, active_execution_id FROM jobs WHERE node_name=?', (node,),
                    )
                )
                if ready and outcome != 'failed' and not successful:
                    raise RuntimeError('Deferred component completion has unfinished jobs')
                recorded = ComponentTerminalOutcome(
                    component, row['shape_json'], row['alignment_generation'],
                    'done' if successful else 'failed',
                    row['stability'] if successful else None,
                    row['instability_origin'] if successful else None,
                )
                if component in settled and (
                    settled[component].expected_shape != recorded.expected_shape
                    or settled[component].expected_alignment_generation != recorded.expected_alignment_generation
                ):
                    raise RuntimeError('Component outcome differs from its recorded start')
                settled.setdefault(component, recorded)
            settled_outcomes = tuple(settled.values())
            self._validate_component_terminal_outcomes(connection, session_id, settled_outcomes)
            supplied_keys = {encode_component_key(result.component) for result in settled_outcomes}
            running_keys = {row[0] for row in connection.execute(
                'SELECT state.component_key FROM component_states AS state '
                'JOIN component_reservations AS reservation USING(component_key) '
                "WHERE reservation.session_id=? AND state.lifecycle='running'", (session_id,),
            )}
            if running_keys - supplied_keys:
                raise RuntimeError('Terminal settlement omitted an owned running component')
            reserved_count = connection.execute(
                'SELECT COUNT(*) FROM component_reservations WHERE session_id=?', (session_id,),
            ).fetchone()[0]
            self._publish_component_terminal_outcomes(connection, session_id, settled_outcomes)
            if failed_nodes:
                if not set(failed_nodes) <= owned_nodes:
                    raise RuntimeError('Failed nodes are outside the session reserved scope')
                connection.executemany(
                    "UPDATE nodes SET status='failed', updated_at=CURRENT_TIMESTAMP WHERE node_name=?",
                    [(node,) for node in failed_nodes],
                )
            finished = connection.execute(
                "UPDATE execution_sessions SET status='terminal', outcome=?, finished_at=?, failures_json=? "
                "WHERE session_id=? AND status='running'",
                (outcome, finished_at, failure_data, session_id),
            ).rowcount
            if finished != 1:
                raise RuntimeError('Execution session changed before terminal settlement')
            released = connection.execute(
                'DELETE FROM component_reservations WHERE session_id=?', (session_id,),
            ).rowcount
            if released != reserved_count:
                raise RuntimeError('Execution session reservations changed before terminal settlement')
            return {'restarts': {}, 'released': released}

        return self.submit_db_mutation(decide, wait=True, priority=0)

    def get_execution_session(self, session_id: str) -> dict | None:
        self._require_execution_session_storage()
        connection = self.db_connection()
        row = connection.execute(
            "SELECT * FROM execution_sessions WHERE session_id=?", (session_id,),
        ).fetchone()
        if row is None:
            return None
        return self._execution_session_from_row(connection, row)

    def list_execution_sessions(self) -> list[dict]:
        self._require_execution_session_storage()
        connection = self.db_connection()
        rows = connection.execute("SELECT * FROM execution_sessions ORDER BY session_id").fetchall()
        return [self._execution_session_from_row(connection, row) for row in rows]

    def list_live_execution_sessions(self) -> list[dict]:
        return [session for session in self.list_execution_sessions()
                if execution_session_liveness(session)["live"]]

    def get_live_main_session(self) -> dict | None:
        return next((session for session in self.list_live_execution_sessions()
                     if session["session_kind"] == "main"), None)

    def get_live_execution_session(self) -> dict | None:
        """Compatibility reader; several live interrupts require an exact ID."""
        sessions = self.list_live_execution_sessions()
        main = next((session for session in sessions if session["session_kind"] == "main"), None)
        if main is not None:
            return main
        if len(sessions) > 1:
            names = ", ".join(session["session_id"] for session in sessions)
            raise RuntimeError(f"Several live interrupt sessions are ambiguous: {names}. Specify a session ID.")
        return sessions[0] if sessions else None

    @staticmethod
    def _execution_session_from_row(connection, row) -> dict:
        # Selected scope is immutable session history. Read every mutable
        # session field together, then attach its immutable scope rows.
        session_id = row["session_id"]
        result = dict(row)
        result["start_component"] = SessionSelectionStorageMixin._stored_session_component(result["start_component"])
        result["failures"] = json.loads(result.pop("failures_json"))
        result["details"] = json.loads(result.pop("details_json"))
        result["selected_components"] = SessionSelectionStorageMixin._read_session_components(connection, session_id)
        result["selected_jobs"] = [
            (node, job_id) for node, job_id, _ in
            ExecutionSessionStorageMixin._read_session_job_roots(
                connection, session_id, components=result["selected_components"],
            )
        ]
        return result
