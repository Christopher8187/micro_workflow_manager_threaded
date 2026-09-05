from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime

from micro_workflow_manager.session_liveness import execution_session_liveness


class ExecutionSessionStorageMixin:
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
        return tuple(sorted({self.validate_node_name(node) for node in component}))

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
        components = [json.dumps(list(component)) for component in component_keys]

        def create(connection):
            connection.execute(
                "INSERT INTO execution_sessions("
                "session_id, session_kind, parent_session_id, command, start_component, "
                "status, started_at, heartbeat_at, hostname, pid, process_identity, details_json) "
                "VALUES(?, ?, ?, ?, ?, 'running', ?, ?, ?, ?, ?, ?)",
                (session_id, session_kind, parent_session_id, command,
                 json.dumps(list(start_component)), started_at, started_at,
                 hostname, pid, process_identity, json.dumps(details or {})),
            )
            connection.executemany(
                "INSERT INTO session_components(session_id, position, component_key) VALUES(?, ?, ?)",
                [(session_id, position, component) for position, component in enumerate(components)],
            )
            connection.executemany(
                "INSERT INTO session_jobs(session_id, position, node_name, job_id) VALUES(?, ?, ?, ?)",
                [(session_id, position, node, job_id) for position, (node, job_id) in enumerate(jobs)],
            )

        self.submit_db_mutation(create, wait=True, priority=0)
        return self.get_execution_session(session_id)

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
        result["start_component"] = tuple(json.loads(result["start_component"]))
        result["failures"] = json.loads(result.pop("failures_json"))
        result["details"] = json.loads(result.pop("details_json"))
        result["selected_components"] = [
            tuple(json.loads(row[0])) for row in connection.execute(
                "SELECT component_key FROM session_components WHERE session_id=? ORDER BY position",
                (session_id,),
            )
        ]
        result["selected_jobs"] = [
            (row[0], row[1]) for row in connection.execute(
                "SELECT node_name, job_id FROM session_jobs WHERE session_id=? ORDER BY position",
                (session_id,),
            )
        ]
        return result
