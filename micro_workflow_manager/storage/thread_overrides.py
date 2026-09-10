"""Persist per-node runtime limits against exact native session ownership."""

from __future__ import annotations

from datetime import datetime

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key
from micro_workflow_manager.node import validate_positive_int
from micro_workflow_manager.session_liveness import execution_session_liveness

from .base import FileStorageBase
from .component_membership import read_active_component_for_node
from .execution_ownership import JobExecutionOwnerStorageMixin
from .execution_sessions import (
    execution_session_from_row_snapshot,
    validate_execution_session_snapshot,
)


def _session(connection, session_id, cache=None, *, allow_terminal=False):
    result = None if cache is None else cache.get(session_id)
    if result is None:
        row = connection.execute(
            "SELECT * FROM execution_sessions WHERE session_id=?", (session_id,),
        ).fetchone()
        if row is None:
            raise RuntimeError("Thread ownership names a missing session: " + str(session_id))
        session = execution_session_from_row_snapshot(connection, row)
        validate_execution_session_snapshot(session)
        result = session, execution_session_liveness(session)["live"]
        if cache is not None:
            cache[session_id] = result
    if result[0]["status"] != "running" and not allow_terminal:
        raise RuntimeError("Thread ownership names a terminal session: " + session_id)
    return result


def _active_job_sessions(connection, component, sessions, *, allow_terminal=False):
    owners = set()
    parent_sessions = {}
    for node in component:
        rows = JobExecutionOwnerStorageMixin._read_active_job_owner_rows(connection, node)
        for row in rows:
            controls = (
                row["active_execution_id"], row["active_pid"],
                row["active_thread_id"], row["active_started_at"],
            )
            if (
                row["job_status"] != "running"
                or any(value is None for value in controls)
                or type(row["active_pid"]) is not int
                or row["active_pid"] < 1
                or type(row["active_thread_id"]) is not int
                or row["active_thread_id"] < 1
            ):
                raise RuntimeError(
                    f"Damaged active thread ownership for {node}/{row['observed_job_id']}"
                )
            try:
                datetime.fromisoformat(row["active_started_at"])
            except (TypeError, ValueError) as error:
                raise RuntimeError(
                    f"Damaged active thread ownership for {node}/{row['observed_job_id']}"
                ) from error
            observed = JobExecutionOwnerStorageMixin._job_owner_observation_from_row(
                connection, node, row["observed_job_id"], row, parent_sessions=parent_sessions,
            )
            owner = None if observed is None else observed["owner"]
            if (
                owner is None
                or observed["active_execution_id"] != row["active_execution_id"]
                or owner["component"] != component
            ):
                raise RuntimeError(
                    f"Damaged active thread ownership for {node}/{row['observed_job_id']}"
                )
            owner_session, _live = _session(
                connection, owner["session_id"], sessions,
                allow_terminal=allow_terminal,
            )
            if owner_session["status"] == "running":
                owners.add(owner["session_id"])
    return owners


def _reservation_for_node(connection, node_name, sessions):
    matches = []
    for row in connection.execute(
        "SELECT component_key, session_id FROM component_reservations "
        "ORDER BY component_key, session_id",
    ):
        try:
            component = decode_component_key(row["component_key"])
        except (TypeError, ValueError) as error:
            raise RuntimeError("Damaged component reservation identity") from error
        if node_name not in component:
            continue
        session_id = row["session_id"]
        session, live = _session(connection, session_id, sessions)
        if component not in session["selected_components"]:
            raise RuntimeError(
                "Thread reservation is outside session selection: " + session_id
            )
        matches.append((component, session_id, live))
    if len(matches) > 1:
        owners = ", ".join(
            f"{component!r} owned by {session_id}"
            for component, session_id, _live in matches
        )
        raise RuntimeError(
            f"Ambiguous thread reservations for {node_name}: " + owners
        )
    return (None, None, False) if not matches else matches[0]


def _stored_node_overrides(connection, node_name, session_cache):
    pending = connection.execute(
        "SELECT value FROM pending_node_thread_overrides WHERE node_name=?",
        (node_name,),
    ).fetchone()
    if pending is not None:
        validate_positive_int(f"pending thread override for {node_name}", pending["value"])
    owned = connection.execute(
        "SELECT value, session_id FROM node_thread_overrides "
        "WHERE node_name=? ORDER BY session_id",
        (node_name,),
    ).fetchall()
    owned_values = {}
    for row in owned:
        validate_positive_int(f"thread override for {node_name}", row["value"])
        session, live = _session(connection, row["session_id"], session_cache)
        owned_values[row["session_id"]] = (row["value"], live, session)
    return pending, owned_values


def _read_thread_override_observation(connection, node_name, sessions, *, allow_terminal_jobs=False):
    node_name = FileStorageBase.validate_node_name(node_name)
    component = read_active_component_for_node(connection, node_name)
    reservation_component, reservation_id, reservation_live = _reservation_for_node(
        connection, node_name, sessions,
    )

    if component is None:
        active = connection.execute(
            "SELECT job_id FROM jobs WHERE node_name=? AND (status='running' "
            "OR active_execution_id IS NOT NULL OR active_pid IS NOT NULL "
            "OR active_thread_id IS NOT NULL OR active_started_at IS NOT NULL) LIMIT 1",
            (node_name,),
        ).fetchone()
        if active is not None:
            raise RuntimeError(
                f"Active job has no current component ownership: {node_name}/{active['job_id']}"
            )
        active_owners = set()
    else:
        active_owners = _active_job_sessions(
            connection, component, sessions,
            allow_terminal=allow_terminal_jobs,
        )
    if active_owners and (
        reservation_id is None
        or active_owners != {reservation_id}
        or reservation_component != component
    ):
        names = sorted(active_owners | ({reservation_id} if reservation_id else set()))
        raise RuntimeError(
            f"Ambiguous thread owner for {node_name}: " + ", ".join(names)
        )

    pending, stored = _stored_node_overrides(connection, node_name, sessions)

    if reservation_live:
        if pending is not None:
            raise RuntimeError(
                f"Pending thread override was not bound to owner {reservation_id}: {node_name}"
            )
        value = None if reservation_id not in stored else stored[reservation_id][0]
        return {"node": node_name, "value": value, "session_id": reservation_id}

    return {
        "node": node_name,
        "value": None if pending is None else pending["value"],
        "session_id": None,
    }


def read_thread_override_observation(
    connection, node_name, *, _sessions=None, _allow_terminal_jobs=False,
):
    """Resolve one raw node from one coherent native database observation."""
    sessions = {} if _sessions is None else _sessions
    connection.execute("SAVEPOINT mwf_thread_override_observation")
    try:
        return _read_thread_override_observation(
            connection, node_name, sessions,
            allow_terminal_jobs=_allow_terminal_jobs,
        )
    finally:
        connection.execute("RELEASE SAVEPOINT mwf_thread_override_observation")


def validate_thread_override_claim(connection, session_id, component):
    """Require every raw member to resolve to the session about to claim work."""
    sessions = {}
    for node_name in component:
        observed = read_thread_override_observation(
            connection, node_name, _sessions=sessions,
        )
        if observed["session_id"] != session_id:
            raise RuntimeError(
                f"Thread ownership for {node_name} is not bound to claiming session {session_id}"
            )


class ThreadOverrideStorageMixin:
    """Write runtime limits only after re-reading their native owner."""

    def read_thread_override_observation(self, node_name):
        node_name = self.validate_node_name(node_name)
        return read_thread_override_observation(self.db_connection(), node_name)

    def read_thread_override_for_session(self, node_name, session_id):
        node_name = self.validate_node_name(node_name)
        if session_id is None:
            return None
        observed = read_thread_override_observation(self.db_connection(), node_name)
        if observed["session_id"] != session_id:
            return None
        return observed["value"]

    def set_thread_override(self, node_name, value, *, expected):
        node_name = self.validate_node_name(node_name)
        value = validate_positive_int("max_threads", value)

        def write(connection):
            current = read_thread_override_observation(connection, node_name)
            if current != expected:
                raise RuntimeError("Thread ownership changed before the update: " + node_name)
            owner = current["session_id"]
            if owner is None:
                changed = connection.execute(
                    "INSERT INTO pending_node_thread_overrides(node_name, value) VALUES(?, ?) "
                    "ON CONFLICT(node_name) DO UPDATE SET value=excluded.value",
                    (node_name, value),
                ).rowcount
            else:
                changed = connection.execute(
                    "INSERT INTO node_thread_overrides(node_name, session_id, value) "
                    "VALUES(?, ?, ?) ON CONFLICT(node_name, session_id) "
                    "DO UPDATE SET value=excluded.value",
                    (node_name, owner, value),
                ).rowcount
            if changed != 1:
                raise RuntimeError("Native thread override was not recorded: " + node_name)
            wanted = {"node": node_name, "value": value, "session_id": owner}
            if read_thread_override_observation(connection, node_name) != wanted:
                raise RuntimeError("Native thread override readback failed: " + node_name)
            return value

        return self.submit_db_mutation(write, wait=True, priority=0)

    def clear_thread_override(self, node_name, *, expected):
        node_name = self.validate_node_name(node_name)

        def clear(connection):
            current = read_thread_override_observation(connection, node_name)
            if current != expected:
                raise RuntimeError("Thread ownership changed before reset: " + node_name)
            owner = current["session_id"]
            if current["value"] is None:
                return False
            if owner is None:
                changed = connection.execute(
                    "DELETE FROM pending_node_thread_overrides WHERE node_name=? AND value=?",
                    (node_name, current["value"]),
                ).rowcount
            else:
                changed = connection.execute(
                    "DELETE FROM node_thread_overrides "
                    "WHERE node_name=? AND session_id=? AND value=?",
                    (node_name, owner, current["value"]),
                ).rowcount
            if changed != 1:
                raise RuntimeError("Native thread override changed before reset: " + node_name)
            wanted = {"node": node_name, "value": None, "session_id": owner}
            if read_thread_override_observation(connection, node_name) != wanted:
                raise RuntimeError("Native thread override reset readback failed: " + node_name)
            return True

        return self.submit_db_mutation(clear, wait=True, priority=0)

    @staticmethod
    def _bind_pending_thread_overrides(connection, session_id, nodes):
        nodes = tuple(sorted(set(nodes)))
        if not nodes:
            return 0
        moved = 0
        for node_name in nodes:
            owned = connection.execute(
                "SELECT value FROM node_thread_overrides "
                "WHERE node_name=? AND session_id=?",
                (node_name, session_id),
            ).fetchone()
            pending = connection.execute(
                "SELECT value FROM pending_node_thread_overrides WHERE node_name=?",
                (node_name,),
            ).fetchone()
            if owned is not None and pending is not None:
                raise RuntimeError(
                    "Session has both pending and owned thread overrides: " + node_name
                )
            if pending is not None:
                value = validate_positive_int(
                    f"pending thread override for {node_name}", pending["value"],
                )
                if connection.execute(
                    "INSERT INTO node_thread_overrides(node_name, session_id, value) "
                    "VALUES(?, ?, ?)", (node_name, session_id, value),
                ).rowcount != 1:
                    raise RuntimeError("Pending thread override was not bound: " + node_name)
                if connection.execute(
                    "DELETE FROM pending_node_thread_overrides "
                    "WHERE node_name=? AND value=?", (node_name, value),
                ).rowcount != 1:
                    raise RuntimeError("Pending thread override changed during admission")
                moved += 1
            observed = read_thread_override_observation(
                connection, node_name, _allow_terminal_jobs=True,
            )
            if observed["session_id"] != session_id:
                raise RuntimeError("Thread ownership did not bind during admission: " + node_name)
            if pending is not None and observed["value"] != pending["value"]:
                raise RuntimeError("Thread override value changed during admission: " + node_name)
        return moved

    @staticmethod
    def _transfer_thread_overrides(
        connection, source_session_id, child_session_id, component,
    ):
        """Copy one transferred component's values while retaining its parent rows."""
        members = tuple(component)
        if not members or members != tuple(sorted(set(members))):
            raise ValueError("Thread override transfer requires one canonical component")
        sessions = {}
        source, source_live = _session(connection, source_session_id, sessions)
        child, child_live = _session(connection, child_session_id, sessions)
        if not source_live or not child_live:
            raise RuntimeError("Thread override transfer requires two live sessions")
        if (
            members not in source["selected_components"]
            or members not in child["selected_components"]
        ):
            raise RuntimeError("Thread override transfer is outside session selection")

        placeholders = ",".join("?" for _ in members)
        if connection.execute(
            "SELECT 1 FROM pending_node_thread_overrides "
            f"WHERE node_name IN ({placeholders}) LIMIT 1", members,
        ).fetchone() is not None:
            raise RuntimeError("Owned component has an unbound pending thread override")
        source_rows = tuple(
            (row["node_name"], validate_positive_int(
                f"thread override for {row['node_name']}", row["value"],
            ))
            for row in connection.execute(
                "SELECT node_name, value FROM node_thread_overrides "
                f"WHERE session_id=? AND node_name IN ({placeholders}) ORDER BY node_name",
                (source_session_id, *members),
            )
        )
        child_rows = tuple(
            (row["node_name"], validate_positive_int(
                f"thread override for {row['node_name']}", row["value"],
            ))
            for row in connection.execute(
                "SELECT node_name, value FROM node_thread_overrides "
                f"WHERE session_id=? AND node_name IN ({placeholders}) ORDER BY node_name",
                (child_session_id, *members),
            )
        )
        if child_rows and child_rows != source_rows:
            raise RuntimeError("Interrupt child has conflicting thread override rows")
        if not child_rows and source_rows:
            inserted = connection.executemany(
                "INSERT INTO node_thread_overrides(node_name, session_id, value) "
                "VALUES(?, ?, ?)",
                [(node, child_session_id, value) for node, value in source_rows],
            ).rowcount
            if inserted != len(source_rows):
                raise RuntimeError("Thread overrides were not transferred completely")
        recorded = tuple(
            (row["node_name"], row["value"])
            for row in connection.execute(
                "SELECT node_name, value FROM node_thread_overrides "
                f"WHERE session_id=? AND node_name IN ({placeholders}) ORDER BY node_name",
                (child_session_id, *members),
            )
        )
        if recorded != source_rows:
            raise RuntimeError("Transferred thread override readback failed")
        return len(source_rows)

    @staticmethod
    def _clear_thread_overrides_for_session(connection, session_id):
        removed = connection.execute(
            "DELETE FROM node_thread_overrides WHERE session_id=?", (session_id,),
        ).rowcount
        if connection.execute(
            "SELECT 1 FROM node_thread_overrides WHERE session_id=?", (session_id,),
        ).fetchone() is not None:
            raise RuntimeError("Session thread overrides were not cleared: " + session_id)
        running = connection.execute(
            "SELECT 1 FROM execution_sessions WHERE status='running' LIMIT 1",
        ).fetchone()
        if running is None:
            aggregate = connection.execute(
                "SELECT value FROM api_thread_limit WHERE singleton=1",
            ).fetchone()
            if aggregate is not None:
                if connection.execute(
                    "DELETE FROM api_thread_limit WHERE singleton=1 AND value=?",
                    (aggregate["value"],),
                ).rowcount != 1:
                    raise RuntimeError("Aggregate API limit changed during session settlement")
                if connection.execute(
                    "SELECT 1 FROM api_thread_limit WHERE singleton=1",
                ).fetchone() is not None:
                    raise RuntimeError("Aggregate API limit was not cleared after the final session")
        return removed

    def read_api_total_limit(self):
        row = self.db_connection().execute(
            "SELECT value FROM api_thread_limit WHERE singleton=1",
        ).fetchone()
        return None if row is None else validate_positive_int("api_total_limit", row["value"])

    def update_api_total_limit(self, value, *, relative=False):
        if type(relative) is not bool:
            raise ValueError("relative must be an exact Boolean")
        if type(value) is not int:
            raise ValueError("api_total_limit change must be an exact integer")
        if not relative:
            value = validate_positive_int("api_total_limit", value)

        def write(connection):
            row = connection.execute(
                "SELECT value FROM api_thread_limit WHERE singleton=1",
            ).fetchone()
            current = (
                1 if row is None
                else validate_positive_int("api_total_limit", row["value"])
            )
            requested = current + value if relative else value
            if requested < 1:
                raise ValueError("effective max_threads must remain at least 1")
            changed = connection.execute(
                "INSERT INTO api_thread_limit(singleton, value) VALUES(1, ?) "
                "ON CONFLICT(singleton) DO UPDATE SET value=excluded.value",
                (requested,),
            ).rowcount
            if changed != 1:
                raise RuntimeError("Aggregate API limit was not recorded")
            recorded = connection.execute(
                "SELECT value FROM api_thread_limit WHERE singleton=1",
            ).fetchone()
            if recorded is None or recorded["value"] != requested:
                raise RuntimeError("Aggregate API limit readback failed")
            return requested

        return self.submit_db_mutation(write, wait=True, priority=0)

    def set_api_total_limit(self, value):
        return self.update_api_total_limit(value, relative=False)

    def clear_api_total_limit(self):
        def clear(connection):
            row = connection.execute(
                "SELECT value FROM api_thread_limit WHERE singleton=1",
            ).fetchone()
            if row is None:
                return False
            if connection.execute(
                "DELETE FROM api_thread_limit WHERE singleton=1 AND value=?",
                (row["value"],),
            ).rowcount != 1:
                raise RuntimeError("Aggregate API limit changed before reset")
            if connection.execute(
                "SELECT 1 FROM api_thread_limit WHERE singleton=1",
            ).fetchone() is not None:
                raise RuntimeError("Aggregate API limit reset readback failed")
            return True

        return self.submit_db_mutation(clear, wait=True, priority=0)
