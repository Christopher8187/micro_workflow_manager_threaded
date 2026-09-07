"""Read native plan data through connection/root observation functions."""

from __future__ import annotations

from dataclasses import dataclass

from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.models import JOB_VALID_STATUSES

from .component_definitions import component_snapshot_from_shape
from .component_states import read_component_state_snapshot, read_component_states_snapshot
from .execution_sessions import (
    read_execution_sessions_snapshot,
    read_live_execution_sessions_snapshot,
)
from .planning_observation import LiveSessionPlanState, NodeJobPlanState
from .preparation_footprint import read_preparation_footprint_snapshot
from .resume_preparation import read_resume_plan_snapshot


@dataclass(frozen=True)
class NativePlanningReader:
    """A query-only reader with no storage mutation methods."""

    connection: object
    project_root: object
    shape_json: str

    def _component_for_node(self, node):
        snapshot = component_snapshot_from_shape(self.shape_json)
        return next(
            (component for component in snapshot.components if node in component),
            (node,),
        )

    def read_component_states(self, components, *, allow_missing: bool):
        return read_component_states_snapshot(
            self.connection,
            components,
            expected_shape=self.shape_json,
            allow_missing=allow_missing,
        )

    def list_live_execution_sessions(self):
        return tuple(
            LiveSessionPlanState(
                session["session_id"], session["session_kind"],
                session["status"], session["outcome"],
            )
            for session in read_live_execution_sessions_snapshot(self.connection)
        )

    def read_component_activity(self, components):
        """Validate and describe normal reservation, hold, and pending activity."""
        sessions = {
            session["session_id"]: session
            for session in read_execution_sessions_snapshot(
                self.connection, running_only=True,
            )
        }
        activity = []
        for component in components:
            component = tuple(component)
            key = encode_component_key(component)
            reservation = self.connection.execute(
                "SELECT session_id FROM component_reservations WHERE component_key=?",
                (key,),
            ).fetchone()
            if reservation is not None:
                owner = sessions.get(reservation["session_id"])
                if owner is None or component not in owner["selected_components"]:
                    raise RuntimeError("Damaged component reservation: " + key)
                activity.append(
                    f"component {component} is reserved by {owner['session_kind']} "
                    f"session {owner['session_id']}"
                )

            pending = self.connection.execute(
                "SELECT pending.*, shape.shape_json FROM pending_component_executions AS pending "
                "JOIN graph_shapes AS shape USING(shape_id) WHERE pending.component_key=?",
                (key,),
            ).fetchall()
            if len(pending) > 1:
                raise RuntimeError("Component has several pending executions: " + key)
            if pending:
                row = pending[0]
                owner = sessions.get(row["session_id"])
                state = read_component_state_snapshot(self.connection, component)
                if (
                    reservation is None
                    or reservation["session_id"] != row["session_id"]
                    or owner is None
                    or component not in owner["selected_components"]
                    or state is None
                    or state["lifecycle"] != "running"
                    or state["shape_json"] != row["shape_json"]
                    or state["alignment_generation"] != row["alignment_generation"]
                ):
                    raise RuntimeError("Damaged pending component execution: " + key)
                from .preparation_footprint import _SnapshotPreparationStorage

                _SnapshotPreparationStorage(
                    self.connection, self.project_root,
                )._validate_pending_component_row(self.connection, row)
                activity.append(
                    f"component {component} has a pending {row['execution_kind']} "
                    f"execution in session {row['session_id']}"
                )
            else:
                state = read_component_state_snapshot(self.connection, component)
                if state is not None and state["lifecycle"] == "running":
                    raise RuntimeError("Running component lost its pending execution: " + key)

            for hold in self.connection.execute(
                "SELECT holds.session_id, holds.hold_count, session.status "
                "FROM component_holds AS holds LEFT JOIN execution_sessions AS session USING(session_id) "
                "WHERE holds.component_key=? ORDER BY holds.session_id",
                (key,),
            ):
                if (
                    hold["status"] != "running"
                    or hold["session_id"] not in sessions
                    or type(hold["hold_count"]) is not int
                    or hold["hold_count"] < 1
                ):
                    raise RuntimeError("Damaged component hold: " + key)
                activity.append(
                    f"component {component} has {hold['hold_count']} hold(s) "
                    f"from session {hold['session_id']}"
                )
        return tuple(activity)

    def read_node_job_counts(self, nodes):
        """Return validated status counts for every selected raw node."""
        nodes = tuple(nodes)
        counts = {node: [] for node in nodes}
        for offset in range(0, len(nodes), 500):
            selected = nodes[offset:offset + 500]
            placeholders = ",".join("?" for _ in selected)
            for row in self.connection.execute(
                "SELECT node_name, status, COUNT(*) AS count FROM jobs "
                f"WHERE node_name IN ({placeholders}) "
                "GROUP BY node_name, status ORDER BY node_name, status",
                selected,
            ):
                if (
                    row["node_name"] not in counts
                    or row["status"] not in JOB_VALID_STATUSES
                    or type(row["count"]) is not int
                    or row["count"] < 1
                ):
                    raise RuntimeError("Damaged selected-node job count")
                counts[row["node_name"]].append((row["status"], row["count"]))
        return tuple(
            NodeJobPlanState(node, tuple(counts[node]))
            for node in nodes
        )

    @staticmethod
    def _guard_text(row):
        operation_id = row["operation_id"]
        if (
            type(operation_id) is not str
            or len(operation_id) != 32
            or any(character not in "0123456789abcdef" for character in operation_id)
            or type(row["owner_pid"]) is not int
            or row["owner_pid"] < 1
            or not isinstance(row["process_identity"], str)
            or not row["process_identity"].strip()
            or not isinstance(row["hostname"], str)
            or not row["hostname"].strip()
            or (
                row["session_id"] is not None
                and (
                    not isinstance(row["session_id"], str)
                    or not row["session_id"].strip()
                )
            )
        ):
            raise RuntimeError(
                "Damaged receiver mutation guard: " + str(row["receiver_node"])
            )
        return (
            f"receiver {row['receiver_node']} has mutation guard {operation_id}"
        )

    def read_receiver_activity(self, receivers, *, include_active_jobs: bool):
        """Describe guards and, where required, exact active job ownership."""
        activity = []
        for receiver in dict.fromkeys(receivers):
            guard = self.connection.execute(
                "SELECT * FROM receiver_mutation_guards WHERE receiver_node=?",
                (receiver,),
            ).fetchone()
            if guard is not None:
                activity.append(self._guard_text(guard))
            if not include_active_jobs:
                continue
            component = self._component_for_node(receiver)
            key = encode_component_key(component)
            reservation = self.connection.execute(
                "SELECT session_id FROM component_reservations WHERE component_key=?",
                (key,),
            ).fetchone()
            state = read_component_state_snapshot(self.connection, component)
            if reservation is not None and state is None:
                raise RuntimeError("Damaged reserved receiver component: " + key)
            if state is not None and state["shape_json"] != self.shape_json:
                raise RuntimeError("Receiver component belongs to a different graph shape: " + key)
            if reservation is not None or (
                state is not None and state["lifecycle"] == "running"
            ):
                activity.extend(self.read_component_activity((component,)))
            for node in component:
                rows = self.connection.execute(
                    "SELECT job_id, status, active_execution_id, active_pid, "
                    "active_thread_id, active_started_at FROM jobs WHERE node_name=? AND "
                    "(status='running' OR active_execution_id IS NOT NULL OR active_pid IS NOT NULL "
                    "OR active_thread_id IS NOT NULL OR active_started_at IS NOT NULL) "
                    "ORDER BY job_id",
                    (node,),
                )
                for row in rows:
                    from .preparation_footprint import _SnapshotPreparationStorage

                    observed = _SnapshotPreparationStorage(
                        self.connection, self.project_root,
                    )._read_job_owner_observation(
                        self.connection, node, row["job_id"],
                    )
                    active_execution = row["active_execution_id"]
                    active_values = (
                        active_execution, row["active_pid"], row["active_thread_id"],
                        row["active_started_at"],
                    )
                    if (
                        row["status"] not in JOB_VALID_STATUSES
                        or any(value is None for value in active_values)
                        or observed is None
                        or observed["status"] != row["status"]
                        or observed["active_execution_id"] != active_execution
                        or observed["owner"] is None
                    ):
                        raise RuntimeError(
                            f"Damaged active job ownership: {node}/{row['job_id']}"
                        )
                    activity.append(
                        f"receiver {receiver} has active job {node}/{row['job_id']} "
                        f"execution {active_execution}"
                    )
        return tuple(activity)

    def read_preparation_footprint(self, components, *, keep_trace: bool):
        return read_preparation_footprint_snapshot(
            self.connection,
            self.project_root,
            components,
            keep_trace=keep_trace,
        )

    def read_resume_plan(self, components, states):
        return read_resume_plan_snapshot(
            self.connection,
            self.project_root,
            components,
            states,
            expected_shape=self.shape_json,
        )
