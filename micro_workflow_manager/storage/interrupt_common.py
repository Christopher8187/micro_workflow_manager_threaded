"""Shared identities for explicit interrupt storage."""

from __future__ import annotations

import json

from micro_workflow_manager.component_identity import component_key, encode_component_key

from .component_definitions import component_snapshot_from_shape
from .component_membership import read_active_component_for_node


class InterruptAdmissionPaused(RuntimeError):
    """A claim is temporarily stopped by an admitted explicit interrupt."""

    def __init__(self, child_session_ids):
        self.child_session_ids = tuple(sorted(set(child_session_ids)))
        super().__init__(
            "Component admission is paused for interrupt session(s): "
            + ", ".join(self.child_session_ids)
        )


def _component_predecessors(shape_json, target):
    snapshot = component_snapshot_from_shape(shape_json)
    by_node = {node: component for component in snapshot.components for node in component}
    data = json.loads(shape_json)
    return tuple(sorted({
        by_node[source]
        for source, receiver in data["edges"]
        if receiver in target and source not in target
    }))


def _component_ancestors(shape_json, target):
    snapshot = component_snapshot_from_shape(shape_json)
    by_node = {node: component for component in snapshot.components for node in component}
    data = json.loads(shape_json)
    direct = {component: set() for component in snapshot.components}
    for source, receiver in data["edges"]:
        source_component = by_node[source]
        receiver_component = by_node[receiver]
        if source_component != receiver_component:
            direct[receiver_component].add(source_component)
    result = {target}
    pending = [target]
    while pending:
        current = pending.pop()
        for predecessor in direct[current]:
            if predecessor not in result:
                result.add(predecessor)
                pending.append(predecessor)
    return tuple(component for component in snapshot.components if component in result)


def read_session_parent_ids(connection, session_id):
    return [row[0] for row in connection.execute(
        "SELECT parent_session_id FROM execution_session_parents "
        "WHERE child_session_id=? ORDER BY parent_session_id",
        (session_id,),
    )]


def read_interrupt_claim_blockers(connection, session_id, component):
    key = encode_component_key(component)
    return tuple(row[0] for row in connection.execute(
        "SELECT child_session_id FROM ("
        "SELECT request.child_session_id FROM interrupt_pause_requests AS request "
        "JOIN interrupt_admissions AS admission ON admission.session_id=request.child_session_id "
        "WHERE request.owner_session_id=? AND request.component_key=? "
        "AND request.state IN ('requested','acknowledged') "
        "AND admission.state IN ('admitted','frozen') UNION "
        "SELECT transfer.child_session_id FROM interrupt_scope_transfers AS transfer "
        "JOIN execution_sessions AS child ON child.session_id=transfer.child_session_id "
        "WHERE transfer.source_session_id=? AND transfer.component_key=? "
        "AND transfer.state='active' AND child.status='running') "
        "ORDER BY child_session_id",
        (session_id, key, session_id, key),
    ))


def _session_selected(connection, session_id, component_key):
    return connection.execute(
        "SELECT session.status, session.session_kind, selected.component_key "
        "FROM execution_sessions AS session "
        "LEFT JOIN session_components AS selected "
        "ON selected.session_id=session.session_id AND selected.component_key=? "
        "WHERE session.session_id=?",
        (component_key, session_id),
    ).fetchone()


def _require_active_component(connection, component):
    for node in component:
        if read_active_component_for_node(connection, node) != component:
            raise RuntimeError("Interrupt selection requires exact active component membership")


def normalize_interrupt_component(component):
    if not isinstance(component, (tuple, list, set, frozenset)) or not component:
        raise ValueError("An interrupt component needs member node names")
    supplied = tuple(component)
    if any(not isinstance(node, str) or not node or node in (".", "..")
           or "/" in node or "\\" in node or ".." in node
           for node in supplied):
        raise ValueError("Invalid interrupt component")
    members = component_key(supplied)
    if len(members) != len(supplied):
        raise ValueError("Invalid interrupt component")
    return members


def _active_predecessor_executions(connection, predecessors):
    from .execution_ownership import JobExecutionOwnerStorageMixin

    result = []
    for component in predecessors:
        key = encode_component_key(component)
        for node in component:
            for row in connection.execute(
                "SELECT job_id FROM jobs WHERE node_name=? AND (status='running' "
                "OR active_execution_id IS NOT NULL OR active_pid IS NOT NULL "
                "OR active_thread_id IS NOT NULL OR active_started_at IS NOT NULL) "
                "ORDER BY job_id", (node,),
            ):
                observed = JobExecutionOwnerStorageMixin._read_job_owner_observation(
                    connection, node, row["job_id"],
                )
                owner = observed["owner"]
                reservation = connection.execute(
                    "SELECT session_id FROM component_reservations WHERE component_key=?", (key,),
                ).fetchone()
                if (observed["state"] != "active" or owner is None
                        or owner["component"] != component
                        or observed["session"]["status"] != "running"
                        or reservation is None or reservation["session_id"] != owner["session_id"]):
                    raise RuntimeError(
                        f"Interrupt predecessor has damaged active ownership: {node}/{row['job_id']}"
                    )
                result.append((component, observed))
    return tuple(result)
