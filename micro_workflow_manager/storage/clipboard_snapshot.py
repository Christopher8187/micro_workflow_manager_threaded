"""Create and validate one same-project native node snapshot."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
import re
import sqlite3
from pathlib import Path

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key
from micro_workflow_manager.models import JOB_VALID_STATUSES
from micro_workflow_manager.project_format import is_link_or_reparse_point
from .component_membership import read_active_component_for_node
from .component_misalignment import ComponentMisalignmentStorageMixin
from .component_result_identity import read_component_state_record, read_retained_successful_result
from .execution_ownership import JobExecutionOwnerStorageMixin
from .input_publications import InputPublicationStorageMixin
from .sqlite.schema import SQLiteSchemaMixin


SNAPSHOT_NAME = ".mwf-node-state.sqlite3"
SNAPSHOT_SIDECARS = (SNAPSHOT_NAME + "-wal", SNAPSHOT_NAME + "-shm")


class _ClipboardNativeValidator(ComponentMisalignmentStorageMixin, InputPublicationStorageMixin):
    _read_execution_owner = staticmethod(JobExecutionOwnerStorageMixin._read_execution_owner)
    _validate_input_edge = staticmethod(InputPublicationStorageMixin._validate_input_edge)


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def _overlaps(component, candidate_key) -> bool:
    try:
        candidate = decode_component_key(candidate_key)
    except (TypeError, ValueError) as error:
        raise RuntimeError("Clipboard observed an invalid component identity") from error
    if (not candidate or any(not isinstance(node, str) or not node for node in candidate)
            or candidate != tuple(sorted(set(candidate)))
            or encode_component_key(candidate) != candidate_key):
        raise RuntimeError("Clipboard observed an invalid component identity")
    return not set(component).isdisjoint(candidate)


def read_project_id(connection) -> str:
    row = connection.execute(
        "SELECT value FROM metadata WHERE key='project_id'"
    ).fetchone()
    value = None if row is None else row[0]
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{32}", value):
        raise RuntimeError("Native project has no valid clipboard identity")
    return value


def _rows(connection, sql, parameters=()):
    return tuple(dict(row) for row in connection.execute(sql, parameters).fetchall())


def _owner_closure(connection, node, instances, managed_producers, causes):
    instance_ids = {row["instance_id"] for row in instances}
    pending = {
        value for row in instances
        for value in (row["last_execution_id"], row["created_by_execution_id"])
        if value is not None
    }
    pending.update(row["execution_id"] for row in managed_producers)
    pending.update(
        row["producer_execution_id"] for row in causes
        if row["producer_execution_id"] is not None
    )
    if instance_ids:
        placeholders = ",".join("?" for _ in instance_ids)
        pending.update(row["execution_id"] for row in connection.execute(
            "SELECT execution_id FROM job_execution_owners WHERE node_name=? "
            f"AND job_instance_id IN ({placeholders}) ORDER BY execution_id",
            (node, *sorted(instance_ids)),
        ))
    owners = {}
    while pending:
        execution_id = pending.pop()
        if execution_id in owners:
            continue
        row = connection.execute(
            "SELECT * FROM job_execution_owners WHERE execution_id=?", (execution_id,)
        ).fetchone()
        if row is None:
            raise RuntimeError("Clipboard job history lost execution owner " + execution_id)
        owner = JobExecutionOwnerStorageMixin._read_execution_owner(connection, execution_id)
        if owner is None or dict(row)["execution_id"] != owner["execution_id"]:
            raise RuntimeError("Clipboard job history has invalid execution ownership")
        owners[execution_id] = dict(row)
        creator = row["created_by_execution_id"]
        if creator is not None:
            pending.add(creator)
    return tuple(owners[key] for key in sorted(owners))


def _preparation_history(connection, causes):
    receipt_ids = sorted({
        row["preparation_id"] for row in causes if row["preparation_id"] is not None
    })
    receipts, attempts = [], {}
    for operation_id in receipt_ids:
        receipt = connection.execute(
            "SELECT * FROM preparation_receipts WHERE operation_id=?", (operation_id,),
        ).fetchone()
        if receipt is None or receipt["state"] != "committed":
            raise RuntimeError("Clipboard component lost committed preparation history")
        receipts.append(dict(receipt))
        guard_id = receipt["guard_id"]
        attempt = connection.execute(
            "SELECT * FROM preparation_attempts WHERE operation_id=?", (guard_id,),
        ).fetchone()
        if attempt is None or attempt["state"] not in ("committed", "aborted"):
            raise RuntimeError("Clipboard component lost its preparation attempt")
        attempts[guard_id] = dict(attempt)
    return tuple(receipts), tuple(attempts[key] for key in sorted(attempts))


def _job_rows(connection, node):
    return _rows(connection, "SELECT * FROM jobs WHERE node_name=? ORDER BY job_id", (node,))


def _instance_rows(connection, node):
    return _rows(connection, "SELECT * FROM job_instances WHERE node_name=? ORDER BY job_id", (node,))


def _peer_work(connection, component, node):
    peers = tuple(member for member in component if member != node)
    return {
        "nodes": tuple(row for peer in peers for row in _rows(
            connection, "SELECT * FROM nodes WHERE node_name=?", (peer,),
        )),
        "jobs": tuple(row for peer in peers for row in _job_rows(connection, peer)),
        "instances": tuple(row for peer in peers for row in _instance_rows(connection, peer)),
        "events": tuple(row for peer in peers for row in _rows(
            connection, "SELECT * FROM job_events WHERE node_name=? ORDER BY event_id", (peer,),
        )),
        "idempotency": tuple(row for peer in peers for row in _rows(
            connection, "SELECT * FROM idempotency WHERE node_name=? ORDER BY key_hash", (peer,),
        )),
        "default_jobs": tuple(row for peer in peers for row in _rows(
            connection, "SELECT * FROM default_job_specs WHERE node_name=? ORDER BY spec_key", (peer,),
        )),
        "sequences": tuple(row for peer in peers for row in _rows(
            connection, "SELECT * FROM job_sequences WHERE node_name=?", (peer,),
        )),
        "managed_inputs": tuple(row for peer in peers for row in _rows(
            connection,
            "SELECT * FROM managed_input_files WHERE receiver_node=? ORDER BY relative_path",
            (peer,),
        )),
        "managed_producers": tuple(row for peer in peers for row in _rows(
            connection,
            "SELECT producer.* FROM managed_input_producers AS producer "
            "WHERE receiver_node=? ORDER BY relative_path, execution_id",
            (peer,),
        )),
    }


def _component_rows(connection, component):
    key = encode_component_key(component)
    state = connection.execute(
        "SELECT * FROM component_states WHERE component_key=?", (key,)
    ).fetchone()
    if state is None:
        raise RuntimeError("Clipboard component has no native state")
    state_record = read_component_state_record(connection, component)
    retained = read_retained_successful_result(connection, state_record)
    result = None
    if retained is not None:
        result = connection.execute(
            "SELECT * FROM component_successful_results WHERE component_key=? "
            "AND shape_id=? AND alignment_generation=?",
            (key, *retained.identity.values),
        ).fetchone()
        if result is None:
            raise RuntimeError("Clipboard component lost its retained result")
    causes = _rows(
        connection,
        "SELECT * FROM component_misalignment_causes WHERE component_key=? "
        "AND alignment_generation=? ORDER BY receiver_node",
        (key, state_record.identity.alignment_generation),
    )
    _ClipboardNativeValidator()._read_component_arrival_causes(
        connection, state_record.snapshot,
    )
    return dict(state), None if result is None else dict(result), causes


@dataclass(frozen=True, slots=True)
class ClipboardObservation:
    project_id: str
    node: str
    component: tuple[str, ...]
    data: dict
    digest: str


def observe_clipboard_node(connection, node) -> ClipboardObservation:
    component = read_active_component_for_node(connection, node)
    if component is None:
        raise RuntimeError("Clipboard node has no active native component: " + node)
    state, result, causes = _component_rows(connection, component)
    preparation_receipts, preparation_attempts = _preparation_history(connection, causes)
    jobs = _job_rows(connection, node)
    instances = _instance_rows(connection, node)
    if (len(jobs) != len(instances)
            or any(type(row["job_id"]) is not int or row["job_id"] < 1
                   or type(row["generation"]) is not int or row["generation"] < 0
                   or row["status"] not in JOB_VALID_STATUSES for row in jobs)
            or tuple((row["node_name"], row["job_id"]) for row in jobs)
            != tuple((row["node_name"], row["job_id"]) for row in instances)):
        raise RuntimeError("Clipboard node has incomplete job identities")
    for job in jobs:
        if JobExecutionOwnerStorageMixin._read_job_owner_observation(
            connection, node, job["job_id"],
        ) is None:
            raise RuntimeError("Clipboard job lost its native ownership observation")
    managed_files = _rows(
        connection,
        "SELECT * FROM managed_input_files WHERE receiver_node=? ORDER BY relative_path",
        (node,),
    )
    managed_producers = _rows(
        connection,
        "SELECT * FROM managed_input_producers WHERE receiver_node=? "
        "ORDER BY relative_path, execution_id",
        (node,),
    )
    validator = _ClipboardNativeValidator()
    validator._require_settled_input_publications(connection, node)
    for item in managed_files:
        validator._read_input_ownership(connection, node, item["relative_path"])
    data = {
        "node_row": next(iter(_rows(connection, "SELECT * FROM nodes WHERE node_name=?", (node,))), None),
        "jobs": jobs,
        "instances": instances,
        "events": _rows(connection, "SELECT * FROM job_events WHERE node_name=? ORDER BY event_id", (node,)),
        "idempotency": _rows(connection, "SELECT * FROM idempotency WHERE node_name=? ORDER BY key_hash", (node,)),
        "default_jobs": _rows(
            connection,
            "SELECT * FROM default_job_specs WHERE node_name=? ORDER BY spec_key", (node,),
        ),
        "sequence": next(iter(_rows(connection, "SELECT * FROM job_sequences WHERE node_name=?", (node,))), None),
        "managed_inputs": managed_files,
        "managed_producers": managed_producers,
        "owners": _owner_closure(connection, node, instances, managed_producers, causes),
        "preparation_receipts": preparation_receipts,
        "preparation_attempts": preparation_attempts,
        "component_state": state,
        "retained_result": result,
        "misalignment_causes": causes,
        "peer_work": _peer_work(connection, component, node),
    }
    if data["node_row"] is None and (jobs or instances):
        raise RuntimeError("Clipboard jobs have no native node summary")
    envelope = {"project_id": read_project_id(connection), "node": node,
                "component": component, "data": data}
    return ClipboardObservation(
        envelope["project_id"], node, component, data, digest(envelope),
    )


def _admitted_session_ids(connection, sessions) -> set[str]:
    session_ids = {session["session_id"] for session in sessions}
    return {
        row["session_id"]
        for row in connection.execute(
            "SELECT session_id FROM execution_sessions "
            "WHERE status='running' AND scope_admitted=1 ORDER BY session_id"
        )
        if row["session_id"] in session_ids
    }


def require_idle_clipboard_component(connection, observation, *, allowed_guard=None):
    from .execution_sessions import read_execution_sessions_snapshot

    state = read_component_state_record(connection, observation.component)
    if state.lifecycle == "running":
        raise RuntimeError("Clipboard copy/paste requires the component to finish or stop first")
    sessions = read_execution_sessions_snapshot(connection, running_only=True)
    admitted_ids = _admitted_session_ids(connection, sessions)
    for session in sessions:
        if session["session_id"] in admitted_ids and any(
                not set(observation.component).isdisjoint(component)
                for component in session["selected_components"]):
            raise RuntimeError(
                "Clipboard copy/paste requires admitted component session "
                + session["session_id"] + " to finish or stop first"
            )
    for table in ("component_reservations", "component_holds", "pending_component_executions"):
        rows = connection.execute(
            "SELECT component_key FROM " + table + " ORDER BY component_key"
        ).fetchall()
        if any(_overlaps(observation.component, row["component_key"]) for row in rows):
            raise RuntimeError("Clipboard copy/paste requires an idle component")
    placeholders = ",".join("?" for _ in observation.component)
    if connection.execute(
        "SELECT 1 FROM api_execution_permits WHERE node_name IN (" + placeholders
        + ") LIMIT 1", observation.component,
    ).fetchone() is not None:
        raise RuntimeError("Clipboard copy/paste requires active API work to finish or stop first")
    for member in observation.component:
        active = connection.execute(
            "SELECT job_id, active_execution_id FROM jobs WHERE node_name=? AND "
            "(status='running' OR active_execution_id IS NOT NULL OR active_pid IS NOT NULL "
            "OR active_thread_id IS NOT NULL OR active_started_at IS NOT NULL) LIMIT 1",
            (member,),
        ).fetchone()
        if active is not None:
            raise RuntimeError(
                f"Clipboard copy/paste requires active job {member}/{active['job_id']} to finish or stop first"
            )
    guards = connection.execute(
        "SELECT operation_id FROM receiver_mutation_guards WHERE receiver_node IN ("
        + ",".join("?" for _ in observation.component) + ")",
        observation.component,
    ).fetchall()
    if any(row["operation_id"] != allowed_guard for row in guards):
        raise RuntimeError("Clipboard component is guarded by another operation")


def require_no_live_clipboard_session(connection, observation):
    """Read-only preflight that leaves stale ownership for startup recovery."""
    from .execution_sessions import read_live_execution_sessions_snapshot

    live = read_live_execution_sessions_snapshot(connection)
    admitted_ids = _admitted_session_ids(connection, live)
    for session in live:
        if session["session_id"] in admitted_ids and any(
                not set(observation.component).isdisjoint(component)
                for component in session["selected_components"]):
            raise RuntimeError(
                "Clipboard copy/paste requires admitted component session "
                + session["session_id"] + " to finish or stop first"
            )
    live_ids = {session["session_id"] for session in live}
    for table in ("component_reservations", "component_holds", "pending_component_executions"):
        rows = connection.execute(
            "SELECT session_id, component_key FROM " + table + " ORDER BY session_id, component_key"
        ).fetchall()
        if any(row["session_id"] in live_ids
               and _overlaps(observation.component, row["component_key"]) for row in rows):
            raise RuntimeError("Clipboard copy/paste requires overlapping live work to finish or stop first")


def require_clipboard_payload(path: Path, observation):
    """Require an ordinary job tree matching the saved native job rows exactly."""
    path = Path(path)
    if is_link_or_reparse_point(path) or not path.is_dir():
        raise RuntimeError("Clipboard node payload is missing or is not ordinary")
    jobs_root = path / "jobs"
    payload_ids = set()
    if is_link_or_reparse_point(jobs_root):
        raise RuntimeError("Clipboard jobs payload is not an ordinary directory")
    if jobs_root.exists():
        if not jobs_root.is_dir():
            raise RuntimeError("Clipboard jobs payload is not an ordinary directory")
        for child in jobs_root.iterdir():
            if (is_link_or_reparse_point(child) or not child.is_dir()
                    or not child.name.isdigit() or str(int(child.name)) != child.name
                    or int(child.name) < 1):
                raise RuntimeError("Clipboard jobs payload has an invalid member: " + child.name)
            input_path = child / "input.json"
            if is_link_or_reparse_point(input_path) or not input_path.is_file():
                raise RuntimeError("Clipboard job payload has no ordinary input: " + child.name)
            payload_ids.add(int(child.name))
    native_ids = {row["job_id"] for row in observation.data["jobs"]}
    extra = sorted(payload_ids - native_ids)
    if extra:
        addresses = ", ".join(f"{observation.node}/{job_id}" for job_id in extra)
        raise RuntimeError("Clipboard payloads have no native job state: " + addresses)
    missing = sorted(native_ids - payload_ids)
    if missing:
        addresses = ", ".join(f"{observation.node}/{job_id}" for job_id in missing)
        raise RuntimeError("Clipboard native jobs have no payload: " + addresses)


def write_native_snapshot(source, destination: Path):
    destination = Path(destination)
    if destination.exists():
        raise RuntimeError("Clipboard snapshot destination already exists")
    snapshot = sqlite3.connect(destination)
    try:
        source.backup(snapshot)
        mode = snapshot.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
        if str(mode).lower() != "delete":
            raise RuntimeError("Clipboard snapshot could not enter standalone journal mode")
    finally:
        snapshot.close()


def open_native_snapshot(path: Path, node: str, project_id: str):
    path = Path(path)
    if is_link_or_reparse_point(path) or not path.is_file():
        raise RuntimeError("Native clipboard snapshot is missing or is not an ordinary file")
    if any(os.path.lexists(path.parent / name) for name in SNAPSHOT_SIDECARS):
        raise RuntimeError("Native clipboard snapshot has unexpected journal sidecars")
    snapshot = sqlite3.connect(path.resolve().as_uri() + "?mode=ro&immutable=1", uri=True)
    snapshot.row_factory = sqlite3.Row
    try:
        SQLiteSchemaMixin.validate_native_database(snapshot)
        if read_project_id(snapshot) != project_id:
            raise RuntimeError("Clipboard snapshot belongs to another project")
        observation = observe_clipboard_node(snapshot, node)
        require_idle_clipboard_component(snapshot, observation)
        return snapshot, observation
    except BaseException:
        snapshot.close()
        raise
