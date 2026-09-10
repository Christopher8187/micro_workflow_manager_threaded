"""Read one job's direct lineage from a native database snapshot."""

from __future__ import annotations

import json

from micro_workflow_manager.storage.component_membership import read_active_component_for_node
from micro_workflow_manager.storage.component_misalignment import ComponentMisalignmentStorageMixin
from micro_workflow_manager.storage.component_result_identity import read_component_state_record
from micro_workflow_manager.storage.execution_ownership import JobExecutionOwnerStorageMixin
from micro_workflow_manager.storage.execution_sessions import (
    execution_session_from_row_snapshot, validate_execution_session_snapshot,
)
from micro_workflow_manager.storage.input_publications import InputPublicationStorageMixin
from micro_workflow_manager.storage.sample_history import read_sample_admission_history
from micro_workflow_manager.storage.session_selection import SessionSelectionStorageMixin

from .preview import PreviewStorage


class _LineageReader(ComponentMisalignmentStorageMixin):
    _read_execution_owner = staticmethod(JobExecutionOwnerStorageMixin._read_execution_owner)
    _validate_input_edge = staticmethod(InputPublicationStorageMixin._validate_input_edge)


def _job_reference(owner):
    return {"node": owner["node_name"], "job_id": owner["job_id"]}


def _execution_identities(connection, observed):
    owner = observed["owner"]
    if owner is None:
        return None, None
    row = connection.execute(
        "SELECT * FROM execution_sessions WHERE session_id=?", (owner["session_id"],),
    ).fetchone()
    session = execution_session_from_row_snapshot(connection, row)
    validate_execution_session_snapshot(session)
    sample_id = None
    if session["command"] == "run sample":
        shape = connection.execute(
            "SELECT shape_json FROM graph_shapes WHERE shape_id=?", (owner["shape_id"],),
        ).fetchone()[0]
        sample_id = read_sample_admission_history(
            SessionSelectionStorageMixin, connection, owner["session_id"],
            component=owner["component"], expected_shape=shape, require_active=False,
        ).sample_id
    interrupt_id = session["session_id"] if session["session_kind"] == "interrupt" else None
    return sample_id, interrupt_id


def read_lineage_snapshot(connection, node, job_id):
    observed = JobExecutionOwnerStorageMixin._read_job_owner_observation(connection, node, job_id)
    if observed is None:
        raise RuntimeError(f"Job does not exist: {node}/{job_id}")
    component = read_active_component_for_node(connection, node)
    if component is None:
        raise RuntimeError(f"Job has no active component: {node}/{job_id}")
    record = read_component_state_record(connection, component)
    if record is None:
        raise RuntimeError(f"Job component has no state: {node}/{job_id}")
    reader = _LineageReader()
    causes = reader._read_component_arrival_causes(connection, record.snapshot)
    instance = connection.execute(
        "SELECT created_by_execution_id FROM job_instances WHERE node_name=? AND job_id=?",
        (node, job_id),
    ).fetchone()
    creator = None if instance[0] is None else reader._read_execution_owner(connection, instance[0])
    if instance[0] is not None and creator is None:
        raise RuntimeError(f"Missing creating execution for {node}/{job_id}")
    children = []
    for row in connection.execute(
        "SELECT child.node_name, child.job_id, child.created_by_execution_id "
        "FROM job_instances AS child JOIN job_execution_owners AS producer "
        "ON producer.execution_id=child.created_by_execution_id "
        "WHERE producer.node_name=? AND producer.job_id=? AND producer.job_instance_id=? "
        "ORDER BY child.node_name, child.job_id",
        (node, job_id, observed["job_instance_id"]),
    ):
        reader._read_execution_owner(connection, row["created_by_execution_id"])
        children.append(_job_reference(row))
    sample_id, interrupt_id = _execution_identities(connection, observed)
    return {
        "schema_version": 1, "node": node, "job_id": job_id,
        "job_status": observed["status"],
        "component": {
            "members": list(component), "state": record.lifecycle,
            "stability": record.stability, "instability_origin": record.instability_origin,
            "misaligned": record.misaligned, "misalignment_causes": causes,
        },
        "sample_id": sample_id, "interrupt_session_id": interrupt_id,
        "created_by": None if creator is None else _job_reference(creator),
        "created_jobs": children,
    }


def lineage_command(root, node, job_id, *, json_output=False):
    storage = PreviewStorage(root)
    try:
        result = read_lineage_snapshot(storage.connection, node, job_id)
    finally:
        storage.close()
    if json_output:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    component = result["component"]
    print(f"Job {node}/{job_id}")
    print(f"  job-status: {result['job_status']}")
    print("  component: {" + ", ".join(component["members"]) + "}")
    for name in ("state", "stability", "instability_origin", "misaligned"):
        value = component[name]
        if type(value) is bool:
            value = "yes" if value else "no"
        print(f"  {name.replace('_', '-')}: {value if value is not None else '(none)'}")
    for cause in component["misalignment_causes"]:
        print("  first cause: " + json.dumps(cause, ensure_ascii=False, sort_keys=True))
    for name in ("sample_id", "interrupt_session_id"):
        print(f"  {name.replace('_', '-')}: {result[name] or '(none)'}")
    creator = result["created_by"]
    print("  created by: " + (
        "script" if creator is None else f"{creator['node']}/{creator['job_id']}"
    ))
    children = ", ".join(f"{child['node']}/{child['job_id']}" for child in result["created_jobs"])
    print("  created jobs: " + (children or "(none)"))
    return 0
