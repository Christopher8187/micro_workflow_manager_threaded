"""Validate and atomically restore one node's native state."""

from micro_workflow_manager.component_identity import encode_component_key
from .component_result_identity import read_component_state_record


def validate_clipboard_support(connection, saved, current):
    if saved.project_id != current.project_id or saved.node != current.node:
        raise RuntimeError("Clipboard snapshot belongs to another project or node")
    if saved.component != current.component:
        raise RuntimeError("Clipboard node belongs to another active component")
    if saved.data["peer_work"] != current.data["peer_work"]:
        raise RuntimeError("Clipboard node cannot coexist with current peer work")
    for owner in saved.data["owners"]:
        row = connection.execute(
            "SELECT * FROM job_execution_owners WHERE execution_id=?",
            (owner["execution_id"],),
        ).fetchone()
        if row is None or dict(row) != owner:
            raise RuntimeError("Clipboard supporting execution history changed")
    for table, rows in (
        ("preparation_receipts", saved.data["preparation_receipts"]),
        ("preparation_attempts", saved.data["preparation_attempts"]),
    ):
        for expected in rows:
            row = connection.execute(
                "SELECT * FROM " + table + " WHERE operation_id=?",
                (expected["operation_id"],),
            ).fetchone()
            if row is None or dict(row) != expected:
                raise RuntimeError("Clipboard supporting preparation history changed")
    result = saved.data["retained_result"]
    if result is not None:
        row = connection.execute(
            "SELECT * FROM component_successful_results WHERE component_key=? "
            "AND shape_id=? AND alignment_generation=?",
            (result["component_key"], result["shape_id"], result["alignment_generation"]),
        ).fetchone()
        if row is None or dict(row) != result:
            raise RuntimeError("Clipboard retained successful history changed")
    for job in saved.data["jobs"]:
        if (job["status"] == "running" or any(job[name] is not None for name in (
            "active_execution_id", "active_pid", "active_thread_id", "active_started_at",
        ))):
            raise RuntimeError("Clipboard snapshot contains active job work")


def _generation_floor(connection, component):
    key = encode_component_key(component)
    values = [row[0] for sql, parameters in (
        ("SELECT alignment_generation FROM component_states WHERE component_key=?", (key,)),
        ("SELECT alignment_generation FROM component_successful_results WHERE component_key=?", (key,)),
        ("SELECT alignment_generation FROM job_execution_owners WHERE component_key=?", (key,)),
    ) for row in connection.execute(sql, parameters)]
    placeholders = ",".join("?" for _ in component)
    values.extend(row[0] for row in connection.execute(
        "SELECT alignment_generation FROM component_misalignment_causes "
        f"WHERE receiver_node IN ({placeholders})", component,
    ))
    return max(values, default=-1) + 1


def _insert_rows(connection, table, rows):
    for row in rows:
        names = tuple(row)
        if connection.execute(
            "INSERT INTO " + table + "(" + ",".join(names) + ") VALUES("
            + ",".join("?" for _ in names) + ")",
            tuple(row[name] for name in names),
        ).rowcount != 1:
            raise RuntimeError("Clipboard row was not restored in " + table)


def _require_rows(connection, table, column, node, order, expected):
    actual = tuple(dict(row) for row in connection.execute(
        "SELECT * FROM " + table + " WHERE " + column + "=? ORDER BY " + order,
        (node,),
    ))
    if actual != tuple(expected):
        raise RuntimeError("Clipboard restored " + table + " rows changed")


def replace_clipboard_node_state(connection, saved, current, operation_id):
    node = saved.node
    key = encode_component_key(saved.component)
    validate_clipboard_support(connection, saved, current)
    from .clipboard_snapshot import require_idle_clipboard_component
    require_idle_clipboard_component(connection, current, allowed_guard=operation_id)
    for table in ("idempotency", "default_job_specs", "job_events"):
        connection.execute("DELETE FROM " + table + " WHERE node_name=?", (node,))
    connection.execute("DELETE FROM managed_input_files WHERE receiver_node=?", (node,))
    connection.execute("DELETE FROM jobs WHERE node_name=?", (node,))
    connection.execute("DELETE FROM job_sequences WHERE node_name=?", (node,))

    saved_node = saved.data["node_row"]
    current_node = current.data["node_row"]
    if saved_node is None and current_node is not None:
        if connection.execute(
            "SELECT 1 FROM pending_node_thread_overrides WHERE node_name=? UNION ALL "
            "SELECT 1 FROM node_thread_overrides WHERE node_name=? LIMIT 1", (node, node),
        ).fetchone() is not None:
            raise RuntimeError("Clipboard cannot remove a node with retained thread settings")
        connection.execute("DELETE FROM nodes WHERE node_name=?", (node,))
    elif saved_node is not None and current_node is None:
        _insert_rows(connection, "nodes", (saved_node,))
    elif saved_node is not None:
        if connection.execute(
            "UPDATE nodes SET status=?, updated_at=? WHERE node_name=?",
            (saved_node["status"], saved_node["updated_at"], node),
        ).rowcount != 1:
            raise RuntimeError("Clipboard node summary was not restored")
    _insert_rows(connection, "jobs", saved.data["jobs"])
    for instance in saved.data["instances"]:
        changed = connection.execute(
            "UPDATE job_instances SET instance_id=?, last_execution_id=?, created_by_execution_id=? "
            "WHERE node_name=? AND job_id=?",
            (instance["instance_id"], instance["last_execution_id"],
             instance["created_by_execution_id"], node, instance["job_id"]),
        ).rowcount
        if changed != 1:
            raise RuntimeError("Clipboard job identity was not restored")
    _insert_rows(connection, "job_events", saved.data["events"])
    _insert_rows(connection, "idempotency", saved.data["idempotency"])
    _insert_rows(connection, "default_job_specs", saved.data["default_jobs"])
    if saved.data["sequence"] is not None:
        _insert_rows(connection, "job_sequences", (saved.data["sequence"],))
    _insert_rows(connection, "managed_input_files", saved.data["managed_inputs"])
    _insert_rows(connection, "managed_input_producers", saved.data["managed_producers"])

    generation = _generation_floor(connection, saved.component)
    current_state = read_component_state_record(connection, saved.component)
    saved_state = saved.data["component_state"]
    retained = saved.data["retained_result"]
    pointer = None
    if retained is not None:
        pointer = (current_state.identity.shape_id, generation)
        if connection.execute(
            "INSERT INTO component_successful_results "
            "(component_key,shape_id,alignment_generation,lifecycle,stability,instability_origin) "
            "VALUES(?,?,?,?,?,?)",
            (key, *pointer, retained["lifecycle"], retained["stability"],
             retained["instability_origin"]),
        ).rowcount != 1:
            raise RuntimeError("Clipboard retained result was not restored")
        expected_result = dict(
            component_key=key, shape_id=pointer[0], alignment_generation=pointer[1],
            lifecycle=retained["lifecycle"], stability=retained["stability"],
            instability_origin=retained["instability_origin"],
        )
        actual_result = connection.execute(
            "SELECT * FROM component_successful_results WHERE component_key=? "
            "AND shape_id=? AND alignment_generation=?", (key, *pointer),
        ).fetchone()
        if actual_result is None or dict(actual_result) != expected_result:
            raise RuntimeError("Clipboard retained result changed during restoration")
    if connection.execute(
        "UPDATE component_states SET shape_id=?, retained_result_shape_id=?, "
        "retained_result_alignment_generation=?, lifecycle=?, stability=?, instability_origin=?, "
        "misaligned=?, alignment_generation=? WHERE component_key=?",
        (current_state.identity.shape_id,
         None if pointer is None else pointer[0], None if pointer is None else pointer[1],
         saved_state["lifecycle"], saved_state["stability"], saved_state["instability_origin"],
         saved_state["misaligned"], generation, key),
    ).rowcount != 1:
        raise RuntimeError("Clipboard component state was not restored")
    expected_state = dict(
        component_key=key, shape_id=current_state.identity.shape_id,
        retained_result_shape_id=None if pointer is None else pointer[0],
        retained_result_alignment_generation=None if pointer is None else pointer[1],
        lifecycle=saved_state["lifecycle"], stability=saved_state["stability"],
        instability_origin=saved_state["instability_origin"],
        misaligned=saved_state["misaligned"], alignment_generation=generation,
    )
    actual_state = connection.execute(
        "SELECT * FROM component_states WHERE component_key=?", (key,),
    ).fetchone()
    if actual_state is None or dict(actual_state) != expected_state:
        raise RuntimeError("Clipboard component state changed during restoration")
    cloned_causes = tuple(
        dict(cause, shape_id=current_state.identity.shape_id,
             alignment_generation=generation)
        for cause in saved.data["misalignment_causes"]
    )
    _insert_rows(connection, "component_misalignment_causes", cloned_causes)
    actual_causes = tuple(dict(row) for row in connection.execute(
        "SELECT * FROM component_misalignment_causes WHERE component_key=? "
        "AND alignment_generation=? ORDER BY receiver_node", (key, generation),
    ))
    if actual_causes != cloned_causes:
        raise RuntimeError("Clipboard misalignment history changed during restoration")
    state = read_component_state_record(connection, saved.component)
    if state is None or state.snapshot != {
        "members": saved.component, "shape_json": current_state.shape_json,
        "lifecycle": saved_state["lifecycle"], "stability": saved_state["stability"],
        "instability_origin": saved_state["instability_origin"],
        "misaligned": bool(saved_state["misaligned"]),
        "alignment_generation": generation,
    }:
        raise RuntimeError("Clipboard component observation changed during restoration")
    for table, column, order, expected_rows in (
        ("nodes", "node_name", "rowid", () if saved_node is None else (saved_node,)),
        ("jobs", "node_name", "job_id", saved.data["jobs"]),
        ("job_instances", "node_name", "job_id", saved.data["instances"]),
        ("job_events", "node_name", "event_id", saved.data["events"]),
        ("idempotency", "node_name", "key_hash", saved.data["idempotency"]),
        ("default_job_specs", "node_name", "spec_key", saved.data["default_jobs"]),
        ("job_sequences", "node_name", "rowid", () if saved.data["sequence"] is None
         else (saved.data["sequence"],)),
        ("managed_input_files", "receiver_node", "relative_path", saved.data["managed_inputs"]),
        ("managed_input_producers", "receiver_node", "relative_path, execution_id",
         saved.data["managed_producers"]),
    ):
        _require_rows(connection, table, column, node, order, expected_rows)
    return generation
