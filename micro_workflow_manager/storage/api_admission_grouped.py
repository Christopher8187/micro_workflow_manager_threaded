"""Ordered grouped writer operations for native API execution permits."""

from __future__ import annotations

from dataclasses import dataclass

from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.models import now


@dataclass(frozen=True, slots=True)
class ApiExecutionPermitMutation:
    action: str
    identity: object


def _rows_by_execution(connection, execution_ids):
    result = {}
    unique_ids = list(dict.fromkeys(execution_ids))
    for offset in range(0, len(unique_ids), 400):
        chunk = unique_ids[offset:offset + 400]
        placeholders = ",".join("?" for _ in chunk)
        rows = connection.execute(
            "SELECT execution_id, session_id, node_name, job_id, generation, acquired_at "
            f"FROM api_execution_permits WHERE execution_id IN ({placeholders})",
            chunk,
        ).fetchall()
        for row in rows:
            execution_id = row["execution_id"]
            if execution_id in result:
                raise RuntimeError("Duplicate API execution permit identity")
            result[execution_id] = row
    return result


def _savepoint(connection, name):
    connection.execute(f"SAVEPOINT {name}")


def _rollback_savepoint(connection, name):
    connection.execute(f"ROLLBACK TO {name}")
    connection.execute(f"RELEASE {name}")


def _release_savepoint(connection, name):
    connection.execute(f"RELEASE {name}")


def _apply_individually(connection, identities, operation, prefix):
    outcomes = []
    for position, identity in enumerate(identities):
        savepoint = f"mwf_api_{prefix}_{position}"
        _savepoint(connection, savepoint)
        try:
            result = operation(connection, identity)
        except BaseException as error:
            _rollback_savepoint(connection, savepoint)
            outcomes.append((False, error))
        else:
            _release_savepoint(connection, savepoint)
            outcomes.append((True, result))
    return outcomes


def _current_owner(connection, identity, *, read_job_owner_observation):
    observed = read_job_owner_observation(
        connection, identity.node_name, identity.job_id,
    )
    owner = None if observed is None else observed["owner"]
    if owner is None or any((
        owner["execution_id"] != identity.execution_id,
        owner["session_id"] != identity.session_id,
        owner["node_name"] != identity.node_name,
        owner["job_id"] != identity.job_id,
        owner["generation"] != identity.generation,
    )):
        raise RuntimeError("API execution permit does not match its immutable owner")
    if (observed["status"] != "running"
            or observed["generation"] != identity.generation
            or observed["active_execution_id"] != identity.execution_id
            or observed["session"] is None
            or observed["session"]["status"] != "running"):
        raise RuntimeError("API permit acquisition requires the exact running job owner")
    return owner


def _require_scope(connection, identity, owner):
    key = encode_component_key(owner["component"])
    row = connection.execute(
        "SELECT reservation.session_id AS reservation_owner, "
        "selected.session_id AS selected_owner "
        "FROM execution_sessions AS session "
        "LEFT JOIN component_reservations AS reservation ON reservation.component_key=? "
        "LEFT JOIN session_components AS selected "
        "ON selected.session_id=session.session_id AND selected.component_key=? "
        "WHERE session.session_id=? AND session.status='running'",
        (key, key, identity.session_id),
    ).fetchone()
    if (row is None or row["reservation_owner"] != identity.session_id
            or row["selected_owner"] != identity.session_id):
        raise RuntimeError("API permit acquisition requires the exact session reservation")


def _apply_acquisitions(
    connection,
    identities,
    *,
    validate_identity,
    permit_from_row,
    permit_row,
    read_job_owner_observation,
    acquire_one,
):
    if not identities:
        return []
    if len(identities) == 1:
        return _apply_individually(connection, identities, acquire_one, "acquire")

    outcomes = [None] * len(identities)
    owners = [None] * len(identities)
    for position, identity in enumerate(identities):
        try:
            validate_identity(identity)
            owners[position] = _current_owner(
                connection,
                identity,
                read_job_owner_observation=read_job_owner_observation,
            )
        except BaseException as error:
            outcomes[position] = (False, error)

    scope_results = {}
    for position, (identity, owner) in enumerate(zip(identities, owners)):
        if owner is None:
            continue
        scope_key = (identity.session_id, encode_component_key(owner["component"]))
        if scope_key not in scope_results:
            try:
                _require_scope(connection, identity, owner)
            except BaseException as error:
                scope_results[scope_key] = error
            else:
                scope_results[scope_key] = True
        if scope_results[scope_key] is not True:
            outcomes[position] = (False, scope_results[scope_key])

    valid_positions = [
        position for position, outcome in enumerate(outcomes) if outcome is None
    ]
    try:
        existing = _rows_by_execution(
            connection,
            [identities[position].execution_id for position in valid_positions],
        )
    except BaseException as error:
        for position in valid_positions:
            outcomes[position] = (False, error)
        return outcomes

    pending_positions = []
    for position in valid_positions:
        identity = identities[position]
        recorded = existing.get(identity.execution_id)
        if recorded is not None:
            try:
                if permit_from_row(recorded) != identity:
                    raise RuntimeError("API execution permit identity changed")
            except BaseException as error:
                outcomes[position] = (False, error)
            else:
                outcomes[position] = (True, True)
            continue
        pending_positions.append(position)

    if not pending_positions:
        return outcomes
    try:
        limit_row = connection.execute(
            "SELECT value FROM api_thread_limit WHERE singleton=1",
        ).fetchone()
        if limit_row is None:
            available = len(pending_positions)
        else:
            limit = limit_row["value"]
            if type(limit) is not int or limit < 1:
                raise RuntimeError("Damaged aggregate API admission limit")
            active = connection.execute(
                "SELECT COUNT(*) FROM api_execution_permits",
            ).fetchone()[0]
            if type(active) is not int or active < 0:
                raise RuntimeError("Damaged API execution permit count")
            available = max(0, limit - active)
    except BaseException as error:
        for position in pending_positions:
            outcomes[position] = (False, error)
        return outcomes

    for position in pending_positions:
        identity = identities[position]
        recorded = existing.get(identity.execution_id)
        if recorded is not None:
            try:
                if permit_from_row(recorded) != identity:
                    raise RuntimeError("API execution permit identity changed")
            except BaseException as error:
                outcomes[position] = (False, error)
            else:
                outcomes[position] = (True, True)
            continue
        if available == 0:
            outcomes[position] = (True, False)
            continue

        acquired_at = now()
        savepoint = f"mwf_api_acquire_{position}"
        _savepoint(connection, savepoint)
        try:
            inserted = connection.execute(
                "INSERT INTO api_execution_permits("
                "execution_id, session_id, node_name, job_id, generation, acquired_at) "
                "VALUES(?,?,?,?,?,?)",
                (
                    identity.execution_id, identity.session_id, identity.node_name,
                    identity.job_id, identity.generation, acquired_at,
                ),
            ).rowcount
            if inserted != 1:
                raise RuntimeError("API execution permit was not recorded")
            recorded = permit_row(connection, identity.execution_id)
            if recorded is None or permit_from_row(recorded) != identity:
                raise RuntimeError("API execution permit readback failed")
            if recorded["acquired_at"] != acquired_at:
                raise RuntimeError("API execution permit acquisition time changed")
        except BaseException as error:
            _rollback_savepoint(connection, savepoint)
            outcomes[position] = (False, error)
        else:
            _release_savepoint(connection, savepoint)
            existing[identity.execution_id] = recorded
            available -= 1
            outcomes[position] = (True, True)
    return outcomes


def _apply_releases(
    connection,
    identities,
    *,
    validate_identity,
    permit_from_row,
    permit_row,
    require_owner,
    release_one,
):
    if not identities:
        return []
    if len(identities) == 1:
        return _apply_individually(connection, identities, release_one, "release")

    outcomes = [None] * len(identities)
    valid_positions = []
    for position, identity in enumerate(identities):
        try:
            validate_identity(identity)
            require_owner(connection, identity, require_current=False)
        except BaseException as error:
            outcomes[position] = (False, error)
        else:
            valid_positions.append(position)

    try:
        existing = _rows_by_execution(
            connection,
            [identities[position].execution_id for position in valid_positions],
        )
    except BaseException as error:
        for position in valid_positions:
            outcomes[position] = (False, error)
        return outcomes

    for position in valid_positions:
        identity = identities[position]
        recorded = existing.get(identity.execution_id)
        try:
            if recorded is None or permit_from_row(recorded) != identity:
                raise RuntimeError("Exact API execution permit is missing")
        except BaseException as error:
            outcomes[position] = (False, error)
            continue

        savepoint = f"mwf_api_release_{position}"
        _savepoint(connection, savepoint)
        try:
            removed = connection.execute(
                "DELETE FROM api_execution_permits WHERE execution_id=? AND session_id=? "
                "AND node_name=? AND job_id=? AND generation=? AND acquired_at=?",
                (
                    identity.execution_id, identity.session_id, identity.node_name,
                    identity.job_id, identity.generation, recorded["acquired_at"],
                ),
            ).rowcount
            if removed != 1 or permit_row(connection, identity.execution_id) is not None:
                raise RuntimeError("API execution permit changed before release")
        except BaseException as error:
            _rollback_savepoint(connection, savepoint)
            outcomes[position] = (False, error)
        else:
            _release_savepoint(connection, savepoint)
            del existing[identity.execution_id]
            outcomes[position] = (True, True)
    return outcomes


def apply_api_execution_permit_mutations(
    connection,
    mutations,
    *,
    validate_identity,
    permit_from_row,
    permit_row,
    read_job_owner_observation,
    require_owner,
    acquire_one,
    release_one,
):
    """Apply consecutive action runs without reordering mixed requests."""
    outcomes = []
    position = 0
    while position < len(mutations):
        action = mutations[position].action
        end = position + 1
        while end < len(mutations) and mutations[end].action == action:
            end += 1
        identities = [mutation.identity for mutation in mutations[position:end]]
        if action == "acquire":
            run = _apply_acquisitions(
                connection,
                identities,
                validate_identity=validate_identity,
                permit_from_row=permit_from_row,
                permit_row=permit_row,
                read_job_owner_observation=read_job_owner_observation,
                acquire_one=acquire_one,
            )
        elif action == "release":
            run = _apply_releases(
                connection,
                identities,
                validate_identity=validate_identity,
                permit_from_row=permit_from_row,
                permit_row=permit_row,
                require_owner=require_owner,
                release_one=release_one,
            )
        else:
            error = RuntimeError("Unknown API execution permit mutation")
            run = [(False, error) for _identity in identities]
        outcomes.extend(run)
        position = end
    return outcomes
