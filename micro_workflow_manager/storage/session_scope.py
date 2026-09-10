"""Validate durable selected reservation admission and nested transfers."""


def _reservation_descends_from(connection, source_session_id, component_key, owner_session_id):
    current = source_session_id
    seen = set()
    while current != owner_session_id:
        if current in seen:
            raise RuntimeError("Interrupt reservation transfer chain contains a cycle")
        seen.add(current)
        children = connection.execute(
            "SELECT transfer.child_session_id FROM interrupt_scope_transfers AS transfer "
            "JOIN execution_sessions AS child ON child.session_id=transfer.child_session_id "
            "JOIN session_components AS selected ON selected.session_id=transfer.child_session_id "
            "AND selected.component_key=transfer.component_key "
            "JOIN interrupt_admissions AS admission ON admission.session_id=transfer.child_session_id "
            "WHERE transfer.source_session_id=? AND transfer.component_key=? "
            "AND transfer.state='active' AND child.status='running' "
            "AND child.scope_admitted=1 "
            "ORDER BY transfer.child_session_id",
            (current, component_key),
        ).fetchall()
        if len(children) != 1:
            return False
        current = children[0]["child_session_id"]
    return True


def require_admitted_reservation_scope(connection, session_id):
    session = connection.execute(
        "SELECT status, scope_admitted FROM execution_sessions WHERE session_id=?",
        (session_id,),
    ).fetchone()
    if (session is None or session["status"] != "running"
            or type(session["scope_admitted"]) is not int
            or session["scope_admitted"] not in (0, 1)):
        raise RuntimeError("Execution session has invalid reservation admission state")
    selected = tuple(row[0] for row in connection.execute(
        "SELECT component_key FROM session_components WHERE session_id=? ORDER BY position",
        (session_id,),
    ))
    if not selected:
        raise RuntimeError("Execution session has no selected reservation scope")
    owned = tuple(row[0] for row in connection.execute(
        "SELECT component_key FROM component_reservations WHERE session_id=? ORDER BY component_key",
        (session_id,),
    ))
    transferred = tuple(row[0] for row in connection.execute(
        "SELECT component_key FROM interrupt_scope_transfers "
        "WHERE source_session_id=? AND state='active' ORDER BY component_key, child_session_id",
        (session_id,),
    ))
    if session["scope_admitted"] == 0:
        if owned or transferred:
            raise RuntimeError("Unadmitted execution session unexpectedly owns component scope")
        return False
    if (len(set((*owned, *transferred))) != len(owned) + len(transferred)
            or set((*owned, *transferred)) != set(selected)):
        raise RuntimeError(
            "Execution session reservation cleanup differs from its admitted scope: "
            f"expected {len(selected)} component reservations"
        )
    owners = {
        row["component_key"]: row["session_id"]
        for row in connection.execute(
            "SELECT component_key, session_id FROM component_reservations "
            "WHERE component_key IN (SELECT component_key FROM session_components WHERE session_id=?)",
            (session_id,),
        )
    }
    for component_key in selected:
        owner = owners.get(component_key)
        if owner is None or not _reservation_descends_from(
            connection, session_id, component_key, owner,
        ):
            raise RuntimeError(
                "Execution session reservation cleanup lost its admitted transfer chain: "
                + component_key
            )
    return True
