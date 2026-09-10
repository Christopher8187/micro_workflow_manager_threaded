"""Validate one complete explicit-interrupt storage snapshot."""

from __future__ import annotations

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key

from .component_definitions import component_snapshot_from_shape
from .interrupt_common import _component_predecessors, read_session_parent_ids
from .session_scope import _reservation_descends_from
from .session_shapes import validate_session_shape_snapshot


def _component(key, label):
    try:
        members = decode_component_key(key)
        if not members or encode_component_key(members) != key:
            raise ValueError
        return members
    except (TypeError, ValueError) as error:
        raise RuntimeError("Damaged interrupt " + label) from error


class InterruptObservationStorageMixin:
    def _read_interrupt_admission(self, connection, session_id):
        row = connection.execute(
            "SELECT * FROM interrupt_admissions WHERE session_id=?", (session_id,),
        ).fetchone()
        if row is None:
            return None
        admission = dict(row)
        session_row = connection.execute(
            "SELECT * FROM execution_sessions WHERE session_id=?", (session_id,),
        ).fetchone()
        if session_row is None:
            raise RuntimeError("Interrupt admission has no execution session")
        if session_row['scope_admitted'] != 1:
            raise RuntimeError("Interrupt admission lost its reservation admission marker")
        session = self._execution_session_from_row(connection, session_row)
        if session["session_kind"] != "interrupt" or session["status"] not in ("running", "terminal"):
            raise RuntimeError("Interrupt admission has an invalid owning session")
        target = _component(admission["target_component_key"], "target component")
        if target not in session["selected_components"]:
            raise RuntimeError("Interrupt target is outside its session scope")
        requested = admission["readiness_override_requested"]
        effective = admission["readiness_overridden"]
        if (type(requested) is not int or requested not in (0, 1)
                or (effective is not None
                    and (type(effective) is not int or effective not in (0, 1)))
                or effective == 1 and requested != 1):
            raise RuntimeError("Interrupt admission has invalid readiness metadata")
        state = admission["state"]
        if state not in ("admitted", "frozen", "settled"):
            raise RuntimeError("Interrupt admission has invalid state")
        frozen = None
        has_frozen_identity = admission["frozen_shape_id"] is not None
        if has_frozen_identity:
            if (type(admission["frozen_shape_id"]) is not int
                    or type(admission["frozen_alignment_generation"]) is not int
                    or admission["frozen_alignment_generation"] < 0
                    or not admission["frozen_at"]
                    or not ((admission["frozen_stability"] == "stable"
                             and admission["frozen_instability_origin"] is None)
                            or (admission["frozen_stability"] == "unstable"
                                and isinstance(admission["frozen_instability_origin"], str)
                                and admission["frozen_instability_origin"].strip()))):
                raise RuntimeError("Interrupt admission has invalid frozen identity")
            frozen = {
                "shape_id": admission["frozen_shape_id"],
                "alignment_generation": admission["frozen_alignment_generation"],
                "lineage": (
                    admission["frozen_stability"],
                    admission["frozen_instability_origin"],
                ),
                "frozen_at": admission["frozen_at"],
            }
        elif state == "frozen":
            raise RuntimeError("Frozen interrupt admission has no target identity")

        parents = read_session_parent_ids(connection, session_id)
        transfers = []
        for transfer in connection.execute(
            "SELECT * FROM interrupt_scope_transfers WHERE child_session_id=? ORDER BY component_key",
            (session_id,),
        ):
            item = dict(transfer)
            item["component"] = _component(item.pop("component_key"), "transfer component")
            if item["component"] not in session["selected_components"]:
                raise RuntimeError("Interrupt transfer is outside selected scope")
            if item["transfer_kind"] == "direct-parent" and item["source_session_id"] not in parents:
                raise RuntimeError("Direct-parent transfer has no parent relationship")
            transfers.append(item)

        pauses = []
        pause_parents = set()
        for request in connection.execute(
            "SELECT * FROM interrupt_pause_requests WHERE child_session_id=? "
            "ORDER BY owner_session_id, component_key", (session_id,),
        ):
            item = dict(request)
            key = item.pop("component_key")
            item["component"] = _component(key, "pause component")
            pause_parents.add(item["owner_session_id"])
            executions = []
            for paused in connection.execute(
                "SELECT * FROM interrupt_paused_executions "
                "WHERE child_session_id=? AND owner_session_id=? AND component_key=? "
                "ORDER BY execution_id", (session_id, item["owner_session_id"], key),
            ):
                execution = dict(paused)
                owner = self._read_execution_owner(connection, execution["execution_id"])
                if (owner is None or owner["session_id"] != execution["owner_session_id"]
                        or owner["component"] != item["component"]
                        or owner["node_name"] != execution["node_name"]
                        or owner["job_id"] != execution["job_id"]
                        or owner["generation"] != execution["generation"]):
                    raise RuntimeError("Interrupt pause lost its exact execution owner")
                execution.pop("component_key")
                executions.append(execution)
            if not executions:
                raise RuntimeError("Interrupt pause request has no captured execution")
            acknowledged = [item["acknowledged_at"] is not None for item in executions]
            if (item["state"] == "requested" and all(acknowledged)) or (
                item["state"] == "acknowledged" and not all(acknowledged)
            ):
                raise RuntimeError("Interrupt pause request disagrees with its acknowledgements")
            item["executions"] = executions
            pauses.append(item)
        if set(parents) != pause_parents:
            raise RuntimeError("Interrupt parents disagree with pause ownership")
        if state in ("admitted", "frozen") and any(
            item["state"] == "released" for item in pauses
        ):
            raise RuntimeError("Active interrupt admission has a released predecessor pause")
        if state == "settled" and any(item["state"] != "released" for item in pauses):
            raise RuntimeError("Settled interrupt admission retains a predecessor pause")

        frozen_parent_states = {}
        for parent in connection.execute(
            "SELECT frozen.*, shape.shape_json, origin.session_kind AS origin_kind, "
            "origin.scope_admitted AS origin_scope_admitted "
            "FROM interrupt_frozen_parent_states AS frozen "
            "JOIN graph_shapes AS shape USING(shape_id) "
            "LEFT JOIN execution_sessions AS origin "
            "ON origin.session_id=frozen.instability_origin WHERE child_session_id=? "
            "ORDER BY component_key", (session_id,),
        ):
            component = _component(parent["component_key"], "frozen parent component")
            if component in frozen_parent_states:
                raise RuntimeError("Interrupt admission has duplicate frozen parent state")
            self._validate_component_state_row(parent)
            try:
                producing = component_snapshot_from_shape(parent["shape_json"])
            except ValueError as error:
                raise RuntimeError("Interrupt frozen parent has a damaged shape") from error
            if component not in producing.components:
                raise RuntimeError("Interrupt frozen parent differs from its historical shape")
            snapshot = {
                "members": component, "shape_json": parent["shape_json"],
                "lifecycle": parent["lifecycle"], "stability": parent["stability"],
                "instability_origin": parent["instability_origin"],
                "misaligned": bool(parent["misaligned"]),
                "alignment_generation": parent["alignment_generation"],
            }
            frozen_parent_states[component] = snapshot
        session_shape = validate_session_shape_snapshot(connection, session)
        direct = set(_component_predecessors(session_shape, target))
        if state == "admitted" and frozen_parent_states:
            raise RuntimeError("Unfrozen interrupt admission retains parent observations")
        if has_frozen_identity and set(frozen_parent_states) != direct:
            raise RuntimeError("Interrupt admission lost its exact frozen parent observations")

        hold_rows = connection.execute(
            "SELECT hold_count FROM component_holds WHERE session_id=? AND component_key=?",
            (session_id, admission["target_component_key"]),
        ).fetchall()
        if len(hold_rows) > 1:
            raise RuntimeError("Interrupt target has ambiguous holds")
        hold = 0 if not hold_rows else hold_rows[0]["hold_count"]
        if type(hold) is not int or hold < 0:
            raise RuntimeError("Interrupt target has invalid hold count")
        if (state == "frozen") != (hold == 1) or hold not in (0, 1):
            raise RuntimeError("Interrupt hold disagrees with admission state")

        selected_keys = {encode_component_key(component) for component in session["selected_components"]}
        owned = {row["component_key"] for row in connection.execute(
            "SELECT component_key FROM component_reservations WHERE session_id=?", (session_id,),
        )}
        reservation_owners = {
            row["component_key"]: row["session_id"]
            for row in connection.execute(
                "SELECT component_key, session_id FROM component_reservations "
                "WHERE component_key IN (SELECT component_key FROM session_components WHERE session_id=?)",
                (session_id,),
            )
        }
        transfer_by_key = {encode_component_key(item["component"]): item for item in transfers}
        if not owned <= selected_keys:
            raise RuntimeError("Interrupt owns a reservation outside its selected scope")
        if session["status"] == "running":
            for key in selected_keys - owned:
                owner = reservation_owners.get(key)
                if owner is None or not _reservation_descends_from(
                    connection, session_id, key, owner,
                ):
                    raise RuntimeError("Running interrupt session lost part of its selected reservation")
        if session["status"] == "terminal" and owned:
            raise RuntimeError("Terminal interrupt session retains selected reservations")
        for key, transfer in transfer_by_key.items():
            if transfer["state"] == "active" and key not in owned:
                owner = reservation_owners.get(key)
                if owner is None or not _reservation_descends_from(
                    connection, session_id, key, owner,
                ):
                    raise RuntimeError("Active interrupt transfer lost its child reservation")
            if transfer["state"] != "active" and key in owned:
                raise RuntimeError("Finished interrupt transfer still owns its reservation")
        return {
            "session": session,
            "target_component": target,
            "readiness_override_requested": bool(requested),
            "readiness_overridden": None if effective is None else bool(effective),
            "state": state,
            "frozen_identity": frozen,
            "parents": parents,
            "transfers": transfers,
            "pauses": pauses,
            "frozen_parent_states": frozen_parent_states,
            "target_hold": hold,
        }

    def get_interrupt_execution_admission(self, session_id):
        self._require_execution_session_storage()
        self._session_text(session_id, "session_id")
        connection = self.db_connection()
        connection.execute("SAVEPOINT mwf_interrupt_observation")
        try:
            observed = self._read_interrupt_admission(connection, session_id)
            if observed is None:
                session = connection.execute(
                    "SELECT session_kind FROM execution_sessions WHERE session_id=?",
                    (session_id,),
                ).fetchone()
                if session is not None and session["session_kind"] == "interrupt":
                    raise RuntimeError("Interrupt session lost its admission")
            return observed
        finally:
            connection.execute("RELEASE SAVEPOINT mwf_interrupt_observation")

    def _read_interrupt_execution_admission(
        self, session_id, *, command, start_component, selected_components,
        selected_jobs, direct_predecessors, readiness_overridden, started_at,
        hostname, pid, process_identity, details, expected_shape,
        expected_job_instances=None,
    ):
        connection = self._new_db_connection()
        try:
            connection.execute("BEGIN")
            observed = self._read_interrupt_admission(connection, session_id)
            if observed is None:
                queries = (
                    "SELECT COUNT(*) FROM execution_sessions WHERE session_id=?",
                    "SELECT COUNT(*) FROM session_components WHERE session_id=?",
                    "SELECT COUNT(*) FROM session_jobs WHERE session_id=?",
                    "SELECT COUNT(*) FROM component_reservations WHERE session_id=?",
                    "SELECT COUNT(*) FROM component_holds WHERE session_id=?",
                    "SELECT COUNT(*) FROM execution_session_parents WHERE child_session_id=?",
                    "SELECT COUNT(*) FROM interrupt_scope_transfers WHERE child_session_id=?",
                    "SELECT COUNT(*) FROM interrupt_pause_requests WHERE child_session_id=?",
                    "SELECT COUNT(*) FROM interrupt_paused_executions WHERE child_session_id=?",
                    "SELECT COUNT(*) FROM interrupt_frozen_parent_states WHERE child_session_id=?",
                    "SELECT COUNT(*) FROM session_fence_authorizations WHERE session_id=?",
                )
                orphaned = sum(
                    connection.execute(statement, (session_id,)).fetchone()[0]
                    for statement in queries
                )
                if orphaned:
                    raise RuntimeError("Interrupt admission rollback left orphan rows")
                return None
            session = observed["session"]
            if validate_session_shape_snapshot(connection, session) != expected_shape:
                raise RuntimeError("Committed interrupt admission has a different graph shape")
            components = [self._session_component(item) for item in selected_components]
            jobs = [(self.validate_node_name(node), self.validate_job_id(job_id))
                    for node, job_id in selected_jobs]
            roots = tuple(self._read_session_job_roots(
                connection, session_id, components=components,
            ))
            if (session["command"] != command
                    or session["start_component"] != self._session_component(start_component)
                    or session["selected_components"] != components
                    or (expected_job_instances is None
                        and [(node, job_id) for node, job_id, _ in roots] != jobs)
                    or session["started_at"] != started_at or session["hostname"] != hostname
                    or session["pid"] != pid or session["process_identity"] != process_identity
                    or (expected_job_instances is None and session["details"] != details)
                    or (expected_job_instances is not None
                        and any(session["details"].get(key) != value for key, value in details.items()))
                    or observed["readiness_override_requested"] is not readiness_overridden
                    or tuple(sorted(self._session_component(item) for item in direct_predecessors))
                    != _component_predecessors(expected_shape, self._session_component(start_component))
                    or (expected_job_instances is not None and roots != tuple(expected_job_instances))):
                raise RuntimeError("Committed interrupt admission differs from its request")
            return observed
        finally:
            connection.close()
