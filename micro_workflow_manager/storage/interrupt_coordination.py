"""Pause, hold, and fence coordination for explicit interrupts."""

from __future__ import annotations

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key
from micro_workflow_manager.component_readiness import (
    calculate_component_readiness, calculate_interrupt_sampled_resume_readiness,
)
from micro_workflow_manager.errors import JobRestartedError

from .component_membership import read_active_component_for_node
from .component_result_identity import observe_current_success, read_component_state_record
from .interrupt_common import (
    InterruptAdmissionPaused,
    _component_ancestors,
    _component_predecessors,
    read_interrupt_claim_blockers,
)


class InterruptCoordinationStorageMixin:
    def _has_interrupt_pause_candidate(self, execution_id):
        """Return whether strict pause validation could currently find a row.

        This is an advisory negative-path probe.  A positive result must flow
        through ``read_active_interrupt_pauses`` so its full current-owner and
        session validation remains authoritative.  A negative result is not
        cached: a pause admitted after this statement is observed at the next
        cooperative boundary, just as one admitted after an ordinary empty
        strict read is today.
        """
        self._session_text(execution_id, "execution_id")
        return self.db_connection().execute(
            "SELECT 1 FROM interrupt_paused_executions "
            "WHERE execution_id=? LIMIT 1",
            (execution_id,),
        ).fetchone() is not None

    def read_active_interrupt_pauses(
        self, session_id, node_name, job_id, generation, execution_id,
    ):
        self._session_text(session_id, "session_id")
        node_name = self.validate_node_name(node_name)
        job_id = self.validate_job_id(job_id)
        connection = self.db_connection()
        observed = self._read_job_owner_observation(connection, node_name, job_id)
        owner = None if observed is None else observed["owner"]
        if (owner is None or observed["state"] != "active"
                or observed["generation"] != generation
                or owner["execution_id"] != execution_id or owner["session_id"] != session_id):
            raise JobRestartedError(f"Job {node_name}/{job_id} changed while checking interrupt pause")
        return tuple(row[0] for row in connection.execute(
            "SELECT paused.child_session_id FROM interrupt_paused_executions AS paused "
            "JOIN interrupt_pause_requests AS request USING(child_session_id, owner_session_id, component_key) "
            "JOIN interrupt_admissions AS admission ON admission.session_id=paused.child_session_id "
            "WHERE paused.execution_id=? AND request.state IN ('requested','acknowledged') "
            "AND admission.state IN ('admitted','frozen') ORDER BY paused.child_session_id",
            (execution_id,),
        ))

    def acknowledge_interrupt_pauses(
        self, session_id, node_name, job_id, generation, execution_id,
    ):
        active = self.read_active_interrupt_pauses(
            session_id, node_name, job_id, generation, execution_id,
        )
        if not active:
            return ()

        def acknowledge(connection):
            observed = self._read_job_owner_observation(connection, node_name, job_id)
            owner = None if observed is None else observed["owner"]
            if (owner is None or observed["state"] != "active"
                    or observed["generation"] != generation
                    or owner["execution_id"] != execution_id or owner["session_id"] != session_id):
                raise JobRestartedError(
                    f"Job {node_name}/{job_id} changed while acknowledging interrupt pause"
                )
            connection.execute(
                "UPDATE interrupt_paused_executions SET acknowledged_at=CURRENT_TIMESTAMP "
                "WHERE execution_id=? AND child_session_id IN ("
                "SELECT session_id FROM interrupt_admissions WHERE state IN ('admitted','frozen')) "
                "AND acknowledged_at IS NULL", (execution_id,),
            )
            acknowledgements = connection.execute(
                "SELECT paused.child_session_id, paused.owner_session_id, "
                "paused.component_key, paused.acknowledged_at "
                "FROM interrupt_paused_executions AS paused "
                "JOIN interrupt_admissions AS admission ON admission.session_id=paused.child_session_id "
                "WHERE paused.execution_id=? AND admission.state IN ('admitted','frozen') "
                "ORDER BY paused.child_session_id",
                (execution_id,),
            ).fetchall()
            expected = {row["child_session_id"] for row in acknowledgements}
            if any(row["acknowledged_at"] is None for row in acknowledgements):
                raise RuntimeError("Interrupt pause acknowledgement was not recorded")
            requests = sorted({
                (row["child_session_id"], row["owner_session_id"], row["component_key"])
                for row in acknowledgements
            })
            for child, owner_session, component_key in requests:
                pending = connection.execute(
                    "SELECT 1 FROM interrupt_paused_executions "
                    "WHERE child_session_id=? AND owner_session_id=? AND component_key=? "
                    "AND acknowledged_at IS NULL LIMIT 1",
                    (child, owner_session, component_key),
                ).fetchone()
                if pending is None:
                    changed = connection.execute(
                        "UPDATE interrupt_pause_requests SET state='acknowledged' "
                        "WHERE child_session_id=? AND owner_session_id=? AND component_key=? "
                        "AND state='requested'", (child, owner_session, component_key),
                    ).rowcount
                    state = connection.execute(
                        "SELECT state FROM interrupt_pause_requests WHERE child_session_id=? "
                        "AND owner_session_id=? AND component_key=?",
                        (child, owner_session, component_key),
                    ).fetchone()
                    if (state is None or state["state"] != "acknowledged"
                            or changed not in (0, 1)):
                        raise RuntimeError("Interrupt pause request was not acknowledged")
            return tuple(sorted(expected))

        return self.submit_db_mutation(acknowledge, wait=True, priority=0)

    def interrupt_component_admission_blockers(self, session_id, component):
        self._session_text(session_id, "session_id")
        members = self._session_component(component)
        return read_interrupt_claim_blockers(
            self.db_connection(), session_id, members,
        )

    def freeze_interrupt_target(
        self, session_id, *, expected_shape, expected_parent_states,
        successful_lineage, readiness_overridden, frozen_at,
        _reserved_sample_builder=None, sample_id=None, _wait=True,
    ):
        self._session_text(session_id, "session_id")
        self._session_text(expected_shape, "expected_shape")
        self._session_time(frozen_at, "frozen_at")
        if type(readiness_overridden) is not bool:
            raise ValueError("Effective interrupt readiness override must be Boolean")
        if not isinstance(successful_lineage, tuple) or len(successful_lineage) != 2:
            raise ValueError("Interrupt target freeze requires its successful lineage")
        stability, origin = successful_lineage
        if not ((stability == "stable" and origin is None)
                or (stability == "unstable" and isinstance(origin, str) and origin.strip())):
            raise ValueError("Interrupt target freeze received invalid lineage")
        if (_reserved_sample_builder is None) != (sample_id is None):
            raise ValueError("Frozen interrupt sampling requires its reader and sample identity")
        parents = {self._session_component(component): dict(state)
                   for component, state in expected_parent_states.items()}

        def freeze(connection):
            admission = connection.execute(
                "SELECT * FROM interrupt_admissions WHERE session_id=?", (session_id,),
            ).fetchone()
            if admission is None or admission["state"] != "admitted":
                raise RuntimeError("Interrupt target is not awaiting freeze")
            if readiness_overridden and not admission["readiness_override_requested"]:
                raise RuntimeError("Interrupt freeze cannot add an unrequested readiness override")
            session = connection.execute(
                "SELECT session.status, session.command, shape.shape_json "
                "FROM execution_sessions AS session "
                "JOIN graph_shapes AS shape ON shape.shape_id=session.admitted_shape_id "
                "WHERE session.session_id=?", (session_id,),
            ).fetchone()
            if (session is None or session["status"] != "running"
                    or session["shape_json"] != expected_shape):
                raise RuntimeError("Interrupt target freeze requires its running session")
            for row in connection.execute(
                "SELECT * FROM interrupt_paused_executions WHERE child_session_id=?", (session_id,),
            ):
                observed = self._read_job_owner_observation(connection, row["node_name"], row["job_id"])
                owner = None if observed is None else observed["owner"]
                same_execution = (
                    owner is not None
                    and owner["execution_id"] == row["execution_id"]
                    and owner["session_id"] == row["owner_session_id"]
                    and owner["node_name"] == row["node_name"]
                    and owner["job_id"] == row["job_id"]
                    and observed["generation"] == row["generation"]
                )
                still_active = same_execution and observed["state"] == "active"
                if still_active and row["acknowledged_at"] is None:
                    return None
                if not same_execution or observed["state"] not in ("active", "last"):
                    raise RuntimeError("Interrupt predecessor changed to an unrecorded active execution")
                if row["acknowledged_at"] is None and connection.execute(
                    "UPDATE interrupt_paused_executions SET acknowledged_at=? "
                    "WHERE child_session_id=? AND execution_id=? AND acknowledged_at IS NULL",
                    (frozen_at, session_id, row["execution_id"]),
                ).rowcount != 1:
                    raise RuntimeError("Terminal interrupt predecessor acknowledgement was not recorded")
            requests = connection.execute(
                "SELECT owner_session_id, component_key FROM interrupt_pause_requests "
                "WHERE child_session_id=? AND state='requested'", (session_id,),
            ).fetchall()
            for request in requests:
                pending = connection.execute(
                    "SELECT 1 FROM interrupt_paused_executions WHERE child_session_id=? "
                    "AND owner_session_id=? AND component_key=? AND acknowledged_at IS NULL LIMIT 1",
                    (session_id, request["owner_session_id"], request["component_key"]),
                ).fetchone()
                if pending is None and connection.execute(
                    "UPDATE interrupt_pause_requests SET state='acknowledged' "
                    "WHERE child_session_id=? AND owner_session_id=? AND component_key=? "
                    "AND state='requested'",
                    (session_id, request["owner_session_id"], request["component_key"]),
                ).rowcount != 1:
                    raise RuntimeError("Terminal interrupt predecessor request was not acknowledged")
            component = decode_component_key(admission["target_component_key"])
            if set(parents) != set(_component_predecessors(expected_shape, component)):
                raise RuntimeError("Interrupt freeze requires every direct predecessor observation")
            for parent, expected in parents.items():
                if self._read_component_state(connection, parent) != expected:
                    return None
            state = connection.execute(
                "SELECT shape_id, alignment_generation FROM component_states WHERE component_key=?",
                (admission["target_component_key"],),
            ).fetchone()
            if state is None:
                raise RuntimeError("Interrupt target state disappeared before freeze")
            inputs = tuple((parent['lifecycle'], parent['stability'], parent['instability_origin'])
                           for parent in parents.values())
            expected_readiness = calculate_component_readiness(inputs, interrupt_start_origin=session_id)
            if session["command"] in ("resume", "resumefrom", "resumebetween"):
                retained_state = read_component_state_record(connection, component)
                retained = (
                    None if retained_state is None
                    else observe_current_success(connection, retained_state)
                )
                if (retained_state is None or retained_state.misaligned):
                    raise RuntimeError('Interrupt resume target is missing or misaligned')
                if (retained_state is not None
                    and retained_state.lifecycle in ("sampled", "failed")
                    and retained is not None
                    and retained.lifecycle == "sampled"):
                    expected_readiness = calculate_interrupt_sampled_resume_readiness(
                        retained.lineage, inputs, interrupt_start_origin=session_id,
                    )
            if expected_readiness != (*successful_lineage, readiness_overridden):
                raise RuntimeError('Interrupt freeze has incompatible parent lineage or readiness')
            if connection.execute(
                "SELECT 1 FROM component_holds WHERE component_key=? LIMIT 1",
                (admission["target_component_key"],),
            ).fetchone() is not None:
                raise RuntimeError("Interrupt target already has a component hold")
            if connection.execute(
                "INSERT INTO component_holds(session_id, component_key, hold_count) VALUES(?, ?, 1)",
                (session_id, admission["target_component_key"]),
            ).rowcount != 1:
                raise RuntimeError("Interrupt target hold was not recorded")
            frozen_parent_rows = []
            for parent, expected in sorted(parents.items()):
                parent_key = encode_component_key(parent)
                shape = connection.execute(
                    "SELECT state.shape_id FROM component_states AS state "
                    "JOIN graph_shapes AS shape ON shape.shape_id=state.shape_id "
                    "WHERE state.component_key=? AND shape.shape_json=?",
                    (parent_key, expected["shape_json"]),
                ).fetchone()
                if shape is None:
                    raise RuntimeError("Interrupt parent lost its producing shape before freeze")
                frozen_parent_rows.append((
                    session_id, parent_key, shape["shape_id"], expected["lifecycle"],
                    expected["stability"], expected["instability_origin"],
                    int(expected["misaligned"]), expected["alignment_generation"],
                ))
            if frozen_parent_rows and connection.executemany(
                "INSERT INTO interrupt_frozen_parent_states("
                "child_session_id, component_key, shape_id, lifecycle, stability, "
                "instability_origin, misaligned, alignment_generation) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?)", frozen_parent_rows,
            ).rowcount != len(frozen_parent_rows):
                raise RuntimeError("Interrupt frozen parent states were not recorded completely")
            if connection.execute(
                "UPDATE interrupt_admissions SET state='frozen', frozen_shape_id=?, "
                "frozen_alignment_generation=?, frozen_stability=?, "
                "frozen_instability_origin=?, readiness_overridden=?, frozen_at=? "
                "WHERE session_id=? AND state='admitted'",
                (state["shape_id"], state["alignment_generation"], stability, origin,
                 int(readiness_overridden), frozen_at, session_id),
            ).rowcount != 1:
                raise RuntimeError("Interrupt target admission changed before freeze")
            if _reserved_sample_builder is not None:
                self._finalize_interrupt_sample(
                    connection, session_id, component=component,
                    expected_shape=expected_shape, sample_id=sample_id,
                    reserved_sample_builder=_reserved_sample_builder,
                )
            return self._read_interrupt_admission(connection, session_id)

        return self.submit_db_mutation(freeze, wait=_wait, priority=0)

    @staticmethod
    def _interrupt_claim_blockers(connection, session_id, component):
        return read_interrupt_claim_blockers(connection, session_id, component)

    @staticmethod
    def _require_interrupt_component_admission(connection, session_id, component):
        blockers = read_interrupt_claim_blockers(connection, session_id, component)
        if blockers:
            raise InterruptAdmissionPaused(blockers)

    def _refuse_interrupt_held_publication(self, connection, receiver, producer_execution_id=None):
        component = read_active_component_for_node(connection, receiver)
        if component is None:
            return
        key = encode_component_key(component)
        rows = connection.execute(
            "SELECT holds.session_id FROM component_holds AS holds "
            "JOIN interrupt_admissions AS admission ON admission.session_id=holds.session_id "
            "WHERE holds.component_key=? AND holds.hold_count>0 "
            "AND admission.target_component_key=? AND admission.state='frozen'",
            (key, key),
        ).fetchall()
        if not rows:
            return
        if len(rows) != 1:
            raise RuntimeError("Interrupt target has ambiguous component holds")
        owner = None if producer_execution_id is None else self._read_execution_owner(
            connection, producer_execution_id,
        )
        if owner is None or owner["session_id"] != rows[0]["session_id"]:
            raise RuntimeError("Interrupt target hold refuses publication into " + receiver)

    def _validate_interrupt_component_start(self, connection, session_id, component, lineage):
        key = encode_component_key(component)
        admission = connection.execute(
            "SELECT * FROM interrupt_admissions WHERE session_id=?", (session_id,),
        ).fetchone()
        if admission is None:
            self._require_interrupt_fence_authority(connection, session_id, key)
            return
        self._require_interrupt_fence_authority(connection, session_id, key)
        if admission["target_component_key"] != key:
            return
        hold = connection.execute(
            "SELECT hold_count FROM component_holds WHERE session_id=? AND component_key=?",
            (session_id, key),
        ).fetchone()
        if admission["state"] != "frozen" or hold is None or hold["hold_count"] != 1:
            raise RuntimeError("Interrupt target must be frozen before component start")
        frozen_lineage = admission["frozen_stability"], admission["frozen_instability_origin"]
        if lineage != frozen_lineage:
            raise RuntimeError("Interrupt target start differs from its frozen successful lineage")

    def _authorize_interrupt_fences(self, connection, session_id, components, created_at):
        boundaries = {}
        for component in components:
            for fence in self._read_interrupt_execution_fences(
                connection, session_id, encode_component_key(component),
            ):
                identity = (
                    encode_component_key(fence["component"]), fence["shape_id"],
                    fence["alignment_generation"], fence["source_session_id"],
                )
                boundaries[identity] = fence
        rows = [
            (session_id, key, shape_id, generation, source, created_at)
            for key, shape_id, generation, source in sorted(boundaries)
        ]
        if rows and connection.executemany(
            "INSERT INTO session_fence_authorizations("
            "session_id, component_key, shape_id, alignment_generation, source_session_id, created_at) "
            "VALUES(?, ?, ?, ?, ?, ?)", rows,
        ).rowcount != len(rows):
            raise RuntimeError("Interrupt fence authority was not recorded completely")

    def _require_interrupt_fence_authority(self, connection, session_id, component_key):
        fences = self._read_interrupt_execution_fences(connection, session_id, component_key)
        if fences:
            raise RuntimeError("Post-interrupt fence refuses component admission: " + component_key)

    def read_interrupt_execution_fences(self, session_id, component):
        self._session_text(session_id, "session_id")
        key = encode_component_key(self._session_component(component))
        return self._read_interrupt_execution_fences(self.db_connection(), session_id, key)

    @staticmethod
    def _read_interrupt_session_boundaries(connection, session_id):
        boundaries = []
        seen = set()
        for row in connection.execute(
            "SELECT component_key FROM session_components WHERE session_id=? ORDER BY position",
            (session_id,),
        ):
            for boundary in InterruptCoordinationStorageMixin._read_interrupt_execution_fences(
                connection, session_id, row["component_key"],
            ):
                identity = (
                    boundary["component"], boundary["shape_id"],
                    boundary["alignment_generation"], boundary["source_session_id"],
                )
                if identity not in seen:
                    seen.add(identity)
                    boundaries.append(boundary)
        return tuple(boundaries)

    @staticmethod
    def _read_interrupt_execution_fences(connection, session_id, component_key):
        session = connection.execute(
            "SELECT shape.shape_json FROM execution_sessions AS session "
            "JOIN graph_shapes AS shape ON shape.shape_id=session.admitted_shape_id "
            "WHERE session.session_id=?", (session_id,),
        ).fetchone()
        if session is None:
            raise RuntimeError("Interrupt fence check requires its execution session")
        component = decode_component_key(component_key)
        fences = []
        for ancestor in _component_ancestors(session["shape_json"], component):
            key = encode_component_key(ancestor)
            state = connection.execute(
                "SELECT retained_result_shape_id, retained_result_alignment_generation "
                "FROM component_states WHERE component_key=?", (key,),
            ).fetchone()
            if state is None or state["retained_result_shape_id"] is None:
                continue
            unapproved = connection.execute(
                "SELECT fence.source_session_id FROM post_interrupt_fences AS fence "
                "WHERE fence.component_key=? AND fence.shape_id=? AND fence.alignment_generation=? "
                "AND fence.source_session_id<>? AND NOT EXISTS ("
                "SELECT 1 FROM session_fence_authorizations AS authority WHERE authority.session_id=? "
                "AND authority.component_key=fence.component_key AND authority.shape_id=fence.shape_id "
                "AND authority.alignment_generation=fence.alignment_generation "
                "AND authority.source_session_id=fence.source_session_id) ORDER BY fence.source_session_id",
                (key, state["retained_result_shape_id"],
                 state["retained_result_alignment_generation"], session_id, session_id),
            ).fetchall()
            for fence in unapproved:
                fences.append({
                    "component": ancestor,
                    "shape_id": state["retained_result_shape_id"],
                    "alignment_generation": state["retained_result_alignment_generation"],
                    "source_session_id": fence["source_session_id"],
                })
        return tuple(fences)
