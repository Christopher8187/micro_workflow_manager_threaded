"""Atomic explicit-interrupt session and scope admission."""

from __future__ import annotations

import json
from collections.abc import Sequence

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key

from .component_definitions import component_snapshot_from_shape
from .interrupt_common import (
    _active_predecessor_executions,
    _component_predecessors,
    _require_active_component,
    _session_selected,
    normalize_interrupt_component,
)
from .session_admission import NoSelectedJobs
from .session_shapes import read_admission_shape


def read_interrupt_admission_conflicts(
    connection, *, start_component, selected_components,
    direct_predecessors, expected_shape,
):
    """Read one exact interrupt conflict snapshot from a caller-owned connection."""
    if not isinstance(expected_shape, str) or not expected_shape:
        raise ValueError("Interrupt conflict observation requires its graph shape")
    start = normalize_interrupt_component(start_component)
    if (not isinstance(selected_components, Sequence)
            or isinstance(selected_components, (str, bytes))):
        raise ValueError("Selected interrupt components must be an ordered sequence")
    components = tuple(normalize_interrupt_component(item) for item in selected_components)
    if not components or start not in components or len(set(components)) != len(components):
        raise ValueError("Interrupt start must belong to distinct selected components")
    selected_nodes = set()
    for component in components:
        if selected_nodes.intersection(component):
            raise ValueError("Selected interrupt components must not overlap")
        selected_nodes.update(component)
    if (not isinstance(direct_predecessors, Sequence)
            or isinstance(direct_predecessors, (str, bytes))):
        raise ValueError("Interrupt predecessors must be an ordered sequence")
    predecessors = tuple(sorted(
        normalize_interrupt_component(item) for item in direct_predecessors
    ))
    if len(set(predecessors)) != len(predecessors):
        raise ValueError("Interrupt predecessors must be distinct")
    connection.execute("SAVEPOINT mwf_interrupt_conflicts")
    try:
        return _read_interrupt_admission_conflicts(
            connection, start, components, predecessors, expected_shape,
            require_registered_shape=False,
        )
    finally:
        connection.execute("RELEASE SAVEPOINT mwf_interrupt_conflicts")


def _read_interrupt_admission_conflicts(
    connection, start_component, selected_components,
    direct_predecessors, expected_shape, *, require_registered_shape=True,
):
    snapshot = component_snapshot_from_shape(expected_shape)
    if require_registered_shape:
        shape_id, revision = read_admission_shape(
            connection, expected_shape, selected_components,
        )
        active_membership_required = True
    else:
        from .component_membership import read_active_component_partition

        active = read_active_component_partition(connection)
        registered = connection.execute(
            "SELECT 1 FROM graph_shapes WHERE shape_json=?", (expected_shape,),
        ).fetchone()
        if registered is not None:
            shape_id, revision = read_admission_shape(
                connection, expected_shape, selected_components,
            )
            active_membership_required = True
        else:
            # A read-only preflight can precede initial topology registration.
            # The writer repeats this observation after registration and still
            # requires the exact durable graph shape and active membership.
            shape_id = None
            revision = None if active is None else active.revision
            active_membership_required = active is not None
    if tuple(direct_predecessors) != _component_predecessors(expected_shape, start_component):
        raise RuntimeError("Interrupt direct predecessors changed before admission")
    for component in (*selected_components, *direct_predecessors):
        if component not in snapshot.components:
            raise RuntimeError("Interrupt scope is outside its admitted graph shape")
        if active_membership_required:
            _require_active_component(connection, component)
    selected_nodes = {node for component in selected_components for node in component}
    selected_keys = tuple(encode_component_key(component) for component in selected_components)
    placeholders = ",".join("?" for _ in selected_keys)
    for node in sorted(selected_nodes):
        row = connection.execute(
            "SELECT job_id FROM jobs WHERE node_name=? AND (status='running' "
            "OR active_execution_id IS NOT NULL OR active_pid IS NOT NULL "
            "OR active_thread_id IS NOT NULL OR active_started_at IS NOT NULL) "
            "ORDER BY job_id LIMIT 1", (node,),
        ).fetchone()
        if row is not None:
            from .execution_ownership import JobExecutionOwnerStorageMixin

            observed = JobExecutionOwnerStorageMixin._read_job_owner_observation(
                connection, node, row["job_id"],
            )
            owner = None if observed is None else observed["owner"]
            if owner is None or observed["state"] != "active":
                raise RuntimeError(
                    f"Interrupt target scope has damaged active job {node}/{row['job_id']}"
                )
            raise RuntimeError(
                f"Interrupt target scope has active job {node}/{row['job_id']} "
                f"owned by session {owner['session_id']} in "
                f"component {encode_component_key(owner['component'])}"
            )
    pending = connection.execute(
        "SELECT component_key FROM pending_component_executions "
        f"WHERE component_key IN ({placeholders}) ORDER BY component_key LIMIT 1",
        selected_keys,
    ).fetchone()
    if pending is not None:
        raise RuntimeError(
            "Interrupt target scope has a begun component execution: "
            + pending["component_key"]
        )
    predecessor_executions = _active_predecessor_executions(
        connection, direct_predecessors,
    )
    parents = tuple(sorted({item[1]["owner"]["session_id"]
                            for item in predecessor_executions}))
    reservations = {}
    transfers = []
    for component in selected_components:
        key = encode_component_key(component)
        row = connection.execute(
            "SELECT session_id FROM component_reservations WHERE component_key=?", (key,),
        ).fetchone()
        if row is None:
            reservations[key] = None
            continue
        source = row["session_id"]
        owner = _session_selected(connection, source, key)
        if owner is None or owner["status"] != "running" or owner["component_key"] is None:
            raise RuntimeError("Interrupt overlap has no exact running reservation owner")
        if owner["session_kind"] == "interrupt" and source not in parents:
            raise RuntimeError("Interrupt scope overlaps an independent interrupt reservation")
        kind = "direct-parent" if source in parents else "future-admission"
        reservations[key] = source
        transfers.append((source, key, kind))
    return {
        "shape_id": shape_id, "partition_revision": revision,
        "parents": parents, "predecessor_executions": predecessor_executions,
        "reservations": reservations, "transfers": tuple(transfers),
    }


class InterruptAdmissionStorageMixin:
    def admit_interrupt_execution(
        self, session_id: str, *, command: str, start_component,
        selected_components, selected_jobs=(), direct_predecessors,
        readiness_overridden: bool, started_at: str, hostname: str, pid: int,
        process_identity: str | None, details: dict | None = None,
        expected_shape: str, _reserved_sample_builder=None,
        sample_id: str | None = None, _wait: bool = True,
    ):
        self._require_execution_session_storage()
        self._session_text(session_id, "session_id")
        self._session_text(command, "command")
        self._session_text(expected_shape, "expected_shape")
        self._session_text(hostname, "hostname")
        self._session_time(started_at, "started_at")
        if type(pid) is not int or pid < 1:
            raise ValueError("pid must be a positive integer")
        if process_identity is not None:
            self._session_text(process_identity, "process_identity")
        if type(readiness_overridden) is not bool:
            raise ValueError("Interrupt readiness override must be Boolean")
        if details is not None and not isinstance(details, dict):
            raise ValueError("Session details must be an object")
        if not isinstance(selected_components, Sequence) or isinstance(selected_components, (str, bytes)):
            raise ValueError("Selected components must be an ordered sequence")
        if not isinstance(selected_jobs, Sequence) or isinstance(selected_jobs, (str, bytes)):
            raise ValueError("Selected jobs must be an ordered sequence")
        start = self._session_component(start_component)
        components = tuple(self._session_component(item) for item in selected_components)
        if not components or start not in components or len(set(components)) != len(components):
            raise ValueError("Interrupt start must belong to distinct selected components")
        nodes = set()
        for component in components:
            if nodes.intersection(component):
                raise ValueError("Selected components must not overlap")
            nodes.update(component)
        predecessors = tuple(sorted(self._session_component(item) for item in direct_predecessors))
        if len(set(predecessors)) != len(predecessors):
            raise ValueError("Interrupt predecessors must be distinct")
        jobs = tuple((self.validate_node_name(node), self.validate_job_id(job_id))
                     for node, job_id in selected_jobs)
        if any(node not in nodes for node, _ in jobs) or len(set(jobs)) != len(jobs):
            raise ValueError("Selected interrupt jobs must be distinct members of its scope")
        if _reserved_sample_builder is not None:
            if jobs or components != (start,) or not callable(_reserved_sample_builder):
                raise ValueError("Reserved interrupt sampling requires one component and no selected jobs")
            self._session_text(sample_id, "sample_id")
        elif sample_id is not None:
            raise ValueError("Sample identity requires a reserved sample reader")

        def admit(connection):
            return self._admit_interrupt_execution(
                connection, session_id, command=command, start_component=start,
                selected_components=components, selected_jobs=jobs,
                direct_predecessors=predecessors,
                readiness_overridden=readiness_overridden, started_at=started_at,
                hostname=hostname, pid=pid, process_identity=process_identity,
                details=dict(details or {}), expected_shape=expected_shape,
                _reserved_sample_builder=_reserved_sample_builder, sample_id=sample_id,
            )

        return self.submit_db_mutation(admit, wait=_wait, priority=0)

    def observe_interrupt_admission_conflicts(
        self, *, start_component, selected_components, direct_predecessors,
        expected_shape,
    ):
        """Read an exact pre-import conflict snapshot without reserving scope."""
        return read_interrupt_admission_conflicts(
            self.db_connection(), start_component=start_component,
            selected_components=selected_components,
            direct_predecessors=direct_predecessors, expected_shape=expected_shape,
        )

    def _observe_interrupt_admission_conflicts(
        self, connection, start_component, selected_components,
        direct_predecessors, expected_shape,
    ):
        return _read_interrupt_admission_conflicts(
            connection, start_component, selected_components,
            direct_predecessors, expected_shape,
        )

    def _admit_interrupt_execution(
        self, connection, session_id, *, command, start_component,
        selected_components, selected_jobs, direct_predecessors,
        readiness_overridden, started_at, hostname, pid, process_identity,
        details, expected_shape, _reserved_sample_builder=None, sample_id=None,
    ):
        observed = self._observe_interrupt_admission_conflicts(
            connection, start_component, selected_components,
            direct_predecessors, expected_shape,
        )
        shape_id, revision = observed["shape_id"], observed["partition_revision"]
        actual_parents = observed["parents"]
        predecessor_executions = observed["predecessor_executions"]
        reservations, transfers = observed["reservations"], observed["transfers"]

        roots = []
        for position, (node, job_id) in enumerate(selected_jobs):
            observed = self._read_job_owner_observation(connection, node, job_id)
            if observed is None:
                raise RuntimeError(f"Selected job does not exist at interrupt admission: {node}/{job_id}")
            roots.append((session_id, position, node, job_id, observed["job_instance_id"]))
        selection_kind = "jobs" if roots else "components"
        if connection.execute(
            "INSERT INTO execution_sessions("
            "session_id, session_kind, command, selection_kind, start_component, status, "
            "started_at, heartbeat_at, hostname, pid, process_identity, details_json, "
            "admitted_shape_id, partition_revision, scope_admitted) "
            "VALUES(?, 'interrupt', ?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?, 1)",
            (session_id, command, selection_kind, encode_component_key(start_component),
             started_at, started_at, hostname, pid, process_identity,
             json.dumps(details), shape_id, revision),
        ).rowcount != 1:
            raise RuntimeError("Interrupt session was not recorded")
        component_rows = [
            (session_id, position, encode_component_key(component))
            for position, component in enumerate(selected_components)
        ]
        if connection.executemany(
            "INSERT INTO session_components(session_id, position, component_key) VALUES(?, ?, ?)",
            component_rows,
        ).rowcount != len(component_rows):
            raise RuntimeError("Interrupt selected scope was not recorded completely")
        if connection.execute(
            "INSERT INTO interrupt_admissions("
            "session_id, target_component_key, readiness_override_requested, state, created_at) "
            "VALUES(?, ?, ?, 'admitted', ?)",
            (session_id, encode_component_key(start_component), int(readiness_overridden), started_at),
        ).rowcount != 1:
            raise RuntimeError("Interrupt admission was not recorded")

        for key, source in reservations.items():
            if source is None:
                changed = connection.execute(
                    "INSERT INTO component_reservations(component_key, session_id) VALUES(?, ?)",
                    (key, session_id),
                ).rowcount
            else:
                changed = connection.execute(
                    "UPDATE component_reservations SET session_id=? "
                    "WHERE component_key=? AND session_id=?",
                    (session_id, key, source),
                ).rowcount
            if changed != 1:
                raise RuntimeError("Interrupt reservation changed during admission")
            component = decode_component_key(key)
            if source is None:
                self._bind_pending_thread_overrides(connection, session_id, component)
            else:
                self._transfer_thread_overrides(
                    connection, source, session_id, component,
                )
        for source, key, kind in transfers:
            if connection.execute(
                "INSERT INTO interrupt_scope_transfers("
                "child_session_id, source_session_id, component_key, transfer_kind, state, created_at) "
                "VALUES(?, ?, ?, ?, 'active', ?)",
                (session_id, source, key, kind, started_at),
            ).rowcount != 1:
                raise RuntimeError("Interrupt scope transfer was not recorded")

        for parent in actual_parents:
            if connection.execute(
                "INSERT INTO execution_session_parents(child_session_id, parent_session_id, created_at) "
                "VALUES(?, ?, ?)", (session_id, parent, started_at),
            ).rowcount != 1:
                raise RuntimeError("Interrupt parent was not recorded")
        grouped = {}
        for component, observed in predecessor_executions:
            owner = observed["owner"]
            grouped.setdefault((owner["session_id"], component), []).append(observed)
        for (owner_session, component), observations in sorted(grouped.items()):
            key = encode_component_key(component)
            if connection.execute(
                "INSERT INTO interrupt_pause_requests("
                "child_session_id, owner_session_id, component_key, state, created_at) "
                "VALUES(?, ?, ?, 'requested', ?)",
                (session_id, owner_session, key, started_at),
            ).rowcount != 1:
                raise RuntimeError("Interrupt pause request was not recorded")
            rows = [(
                session_id, owner_session, key, item["owner"]["execution_id"],
                item["owner"]["node_name"], item["owner"]["job_id"], item["owner"]["generation"],
            ) for item in observations]
            if connection.executemany(
                "INSERT INTO interrupt_paused_executions("
                "child_session_id, owner_session_id, component_key, execution_id, "
                "node_name, job_id, generation) VALUES(?, ?, ?, ?, ?, ?, ?)", rows,
            ).rowcount != len(rows):
                raise RuntimeError("Interrupt paused executions were not recorded completely")

        if _reserved_sample_builder is not None:
            from .sample_planning import SamplePlan
            plan = _reserved_sample_builder(connection)
            if not isinstance(plan, SamplePlan):
                raise ValueError("Reserved sample reader must return a SamplePlan")
            selected_roots, selection = plan.admission_record(
                sample_id=sample_id, component=start_component, expected_shape=expected_shape,
            )
            if not selected_roots:
                raise NoSelectedJobs("Sample selected no jobs; no work was started")
            addresses = [(node, job_id) for node, job_id, _ in selected_roots]
            if len(addresses) != len(set(addresses)):
                raise ValueError("Reserved sample selection contains duplicate jobs")
            roots = []
            for position, (node, job_id, instance) in enumerate(selected_roots):
                observed = self._read_job_owner_observation(connection, node, job_id)
                if (node not in start_component or observed is None
                        or observed["job_instance_id"] != instance):
                    raise RuntimeError(f"Sampled job changed after interrupt reservation: {node}/{job_id}")
                roots.append((session_id, position, node, job_id, instance))
            details = {**details, "selection": selection}
            if connection.execute(
                "UPDATE execution_sessions SET selection_kind='jobs', details_json=? "
                "WHERE session_id=? AND status='running' AND selection_kind='components'",
                (json.dumps(details), session_id),
            ).rowcount != 1:
                raise RuntimeError("Interrupt sample selection was not retained")
        if roots and connection.executemany(
            "INSERT INTO session_jobs(session_id, position, node_name, job_id, job_instance_id) "
            "VALUES(?, ?, ?, ?, ?)", roots,
        ).rowcount != len(roots):
            raise RuntimeError("Interrupt selected jobs were not recorded completely")
        self._authorize_interrupt_fences(connection, session_id, selected_components, started_at)
        from .session_scope import require_admitted_reservation_scope
        for source_session_id in sorted({source for source, _, _ in transfers}):
            if not require_admitted_reservation_scope(connection, source_session_id):
                raise RuntimeError("Interrupt source reservation admission marker changed")
        if not require_admitted_reservation_scope(connection, session_id):
            raise RuntimeError("Interrupt reservation admission marker changed")
        row = connection.execute(
            "SELECT * FROM execution_sessions WHERE session_id=?", (session_id,),
        ).fetchone()
        return self._execution_session_from_row(connection, row)

    def _finalize_interrupt_sample(
        self, connection, session_id, *, component, expected_shape,
        sample_id, reserved_sample_builder,
    ):
        """Replace provisional sample metadata with one frozen observation."""
        from .sample_history import read_sample_admission_history
        from .sample_planning import SamplePlan

        if not callable(reserved_sample_builder):
            raise ValueError("Frozen interrupt sampling requires its sample reader")
        self._session_text(sample_id, "sample_id")
        plan = reserved_sample_builder(connection)
        if not isinstance(plan, SamplePlan):
            raise ValueError("Reserved sample reader must return a SamplePlan")
        selected_roots, selection = plan.admission_record(
            sample_id=sample_id, component=component, expected_shape=expected_shape,
        )
        if not selected_roots:
            raise NoSelectedJobs("Frozen interrupt sample selected no jobs; no work was prepared")
        if len({(node, job_id) for node, job_id, _ in selected_roots}) != len(selected_roots):
            raise ValueError("Frozen interrupt sample contains duplicate jobs")
        for node, job_id, instance in selected_roots:
            observed = self._read_job_owner_observation(connection, node, job_id)
            if (node not in component or observed is None
                    or observed["job_instance_id"] != instance):
                raise RuntimeError(
                    f"Sampled job changed during interrupt freeze: {node}/{job_id}"
                )

        # Refuse damaged provisional metadata before replacing it. The final
        # observation may legitimately differ after a predecessor acknowledges.
        read_sample_admission_history(
            self, connection, session_id,
            component=component, expected_shape=expected_shape,
        )
        session = connection.execute(
            "SELECT details_json FROM execution_sessions "
            "WHERE session_id=? AND status='running' AND selection_kind='jobs'",
            (session_id,),
        ).fetchone()
        if session is None:
            raise RuntimeError("Interrupt sample lost its running session")
        try:
            details = json.loads(session["details_json"])
        except (TypeError, json.JSONDecodeError) as error:
            raise RuntimeError("Interrupt sample has damaged session details") from error
        if not isinstance(details, dict) or "selection" not in details:
            raise RuntimeError("Interrupt sample lost its provisional selection")
        previous = connection.execute(
            "SELECT COUNT(*) FROM session_jobs WHERE session_id=?", (session_id,),
        ).fetchone()[0]
        deleted = connection.execute(
            "DELETE FROM session_jobs WHERE session_id=?", (session_id,),
        ).rowcount
        if deleted != previous:
            raise RuntimeError("Interrupt sample roots changed during freeze")
        rows = [
            (session_id, position, node, job_id, instance)
            for position, (node, job_id, instance) in enumerate(selected_roots)
        ]
        if connection.executemany(
            "INSERT INTO session_jobs(session_id, position, node_name, job_id, job_instance_id) "
            "VALUES(?, ?, ?, ?, ?)", rows,
        ).rowcount != len(rows):
            raise RuntimeError("Frozen interrupt sample roots were not recorded completely")
        details["selection"] = selection
        if connection.execute(
            "UPDATE execution_sessions SET details_json=? "
            "WHERE session_id=? AND status='running' AND selection_kind='jobs'",
            (json.dumps(details), session_id),
        ).rowcount != 1:
            raise RuntimeError("Frozen interrupt sample selection was not recorded")
        retained = read_sample_admission_history(
            self, connection, session_id,
            component=component, expected_shape=expected_shape,
        )
        if retained.plan != plan or retained.roots != tuple(selected_roots):
            raise RuntimeError("Frozen interrupt sample readback differs from its observation")
        return plan
