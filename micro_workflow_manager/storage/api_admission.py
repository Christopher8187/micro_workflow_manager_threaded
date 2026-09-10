"""Atomic native admission for project-wide API execution capacity."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime

from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.models import RUNNING, now

from .priorities import ADMISSION_PRIORITY
from .api_admission_grouped import (
    ApiExecutionPermitMutation,
    apply_api_execution_permit_mutations,
)


@dataclass(frozen=True, slots=True)
class ApiExecutionPermitIdentity:
    session_id: str
    node_name: str
    job_id: int
    generation: int
    execution_id: str


def _validate_identity(identity: ApiExecutionPermitIdentity) -> None:
    if not isinstance(identity, ApiExecutionPermitIdentity):
        raise TypeError("API execution permit identity is required")
    if type(identity.session_id) is not str or not identity.session_id.strip():
        raise ValueError("API execution permit requires a session ID")
    if type(identity.node_name) is not str or not identity.node_name.strip():
        raise ValueError("API execution permit requires a node name")
    if type(identity.job_id) is not int or identity.job_id < 1:
        raise ValueError("API execution permit job ID must be a positive integer")
    if type(identity.generation) is not int or identity.generation < 0:
        raise ValueError("API execution permit generation must be a non-negative integer")
    execution_id = identity.execution_id
    if (type(execution_id) is not str or len(execution_id) != 32
            or any(character not in "0123456789abcdef" for character in execution_id)):
        raise ValueError("API execution permit requires a native execution ID")


def _permit_from_row(row) -> ApiExecutionPermitIdentity:
    identity = ApiExecutionPermitIdentity(
        session_id=row["session_id"],
        node_name=row["node_name"],
        job_id=row["job_id"],
        generation=row["generation"],
        execution_id=row["execution_id"],
    )
    _validate_identity(identity)
    acquired_at = row["acquired_at"]
    if type(acquired_at) is not str or not acquired_at.strip():
        raise RuntimeError("Damaged API execution permit acquisition time")
    try:
        parsed = datetime.fromisoformat(acquired_at)
    except ValueError as error:
        raise RuntimeError("Damaged API execution permit acquisition time") from error
    return identity


def _permit_row(connection, execution_id):
    return connection.execute(
        "SELECT execution_id, session_id, node_name, job_id, generation, acquired_at "
        "FROM api_execution_permits WHERE execution_id=?",
        (execution_id,),
    ).fetchone()


def _require_owner(connection, identity, *, require_current):
    from .execution_ownership import JobExecutionOwnerStorageMixin

    owner = JobExecutionOwnerStorageMixin._read_execution_owner(
        connection, identity.execution_id,
    )
    if owner is None or any((
        owner["execution_id"] != identity.execution_id,
        owner["session_id"] != identity.session_id,
        owner["node_name"] != identity.node_name,
        owner["job_id"] != identity.job_id,
        owner["generation"] != identity.generation,
    )):
        raise RuntimeError("API execution permit does not match its immutable owner")
    if not require_current:
        return owner
    observed = JobExecutionOwnerStorageMixin._read_job_owner_observation(
        connection, identity.node_name, identity.job_id,
    )
    if (observed is None or observed["status"] != RUNNING
            or observed["generation"] != identity.generation
            or observed["active_execution_id"] != identity.execution_id
            or observed["owner"] != owner
            or observed["session"] is None
            or observed["session"]["status"] != "running"):
        raise RuntimeError("API permit acquisition requires the exact running job owner")
    key = encode_component_key(owner["component"])
    scope = connection.execute(
        "SELECT reservation.session_id AS reservation_owner, "
        "selected.session_id AS selected_owner "
        "FROM execution_sessions AS session "
        "LEFT JOIN component_reservations AS reservation ON reservation.component_key=? "
        "LEFT JOIN session_components AS selected "
        "ON selected.session_id=session.session_id AND selected.component_key=? "
        "WHERE session.session_id=? AND session.status='running'",
        (key, key, identity.session_id),
    ).fetchone()
    if (scope is None or scope["reservation_owner"] != identity.session_id
            or scope["selected_owner"] != identity.session_id):
        raise RuntimeError("API permit acquisition requires the exact session reservation")
    return owner


def _try_acquire_api_execution_permit(connection, identity) -> bool:
    _validate_identity(identity)
    _require_owner(connection, identity, require_current=True)
    existing = _permit_row(connection, identity.execution_id)
    if existing is not None:
        if _permit_from_row(existing) != identity:
            raise RuntimeError("API execution permit identity changed")
        return True

    limit_row = connection.execute(
        "SELECT value FROM api_thread_limit WHERE singleton=1",
    ).fetchone()
    if limit_row is not None:
        limit = limit_row["value"]
        if type(limit) is not int or limit < 1:
            raise RuntimeError("Damaged aggregate API admission limit")
        active = connection.execute(
            "SELECT COUNT(*) FROM api_execution_permits",
        ).fetchone()[0]
        if type(active) is not int or active < 0:
            raise RuntimeError("Damaged API execution permit count")
        if active >= limit:
            return False

    acquired_at = now()
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
    recorded = _permit_row(connection, identity.execution_id)
    if recorded is None or _permit_from_row(recorded) != identity:
        raise RuntimeError("API execution permit readback failed")
    if recorded["acquired_at"] != acquired_at:
        raise RuntimeError("API execution permit acquisition time changed")
    return True


def _release_api_execution_permit(connection, identity) -> bool:
    _validate_identity(identity)
    _require_owner(connection, identity, require_current=False)
    existing = _permit_row(connection, identity.execution_id)
    if existing is None or _permit_from_row(existing) != identity:
        raise RuntimeError("Exact API execution permit is missing")
    removed = connection.execute(
        "DELETE FROM api_execution_permits WHERE execution_id=? AND session_id=? "
        "AND node_name=? AND job_id=? AND generation=? AND acquired_at=?",
        (
            identity.execution_id, identity.session_id, identity.node_name,
            identity.job_id, identity.generation, existing["acquired_at"],
        ),
    ).rowcount
    if removed != 1 or _permit_row(connection, identity.execution_id) is not None:
        raise RuntimeError("API execution permit changed before release")
    return True


def _apply_api_execution_permit_mutations(connection, mutations):
    from .execution_ownership import JobExecutionOwnerStorageMixin

    return apply_api_execution_permit_mutations(
        connection,
        mutations,
        validate_identity=_validate_identity,
        permit_from_row=_permit_from_row,
        permit_row=_permit_row,
        read_job_owner_observation=(
            JobExecutionOwnerStorageMixin._read_job_owner_observation
        ),
        require_owner=_require_owner,
        acquire_one=_try_acquire_api_execution_permit,
        release_one=_release_api_execution_permit,
    )


def _read_api_execution_permits(connection, *, require_current=True):
    rows = connection.execute(
        "SELECT execution_id, session_id, node_name, job_id, generation, acquired_at "
        "FROM api_execution_permits ORDER BY session_id, node_name, job_id, generation",
    ).fetchall()
    result = []
    for row in rows:
        identity = _permit_from_row(row)
        _require_owner(connection, identity, require_current=require_current)
        result.append(identity)
    return tuple(result)


def _clear_api_execution_permits_for_session(connection, session_id) -> int:
    if type(session_id) is not str or not session_id.strip():
        raise ValueError("API permit cleanup requires a session ID")
    rows = connection.execute(
        "SELECT execution_id, session_id, node_name, job_id, generation, acquired_at "
        "FROM api_execution_permits WHERE session_id=? "
        "ORDER BY node_name, job_id, generation",
        (session_id,),
    ).fetchall()
    identities = tuple(_permit_from_row(row) for row in rows)
    for identity in identities:
        _require_owner(connection, identity, require_current=False)
        _release_api_execution_permit(connection, identity)
    if connection.execute(
        "SELECT 1 FROM api_execution_permits WHERE session_id=? LIMIT 1",
        (session_id,),
    ).fetchone() is not None:
        raise RuntimeError("Session API execution permits were not cleared")
    return len(identities)


class ApiExecutionAdmissionStorageMixin:
    _try_acquire_api_execution_permit = staticmethod(_try_acquire_api_execution_permit)
    _release_api_execution_permit = staticmethod(_release_api_execution_permit)
    _apply_api_execution_permit_mutations = staticmethod(
        _apply_api_execution_permit_mutations
    )
    _read_api_execution_permits = staticmethod(_read_api_execution_permits)
    _clear_api_execution_permits_for_session = staticmethod(
        _clear_api_execution_permits_for_session
    )

    def _api_execution_permit_identity(
        self, session_id, node_name, job_id, generation, execution_id,
    ):
        identity = ApiExecutionPermitIdentity(
            session_id=session_id,
            node_name=self.validate_node_name(node_name),
            job_id=self.validate_job_id(job_id),
            generation=generation,
            execution_id=execution_id,
        )
        _validate_identity(identity)
        return identity

    def try_acquire_api_execution_permit(
        self, session_id, node_name, job_id, generation, execution_id, *, _wait=True,
    ):
        identity = self._api_execution_permit_identity(
            session_id, node_name, job_id, generation, execution_id,
        )
        future = self.submit_grouped_db_mutation(
            ("api-execution-permit-decisions", ADMISSION_PRIORITY),
            ApiExecutionPermitMutation("acquire", identity),
            _apply_api_execution_permit_mutations,
            wait=False,
            priority=ADMISSION_PRIORITY,
            collect_seconds=0.003,
        )
        if not _wait:
            return future
        try:
            return future.result()
        except BaseException:
            while not future.done():
                try:
                    future.result()
                except BaseException:
                    pass
            raise

    def release_api_execution_permit(
        self, session_id, node_name, job_id, generation, execution_id, *, _wait=True,
    ):
        identity = self._api_execution_permit_identity(
            session_id, node_name, job_id, generation, execution_id,
        )
        future = self.submit_grouped_db_mutation(
            ("api-execution-permit-decisions", ADMISSION_PRIORITY),
            ApiExecutionPermitMutation("release", identity),
            _apply_api_execution_permit_mutations,
            wait=False,
            priority=ADMISSION_PRIORITY,
            collect_seconds=0.003,
        )
        if not _wait:
            return future
        try:
            return future.result()
        except BaseException:
            while not future.done():
                try:
                    future.result()
                except BaseException:
                    pass
            raise

    def _read_api_execution_permit_decision(
        self, session_id, node_name, job_id, generation, execution_id, *, present,
    ):
        if type(present) is not bool:
            raise TypeError("API permit readback presence must be Boolean")
        identity = self._api_execution_permit_identity(
            session_id, node_name, job_id, generation, execution_id,
        )
        connection = self._new_db_connection()
        try:
            row = _permit_row(connection, execution_id)
            if present:
                if row is None:
                    return None
                if _permit_from_row(row) != identity:
                    raise RuntimeError("API execution permit identity changed")
                # This readback resolves the completed writer decision. A
                # restart may already have advanced the live job generation,
                # but neither the immutable execution owner nor this permit
                # row may change. The controller checks live currency only
                # after it has resolved and, if needed, released capacity.
                _require_owner(connection, identity, require_current=False)
                return True
            if row is not None:
                if _permit_from_row(row) != identity:
                    raise RuntimeError("API execution permit identity changed")
                return None
            _require_owner(connection, identity, require_current=False)
            return True
        finally:
            connection.close()

    def read_api_execution_permits(self):
        return tuple(
            asdict(identity)
            for identity in _read_api_execution_permits(self.db_connection())
        )
