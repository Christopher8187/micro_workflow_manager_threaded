from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Callable, Iterator, TypeVar

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key
from micro_workflow_manager.errors import JobRestartedError
from micro_workflow_manager.models import CANCELLED, FAILED, RUNNING
from micro_workflow_manager.session_liveness import execution_session_liveness


T = TypeVar("T")
_EXECUTION_FENCE_DEPTH = ContextVar("mwf_execution_fence_depth", default=0)


def execution_fence_held() -> bool:
    return _EXECUTION_FENCE_DEPTH.get() > 0


def _move_restart_output(source, destination):
    from .recovery_moves import move_without_replacement

    return move_without_replacement(source, destination)


def _discard_restart_output(path):
    if path.is_dir():
        path.rmdir()
    else:
        path.unlink()


class JobRestartStorageMixin:
    """Execution fencing and manual restart operations."""

    def _read_owned_restart_target(self, connection, node_name, job_id):
        observed = self._read_job_owner_observation(connection, node_name, job_id)
        if observed is None:
            raise RuntimeError(f'Job does not exist: {node_name}/{job_id}')
        owner = observed['owner']
        if owner is None:
            raise RuntimeError(f'Job {node_name}/{job_id} has no recorded execution owner')
        session = observed['session']
        liveness = execution_session_liveness(session)
        if not liveness['live']:
            raise RuntimeError(
                f"Job {node_name}/{job_id} belongs to session {owner['session_id']}, "
                f"which cannot accept restart: {liveness['reason']}"
            )
        reservations = connection.execute(
            'SELECT component_key, session_id FROM component_reservations',
        ).fetchall()
        overlapping = [row for row in reservations
                       if set(owner['component']).intersection(decode_component_key(row['component_key']))]
        if (len(overlapping) != 1
                or overlapping[0]['component_key'] != encode_component_key(owner['component'])
                or overlapping[0]['session_id'] != owner['session_id']):
            raise RuntimeError(
                f"Job {node_name}/{job_id} has no unambiguous reservation "
                f"for its owning session {owner['session_id']}"
            )
        status = observed['status']
        if status == RUNNING and observed['active_execution_id']:
            mode = 'running'
        elif status in {FAILED, CANCELLED} and observed['active_execution_id'] is None:
            mode = 'failed'
        else:
            raise RuntimeError(
                f'Job {node_name}/{job_id} has status {status!r}. Restart requires '
                'an active running execution or failed/cancelled work with no active execution.'
            )
        return {
            'node': node_name, 'job_id': job_id, 'mode': mode, 'owner': owner,
            'job_instance_id': observed['job_instance_id'], 'generation': observed['generation'],
            'status': status, 'active_execution_id': observed['active_execution_id'],
        }

    def plan_owned_job_restarts(self, addresses) -> list[dict[str, Any]]:
        """Resolve eligible jobs through their exact native owning sessions."""
        self._require_execution_session_storage()
        addresses = [(self.validate_node_name(node), self.validate_job_id(job_id))
                     for node, job_id in addresses]
        if len(set(addresses)) != len(addresses):
            raise ValueError('Restart selection contains duplicate jobs')
        targets = [self._read_owned_restart_target(self.db_connection(), node, job_id)
                   for node, job_id in addresses]
        owners = {target['owner']['session_id'] for target in targets}
        if len(owners) > 1:
            raise RuntimeError('Restart selection has several owning sessions: ' + ', '.join(sorted(owners)))
        return targets

    def _read_component_restart_plan(self, connection, node, *, failed_only):
        reservations = connection.execute(
            'SELECT r.component_key, r.session_id, selected.session_id AS selected_session, s.* '
            'FROM component_reservations AS r '
            'LEFT JOIN execution_sessions AS s ON s.session_id=r.session_id '
            'LEFT JOIN session_components AS selected '
            'ON selected.session_id=r.session_id AND selected.component_key=r.component_key',
        ).fetchall()
        containing = [(row, decode_component_key(row['component_key'])) for row in reservations
                      if node in decode_component_key(row['component_key'])]
        if len(containing) != 1:
            raise RuntimeError(f'Node {node} has no unambiguous component reservation')
        row, component = containing[0]
        if (row['selected_session'] is None or sum(
                bool(set(component).intersection(decode_component_key(other['component_key'])))
                for other in reservations) != 1):
            raise RuntimeError(f'Node {node} has damaged component reservation ownership')
        liveness = execution_session_liveness(dict(row))
        if not liveness['live']:
            raise RuntimeError(f"Session {row['session_id']} cannot accept restart: {liveness['reason']}")
        placeholders = ','.join('?' for _ in component)
        jobs = connection.execute(
            f'SELECT node_name, job_id, status, active_execution_id FROM jobs '
            f'WHERE node_name IN ({placeholders}) ORDER BY node_name, job_id', component,
        ).fetchall()
        targets = [self._read_owned_restart_target(connection, job['node_name'], job['job_id'])
                   for job in jobs if job['status'] in {FAILED, CANCELLED}
                   or (not failed_only and job['status'] == RUNNING)]
        conflicts = [target for target in targets if target['owner']['session_id'] != row['session_id']
                     or target['owner']['component'] != component]
        if conflicts:
            raise RuntimeError('Restart component contains jobs with another recorded owner')
        return {'node': node, 'component': component, 'session_id': row['session_id'],
                'failed_only': failed_only, 'targets': targets}

    def plan_owned_component_restart(self, node_name, *, failed_only=False) -> dict[str, Any]:
        """Select eligible jobs from the node's exact reserved component."""
        self._require_execution_session_storage()
        return self._read_component_restart_plan(
            self.db_connection(), self.validate_node_name(node_name), failed_only=failed_only,
        )

    def request_owned_job_restarts(
        self, targets, *, requested_by_pid: int | None = None,
        reason: str = 'second-terminal restart',
        component_plan: dict | None = None,
    ) -> list[dict[str, Any]]:
        """Recheck and restart selected generations with durable output recovery."""
        self._require_execution_session_storage()
        from .restart_application import apply_owned_restarts

        return apply_owned_restarts(
            self, targets, requested_by_pid=requested_by_pid, reason=reason,
            component_plan=component_plan,
        )

    def job_restart_revision(self) -> int:
        """Return the project-wide generation-change revision.

        The scheduler polls this one metadata row instead of materializing every
        active execution lease on every 50 ms tick. A full lease comparison is
        only needed after an actual restart request changes the revision.
        """
        row = self.db_connection().execute(
            "SELECT value FROM metadata WHERE key='job_restart_revision'"
        ).fetchone()
        if row is None:
            return 0
        try:
            return max(0, int(row[0]))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _increment_job_restart_revision(connection) -> None:
        connection.execute(
            "INSERT INTO metadata(key, value) "
            "VALUES('job_restart_revision', '1') "
            "ON CONFLICT(key) DO UPDATE SET "
            "value=CAST(metadata.value AS INTEGER) + 1"
        )

    def job_execution_is_current(
        self,
        node_name: str,
        job_id: int,
        generation: int,
        execution_id: str | None = None,
    ) -> bool:
        row = self.db_connection().execute(
            "SELECT generation, active_execution_id FROM jobs WHERE node_name=? AND job_id=?",
            (node_name, self.validate_job_id(job_id)),
        ).fetchone()
        if row is None or int(row["generation"]) != int(generation):
            return False
        if execution_id is not None and row["active_execution_id"] != execution_id:
            return False
        return True

    def active_job_executions(self) -> dict[tuple[str, int], tuple[int, str]]:
        """Return all active execution leases with one indexed SQLite scan."""
        rows = self.db_connection().execute(
            "SELECT node_name, job_id, generation, active_execution_id FROM jobs "
            "WHERE active_execution_id IS NOT NULL"
        ).fetchall()
        return {
            (str(row["node_name"]), int(row["job_id"])): (
                int(row["generation"]),
                str(row["active_execution_id"]),
            )
            for row in rows
        }

    @contextmanager
    def guard_job_execution(
        self,
        node_name: str,
        job_id: int,
        generation: int,
        execution_id: str | None = None,
    ) -> Iterator[None]:
        with self.filesystem_interprocess_lock(
            "execution-fences",
            self.job_execution_lock_name(node_name, job_id),
        ):
            if not self.job_execution_is_current(node_name, job_id, generation, execution_id):
                raise JobRestartedError(
                    f"Job {node_name}/{job_id} generation {generation} was restarted"
                )
            token = _EXECUTION_FENCE_DEPTH.set(_EXECUTION_FENCE_DEPTH.get() + 1)
            try:
                yield
            finally:
                _EXECUTION_FENCE_DEPTH.reset(token)

    def run_guarded_job_side_effect(
        self,
        node_name: str,
        job_id: int,
        generation: int,
        execution_id: str,
        action: Callable[[], T],
    ) -> T:
        with self.guard_job_execution(node_name, job_id, generation, execution_id):
            return action()

    def _restart_receipt_state(self, operation_id):
        from .file_operation_receipts import read_file_receipt

        connection = self._new_db_connection()
        try:
            return read_file_receipt(connection, 'restart_receipts', operation_id)
        finally:
            connection.close()

    def request_job_restart(
        self,
        node_name: str,
        job_id: int,
        *,
        requested_by_pid: int | None = None,
        reason: str = "manual restart",
    ) -> dict[str, Any]:
        self._require_execution_session_storage()
        node_name, job_id = self.validate_node_name(node_name), self.validate_job_id(job_id)
        observed = self._read_job_owner_observation(self.db_connection(), node_name, job_id)
        if observed is None:
            raise FileNotFoundError(f"Job does not exist: {node_name}/{job_id}")
        owner_live = (
            observed['session'] is not None
            and execution_session_liveness(observed['session'])['live']
        )
        if owner_live and observed['status'] in (RUNNING, FAILED, CANCELLED):
            target = self._read_owned_restart_target(self.db_connection(), node_name, job_id)
            return self.request_owned_job_restarts(
                [target], requested_by_pid=requested_by_pid, reason=reason,
            )[0]
        from .restart_application import apply_independent_restart

        return apply_independent_restart(
            self, node_name, job_id, requested_by_pid=requested_by_pid, reason=reason,
        )

    def request_active_job_restart(
        self,
        node_name: str,
        job_id: int,
        *,
        requested_by_pid: int | None = None,
        reason: str = "second-terminal active-job restart",
    ) -> dict[str, Any]:
        self._require_execution_session_storage()
        node_name = self.validate_node_name(node_name)
        job_id = self.validate_job_id(job_id)
        if not self.job_exists(node_name, job_id):
            raise FileNotFoundError(f'Job does not exist: {node_name}/{job_id}')
        target = self._read_owned_restart_target(self.db_connection(), node_name, job_id)
        if target['mode'] != 'running':
            raise RuntimeError(f'Job {node_name}/{job_id} is not currently running')
        return self.request_owned_job_restarts(
            [target], requested_by_pid=requested_by_pid, reason=reason,
        )[0]
