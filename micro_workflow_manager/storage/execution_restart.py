from __future__ import annotations

import json
import os
import shutil
from contextlib import ExitStack, contextmanager
from datetime import datetime
from typing import Any, Callable, Iterator, TypeVar
from uuid import uuid4

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key
from micro_workflow_manager.errors import JobRestartedError
from micro_workflow_manager.models import CANCELLED, FAILED, QUEUED, RUNNING
from micro_workflow_manager.session_liveness import execution_session_liveness


T = TypeVar("T")


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
        """Recheck the planned owners and replace all selected generations together.

        Output staging compensates synchronous failures. Crash recovery across
        SQLite and the filesystem requires a separate durable recovery record.
        """
        self._require_execution_session_storage()
        targets = list(targets)
        addresses = [(self.validate_node_name(target['node']), self.validate_job_id(target['job_id']))
                     for target in targets]
        if len(set(addresses)) != len(addresses):
            raise ValueError('Restart selection contains duplicate jobs')
        if not targets and component_plan is None:
            return []
        if targets and len({target['owner']['session_id'] for target in targets}) != 1:
            raise RuntimeError('Restart selection must have one owning session')
        requested_at = datetime.now().isoformat(timespec='milliseconds')
        requester = os.getpid() if requested_by_pid is None else requested_by_pid
        staged = []
        restarted = []
        committed = False
        with ExitStack() as fences:
            for node, job_id in sorted(addresses):
                fences.enter_context(self.filesystem_interprocess_lock(
                    'execution-fences', self.job_execution_lock_name(node, job_id),
                ))
            try:
                # Handle notifications below, after marking the commit. A failed
                # wake must never restore old output over a committed restart.
                with self._database_write_lock():
                    connection = self.db_connection()
                    connection.execute('BEGIN IMMEDIATE')
                    try:
                        if component_plan is not None:
                            current_plan = self._read_component_restart_plan(
                                connection, component_plan['node'], failed_only=component_plan['failed_only'],
                            )
                            if current_plan != component_plan or current_plan['targets'] != targets:
                                raise RuntimeError('Restart component selection or ownership changed after preflight')
                        if not targets:
                            connection.commit()
                            committed = True
                            return []
                        for target in targets:
                            current = self._read_owned_restart_target(connection, target['node'], target['job_id'])
                            if current != target:
                                raise RuntimeError(
                                    f"Restart ownership or state changed for {target['node']}/{target['job_id']}"
                                )
                        for node, job_id in addresses:
                            output = self.output_file(node, job_id)
                            if output.exists() or output.is_symlink():
                                saved = output.with_name(f'.restart-{uuid4().hex}-output.json')
                                output.rename(saved)
                                staged.append((output, saved))
                        for target in targets:
                            node, job_id = target['node'], target['job_id']
                            generation = target['generation'] + 1
                            row = connection.execute(
                                'SELECT runtime_json FROM jobs WHERE node_name=? AND job_id=?', (node, job_id),
                            ).fetchone()
                            runtime = json.loads(row['runtime_json']) if row['runtime_json'] else None
                            if runtime:
                                runtime = {**runtime, 'state': 'restarted', 'updated_at': requested_at,
                                           'restart_reason': reason, 'previous_generation': target['generation'],
                                           'generation': generation}
                            changed = connection.execute(
                                "UPDATE jobs SET generation=?, active_execution_id=NULL, active_pid=NULL, "
                                "active_thread_id=NULL, active_started_at=NULL, restart_requested_at=?, "
                                "restart_requested_by_pid=?, restart_reason=?, status='queued', status_json='{}', "
                                "runtime_json=? WHERE node_name=? AND job_id=? AND generation=? AND status=? "
                                "AND active_execution_id IS ? AND EXISTS (SELECT 1 FROM job_instances AS i "
                                "WHERE i.node_name=jobs.node_name AND i.job_id=jobs.job_id "
                                "AND i.instance_id=? AND i.last_execution_id=?)",
                                (generation, requested_at, requester, reason,
                                 json.dumps(runtime, ensure_ascii=False, separators=(',', ':')) if runtime else None,
                                 node, job_id, target['generation'], target['status'], target['active_execution_id'],
                                 target['job_instance_id'], target['owner']['execution_id']),
                            ).rowcount
                            if changed != 1:
                                raise RuntimeError(f'Restart state changed for {node}/{job_id}')
                            owner_data = {
                                'session_id': target['owner']['session_id'],
                                'execution_id': target['owner']['execution_id'],
                                'job_instance_id': target['job_instance_id'],
                                'component': list(target['owner']['component']),
                            }
                            events = [
                                ('queued', {**owner_data, 'previous_status': target['status'], 'status': QUEUED}),
                                ('restart_requested', {**owner_data, 'previous_generation': target['generation'],
                                                       'generation': generation, 'reason': reason,
                                                       'requested_by_pid': requester}),
                            ]
                            connection.executemany(
                                'INSERT INTO job_events(node_name, job_id, time, event, data_json) VALUES(?, ?, ?, ?, ?)',
                                [(node, job_id, requested_at, event, json.dumps(data, separators=(',', ':')))
                                 for event, data in events],
                            )
                            restarted.append({
                                'node': node, 'job_id': job_id, 'mode': target['mode'],
                                'session_id': target['owner']['session_id'],
                                'previous_generation': target['generation'], 'generation': generation,
                                'requested_at': requested_at, 'warnings': [],
                            })
                        connection.executemany(
                            'INSERT INTO nodes(node_name, status) VALUES(?, ?) '
                            'ON CONFLICT(node_name) DO UPDATE SET status=excluded.status, '
                            'updated_at=CURRENT_TIMESTAMP WHERE nodes.status IS NOT excluded.status',
                            [(node, RUNNING) for node in sorted({node for node, _ in addresses})],
                        )
                        self._increment_job_restart_revision(connection)
                        connection.commit()
                        committed = True
                    except BaseException:
                        connection.rollback()
                        raise
            except BaseException as error:
                if not committed:
                    for output, saved in reversed(staged):
                        try:
                            saved.rename(output)
                        except OSError as restore_error:
                            note = f'Restart output restoration failed; retained at {saved}: {restore_error}'
                            error.__notes__ = [*getattr(error, '__notes__', ()), note]
                raise
            for output, saved in staged:
                try:
                    if saved.is_dir() and not saved.is_symlink():
                        shutil.rmtree(saved)
                    else:
                        saved.unlink()
                except OSError as error:
                    restarted[0]['warnings'].append(f'Restart committed; staged output cleanup requires attention at {saved}: {error}')
        for node in sorted({node for node, _ in addresses}):
            try:
                self.notify_queue_change(node)
            except Exception as error:
                restarted[0]['warnings'].append(f'Restart committed; scheduler notification requires attention: {error}')
        return restarted

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
            yield

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

    def _remove_restart_artifact(self, path):
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            self.remove_if_exists(path)

    def _request_job_restart_locked(
        self,
        node_name: str,
        job_id: int,
        *,
        requested_by_pid: int | None,
        reason: str,
        require_running_execution: bool,
    ) -> dict[str, Any]:
        control = self.read_job_control(node_name, job_id)
        previous_status = self.get_job_status(node_name, job_id) or QUEUED
        if require_running_execution:
            if previous_status != RUNNING or not control.get("active_execution_id"):
                raise RuntimeError(
                    f"Job {node_name}/{job_id} is not currently running. Only a live "
                    "running attempt can be restarted inside an existing run/runfrom sequence."
                )

        requested_at = datetime.now().isoformat(timespec="seconds")
        previous_generation = int(control["generation"])
        generation = previous_generation + 1
        runtime = self.read_job_runtime(node_name, job_id)
        if runtime:
            runtime = {
                **runtime,
                "state": "restarted",
                "updated_at": requested_at,
                "restart_reason": reason,
                "previous_generation": previous_generation,
                "generation": generation,
            }
        status_extra: dict[str, Any] = {}
        with self.db_transaction() as connection:
            connection.execute(
                "UPDATE jobs SET generation=?, active_execution_id=NULL, active_pid=NULL, "
                "active_thread_id=NULL, active_started_at=NULL, restart_requested_at=?, "
                "restart_requested_by_pid=?, restart_reason=?, status=?, status_json=?, runtime_json=? "
                "WHERE node_name=? AND job_id=?",
                (
                    generation,
                    requested_at,
                    requested_by_pid or os.getpid(),
                    reason,
                    QUEUED,
                    json.dumps(status_extra),
                    json.dumps(runtime, ensure_ascii=False, separators=(",", ":")) if runtime else None,
                    node_name,
                    job_id,
                ),
            )
            self._increment_job_restart_revision(connection)
        self.append_job_event(
            node_name,
            job_id,
            "queued",
            previous_status=previous_status,
            status=QUEUED,
        )
        self.append_job_event(
            node_name,
            job_id,
            "restart_requested",
            previous_generation=previous_generation,
            generation=generation,
            reason=reason,
            requested_by_pid=requested_by_pid or os.getpid(),
        )

        base = self.job_base_dir(node_name, job_id)
        self._remove_restart_artifact(base / "output.json")
        return {
            "node": node_name,
            "job_id": job_id,
            "previous_generation": previous_generation,
            "generation": generation,
            "requested_at": requested_at,
        }

    def request_job_restart(
        self,
        node_name: str,
        job_id: int,
        *,
        requested_by_pid: int | None = None,
        reason: str = "manual restart",
    ) -> dict[str, Any]:
        if not self.job_exists(node_name, job_id):
            raise FileNotFoundError(f"Job does not exist: {node_name}/{job_id}")
        with self.filesystem_interprocess_lock(
            "execution-fences",
            self.job_execution_lock_name(node_name, job_id),
        ):
            return self._request_job_restart_locked(
                node_name,
                job_id,
                requested_by_pid=requested_by_pid,
                reason=reason,
                require_running_execution=False,
            )

    def request_active_job_restart(
        self,
        node_name: str,
        job_id: int,
        *,
        requested_by_pid: int | None = None,
        reason: str = "second-terminal active-job restart",
    ) -> dict[str, Any]:
        if not self.job_exists(node_name, job_id):
            raise FileNotFoundError(f"Job does not exist: {node_name}/{job_id}")
        with self.filesystem_interprocess_lock(
            "execution-fences",
            self.job_execution_lock_name(node_name, job_id),
        ):
            return self._request_job_restart_locked(
                node_name,
                job_id,
                requested_by_pid=requested_by_pid,
                reason=reason,
                require_running_execution=True,
            )
