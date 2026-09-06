from __future__ import annotations

import json
import os
import time
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import datetime
from threading import Lock, get_ident
from typing import Any
from uuid import uuid4

from micro_workflow_manager.models import QUEUED, RUNNING
from micro_workflow_manager.component_identity import decode_component_key, encode_component_key


from .priorities import ADMISSION_PRIORITY


@dataclass(slots=True, frozen=True)
class ExecutionClaimBatch:
    node_name: str
    job_ids: tuple[int, ...]
    execution_ids: tuple[str, ...]
    started_at: str
    pid: int
    thread_id: int
    event_time: str
    task_started_data: dict[str, Any] | None = None
    task_started_mask: tuple[bool, ...] | None = None
    session_id: str | None = None
    component: tuple[str, ...] | None = None

    @property
    def mutation_weight(self) -> int:
        return len(self.job_ids)


class JobExecutionClaimStorageMixin:
    """Runtime state plus execution-lease claiming and release."""

    def _init_job_execution_claim_state(self) -> None:
        self._claim_batch_lock = Lock()
        self._claim_batches: dict[
            tuple[str, int, str | None, tuple[str, ...] | None],
            list[tuple[int, str, Future[tuple[int, str]]]],
        ] = {}

    def job_execution_lock_name(self, node_name: str, job_id: int) -> str:
        self.validate_node_name(node_name)
        self.validate_job_id(job_id)
        return f"job-{node_name}-{job_id}-execution"

    def normalize_job_control(self, data: Any) -> dict[str, Any]:
        if not isinstance(data, dict):
            data = {}
        try:
            generation = int(data.get("generation", 0) or 0)
        except (TypeError, ValueError):
            generation = 0
        return {**data, "version": 1, "generation": max(0, generation)}

    def read_job_control(self, node_name: str, job_id: int) -> dict[str, Any]:
        row = self.db_connection().execute(
            "SELECT generation, active_execution_id, active_pid, active_thread_id, "
            "active_started_at, restart_requested_at, restart_requested_by_pid, restart_reason "
            "FROM jobs WHERE node_name=? AND job_id=?",
            (node_name, self.validate_job_id(job_id)),
        ).fetchone()
        if row is None:
            return self.normalize_job_control({})
        return self.normalize_job_control(dict(row))

    def current_job_generation(self, node_name: str, job_id: int) -> int:
        return int(self.read_job_control(node_name, job_id)["generation"])

    @staticmethod
    def _execution_claim_owner_error(connection, batch: ExecutionClaimBatch, checked: dict):
        if batch.session_id is None:
            return None
        if batch.node_name not in batch.component:
            return RuntimeError(f'Claimed node {batch.node_name} is not a member of component {batch.component!r}')
        key = (batch.session_id, encode_component_key(batch.component))
        if key not in checked:
            checked[key] = connection.execute(
                'SELECT session.status, reservation.session_id AS owner, selected.component_key AS selected_key '
                'FROM execution_sessions AS session '
                'LEFT JOIN component_reservations AS reservation ON reservation.component_key=? '
                'LEFT JOIN session_components AS selected '
                'ON selected.session_id=session.session_id AND selected.component_key=reservation.component_key '
                'WHERE session.session_id=?', (key[1], key[0]),
            ).fetchone()
        row = checked[key]
        if row is None or row['status'] != 'running':
            return RuntimeError('Job claims require an existing running session: ' + batch.session_id)
        if row['owner'] != batch.session_id:
            return RuntimeError(f'Component {batch.component!r} reservation belongs to {row["owner"]!r}, '
                                f'not claiming session {batch.session_id}')
        if row['selected_key'] is None:
            return RuntimeError(f'Component {batch.component!r} is outside session {batch.session_id} selected scope')
        return None

    @staticmethod
    def _apply_execution_claim_batches(connection, batches: list[ExecutionClaimBatch]):
        """Claim simultaneous node bursts with one set of SQL operations.

        Hoeflein members commonly reach the mutation writer within the same few
        milliseconds. Treating every node as an unrelated savepoint preserved
        correctness but made a ten-node startup wave pay the query/update/event
        setup cost ten times. This grouped mutation validates all requested rows,
        then performs one ``executemany`` for leases and one for events across
        every valid node burst in the collection window.
        """
        if not batches:
            return []

        rows_by_node: dict[str, dict[int, Any]] = {}
        requested_by_node: dict[str, set[int]] = {}
        for batch in batches:
            requested_by_node.setdefault(batch.node_name, set()).update(batch.job_ids)

        for node_name, requested_ids in requested_by_node.items():
            rows = []
            ordered_ids = sorted(requested_ids)
            for offset in range(0, len(ordered_ids), 500):
                chunk = ordered_ids[offset:offset + 500]
                placeholders = ",".join("?" for _ in chunk)
                rows.extend(
                    connection.execute(
                        "SELECT j.job_id, j.generation, j.status, j.active_execution_id, i.instance_id FROM jobs AS j "
                        "LEFT JOIN job_instances AS i USING(node_name, job_id) "
                        f"WHERE j.node_name=? AND j.job_id IN ({placeholders})",
                        [node_name, *chunk],
                    ).fetchall()
                )
            rows_by_node[node_name] = {int(row["job_id"]): row for row in rows}

        updates = []
        owners = []
        events = []
        outcomes = []
        checked_owners = {}
        accepted_addresses = set()
        for batch in batches:
            owner_error = JobExecutionClaimStorageMixin._execution_claim_owner_error(connection, batch, checked_owners)
            if owner_error is not None:
                outcomes.append((False, owner_error))
                continue
            node_rows = rows_by_node[batch.node_name]
            missing = [job_id for job_id in batch.job_ids if job_id not in node_rows]
            if missing:
                outcomes.append((False, FileNotFoundError(
                    f"Job does not exist: {batch.node_name}/{missing[0]}"
                )))
                continue

            if any(type(node_rows[job_id]['instance_id']) is not str
                   or len(node_rows[job_id]['instance_id']) != 32
                   or any(character not in '0123456789abcdef' for character in node_rows[job_id]['instance_id'])
                   for job_id in batch.job_ids):
                outcomes.append((False, RuntimeError('Job claims require valid current job instances')))
                continue

            if any(node_rows[job_id]['active_execution_id'] is not None
                   or (batch.node_name, job_id) in accepted_addresses for job_id in batch.job_ids):
                outcomes.append((False, RuntimeError('Jobs already claimed cannot receive another active execution')))
                continue

            results = []
            mask = batch.task_started_mask
            if mask is not None and len(mask) != len(batch.job_ids):
                outcomes.append((False, ValueError("task_started_mask length mismatch")))
                continue
            accepted_addresses.update((batch.node_name, job_id) for job_id in batch.job_ids)
            for index, (job_id, execution_id) in enumerate(zip(batch.job_ids, batch.execution_ids)):
                row = node_rows[job_id]
                generation = int(row["generation"])
                previous_status = str(row["status"])
                status_extra = {
                    "started_at": batch.started_at,
                    "generation": generation,
                    "execution_id": execution_id,
                    "pid": batch.pid,
                }
                owner_data = {}
                if batch.session_id is not None:
                    owner_data = {'session_id': batch.session_id, 'component': batch.component}
                    owners.append((execution_id, batch.node_name, job_id, generation,
                                   batch.session_id, encode_component_key(batch.component), row['instance_id']))
                updates.append(
                    (
                        execution_id,
                        batch.pid,
                        batch.thread_id,
                        batch.started_at,
                        RUNNING,
                        json.dumps(status_extra, ensure_ascii=False, separators=(",", ":")),
                        batch.node_name,
                        job_id,
                        generation,
                    )
                )
                events.append(
                    (
                        batch.node_name,
                        job_id,
                        batch.event_time,
                        "started",
                        json.dumps(
                            {
                                "previous_status": previous_status,
                                "status": RUNNING,
                                **status_extra,
                                **owner_data,
                            },
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                    )
                )
                record_task_started = (
                    batch.task_started_data is not None
                    and (mask is None or mask[index])
                )
                if record_task_started:
                    events.append(
                        (
                            batch.node_name,
                            job_id,
                            batch.event_time,
                            "task_started",
                            json.dumps(
                                batch.task_started_data,
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ),
                        )
                    )
                results.append((generation, execution_id))
            outcomes.append((True, results))

        if updates:
            changed = connection.executemany(
                "UPDATE jobs SET active_execution_id=?, active_pid=?, "
                "active_thread_id=?, active_started_at=?, "
                "restart_requested_at=NULL, restart_requested_by_pid=NULL, "
                "restart_reason=NULL, runtime_json=NULL, status=?, status_json=? "
                "WHERE node_name=? AND job_id=? AND generation=? AND active_execution_id IS NULL",
                updates,
            ).rowcount
            if changed != len(updates):
                raise RuntimeError('Job claim could not acquire every requested active execution')
            if owners:
                connection.executemany(
                    "INSERT INTO job_execution_owners(execution_id, node_name, job_id, generation, session_id, component_key, job_instance_id) "
                    "VALUES(?, ?, ?, ?, ?, ?, ?)", owners,
                )
                changed = connection.executemany(
                    'UPDATE job_instances SET last_execution_id=? WHERE node_name=? AND job_id=?',
                    [(owner[0], owner[1], owner[2]) for owner in owners],
                ).rowcount
                if changed != len(owners):
                    raise RuntimeError('Job claims require a current job instance for every owner')
            connection.executemany(
                "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
                "VALUES(?, ?, ?, ?, ?)",
                events,
            )
        return outcomes

    def claim_job_execution(
        self,
        node_name: str,
        job_id: int,
        *,
        started_at: str,
        priority: int = ADMISSION_PRIORITY,
        session_id: str | None = None,
        component=None,
    ) -> tuple[int, str]:
        node_name = self.validate_node_name(node_name)
        job_id = self.validate_job_id(job_id)
        future: Future[tuple[int, str]] = Future()
        if session_id is not None:
            self._session_text(session_id, 'session_id')
        component = self._session_component(component) if component is not None else None
        key = (node_name, priority, session_id, component)
        with self._claim_batch_lock:
            batch = self._claim_batches.get(key)
            leader = batch is None
            if batch is None:
                batch = []
                self._claim_batches[key] = batch
            batch.append((job_id, started_at, future))

        if leader:
            # Preloaded threaded workers reach this point together. A tiny
            # collection window turns their individual restart leases into one
            # SQL update/event batch without preclaiming beyond max_threads.
            time.sleep(0.001)
            with self._claim_batch_lock:
                requests = self._claim_batches.pop(key)
            try:
                leases = self.claim_job_executions_batch(
                    node_name,
                    [request[0] for request in requests],
                    started_at=requests[0][1],
                    priority=priority,
                    session_id=session_id,
                    component=component,
                )
            except BaseException as error:
                for _job_id, _started_at, request_future in requests:
                    request_future.set_exception(error)
            else:
                for lease, (_job_id, _started_at, request_future) in zip(
                    leases,
                    requests,
                ):
                    request_future.set_result(lease)
        return future.result()

    def claim_job_executions_batch(
        self,
        node_name: str,
        job_ids: list[int],
        *,
        started_at: str,
        priority: int = ADMISSION_PRIORITY,
        task_started_data: dict[str, Any] | None = None,
        task_started_mask: list[bool] | tuple[bool, ...] | None = None,
        session_id: str | None = None,
        component=None,
    ) -> list[tuple[int, str]]:
        """Claim one preloaded API admission burst in one state mutation."""
        node_name = self.validate_node_name(node_name)
        normalized = [self.validate_job_id(job_id) for job_id in job_ids]
        if session_id is not None:
            self._session_text(session_id, 'session_id')
        session_capable = self._metadata_value('database_schema_version') == '5'
        if not session_capable and (session_id is not None or component is not None):
            raise RuntimeError('Explicit claim ownership requires a session-capable database')
        if session_capable and (session_id is None or component is None):
            raise RuntimeError('Job claims require explicit session_id and component in session-capable storage')
        if not normalized:
            return []
        if len(normalized) != len(set(normalized)):
            raise ValueError("job_ids contains duplicates")
        batch = ExecutionClaimBatch(
            node_name=node_name,
            job_ids=tuple(normalized),
            execution_ids=tuple(uuid4().hex for _ in normalized),
            started_at=started_at,
            pid=os.getpid(),
            thread_id=get_ident(),
            event_time=datetime.now().isoformat(timespec="milliseconds"),
            task_started_data=(dict(task_started_data) if task_started_data is not None else None),
            task_started_mask=(
                tuple(bool(value) for value in task_started_mask)
                if task_started_mask is not None
                else None
            ),
            session_id=session_id,
            component=self._session_component(component) if component is not None else None,
        )
        return self.submit_grouped_db_mutation(
            ("execution-claims", priority),
            batch,
            self._apply_execution_claim_batches,
            wait=True,
            priority=priority,
            collect_seconds=0.003,
        )

    def get_job_execution_owner(self, execution_id: str) -> dict | None:
        self._require_execution_session_storage()
        row = self.db_connection().execute(
            'SELECT * FROM job_execution_owners WHERE execution_id=?', (execution_id,),
        ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result['component'] = decode_component_key(result.pop('component_key'))
        return result

    def read_job_current_owner(self, node_name: str, job_id: int) -> dict | None:
        """Read the active claim or explicit last committed claim for this job."""
        self._require_execution_session_storage()
        node_name = self.validate_node_name(node_name)
        job_id = self.validate_job_id(job_id)
        return self._read_job_current_owner(self.db_connection(), node_name, job_id)

    @staticmethod
    def _read_job_current_owner(connection, node_name: str, job_id: int) -> dict | None:
        observation = JobExecutionClaimStorageMixin._read_job_owner_observation(connection, node_name, job_id)
        return None if observation is None else observation['owner']

    def read_job_owner_observation(self, node_name: str, job_id: int) -> dict | None:
        """Read one coherent job, ownership, and owning-session observation."""
        self._require_execution_session_storage()
        node_name = self.validate_node_name(node_name)
        job_id = self.validate_job_id(job_id)
        return self._read_job_owner_observation(self.db_connection(), node_name, job_id)

    @staticmethod
    def _read_job_owner_observation(connection, node_name: str, job_id: int) -> dict | None:
        row = connection.execute(
            'SELECT j.generation AS job_generation, j.status AS job_status, '
            'j.active_execution_id, j.active_pid, j.active_started_at, '
            'i.instance_id, i.last_execution_id, o.*, s.session_id AS owner_session, '
            'selected.session_id AS selected_session, '
            's.session_kind AS owner_session_kind, s.status AS owner_session_status, '
            's.parent_session_id AS owner_parent_session, s.outcome AS owner_session_outcome, '
            's.hostname AS owner_hostname, s.pid AS owner_pid, '
            's.process_identity AS owner_process_identity, s.heartbeat_at AS owner_heartbeat, '
            's.started_at AS owner_started_at, s.finished_at AS owner_finished_at '
            'FROM jobs AS j LEFT JOIN job_instances AS i USING(node_name, job_id) '
            'LEFT JOIN job_execution_owners AS o '
            'ON o.execution_id=COALESCE(j.active_execution_id, i.last_execution_id) '
            'LEFT JOIN execution_sessions AS s ON s.session_id=o.session_id '
            'LEFT JOIN session_components AS selected '
            'ON selected.session_id=o.session_id AND selected.component_key=o.component_key '
            'WHERE j.node_name=? AND j.job_id=?', (node_name, job_id),
        ).fetchone()
        if row is None:
            return None
        instance = row['instance_id']
        if (type(instance) is not str or len(instance) != 32
                or any(character not in '0123456789abcdef' for character in instance)):
            raise RuntimeError(f'Missing or damaged current job instance for {node_name}/{job_id}')
        active = row['active_execution_id']
        last = row['last_execution_id']
        if active is not None and active != last:
            raise RuntimeError(f'Ambiguous current execution ownership for {node_name}/{job_id}')
        observation = {
            'job_instance_id': instance, 'generation': row['job_generation'],
            'status': row['job_status'], 'active_execution_id': active,
            'active_pid': row['active_pid'], 'active_started_at': row['active_started_at'],
            'state': 'unclaimed', 'owner': None, 'session': None,
        }
        if active is None and last is None:
            return observation
        if (row['execution_id'] is None or row['owner_session'] is None or row['selected_session'] is None
                or row['node_name'] != node_name or row['job_id'] != job_id
                or row['job_instance_id'] != instance
                or type(row['generation']) is not int or row['generation'] < 0
                or row['generation'] > row['job_generation']
                or (active is not None and row['generation'] != row['job_generation'])):
            raise RuntimeError(f'Damaged current execution ownership for {node_name}/{job_id}')
        component = decode_component_key(row['component_key'])
        if node_name not in component:
            raise RuntimeError(f'Damaged current execution component for {node_name}/{job_id}')
        observation['owner'] = {
            'execution_id': row['execution_id'], 'node_name': row['node_name'],
            'job_id': row['job_id'], 'generation': row['generation'],
            'session_id': row['session_id'], 'component': component,
            'job_instance_id': row['job_instance_id'],
        }
        observation['state'] = 'active' if active is not None else 'last'
        observation['session'] = {
            'session_id': row['owner_session'], 'session_kind': row['owner_session_kind'],
            'status': row['owner_session_status'], 'parent_session_id': row['owner_parent_session'],
            'outcome': row['owner_session_outcome'],
            'hostname': row['owner_hostname'], 'pid': row['owner_pid'],
            'process_identity': row['owner_process_identity'], 'heartbeat_at': row['owner_heartbeat'],
            'started_at': row['owner_started_at'], 'finished_at': row['owner_finished_at'],
        }
        return observation

    def release_unstarted_job_execution(
        self,
        node_name: str,
        job_id: int,
        generation: int,
        execution_id: str,
    ) -> None:
        """Requeue a preclaimed item that a failed burst never started."""
        job_id = self.validate_job_id(job_id)
        event_time = datetime.now().isoformat(timespec="milliseconds")

        def release(connection):
            changed = connection.execute(
                "UPDATE jobs SET status=?, status_json='{}', "
                "active_execution_id=NULL, active_pid=NULL, "
                "active_thread_id=NULL, active_started_at=NULL "
                "WHERE node_name=? AND job_id=? AND generation=? "
                "AND active_execution_id=?",
                (QUEUED, node_name, job_id, generation, execution_id),
            ).rowcount
            if changed:
                connection.execute(
                    "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
                    "VALUES(?, ?, ?, 'queued', ?)",
                    (
                        node_name,
                        job_id,
                        event_time,
                        json.dumps(
                            {
                                "previous_status": RUNNING,
                                "status": QUEUED,
                                "reason": "preclaimed burst was not started",
                            },
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                    ),
                )
            return bool(changed)

        changed = self.submit_db_mutation(release, priority=0)
        if changed:
            self.notify_queue_change(node_name)
