"""Capture and commit producer-aware job preparation inside a caller's transaction."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class PreparationJob:
    job_id: int
    parent_json: str | None
    status: str
    generation: int
    active_execution_id: str | None
    instance_id: str
    last_execution_id: str | None
    active_pid: int | None = None
    active_thread_id: int | None = None
    active_started_at: str | None = None
    created_by_execution_id: str | None = None

    @property
    def producer(self):
        parent = json.loads(self.parent_json) if self.parent_json else None
        members = parent.get('_mwf_from_component') if isinstance(parent, dict) else None
        return tuple(members) if isinstance(members, list) and members else None


@dataclass(frozen=True)
class NodeJobPreparation:
    node: str
    jobs: tuple[PreparationJob, ...]
    orphan_creations: tuple[tuple, ...]
    delete_ids: tuple[int, ...]
    reset_ids: tuple[int, ...]
    orphan_ids: tuple[int, ...]
    clear_output: bool
    mark_queued: bool


def _read_jobs(connection, node):
    return tuple(PreparationJob(*row) for row in connection.execute(
        'SELECT j.job_id, j.parent_json, j.status, j.generation, j.active_execution_id, '
        'i.instance_id, i.last_execution_id, j.active_pid, j.active_thread_id, j.active_started_at, '
        'i.created_by_execution_id '
        'FROM jobs AS j LEFT JOIN job_instances AS i USING(node_name, job_id) '
        'WHERE j.node_name=? ORDER BY j.job_id', (node,),
    ))


def _read_orphan_creations(connection, node):
    return tuple(tuple(row) for row in connection.execute(
        "SELECT events.event_id, events.job_id, events.data_json FROM job_events AS events "
        "WHERE events.node_name=? AND events.event='created' "
        'AND NOT EXISTS(SELECT 1 FROM jobs WHERE jobs.node_name=events.node_name AND jobs.job_id=events.job_id) '
        "AND events.event_id=(SELECT MAX(previous.event_id) FROM job_events AS previous "
        "WHERE previous.node_name=events.node_name AND previous.job_id=events.job_id AND previous.event='created') "
        'ORDER BY events.job_id', (node,),
    ))


def read_job_preparation(storage, nodes, producers, *, reset_retained: bool, preserve_external: bool):
    """Read exact job instances and orphan provenance from one ordered snapshot."""
    connection = storage.db_connection()
    connection.execute('SAVEPOINT mwf_job_preparation')
    try:
        result = []
        for node in nodes:
            node = storage.validate_node_name(node)
            jobs = _read_jobs(connection, node)
            orphans = _read_orphan_creations(connection, node)
            for job in jobs:
                if (type(job.instance_id) is not str or len(job.instance_id) != 32
                        or any(character not in '0123456789abcdef' for character in job.instance_id)):
                    raise RuntimeError(f'Preparation requires an exact job instance: {node}/{job.job_id}')
                if job.last_execution_id is not None or job.created_by_execution_id is not None:
                    storage._read_job_owner_observation(connection, node, job.job_id)
            delete_ids = tuple(job.job_id for job in jobs if job.producer in producers)
            deleted = set(delete_ids)
            retained = [job for job in jobs if job.job_id not in deleted]
            reset_ids = tuple(job.job_id for job in retained
                              if reset_retained and (not preserve_external or job.producer is None))
            reset = set(reset_ids)
            orphan_ids = []
            for _, job_id, data_json in orphans:
                data = storage._decode_event_data(data_json)
                producer = None if data is None else data.get('producer_component')
                if isinstance(producer, list) and tuple(producer) in producers:
                    orphan_ids.append(job_id)
            result.append(NodeJobPreparation(
                node, jobs, orphans, delete_ids, reset_ids, tuple(orphan_ids),
                reset_retained and not any(job.job_id not in reset for job in retained), reset_retained,
            ))
        return tuple(result)
    finally:
        connection.execute('RELEASE SAVEPOINT mwf_job_preparation')


def apply_job_preparation(connection, preparations, *, keep_trace: bool):
    """Recheck captured identities and apply all job changes without committing."""
    for plan in preparations:
        if _read_jobs(connection, plan.node) != plan.jobs or _read_orphan_creations(connection, plan.node) != plan.orphan_creations:
            raise RuntimeError('Jobs changed during full preparation: ' + plan.node)
        affected = set(plan.delete_ids) | set(plan.reset_ids)
        if any(job.active_execution_id is not None or job.status == 'running' or job.active_pid is not None
               or job.active_thread_id is not None or job.active_started_at is not None
               for job in plan.jobs if job.job_id in affected):
            raise RuntimeError('Preparation cannot change active jobs: ' + plan.node)
    now = datetime.now().isoformat(timespec='milliseconds')
    for plan in preparations:
        if not keep_trace:
            connection.executemany(
                'DELETE FROM job_events WHERE node_name=? AND job_id=?',
                [(plan.node, job_id) for job_id in set(plan.delete_ids) | set(plan.reset_ids) | set(plan.orphan_ids)],
            )
        connection.executemany(
            'DELETE FROM idempotency WHERE node_name=? AND job_id=?',
            [(plan.node, job_id) for job_id in plan.delete_ids],
        )
        deleted = connection.executemany(
            'DELETE FROM jobs WHERE node_name=? AND job_id=?', [(plan.node, job_id) for job_id in plan.delete_ids],
        ).rowcount
        if deleted != len(plan.delete_ids):
            raise RuntimeError('Prepared job deletion was not applied: ' + plan.node)
        reset = connection.executemany(
            "UPDATE jobs SET status='queued', status_json='{}', runtime_json=NULL, active_execution_id=NULL, "
            'active_pid=NULL, active_thread_id=NULL, active_started_at=NULL, restart_requested_at=NULL, '
            'restart_requested_by_pid=NULL, restart_reason=NULL WHERE node_name=? AND job_id=?',
            [(plan.node, job_id) for job_id in plan.reset_ids],
        ).rowcount
        cleared = connection.executemany(
            'UPDATE job_instances SET last_execution_id=NULL WHERE node_name=? AND job_id=?',
            [(plan.node, job_id) for job_id in plan.reset_ids],
        ).rowcount
        if reset != len(plan.reset_ids) or cleared != len(plan.reset_ids):
            raise RuntimeError('Prepared job reset was not applied: ' + plan.node)
        previous = {job.job_id: job.status for job in plan.jobs}
        connection.executemany(
            "INSERT INTO job_events(node_name, job_id, time, event, data_json) VALUES(?, ?, ?, 'queued', ?)",
            [(plan.node, job_id, now, json.dumps({'previous_status': previous[job_id], 'status': 'queued'},
                                               separators=(',', ':'))) for job_id in plan.reset_ids],
        )
        if plan.delete_ids:
            connection.execute(
                'INSERT INTO job_sequences(node_name, next_job_id) '
                'SELECT ?, COALESCE(MAX(job_id), 0)+1 FROM jobs WHERE node_name=? '
                'ON CONFLICT(node_name) DO UPDATE SET next_job_id=excluded.next_job_id', (plan.node, plan.node),
            )
        if plan.mark_queued:
            connection.execute(
                "INSERT INTO nodes(node_name, status) VALUES(?, 'queued') "
                'ON CONFLICT(node_name) DO UPDATE SET status=excluded.status, '
                'updated_at=CURRENT_TIMESTAMP WHERE nodes.status IS NOT excluded.status', (plan.node,),
            )
