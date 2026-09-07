from __future__ import annotations

import hashlib
import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Iterable
from uuid import uuid4

from micro_workflow_manager.models import Job, QUEUED
from .preparation_guards import refuse_receiver_mutation


class JobBatchStorageMixin:
    """Prepared and bulk job creation."""

    def prepare_jobs_batch(self, jobs: list[Job]) -> list[Path]:
        """Claim unpublished job directories and write their inputs before SQL."""
        if not jobs:
            return []
        node_names = {job.node_name for job in jobs}
        if len(node_names) != 1:
            raise ValueError("prepare_jobs_batch requires jobs for one node")
        ids = [self.validate_job_id(job.job_id) for job in jobs]
        if len(set(ids)) != len(ids):
            raise ValueError("batch contains duplicate job ids")
        for job in jobs:
            self.json_text(Path("input.json"), job.params)

        written_dirs = [self.job_base_dir(job.node_name, job.job_id) for job in jobs]
        prepared_jobs = []
        prepared_lock = Lock()

        def write_job_payload(job: Job) -> None:
            self.job_base_dir(job.node_name, job.job_id).mkdir(exist_ok=False)
            with prepared_lock:
                prepared_jobs.append(job)
            input_path = self.input_file(job.node_name, job.job_id)
            input_text = self.json_text(input_path, job.params)

            def write_new_payload():
                with input_path.open("x", encoding="utf-8") as file:
                    file.write(input_text)

            self.retry_fs(write_new_payload)

        try:
            if len(jobs) < 8:
                for job in jobs:
                    write_job_payload(job)
            else:
                # Payload paths are disjoint and registration has not happened
                # yet.  Serial tiny-file creation is particularly expensive on
                # Windows and made a 2,400-job fan-out take about a minute even
                # though its single SQLite commit was fast.
                with ThreadPoolExecutor(
                    max_workers=min(32, len(jobs)),
                    thread_name_prefix="mwf-job-publish",
                ) as executor:
                    list(executor.map(write_job_payload, jobs))
        except BaseException as error:
            self.discard_prepared_jobs(prepared_jobs, publication_error=error)
            raise
        return written_dirs

    def discard_prepared_jobs(self, jobs: list[Job], *, publication_error: BaseException | None = None) -> None:
        try:
            self._discard_unregistered_job_inputs(jobs)
        except BaseException as cleanup_error:
            if publication_error is None:
                raise
            publication_error.__notes__ = [
                *getattr(publication_error, '__notes__', ()),
                f'Job publication retained prepared files because cleanup failed: {cleanup_error}',
            ]

    def _discard_unregistered_job_inputs(self, jobs):
        by_node = {}
        for job in jobs:
            by_node.setdefault(job.node_name, []).append(job.job_id)
        for node_name, job_ids in by_node.items():
            # A failed caller can observe a successful COMMIT followed by a
            # notification error. Protect any live job, including a replacement
            # at the same numeric address. Serialize with explicit publishers
            # and the SQLite writer while deciding which directories are unused.
            with self.interprocess_lock(f'node-{node_name}-jobs'), self._database_write_lock():
                connection = self._new_db_connection()
                try:
                    connection.execute('BEGIN IMMEDIATE')
                    for job_id in job_ids:
                        live = connection.execute(
                            'SELECT 1 FROM jobs WHERE node_name=? AND job_id=?', (node_name, job_id),
                        ).fetchone()
                        if live is None:
                            shutil.rmtree(self.job_base_dir(node_name, job_id), ignore_errors=True)
                finally:
                    connection.rollback()
                    connection.close()

    def commit_prepared_jobs_batch(
        self,
        jobs: list[Job],
        *,
        idempotency_keys: list[str | None] | None = None,
        producer_execution_id: str | None = None,
        expected_shape: str | None = None,
    ) -> list[Job]:
        """Commit prepared job payloads with one SQLite transaction."""
        if not jobs:
            return []
        node_names = {job.node_name for job in jobs}
        if len(node_names) != 1:
            raise ValueError("commit_prepared_jobs_batch requires jobs for one node")
        node_name = next(iter(node_names))
        if idempotency_keys is None:
            idempotency_keys = [None] * len(jobs)
        if len(idempotency_keys) != len(jobs):
            raise ValueError("idempotency_keys must match jobs length")

        ids = [self.validate_job_id(job.job_id) for job in jobs]
        if len(set(ids)) != len(ids):
            raise ValueError("batch contains duplicate job ids")
        existing = set(self.list_job_ids(node_name))
        collisions = sorted(existing.intersection(ids))
        if collisions:
            raise ValueError(f"Job {node_name}/{collisions[0]} already exists")

        idempotency_rows = []
        seen_key_hashes: set[str] = set()
        for job, key in zip(jobs, idempotency_keys):
            if key is None:
                continue
            key_hash = self.idempotency_key_hash(key)
            if key_hash in seen_key_hashes:
                raise ValueError("batch contains duplicate idempotency keys")
            seen_key_hashes.add(key_hash)
            idempotency_rows.append((node_name, key_hash, key, job.job_id))

        job_rows = []
        event_rows = []
        event_time = datetime.now().isoformat(timespec="milliseconds")
        for job in jobs:
            parent_json = self._job_parent_json(job)
            job_rows.append(
                (job.node_name, job.job_id, parent_json, job.created_at, QUEUED)
            )
            event_rows.append(
                (
                    job.node_name,
                    job.job_id,
                    event_time,
                    "created",
                    json.dumps(
                        {
                            "status": QUEUED,
                            "parent": job.parent,
                            "producer_component": list(job.producer_component or ()),
                            "job_kind": job.job_kind,
                        },
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                )
            )

        with self.db_transaction() as connection:
            for job in jobs:
                self._validate_job_producer(connection, job, producer_execution_id)
            refuse_receiver_mutation(connection, node_name)
            connection.executemany(
                "INSERT INTO jobs(node_name, job_id, parent_json, created_at, status, status_json) "
                "VALUES(?, ?, ?, ?, ?, '{}')",
                job_rows,
            )
            for job in jobs:
                self._record_job_producer(connection, job.node_name, job.job_id, producer_execution_id)
            arrival_identity = self._mark_component_job_arrival(
                connection, node_name, jobs[0].job_id, expected_shape=expected_shape,
            )
            self.insert_job_created_events(
                connection,
                [
                    (node_name, job_id, event_time, data_json)
                    for node_name, job_id, event_time, _event_name, data_json in event_rows
                ],
            )
            if idempotency_rows:
                connection.executemany(
                    "INSERT INTO idempotency(node_name, key_hash, key_text, job_id) "
                    "VALUES(?, ?, ?, ?) "
                    "ON CONFLICT(node_name, key_hash) DO UPDATE SET "
                    "key_text=excluded.key_text, job_id=excluded.job_id",
                    idempotency_rows,
                )
            connection.execute(
                "INSERT INTO nodes(node_name, status) VALUES(?, ?) "
                "ON CONFLICT(node_name) DO UPDATE SET status=excluded.status, "
                "updated_at=CURRENT_TIMESTAMP",
                (node_name, QUEUED),
            )
            connection.execute(
                "INSERT INTO job_sequences(node_name, next_job_id) VALUES(?, ?) "
                "ON CONFLICT(node_name) DO UPDATE SET "
                "next_job_id=MAX(job_sequences.next_job_id, excluded.next_job_id)",
                (node_name, max(ids) + 1),
            )
        if arrival_identity is not None:
            self._component_arrival_latches[node_name] = arrival_identity
        self.notify_queue_change(node_name)
        return jobs

    def commit_prepared_jobs_batch_resolving_idempotency(
        self,
        jobs: list[Job],
        *,
        idempotency_keys: list[str | None] | None = None,
        producer_execution_id: str | None = None,
        expected_shape: str | None = None,
    ) -> tuple[list[Job], dict[str, int]]:
        """Atomically resolve idempotency races and commit the remaining jobs."""
        if not jobs:
            return [], {}
        if idempotency_keys is None:
            idempotency_keys = [None] * len(jobs)
        if len(idempotency_keys) != len(jobs):
            raise ValueError("idempotency_keys must match jobs length")
        node_names = {job.node_name for job in jobs}
        if len(node_names) != 1:
            raise ValueError("batch commit requires jobs for one node")
        node_name = next(iter(node_names))

        requested: dict[str, str] = {}
        for key in idempotency_keys:
            if key is None:
                continue
            key_hash = self.idempotency_key_hash(key)
            if key_hash in requested:
                raise ValueError("batch contains duplicate idempotency keys")
            requested[key_hash] = key

        arrival_identity = None
        with self.db_transaction() as connection:
            for job in jobs:
                self._validate_job_producer(connection, job, producer_execution_id)
            existing_by_key: dict[str, int] = {}
            if requested:
                hashes = list(requested)
                placeholders = ",".join("?" for _ in hashes)
                rows = connection.execute(
                    "SELECT i.key_hash, i.key_text, i.job_id "
                    "FROM idempotency AS i "
                    "JOIN jobs AS j ON j.node_name=i.node_name AND j.job_id=i.job_id "
                    f"WHERE i.node_name=? AND i.key_hash IN ({placeholders})",
                    [node_name, *hashes],
                ).fetchall()
                for row in rows:
                    key_hash = str(row["key_hash"])
                    key = requested[key_hash]
                    if str(row["key_text"]) != key:
                        raise RuntimeError(
                            f"idempotency hash collision for node {node_name!r}"
                        )
                    existing_by_key[key] = int(row["job_id"])

            commit_jobs: list[Job] = []
            commit_keys: list[str | None] = []
            for job, key in zip(jobs, idempotency_keys):
                if key is not None and key in existing_by_key:
                    continue
                commit_jobs.append(job)
                commit_keys.append(key)

            ids = [self.validate_job_id(job.job_id) for job in commit_jobs]
            if len(set(ids)) != len(ids):
                raise ValueError("batch contains duplicate job ids")
            if ids:
                placeholders = ",".join("?" for _ in ids)
                collision = connection.execute(
                    f"SELECT job_id FROM jobs WHERE node_name=? AND job_id IN ({placeholders}) LIMIT 1",
                    [node_name, *ids],
                ).fetchone()
                if collision is not None:
                    raise ValueError(f"Job {node_name}/{int(collision['job_id'])} already exists")

            event_time = datetime.now().isoformat(timespec="milliseconds")
            job_rows = []
            event_rows = []
            idempotency_rows = []
            for job, key in zip(commit_jobs, commit_keys):
                parent_json = self._job_parent_json(job)
                job_rows.append((job.node_name, job.job_id, parent_json, job.created_at, QUEUED))
                event_rows.append((
                    job.node_name,
                    job.job_id,
                    event_time,
                    "created",
                    json.dumps(
                        {
                            "status": QUEUED,
                            "parent": job.parent,
                            "producer_component": list(job.producer_component or ()),
                            "job_kind": job.job_kind,
                        },
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                ))
                if key is not None:
                    idempotency_rows.append((
                        node_name, self.idempotency_key_hash(key), key, job.job_id
                    ))

            if job_rows:
                refuse_receiver_mutation(connection, node_name)
                connection.executemany(
                    "INSERT INTO jobs(node_name, job_id, parent_json, created_at, status, status_json) "
                    "VALUES(?, ?, ?, ?, ?, '{}')",
                    job_rows,
                )
                for job in commit_jobs:
                    self._record_job_producer(connection, job.node_name, job.job_id, producer_execution_id)
                arrival_identity = self._mark_component_job_arrival(
                    connection, node_name, commit_jobs[0].job_id, expected_shape=expected_shape,
                )
                self.insert_job_created_events(
                    connection,
                    [
                        (node_name, job_id, event_time, data_json)
                        for node_name, job_id, event_time, _event_name, data_json in event_rows
                    ],
                )
                if idempotency_rows:
                    connection.executemany(
                        "INSERT INTO idempotency(node_name, key_hash, key_text, job_id) "
                        "VALUES(?, ?, ?, ?)",
                        idempotency_rows,
                    )
                connection.execute(
                    "INSERT INTO nodes(node_name, status) VALUES(?, ?) "
                    "ON CONFLICT(node_name) DO UPDATE SET status=excluded.status, "
                    "updated_at=CURRENT_TIMESTAMP",
                    (node_name, QUEUED),
                )
        if commit_jobs:
            if arrival_identity is not None:
                self._component_arrival_latches[node_name] = arrival_identity
            self.notify_queue_change(node_name)
        return commit_jobs, existing_by_key

    def create_jobs_batch(
        self,
        jobs: list[Job],
        *,
        idempotency_keys: list[str | None] | None = None,
        expected_shape: str | None = None,
    ) -> list[Job]:
        """Prepare and commit many jobs; callers may split the phases for concurrency."""
        if jobs:
            self.validate_job_receiver_shape(jobs[0].node_name, expected_shape=expected_shape)
        self.prepare_jobs_batch(jobs)
        try:
            return self.commit_prepared_jobs_batch(
                jobs, idempotency_keys=idempotency_keys, expected_shape=expected_shape,
            )
        except BaseException as error:
            self.discard_prepared_jobs(jobs, publication_error=error)
            raise

    def ensure_job(self, job: Job, *, expected_shape: str | None = None) -> Job:
        self.validate_job_id(job.job_id)
        self.json_text(Path("input.json"), job.params)
        if not self.job_exists(job.node_name, job.job_id):
            self.create_job(job, expected_shape=expected_shape)
            return job

        existing_params = self.read_json(self.input_file(job.node_name, job.job_id), default={})
        if existing_params != job.params:
            self.atomic_write_json(self.input_file(job.node_name, job.job_id), job.params)
            self.set_job_status(job.node_name, job.job_id, QUEUED)

        metadata = self.read_job_metadata(job.node_name, job.job_id)
        if metadata.get("parent") is not None:
            with self.db_transaction() as connection:
                connection.execute(
                    "UPDATE jobs SET parent_json=NULL WHERE node_name=? AND job_id=?",
                    (job.node_name, job.job_id),
                )
        return job
