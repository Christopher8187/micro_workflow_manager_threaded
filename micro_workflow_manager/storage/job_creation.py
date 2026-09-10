from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

from micro_workflow_manager.models import Job, QUEUED
from .auto_job_creation import AutoJobCreationStorageMixin
from .preparation_guards import refuse_receiver_mutation


class JobCreationStorageMixin(AutoJobCreationStorageMixin):
    """Single-job creation and idempotent commit paths."""

    def read_job_metadata(self, node_name: str, job_id: int) -> dict[str, Any]:
        job_id = self.validate_job_id(job_id)
        row = self.db_connection().execute(
            "SELECT * FROM jobs WHERE node_name=? AND job_id=?",
            (node_name, job_id),
        ).fetchone()
        if row is None:
            return {}
        raw_parent = json.loads(row["parent_json"]) if row["parent_json"] else None
        producer_component = None
        job_kind = None
        parent = raw_parent
        if isinstance(raw_parent, dict):
            raw_parent = dict(raw_parent)
            producer_component = raw_parent.pop("_mwf_from_component", raw_parent.pop("from_component", None))
            job_kind = raw_parent.pop("_mwf_job_kind", raw_parent.pop("job_kind", None))
            parent = raw_parent or None
        return {
            "job_id": int(row["job_id"]),
            "node_name": str(row["node_name"]),
            "parent": parent,
            "producer_component": tuple(producer_component) if isinstance(producer_component, list) else None,
            "job_kind": job_kind,
            "created_at": str(row["created_at"]),
        }

    def create_job(self, job: Job, *, producer_execution_id: str | None = None,
                   expected_shape: str | None = None, idempotency_key: str | None = None) -> Job:
        self.validate_job_id(job.job_id)
        input_text = self.json_text(Path("input.json"), job.params)
        if self.job_exists(job.node_name, job.job_id):
            raise ValueError(f"Job {job.node_name}/{job.job_id} already exists")
        self.validate_job_receiver_shape(job.node_name, expected_shape=expected_shape)
        created, actual_id = self._commit_single_job(
            job, idempotency_key=idempotency_key, producer_execution_id=producer_execution_id,
            expected_shape=expected_shape, input_text=input_text,
        )
        if not created:
            return self.load_job(job.node_name, actual_id)
        return job

    def commit_prepared_job_resolving_idempotency(
        self,
        job: Job,
        *,
        idempotency_key: str | None = None,
        producer_execution_id: str | None = None,
        expected_shape: str | None = None,
    ) -> tuple[bool, int]:
        """Register an already prepared input through the queue-state writer."""
        return self._commit_single_job(
            job, idempotency_key=idempotency_key, producer_execution_id=producer_execution_id,
            expected_shape=expected_shape,
        )

    def _commit_single_job(
        self, job, *, idempotency_key=None, producer_execution_id=None,
        expected_shape=None, input_text=None,
    ):
        """Resolve identity before creating a new input and its durable job."""
        job_id = self.validate_job_id(job.job_id)
        key_hash = (
            self.idempotency_key_hash(idempotency_key)
            if idempotency_key is not None
            else None
        )
        parent_json = self._job_parent_json(job)
        event_time = datetime.now().isoformat(timespec="milliseconds")
        event_data = json.dumps(
            {
                "status": QUEUED,
                "parent": job.parent,
                "producer_component": list(job.producer_component or ()),
                "job_kind": job.job_kind,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )

        arrival_identity = None
        created_input = None

        def publish(connection):
            nonlocal arrival_identity, created_input
            self._validate_job_producer(connection, job, producer_execution_id)
            if idempotency_key is not None:
                row = connection.execute(
                    "SELECT i.key_text, i.job_id FROM idempotency AS i "
                    "JOIN jobs AS j ON j.node_name=i.node_name AND j.job_id=i.job_id "
                    "WHERE i.node_name=? AND i.key_hash=?",
                    (job.node_name, key_hash),
                ).fetchone()
                if row is not None:
                    if str(row["key_text"]) != idempotency_key:
                        raise RuntimeError(
                            f"idempotency hash collision for node {job.node_name!r}"
                        )
                    return False, int(row["job_id"])

            collision = connection.execute(
                "SELECT 1 FROM jobs WHERE node_name=? AND job_id=?",
                (job.node_name, job_id),
            ).fetchone()
            if collision is not None:
                raise ValueError(f"Job {job.node_name}/{job_id} already exists")

            refuse_receiver_mutation(connection, job.node_name)
            self._refuse_interrupt_held_publication(
                connection, job.node_name, producer_execution_id,
            )
            if input_text is not None:
                input_directory = self.job_base_dir(job.node_name, job_id)
                input_directory.mkdir(exist_ok=False)
                created_input = input_directory
                input_path = self.input_file(job.node_name, job_id)
                with input_path.open('x', encoding='utf-8') as file:
                    file.write(input_text)

            connection.execute(
                "INSERT INTO jobs(node_name, job_id, parent_json, created_at, status, status_json) "
                "VALUES(?, ?, ?, ?, ?, '{}')",
                (job.node_name, job_id, parent_json, job.created_at, QUEUED),
            )
            self._record_job_producer(connection, job.node_name, job_id, producer_execution_id)
            arrival_identity = self._mark_component_job_arrival(
                connection, job.node_name, job_id, expected_shape=expected_shape,
            )
            self.insert_job_created_events(
                connection,
                [(job.node_name, job_id, event_time, event_data)],
            )
            if idempotency_key is not None:
                connection.execute(
                    "INSERT INTO idempotency(node_name, key_hash, key_text, job_id) "
                    "VALUES(?, ?, ?, ?)",
                    (job.node_name, key_hash, idempotency_key, job_id),
                )
            connection.execute(
                "INSERT INTO nodes(node_name, status) VALUES(?, ?) "
                "ON CONFLICT(node_name) DO UPDATE SET status=excluded.status, "
                "updated_at=CURRENT_TIMESTAMP WHERE nodes.status IS NOT excluded.status",
                (job.node_name, QUEUED),
            )
            connection.execute(
                "INSERT INTO job_sequences(node_name, next_job_id) VALUES(?, ?) "
                "ON CONFLICT(node_name) DO UPDATE SET "
                "next_job_id=MAX(job_sequences.next_job_id, excluded.next_job_id)",
                (job.node_name, job_id + 1),
            )
            return True, job_id

        def publish_with_cleanup(connection):
            nonlocal created_input
            try:
                return publish(connection)
            except BaseException as error:
                if created_input is not None:
                    try:
                        shutil.rmtree(created_input)
                        created_input = None
                    except BaseException as cleanup_error:
                        error.__notes__ = [*getattr(error, '__notes__', ()),
                                          f'Job input cleanup failed: {cleanup_error}']
                raise

        future = self.submit_db_mutation(publish_with_cleanup, priority=0, wait=False)
        try:
            result = future.result()
        except BaseException as error:
            while not future.done():
                try:
                    future.result()
                except BaseException:
                    pass
            if created_input is not None:
                self.discard_prepared_jobs([job], publication_error=error)
            raise
        if result[0]:
            if arrival_identity is not None:
                self._component_arrival_latches[job.node_name] = arrival_identity
            self.notify_queue_change(job.node_name)
        return result
