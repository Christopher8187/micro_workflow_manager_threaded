from __future__ import annotations

import hashlib
import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from micro_workflow_manager.models import Job, QUEUED


class RefreshableQueuedJobSource:
    """Incrementally expose queued jobs that appear while a node pump is active.

    The cursor follows SQLite insertion order rather than ``job_id`` order. Batch
    producers reserve IDs before writing payloads and may commit those batches out
    of ID order; ``rowid`` still advances in commit order, so no late lower-ID batch
    is skipped. Existing jobs are included on the first pull, and newly inserted
    queued jobs become visible to later pulls.
    """

    def __init__(self, storage, node_name: str):
        self.storage = storage
        self.node_name = storage.validate_node_name(node_name)
        self.last_rowid = 0

    def pull(self, max_items: int) -> list[int]:
        if type(max_items) is not int or max_items < 0:
            raise ValueError("max_items must be an integer >= 0")
        if max_items == 0:
            return []
        rows = self.storage.db_connection().execute(
            "SELECT rowid AS source_rowid, job_id FROM jobs "
            "WHERE node_name=? AND status=? AND rowid>? "
            "ORDER BY rowid LIMIT ?",
            (self.node_name, QUEUED, self.last_rowid, max_items),
        ).fetchall()
        if not rows:
            return []
        self.last_rowid = max(int(row["source_rowid"]) for row in rows)
        return [int(row["job_id"]) for row in rows]

    def __iter__(self):
        while True:
            job_ids = self.pull(512)
            if not job_ids:
                return
            yield from job_ids


class QueuedJobObjectSource:
    """Load a snapshot of queued job payloads in database-sized bursts."""

    def __init__(self, storage, node_name: str, job_ids: Iterable[int]):
        self.storage = storage
        self.node_name = storage.validate_node_name(node_name)
        self.job_ids = iter(job_ids)

    def __iter__(self):
        while True:
            batch = []
            for _ in range(64):
                try:
                    batch.append(next(self.job_ids))
                except StopIteration:
                    break
            if not batch:
                return
            yield from self.storage.load_jobs_batch(self.node_name, batch)


class RefreshableQueuedJobObjectSource:
    """Refreshable queue cursor that preloads each admission burst."""

    def __init__(self, storage, node_name: str):
        self.storage = storage
        self.node_name = storage.validate_node_name(node_name)
        self.job_ids = RefreshableQueuedJobSource(storage, self.node_name)

    def pull(self, max_items: int) -> list[Job]:
        job_ids = self.job_ids.pull(max_items)
        return self.storage.load_jobs_batch(self.node_name, job_ids)

    def __iter__(self):
        while True:
            jobs = self.pull(512)
            if not jobs:
                return
            yield from jobs


class JobFileStorageMixin:
    """Job metadata in SQLite; job inputs/outputs and returned files on disk."""

    def idempotency_key_hash(self, key: str) -> str:
        if not isinstance(key, str) or not key.strip():
            raise ValueError("idempotency_key must be a non-empty string")
        return hashlib.sha256(key.encode("utf-8")).hexdigest()

    def lookup_idempotent_job(self, node_name: str, key: str) -> Job | None:
        key_hash = self.idempotency_key_hash(key)
        row = self.db_connection().execute(
            "SELECT job_id FROM idempotency WHERE node_name=? AND key_hash=?",
            (node_name, key_hash),
        ).fetchone()
        if row is None or not self.job_exists(node_name, int(row["job_id"])):
            return None
        return self.load_job(node_name, int(row["job_id"]))

    def record_idempotent_job(self, node_name: str, key: str, job_id: int):
        job_id = self.validate_job_id(job_id)
        key_hash = self.idempotency_key_hash(key)
        with self.db_transaction() as connection:
            connection.execute(
                "INSERT INTO idempotency(node_name, key_hash, key_text, job_id) "
                "VALUES(?, ?, ?, ?) "
                "ON CONFLICT(node_name, key_hash) DO UPDATE SET "
                "key_text=excluded.key_text, job_id=excluded.job_id",
                (node_name, key_hash, key, job_id),
            )

    def lookup_idempotent_jobs_batch(
        self,
        node_name: str,
        keys: list[str | None],
    ) -> dict[str, Job]:
        """Resolve existing idempotent jobs with one targeted SQLite query."""
        requested = {
            self.idempotency_key_hash(key): key
            for key in keys
            if key is not None
        }
        if not requested:
            return {}
        hashes = list(requested)
        placeholders = ",".join("?" for _ in hashes)
        rows = self.db_connection().execute(
            "SELECT i.key_hash, i.key_text, i.job_id "
            "FROM idempotency AS i "
            "JOIN jobs AS j ON j.node_name=i.node_name AND j.job_id=i.job_id "
            f"WHERE i.node_name=? AND i.key_hash IN ({placeholders})",
            [node_name, *hashes],
        ).fetchall()
        result: dict[str, Job] = {}
        for row in rows:
            key_hash = str(row["key_hash"])
            requested_key = requested[key_hash]
            if str(row["key_text"]) != requested_key:
                raise RuntimeError(
                    f"idempotency hash collision for node {node_name!r}"
                )
            result[requested_key] = self.load_job(node_name, int(row["job_id"]))
        return result

    def next_job_id(self, node_name: str) -> int:
        row = self.db_connection().execute(
            "SELECT next_job_id FROM job_sequences WHERE node_name=?",
            (node_name,),
        ).fetchone()
        if row is not None:
            return int(row["next_job_id"])
        row = self.db_connection().execute(
            "SELECT COALESCE(MAX(job_id), 0) + 1 AS next_id FROM jobs WHERE node_name=?",
            (node_name,),
        ).fetchone()
        return int(row["next_id"])

    def reserve_job_ids(self, node_name: str, count: int) -> list[int]:
        if type(count) is not int or count < 1:
            raise ValueError("count must be a positive integer")
        node_name = self.validate_node_name(node_name)

        def reserve(connection):
            row = connection.execute(
                "SELECT next_job_id FROM job_sequences WHERE node_name=?",
                (node_name,),
            ).fetchone()
            if row is None:
                start = int(
                    connection.execute(
                        "SELECT COALESCE(MAX(job_id), 0) + 1 FROM jobs WHERE node_name=?",
                        (node_name,),
                    ).fetchone()[0]
                )
                connection.execute(
                    "INSERT INTO job_sequences(node_name, next_job_id) VALUES(?, ?)",
                    (node_name, start + count),
                )
            else:
                start = int(row["next_job_id"])
                connection.execute(
                    "UPDATE job_sequences SET next_job_id=? WHERE node_name=?",
                    (start + count, node_name),
                )
            return list(range(start, start + count))

        # Queue publication outranks claims/checkpoints while a live router is
        # still handing work to its component peers. This prevents consumer
        # startup from throttling the producer that is feeding it.
        return self.submit_db_mutation(reserve, priority=0)

    def advance_job_sequence(self, node_name: str, next_job_id: int) -> None:
        node_name = self.validate_node_name(node_name)
        next_job_id = self.validate_job_id(next_job_id)
        with self.db_transaction() as connection:
            connection.execute(
                "INSERT INTO job_sequences(node_name, next_job_id) VALUES(?, ?) "
                "ON CONFLICT(node_name) DO UPDATE SET "
                "next_job_id=MAX(job_sequences.next_job_id, excluded.next_job_id)",
                (node_name, next_job_id),
            )

    def rewind_job_sequence_to_available(self, node_name: str) -> int:
        """Reset a quiescent node's allocator to its first available tail ID.

        Fresh ``run``/``runfrom`` cleanup may deliberately delete jobs that will
        be recreated immediately. The high-fanout sequence allocator introduced
        in 0.3.10 otherwise keeps advancing and changes deterministic job IDs on
        every fresh run. Call this only while the active-run slot is held and no
        worker is registering jobs for the node.
        """
        node_name = self.validate_node_name(node_name)
        with self.db_transaction() as connection:
            next_id = int(
                connection.execute(
                    "SELECT COALESCE(MAX(job_id), 0) + 1 FROM jobs WHERE node_name=?",
                    (node_name,),
                ).fetchone()[0]
            )
            connection.execute(
                "INSERT INTO job_sequences(node_name, next_job_id) VALUES(?, ?) "
                "ON CONFLICT(node_name) DO UPDATE SET next_job_id=excluded.next_job_id",
                (node_name, next_id),
            )
        return next_id

    def job_exists(self, node_name: str, job_id: int) -> bool:
        job_id = self.validate_job_id(job_id)
        row = self.db_connection().execute(
            "SELECT 1 FROM jobs WHERE node_name=? AND job_id=?",
            (node_name, job_id),
        ).fetchone()
        return row is not None

    def default_job_spec_key(self, start_job_id: int, number: int) -> str:
        return f"{start_job_id}:{number}"

    def default_job_spec_current(
        self,
        node_name: str,
        *,
        start_job_id: int,
        number: int,
        params: dict[str, Any],
    ) -> bool:
        key = self.default_job_spec_key(start_job_id, number)
        row = self.db_connection().execute(
            "SELECT start_job_id, number, params_signature FROM default_job_specs "
            "WHERE node_name=? AND spec_key=?",
            (node_name, key),
        ).fetchone()
        if row is None:
            return False
        if (
            int(row["start_job_id"]) != start_job_id
            or int(row["number"]) != number
            or row["params_signature"] != self.json_signature(params)
        ):
            return False
        count = self.db_connection().execute(
            "SELECT COUNT(*) FROM jobs WHERE node_name=? AND job_id>=? AND job_id<?",
            (node_name, start_job_id, start_job_id + number),
        ).fetchone()[0]
        return int(count) == number

    def write_default_job_spec(
        self,
        node_name: str,
        *,
        start_job_id: int,
        number: int,
        params: dict[str, Any],
    ):
        key = self.default_job_spec_key(start_job_id, number)
        with self.db_transaction() as connection:
            connection.execute(
                "INSERT INTO default_job_specs"
                "(node_name, spec_key, start_job_id, number, params_signature) "
                "VALUES(?, ?, ?, ?, ?) "
                "ON CONFLICT(node_name, spec_key) DO UPDATE SET "
                "start_job_id=excluded.start_job_id, number=excluded.number, "
                "params_signature=excluded.params_signature",
                (node_name, key, start_job_id, number, self.json_signature(params)),
            )

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

    def create_job(self, job: Job):
        self.validate_job_id(job.job_id)
        self.json_text(Path("input.json"), job.params)
        if self.job_exists(job.node_name, job.job_id):
            raise ValueError(f"Job {job.node_name}/{job.job_id} already exists")

        job_dir = self.job_dir(job.node_name, job.job_id)
        input_path = self.input_file(job.node_name, job.job_id)
        self.atomic_write_json(input_path, job.params)
        stored_parent = dict(job.parent) if job.parent is not None else None
        if stored_parent is not None and job.producer_component is not None:
            stored_parent["_mwf_from_component"] = list(job.producer_component)
            stored_parent["_mwf_job_kind"] = job.job_kind
        parent_json = json.dumps(stored_parent, ensure_ascii=False) if stored_parent is not None else None
        try:
            with self.db_transaction() as connection:
                connection.execute(
                    "INSERT INTO jobs(node_name, job_id, parent_json, created_at, status, status_json) "
                    "VALUES(?, ?, ?, ?, ?, '{}')",
                    (job.node_name, job.job_id, parent_json, job.created_at, QUEUED),
                )
        except BaseException:
            shutil.rmtree(job_dir, ignore_errors=True)
            raise
        self.append_job_event(
            job.node_name,
            job.job_id,
            "created",
            status=QUEUED,
            parent=job.parent,
            producer_component=list(job.producer_component or ()),
            job_kind=job.job_kind,
        )
        self.advance_job_sequence(job.node_name, job.job_id + 1)
        self.notify_queue_change()

    def commit_prepared_job_resolving_idempotency(
        self,
        job: Job,
        *,
        idempotency_key: str | None = None,
    ) -> tuple[bool, int]:
        """Publish one prepared job through the grouped queue-state writer.

        ID reservation happens before payload I/O. This final transaction
        combines job registration, the created event, idempotency, node queue
        state, and sequence advancement. Concurrent producers therefore pay one
        group commit instead of a chain of advisory-lock transactions.
        """
        job_id = self.validate_job_id(job.job_id)
        key_hash = (
            self.idempotency_key_hash(idempotency_key)
            if idempotency_key is not None
            else None
        )
        stored_parent = dict(job.parent) if job.parent is not None else None
        if stored_parent is not None and job.producer_component is not None:
            stored_parent["_mwf_from_component"] = list(job.producer_component)
            stored_parent["_mwf_job_kind"] = job.job_kind
        parent_json = (
            json.dumps(stored_parent, ensure_ascii=False)
            if stored_parent is not None
            else None
        )
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

        def publish(connection):
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

            connection.execute(
                "INSERT INTO jobs(node_name, job_id, parent_json, created_at, status, status_json) "
                "VALUES(?, ?, ?, ?, ?, '{}')",
                (job.node_name, job_id, parent_json, job.created_at, QUEUED),
            )
            connection.execute(
                "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
                "VALUES(?, ?, ?, 'created', ?)",
                (job.node_name, job_id, event_time, event_data),
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

        result = self.submit_db_mutation(publish, priority=0)
        if result[0]:
            self.notify_queue_change()
        return result

    def create_auto_id_job(
        self,
        *,
        node_name: str,
        params: dict[str, Any],
        parent: dict[str, Any] | None,
        producer_component: tuple[str, ...] | None,
        job_kind: str | None,
        idempotency_key: str | None = None,
    ) -> Job:
        """Prepare one payload, then allocate and publish it in one mutation.

        A single-child router previously waited for an ID reservation mutation,
        wrote ``input.json``, then waited for a publication mutation. Staging the
        unpublished payload first lets the priority queue writer allocate its ID,
        move the file, insert the job/event, and advance the sequence together.
        """
        node_name = self.validate_node_name(node_name)
        provisional = Job(
            job_id=1,
            node_name=node_name,
            params=dict(params),
            parent=dict(parent) if parent is not None else None,
            producer_component=producer_component,
            job_kind=job_kind,
        )
        input_text = self.json_text(Path("input.json"), provisional.params)
        staging_dir = self.project_dir / ".mwf" / "staged-jobs" / uuid4().hex
        staging_dir.mkdir(parents=True, exist_ok=False)
        staging_input = staging_dir / "input.json"
        with staging_input.open("x", encoding="utf-8") as file:
            file.write(input_text)

        key_hash = (
            self.idempotency_key_hash(idempotency_key)
            if idempotency_key is not None
            else None
        )
        stored_parent = (
            dict(provisional.parent) if provisional.parent is not None else None
        )
        if stored_parent is not None and producer_component is not None:
            stored_parent["_mwf_from_component"] = list(producer_component)
            stored_parent["_mwf_job_kind"] = job_kind
        parent_json = (
            json.dumps(stored_parent, ensure_ascii=False)
            if stored_parent is not None
            else None
        )
        event_time = datetime.now().isoformat(timespec="milliseconds")
        event_data = json.dumps(
            {
                "status": QUEUED,
                "parent": provisional.parent,
                "producer_component": list(producer_component or ()),
                "job_kind": job_kind,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        published_dir: list[Path] = []

        def publish(connection):
            if idempotency_key is not None:
                row = connection.execute(
                    "SELECT i.key_text, i.job_id FROM idempotency AS i "
                    "JOIN jobs AS j ON j.node_name=i.node_name AND j.job_id=i.job_id "
                    "WHERE i.node_name=? AND i.key_hash=?",
                    (node_name, key_hash),
                ).fetchone()
                if row is not None:
                    if str(row["key_text"]) != idempotency_key:
                        raise RuntimeError(
                            f"idempotency hash collision for node {node_name!r}"
                        )
                    return False, int(row["job_id"])

            sequence = connection.execute(
                "SELECT next_job_id FROM job_sequences WHERE node_name=?",
                (node_name,),
            ).fetchone()
            if sequence is None:
                job_id = int(
                    connection.execute(
                        "SELECT COALESCE(MAX(job_id), 0) + 1 FROM jobs "
                        "WHERE node_name=?",
                        (node_name,),
                    ).fetchone()[0]
                )
                connection.execute(
                    "INSERT INTO job_sequences(node_name, next_job_id) VALUES(?, ?)",
                    (node_name, job_id + 1),
                )
            else:
                job_id = int(sequence["next_job_id"])
                connection.execute(
                    "UPDATE job_sequences SET next_job_id=? WHERE node_name=?",
                    (job_id + 1, node_name),
                )

            final_dir = self.job_dir(node_name, job_id)
            final_input = self.input_file(node_name, job_id)
            self.retry_fs(lambda: os.replace(staging_input, final_input))
            published_dir.append(final_dir)

            connection.execute(
                "INSERT INTO jobs(node_name, job_id, parent_json, created_at, status, status_json) "
                "VALUES(?, ?, ?, ?, ?, '{}')",
                (
                    node_name,
                    job_id,
                    parent_json,
                    provisional.created_at,
                    QUEUED,
                ),
            )
            connection.execute(
                "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
                "VALUES(?, ?, ?, 'created', ?)",
                (node_name, job_id, event_time, event_data),
            )
            if idempotency_key is not None:
                connection.execute(
                    "INSERT INTO idempotency(node_name, key_hash, key_text, job_id) "
                    "VALUES(?, ?, ?, ?)",
                    (node_name, key_hash, idempotency_key, job_id),
                )
            connection.execute(
                "INSERT INTO nodes(node_name, status) VALUES(?, ?) "
                "ON CONFLICT(node_name) DO UPDATE SET status=excluded.status, "
                "updated_at=CURRENT_TIMESTAMP",
                (node_name, QUEUED),
            )
            return True, job_id

        try:
            created, job_id = self.submit_db_mutation(publish, priority=0)
        except BaseException:
            if published_dir:
                shutil.rmtree(published_dir[0], ignore_errors=True)
            raise
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)

        if created:
            self.notify_queue_change()
            return Job(
                job_id=job_id,
                node_name=node_name,
                params=provisional.params,
                parent=provisional.parent,
                producer_component=producer_component,
                job_kind=job_kind,
                created_at=provisional.created_at,
            )
        return self.load_job(node_name, job_id)

    def prepare_jobs_batch(self, jobs: list[Job]) -> list[Path]:
        """Write unpublished job inputs outside the registration lock.

        Reserved auto IDs are unique and the jobs are not query-visible until
        the later SQLite commit. Their first ``input.json`` therefore does not
        need the temporary-file-plus-replace sequence used when overwriting a
        visible file. A direct exclusive create removes one open and one rename
        from every routed job, which is significant on Windows.
        """
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

        written_dirs: list[Path] = []
        try:
            for job in jobs:
                job_dir = self.job_dir(job.node_name, job.job_id)
                written_dirs.append(job_dir)
                input_path = self.input_file(job.node_name, job.job_id)
                input_text = self.json_text(input_path, job.params)

                def write_new_payload():
                    with input_path.open("x", encoding="utf-8") as file:
                        file.write(input_text)

                self.retry_fs(write_new_payload)
        except BaseException:
            for job_dir in written_dirs:
                shutil.rmtree(job_dir, ignore_errors=True)
            raise
        return written_dirs

    def discard_prepared_jobs(self, jobs: list[Job]) -> None:
        for job in jobs:
            shutil.rmtree(self.job_base_dir(job.node_name, job.job_id), ignore_errors=True)

    def commit_prepared_jobs_batch(
        self,
        jobs: list[Job],
        *,
        idempotency_keys: list[str | None] | None = None,
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
            stored_parent = dict(job.parent) if job.parent is not None else None
            if stored_parent is not None and job.producer_component is not None:
                stored_parent["_mwf_from_component"] = list(job.producer_component)
                stored_parent["_mwf_job_kind"] = job.job_kind
            parent_json = (
                json.dumps(stored_parent, ensure_ascii=False)
                if stored_parent is not None
                else None
            )
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
            connection.executemany(
                "INSERT INTO jobs(node_name, job_id, parent_json, created_at, status, status_json) "
                "VALUES(?, ?, ?, ?, ?, '{}')",
                job_rows,
            )
            connection.executemany(
                "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
                "VALUES(?, ?, ?, ?, ?)",
                event_rows,
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
        self.notify_queue_change()
        return jobs

    def commit_prepared_jobs_batch_resolving_idempotency(
        self,
        jobs: list[Job],
        *,
        idempotency_keys: list[str | None] | None = None,
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

        with self.db_transaction() as connection:
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
                stored_parent = dict(job.parent) if job.parent is not None else None
                if stored_parent is not None and job.producer_component is not None:
                    stored_parent["_mwf_from_component"] = list(job.producer_component)
                    stored_parent["_mwf_job_kind"] = job.job_kind
                parent_json = (
                    json.dumps(stored_parent, ensure_ascii=False)
                    if stored_parent is not None
                    else None
                )
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
                connection.executemany(
                    "INSERT INTO jobs(node_name, job_id, parent_json, created_at, status, status_json) "
                    "VALUES(?, ?, ?, ?, ?, '{}')",
                    job_rows,
                )
                connection.executemany(
                    "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
                    "VALUES(?, ?, ?, ?, ?)",
                    event_rows,
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
            self.notify_queue_change()
        return commit_jobs, existing_by_key

    def create_jobs_batch(
        self,
        jobs: list[Job],
        *,
        idempotency_keys: list[str | None] | None = None,
    ) -> list[Job]:
        """Prepare and commit many jobs; callers may split the phases for concurrency."""
        self.prepare_jobs_batch(jobs)
        try:
            return self.commit_prepared_jobs_batch(
                jobs, idempotency_keys=idempotency_keys
            )
        except BaseException:
            self.discard_prepared_jobs(jobs)
            raise

    def ensure_job(self, job: Job) -> Job:
        self.validate_job_id(job.job_id)
        self.json_text(Path("input.json"), job.params)
        if not self.job_exists(job.node_name, job.job_id):
            self.create_job(job)
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

    def load_job(self, node_name: str, job_id: int) -> Job:
        metadata = self.read_job_metadata(node_name, job_id)
        if not metadata:
            raise FileNotFoundError(f"Job does not exist: {node_name}/{job_id}")
        params = self.read_json(self.input_file(node_name, job_id), default={})
        return Job(
            job_id=metadata["job_id"],
            node_name=metadata["node_name"],
            params=params,
            parent=metadata.get("parent"),
            producer_component=metadata.get("producer_component"),
            job_kind=metadata.get("job_kind"),
            created_at=metadata["created_at"],
        )

    def load_jobs_batch(self, node_name: str, job_ids: list[int]) -> list[Job]:
        """Load one admission burst without per-job metadata queries.

        API admission used to alternate one filesystem read with one execution
        claim. On Windows that spacing prevented the group-commit lane from
        seeing a useful claim burst. Loading metadata and payloads first keeps
        queue semantics unchanged while allowing following claims to coalesce.
        """
        node_name = self.validate_node_name(node_name)
        normalized = [self.validate_job_id(job_id) for job_id in job_ids]
        if not normalized:
            return []
        if len(normalized) != len(set(normalized)):
            raise ValueError("job_ids contains duplicates")

        rows = []
        connection = self.db_connection()
        for offset in range(0, len(normalized), 500):
            chunk = normalized[offset:offset + 500]
            placeholders = ",".join("?" for _ in chunk)
            rows.extend(
                connection.execute(
                    "SELECT job_id, node_name, parent_json, created_at FROM jobs "
                    f"WHERE node_name=? AND job_id IN ({placeholders})",
                    [node_name, *chunk],
                ).fetchall()
            )
        rows_by_id = {int(row["job_id"]): row for row in rows}
        missing = [job_id for job_id in normalized if job_id not in rows_by_id]
        if missing:
            raise FileNotFoundError(f"Job does not exist: {node_name}/{missing[0]}")

        def load_params(job_id: int) -> dict[str, Any]:
            return self.read_json(self.input_file(node_name, job_id), default={})

        if os.name == "nt" and len(normalized) >= 8:
            with ThreadPoolExecutor(
                max_workers=min(32, len(normalized)),
                thread_name_prefix="mwf-job-prefetch",
            ) as executor:
                params_list = list(executor.map(load_params, normalized))
        else:
            params_list = [load_params(job_id) for job_id in normalized]

        jobs: list[Job] = []
        for job_id, params in zip(normalized, params_list):
            row = rows_by_id[job_id]
            raw_parent = json.loads(row["parent_json"]) if row["parent_json"] else None
            producer_component = None
            job_kind = None
            parent = raw_parent
            if isinstance(raw_parent, dict):
                parent = dict(raw_parent)
                producer_component = parent.pop(
                    "_mwf_from_component",
                    parent.pop("from_component", None),
                )
                job_kind = parent.pop(
                    "_mwf_job_kind",
                    parent.pop("job_kind", None),
                )
                parent = parent or None
            jobs.append(
                Job(
                    job_id=job_id,
                    node_name=str(row["node_name"]),
                    params=params,
                    parent=parent,
                    producer_component=(
                        tuple(producer_component)
                        if isinstance(producer_component, list)
                        else None
                    ),
                    job_kind=job_kind,
                    created_at=str(row["created_at"]),
                )
            )
        return jobs

    def read_job_status_data(self, node_name: str, job_id: int) -> dict[str, Any]:
        row = self.db_connection().execute(
            "SELECT status, status_json FROM jobs WHERE node_name=? AND job_id=?",
            (node_name, self.validate_job_id(job_id)),
        ).fetchone()
        if row is None:
            return {}
        try:
            extra = json.loads(row["status_json"] or "{}")
        except json.JSONDecodeError:
            extra = {}
        if not isinstance(extra, dict):
            extra = {}
        return {
            "job_id": job_id,
            "node_name": node_name,
            "status": row["status"],
            **extra,
        }

    def set_job_status(self, node_name: str, job_id: int, status: str, **extra):
        job_id = self.validate_job_id(job_id)
        status = self.validate_status(status)
        terminal = status in {"done", "failed", "cancelled", "skipped"}
        status_json = json.dumps(extra, ensure_ascii=False, separators=(",", ":"))
        event_time = datetime.now().isoformat(timespec="milliseconds")

        def update(connection):
            row = connection.execute(
                "SELECT status FROM jobs WHERE node_name=? AND job_id=?",
                (node_name, job_id),
            ).fetchone()
            if row is None:
                raise FileNotFoundError(f"Job does not exist: {node_name}/{job_id}")
            old_status = str(row["status"])
            if terminal:
                connection.execute(
                    "UPDATE jobs SET status=?, status_json=?, active_execution_id=NULL, "
                    "active_pid=NULL, active_thread_id=NULL, active_started_at=NULL "
                    "WHERE node_name=? AND job_id=?",
                    (
                        status,
                        status_json,
                        node_name,
                        job_id,
                    ),
                )
            else:
                connection.execute(
                    "UPDATE jobs SET status=?, status_json=? WHERE node_name=? AND job_id=?",
                    (
                        status,
                        status_json,
                        node_name,
                        job_id,
                    ),
                )
            if old_status != status or extra:
                event_name = {
                    "queued": "queued",
                    "running": "started",
                    "done": "done",
                    "failed": "failed",
                    "cancelled": "cancelled",
                    "skipped": "skipped",
                }.get(status, "status_changed")
                event_data = json.dumps(
                    {"previous_status": old_status, "status": status, **extra},
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                connection.execute(
                    "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
                    "VALUES(?, ?, ?, ?, ?)",
                    (node_name, job_id, event_time, event_name, event_data),
                )

        self.submit_db_mutation(update)
        if status == QUEUED:
            self.notify_queue_change()

    def get_job_status(self, node_name: str, job_id: int) -> str | None:
        row = self.db_connection().execute(
            "SELECT status FROM jobs WHERE node_name=? AND job_id=?",
            (node_name, self.validate_job_id(job_id)),
        ).fetchone()
        return None if row is None else str(row["status"])

    def list_job_ids(self, node_name: str) -> list[int]:
        rows = self.db_connection().execute(
            "SELECT job_id FROM jobs WHERE node_name=? ORDER BY job_id",
            (node_name,),
        ).fetchall()
        return [int(row["job_id"]) for row in rows]

    def list_jobs(self, node_name: str, status: str | None = None) -> list[dict]:
        sql = "SELECT job_id FROM jobs WHERE node_name=?"
        args: list[Any] = [node_name]
        if status is not None:
            sql += " AND status=?"
            args.append(status)
        sql += " ORDER BY job_id"
        rows = self.db_connection().execute(sql, args).fetchall()
        result = []
        for row in rows:
            job_id = int(row["job_id"])
            result.append({
                **self.read_job_metadata(node_name, job_id),
                **self.read_job_status_data(node_name, job_id),
            })
        return result

    def list_job_parent_metadata(
        self,
        node_names: list[str] | tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        """Read producer metadata for many jobs with one SQLite snapshot."""
        args: list[Any] = []
        sql = "SELECT node_name, job_id, parent_json FROM jobs"
        if node_names is not None:
            normalized = [self.validate_node_name(name) for name in node_names]
            if not normalized:
                return []
            placeholders = ",".join("?" for _ in normalized)
            sql += f" WHERE node_name IN ({placeholders})"
            args.extend(normalized)
        sql += " ORDER BY node_name, job_id"
        rows = self.db_connection().execute(sql, args).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            raw_parent = json.loads(row["parent_json"]) if row["parent_json"] else None
            producer_component = None
            parent = raw_parent
            if isinstance(raw_parent, dict):
                parent = dict(raw_parent)
                producer_component = parent.pop(
                    "_mwf_from_component",
                    parent.pop("from_component", None),
                )
                parent.pop("_mwf_job_kind", parent.pop("job_kind", None))
                parent = parent or None
            result.append(
                {
                    "node_name": str(row["node_name"]),
                    "job_id": int(row["job_id"]),
                    "parent": parent,
                    "producer_component": (
                        tuple(producer_component)
                        if isinstance(producer_component, list)
                        else None
                    ),
                }
            )
        return result

    def job_is_queued(self, node_name: str, job_id: int) -> bool:
        return self.get_job_status(node_name, job_id) == QUEUED

    def iter_job_ids(self, node_name: str):
        yield from self.list_job_ids(node_name)

    def queued_job_source(self, node_name: str) -> RefreshableQueuedJobSource:
        return RefreshableQueuedJobSource(self, node_name)

    def queued_job_object_source(
        self,
        node_name: str,
        *,
        refreshable: bool,
    ) -> QueuedJobObjectSource | RefreshableQueuedJobObjectSource:
        if refreshable:
            return RefreshableQueuedJobObjectSource(self, node_name)
        return QueuedJobObjectSource(
            self,
            node_name,
            self.iter_queued_job_ids(node_name),
        )

    def iter_queued_job_ids(self, node_name: str):
        # Preserve the public snapshot iterator and deterministic job-id order.
        # Component API pumps use ``queued_job_source`` so they can refill from
        # jobs inserted after the pump starts.
        rows = self.db_connection().execute(
            "SELECT job_id FROM jobs WHERE node_name=? AND status=? ORDER BY job_id",
            (node_name, QUEUED),
        ).fetchall()
        for row in rows:
            yield int(row["job_id"])

    def queued_job_ids(self, node_name: str) -> list[int]:
        return list(self.iter_queued_job_ids(node_name))

    def has_queued_jobs(self, node_name: str) -> bool:
        row = self.db_connection().execute(
            "SELECT 1 FROM jobs WHERE node_name=? AND status=? LIMIT 1",
            (node_name, QUEUED),
        ).fetchone()
        return row is not None

    def queued_nodes(self, node_names: list[str] | tuple[str, ...]) -> set[str]:
        """Return queued members of a component with one indexed query."""
        normalized = [self.validate_node_name(name) for name in node_names]
        if not normalized:
            return set()
        placeholders = ",".join("?" for _ in normalized)
        rows = self.db_connection().execute(
            f"SELECT DISTINCT node_name FROM jobs WHERE status=? "
            f"AND node_name IN ({placeholders})",
            [QUEUED, *normalized],
        ).fetchall()
        return {str(row["node_name"]) for row in rows}

    def queued_jobs(self, node_name: str) -> list[Job]:
        return [self.load_job(node_name, job_id) for job_id in self.iter_queued_job_ids(node_name)]

    def delete_job(self, node_name: str, job_id: int, *, remove_payload: bool = True) -> bool:
        job_id = self.validate_job_id(job_id)
        with self.db_transaction() as connection:
            existed = connection.execute(
                "SELECT 1 FROM jobs WHERE node_name=? AND job_id=?",
                (node_name, job_id),
            ).fetchone() is not None
            connection.execute(
                "DELETE FROM idempotency WHERE node_name=? AND job_id=?",
                (node_name, job_id),
            )
            connection.execute(
                "DELETE FROM job_events WHERE node_name=? AND job_id=?",
                (node_name, job_id),
            )
            connection.execute(
                "DELETE FROM jobs WHERE node_name=? AND job_id=?",
                (node_name, job_id),
            )
        if remove_payload:
            shutil.rmtree(self.job_base_dir(node_name, job_id), ignore_errors=True)
        return existed

    def delete_jobs_batch(
        self,
        node_name: str,
        job_ids: list[int],
        *,
        remove_payload: bool = True,
    ) -> int:
        """Delete selected jobs with one transaction and bulk directory removal."""
        node_name = self.validate_node_name(node_name)
        normalized = sorted({self.validate_job_id(job_id) for job_id in job_ids})
        if not normalized:
            return 0
        existing_ids = self.list_job_ids(node_name)
        targets = sorted(set(existing_ids).intersection(normalized))
        if not targets:
            return 0
        remove_whole_jobs_dir = targets == existing_ids

        with self.db_transaction() as connection:
            if remove_whole_jobs_dir:
                connection.execute("DELETE FROM idempotency WHERE node_name=?", (node_name,))
                connection.execute("DELETE FROM job_events WHERE node_name=?", (node_name,))
                connection.execute("DELETE FROM jobs WHERE node_name=?", (node_name,))
            else:
                for offset in range(0, len(targets), 500):
                    chunk = targets[offset:offset + 500]
                    placeholders = ",".join("?" for _ in chunk)
                    args = [node_name, *chunk]
                    connection.execute(
                        f"DELETE FROM idempotency WHERE node_name=? AND job_id IN ({placeholders})",
                        args,
                    )
                    connection.execute(
                        f"DELETE FROM job_events WHERE node_name=? AND job_id IN ({placeholders})",
                        args,
                    )
                    connection.execute(
                        f"DELETE FROM jobs WHERE node_name=? AND job_id IN ({placeholders})",
                        args,
                    )

        if remove_payload:
            jobs_dir = self.jobs_dir(node_name)

            def remove_job_payload(job_id: int) -> None:
                shutil.rmtree(
                    self.safe_join(jobs_dir, str(job_id)),
                    ignore_errors=True,
                )

            # Windows directory removal is disproportionately expensive and was
            # still serial after the SQLite deletion became batched. Job payload
            # trees are independent, so remove a bounded number concurrently.
            if os.name != "nt" or len(targets) < 8:
                for job_id in targets:
                    remove_job_payload(job_id)
            else:
                with ThreadPoolExecutor(
                    max_workers=min(32, len(targets)),
                    thread_name_prefix="mwf-job-cleanup",
                ) as executor:
                    list(executor.map(remove_job_payload, targets))
            if remove_whole_jobs_dir:
                # Clear abandoned unpublished payloads too, then recreate the
                # conventional directory expected by filesystem integrations.
                shutil.rmtree(jobs_dir, ignore_errors=True)
                self.jobs_dir(node_name)
        return len(targets)

    def reset_jobs_for_run_batch(self, node_name: str, job_ids: list[int]) -> int:
        """Requeue retained jobs with one state/event transaction."""
        node_name = self.validate_node_name(node_name)
        normalized = sorted({self.validate_job_id(job_id) for job_id in job_ids})
        if not normalized:
            return 0
        event_time = datetime.now().isoformat(timespec="milliseconds")

        with self.db_transaction() as connection:
            rows = []
            for offset in range(0, len(normalized), 500):
                chunk = normalized[offset:offset + 500]
                placeholders = ",".join("?" for _ in chunk)
                rows.extend(
                    connection.execute(
                        f"SELECT job_id, status FROM jobs WHERE node_name=? "
                        f"AND job_id IN ({placeholders})",
                        [node_name, *chunk],
                    ).fetchall()
                )
            if not rows:
                return 0
            existing = [int(row["job_id"]) for row in rows]
            for offset in range(0, len(existing), 500):
                chunk = existing[offset:offset + 500]
                placeholders = ",".join("?" for _ in chunk)
                connection.execute(
                    "UPDATE jobs SET status=?, status_json='{}', runtime_json=NULL, "
                    "active_execution_id=NULL, active_pid=NULL, active_thread_id=NULL, "
                    "active_started_at=NULL, restart_requested_at=NULL, "
                    "restart_requested_by_pid=NULL, restart_reason=NULL "
                    f"WHERE node_name=? AND job_id IN ({placeholders})",
                    [QUEUED, node_name, *chunk],
                )
            previous = {int(row["job_id"]): str(row["status"]) for row in rows}
            connection.executemany(
                "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
                "VALUES(?, ?, ?, 'queued', ?)",
                [
                    (
                        node_name,
                        job_id,
                        event_time,
                        json.dumps(
                            {
                                "previous_status": previous[job_id],
                                "status": QUEUED,
                            },
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                    )
                    for job_id in existing
                ],
            )
        self.notify_queue_change()
        return len(existing)

    def delete_node_jobs(self, node_name: str, *, remove_payload: bool = True) -> int:
        ids = self.list_job_ids(node_name)
        with self.db_transaction() as connection:
            connection.execute("DELETE FROM idempotency WHERE node_name=?", (node_name,))
            connection.execute("DELETE FROM job_events WHERE node_name=?", (node_name,))
            connection.execute("DELETE FROM default_job_specs WHERE node_name=?", (node_name,))
            connection.execute("DELETE FROM jobs WHERE node_name=?", (node_name,))
            connection.execute(
                "INSERT INTO job_sequences(node_name, next_job_id) VALUES(?, 1) "
                "ON CONFLICT(node_name) DO UPDATE SET next_job_id=1",
                (node_name,),
            )
        if remove_payload:
            shutil.rmtree(self.jobs_dir(node_name), ignore_errors=True)
            self.jobs_dir(node_name)
        return len(ids)

    def write_output(self, node_name: str, job_id: int, data: dict):
        self.validate_job_id(job_id)
        self.atomic_write_json(self.output_file(node_name, job_id), data)

    def write_text(self, node_name: str, job_id: int, filename: str, content: str) -> Path:
        path = self.safe_join(self.files_dir(node_name, job_id), filename)
        self.atomic_write_text(path, content)
        return path

    def write_bytes(self, node_name: str, job_id: int, filename: str, content: bytes) -> Path:
        path = self.safe_join(self.files_dir(node_name, job_id), filename)
        self.atomic_write_bytes(path, content)
        return path

    def unique_target(self, directory: Path, filename: str) -> Path:
        target = self.safe_join(directory, Path(filename).name)
        if not target.exists():
            return target
        stem = target.stem
        suffix = target.suffix
        index = 2
        while True:
            candidate = self.safe_join(directory, f"{stem}_{index}{suffix}")
            if not candidate.exists():
                return candidate
            index += 1

    def extract_files(self, result: Any, explicit: bool = False) -> list[Path]:
        files: list[Path] = []
        if result is None:
            return files
        if isinstance(result, Path):
            return [result]
        if isinstance(result, str):
            return [Path(result)] if explicit else []
        if isinstance(result, list | tuple):
            for item in result:
                files.extend(self.extract_files(item, explicit=explicit))
            return files
        if isinstance(result, dict):
            if "file" in result:
                files.extend(self.extract_files(result["file"], explicit=True))
            if "files" in result:
                files.extend(self.extract_files(result["files"], explicit=True))
        return files

    def store_returned_files(self, node_name: str, job_id: int, result: Any) -> list[str]:
        files = self.extract_files(result)
        stored: list[str] = []
        if not files:
            return stored
        destination = self.files_dir(node_name, job_id)
        for file in files:
            source = Path(file)
            if not source.exists():
                raise FileNotFoundError(f"Returned file does not exist: {source}")
            if not source.is_file():
                raise ValueError(f"Returned path is not a file: {source}")
            if source.parent.resolve() == destination.resolve():
                stored.append(str(source))
                continue
            target = self.unique_target(destination, source.name)
            self.atomic_copy_file(source, target)
            stored.append(str(target))
        return stored
