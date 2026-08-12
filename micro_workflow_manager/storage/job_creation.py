from __future__ import annotations

import hashlib
import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from micro_workflow_manager.models import Job, QUEUED


@dataclass(slots=True)
class AutoJobPublish:
    provisional: Job
    staging_input: Path
    idempotency_key: str | None
    key_hash: str | None
    parent_json: str | None
    event_time: str
    event_data: str
    parent_event_node: str | None
    parent_event_job_id: int | None
    parent_event_data: dict[str, Any] | None
    published_dir: Path | None = None


class JobCreationStorageMixin:
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
        self.append_job_created_event(
            job.node_name,
            job.job_id,
            status=QUEUED,
            parent=job.parent,
            producer_component=list(job.producer_component or ()),
            job_kind=job.job_kind,
        )
        self.advance_job_sequence(job.node_name, job.job_id + 1)
        self.notify_queue_change(job.node_name)

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

        result = self.submit_db_mutation(publish, priority=0)
        if result[0]:
            self.notify_queue_change(job.node_name)
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
        parent_event: tuple[str, int, dict[str, Any]] | None = None,
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
        staging_root = self.project_dir / ".mwf" / "staged-jobs"
        staging_root.mkdir(parents=True, exist_ok=True)
        staging_input = staging_root / f"{uuid4().hex}.json"
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
        parent_event_node = None
        parent_event_job_id = None
        parent_event_data = None
        if parent_event is not None:
            parent_event_node, parent_event_job_id, parent_event_data = parent_event
            parent_event_node = self.validate_node_name(parent_event_node)
            parent_event_job_id = self.validate_job_id(parent_event_job_id)
            parent_event_data = dict(parent_event_data)

        publish = AutoJobPublish(
            provisional=provisional,
            staging_input=staging_input,
            idempotency_key=idempotency_key,
            key_hash=key_hash,
            parent_json=parent_json,
            event_time=event_time,
            event_data=event_data,
            parent_event_node=parent_event_node,
            parent_event_job_id=parent_event_job_id,
            parent_event_data=parent_event_data,
        )

        try:
            created, job_id = self.submit_grouped_db_mutation(
                ("auto-job-publish",),
                publish,
                self._apply_auto_job_publishes,
                priority=0,
                collect_seconds=0.001,
            )
        except BaseException:
            if publish.published_dir is not None:
                shutil.rmtree(publish.published_dir, ignore_errors=True)
            raise
        finally:
            self.remove_if_exists(staging_input)

        if created:
            self.notify_queue_change(node_name)
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

    def _apply_auto_job_publishes(
        self,
        connection,
        publishes: list[AutoJobPublish],
    ):
        """Allocate and publish concurrent one-child routes in one mutation.

        The public operation remains one durable job creation per caller. This
        only combines the sequence lookup/update, SQLite statements, and
        savepoint paid by simultaneous producers targeting the same node.
        """
        if not publishes:
            return []
        by_node: dict[str, list[tuple[int, AutoJobPublish]]] = {}
        for index, item in enumerate(publishes):
            by_node.setdefault(item.provisional.node_name, []).append((index, item))

        outcomes: list[tuple[bool, tuple[bool, int]] | None] = [None] * len(publishes)
        pending: list[tuple[AutoJobPublish, int]] = []
        next_ids: dict[str, int] = {}
        for node_name, indexed in by_node.items():
            keyed = [item for _index, item in indexed if item.key_hash is not None]
            existing_by_hash: dict[str, tuple[str, int]] = {}
            if keyed:
                hashes = sorted({str(item.key_hash) for item in keyed})
                for offset in range(0, len(hashes), 500):
                    chunk = hashes[offset:offset + 500]
                    placeholders = ",".join("?" for _ in chunk)
                    rows = connection.execute(
                        "SELECT i.key_hash, i.key_text, i.job_id FROM idempotency AS i "
                        "JOIN jobs AS j ON j.node_name=i.node_name AND j.job_id=i.job_id "
                        f"WHERE i.node_name=? AND i.key_hash IN ({placeholders})",
                        [node_name, *chunk],
                    ).fetchall()
                    existing_by_hash.update({
                        str(row["key_hash"]): (str(row["key_text"]), int(row["job_id"]))
                        for row in rows
                    })

            sequence = connection.execute(
                "SELECT next_job_id FROM job_sequences WHERE node_name=?",
                (node_name,),
            ).fetchone()
            next_job_id = (
                int(sequence["next_job_id"])
                if sequence is not None
                else int(
                    connection.execute(
                        "SELECT COALESCE(MAX(job_id), 0) + 1 FROM jobs WHERE node_name=?",
                        (node_name,),
                    ).fetchone()[0]
                )
            )

            resolved_by_hash = dict(existing_by_hash)
            for index, item in indexed:
                if item.key_hash is not None:
                    resolved = resolved_by_hash.get(item.key_hash)
                    if resolved is not None:
                        key_text, job_id = resolved
                        if key_text != item.idempotency_key:
                            raise RuntimeError(
                                f"idempotency hash collision for node {node_name!r}"
                            )
                        outcomes[index] = (True, (False, job_id))
                        continue

                job_id = next_job_id
                next_job_id += 1
                pending.append((item, job_id))
                outcomes[index] = (True, (True, job_id))
                if item.key_hash is not None:
                    resolved_by_hash[item.key_hash] = (
                        str(item.idempotency_key),
                        job_id,
                    )
            next_ids[node_name] = next_job_id

        published: list[Path] = []
        try:
            for item, job_id in pending:
                item_node = item.provisional.node_name
                final_dir = self.job_dir(item_node, job_id)
                final_input = self.input_file(item_node, job_id)
                self.retry_fs(
                    lambda source=item.staging_input, target=final_input: os.replace(
                        source, target
                    )
                )
                item.published_dir = final_dir
                published.append(final_dir)

            if pending:
                connection.executemany(
                    "INSERT INTO jobs(node_name, job_id, parent_json, created_at, status, status_json) "
                    "VALUES(?, ?, ?, ?, ?, '{}')",
                    [
                        (
                            item.provisional.node_name,
                            job_id,
                            item.parent_json,
                            item.provisional.created_at,
                            QUEUED,
                        )
                        for item, job_id in pending
                    ],
                )
                self.insert_job_created_events(
                    connection,
                    [
                        (
                            item.provisional.node_name,
                            job_id,
                            item.event_time,
                            item.event_data,
                        )
                        for item, job_id in pending
                    ],
                )
                idempotency_rows = [
                    (
                        item.provisional.node_name,
                        item.key_hash,
                        item.idempotency_key,
                        job_id,
                    )
                    for item, job_id in pending
                    if item.key_hash is not None
                ]
                if idempotency_rows:
                    connection.executemany(
                        "INSERT INTO idempotency(node_name, key_hash, key_text, job_id) "
                        "VALUES(?, ?, ?, ?)",
                        idempotency_rows,
                    )
                connection.executemany(
                    "INSERT INTO job_sequences(node_name, next_job_id) VALUES(?, ?) "
                    "ON CONFLICT(node_name) DO UPDATE SET next_job_id=excluded.next_job_id",
                    sorted(next_ids.items()),
                )
                connection.executemany(
                    "INSERT INTO nodes(node_name, status) VALUES(?, ?) "
                    "ON CONFLICT(node_name) DO UPDATE SET status=excluded.status, "
                    "updated_at=CURRENT_TIMESTAMP",
                    [(node_name, QUEUED) for node_name in sorted(next_ids)],
                )
            parent_events = []
            for item, outcome in zip(publishes, outcomes):
                if outcome is None:
                    raise RuntimeError("auto-job publish group lost an outcome")
                _succeeded, (_created, job_id) = outcome
                if (
                    item.parent_event_node is None
                    or item.parent_event_job_id is None
                    or item.parent_event_data is None
                ):
                    continue
                data = dict(item.parent_event_data)
                data["jobs"] = [
                    {
                        "node": item.provisional.node_name,
                        "job_id": job_id,
                        "params": item.provisional.params,
                    }
                ]
                parent_events.append(
                    (
                        item.parent_event_node,
                        item.parent_event_job_id,
                        item.event_time,
                        "jobs_created",
                        json.dumps(
                            data,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                    )
                )
            if parent_events:
                connection.executemany(
                    "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
                    "VALUES(?, ?, ?, ?, ?)",
                    parent_events,
                )
        except BaseException:
            for path in published:
                shutil.rmtree(path, ignore_errors=True)
            raise
        if any(outcome is None for outcome in outcomes):
            raise RuntimeError("auto-job publish group lost an outcome")
        return outcomes
