from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
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
    producer_execution_id: str | None = None
    expected_shape: str | None = None
    published_dir: Path | None = None
    arrival_identity: tuple | None = None


class AutoJobCreationStorageMixin:
    """Allocate and publish grouped auto-ID jobs with their durable creators."""

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
        producer_execution_id: str | None = None,
        expected_shape: str | None = None,
    ) -> Job:
        """Prepare one payload, then allocate and publish it in one mutation.

        A single-child router previously waited for an ID reservation mutation,
        wrote ``input.json``, then waited for a publication mutation. Staging the
        unpublished payload first lets the priority queue writer allocate its ID,
        move the file, insert the job/event, and advance the sequence together.
        """
        node_name = self.validate_node_name(node_name)
        self.validate_job_receiver_shape(node_name, expected_shape=expected_shape)
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
        parent_json = self._job_parent_json(provisional)
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
            producer_execution_id=producer_execution_id,
            expected_shape=expected_shape,
        )

        future = None
        try:
            future = self.submit_grouped_db_mutation(
                ("auto-job-publish",),
                publish,
                self._apply_auto_job_publishes,
                wait=False,
                priority=0,
                collect_seconds=0.001,
            )
            created, job_id = future.result()
        except BaseException as error:
            # Drain an interrupted waiter before deciding whether payloads can
            # be removed. The queued writer may still commit that exact job.
            if future is not None:
                while not future.done():
                    try:
                        future.result()
                    except BaseException:
                        pass
            if publish.published_dir is not None:
                self.discard_prepared_jobs([Job(
                    node_name=node_name, job_id=int(publish.published_dir.name), params=provisional.params,
                )], publication_error=error)
            raise
        finally:
            self.remove_if_exists(staging_input)

        if created:
            if publish.arrival_identity is not None:
                self._component_arrival_latches[node_name] = publish.arrival_identity
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
        outcomes: list[tuple[bool, tuple[bool, int] | Exception] | None] = [None] * len(publishes)
        for index, item in enumerate(publishes):
            try:
                self._validate_job_producer(connection, item.provisional, item.producer_execution_id)
            except Exception as error:
                outcomes[index] = (False, error)
            else:
                by_node.setdefault(item.provisional.node_name, []).append((index, item))

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

        published: list[AutoJobPublish] = []
        try:
            for item, job_id in pending:
                item_node = item.provisional.node_name
                final_dir = self.job_base_dir(item_node, job_id)
                final_dir.mkdir(exist_ok=False)
                item.published_dir = final_dir
                published.append(item)
                final_input = self.input_file(item_node, job_id)
                self.retry_fs(
                    lambda source=item.staging_input, target=final_input: os.replace(
                        source, target
                    )
                )

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
                for item, job_id in pending:
                    self._record_job_producer(
                        connection, item.provisional.node_name, job_id, item.producer_execution_id,
                    )
                for item, job_id in pending:
                    item.arrival_identity = self._mark_component_job_arrival(
                        connection, item.provisional.node_name, job_id, expected_shape=item.expected_shape,
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
                if not outcome[0]:
                    continue
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
        except BaseException as error:
            for item in published:
                try:
                    shutil.rmtree(item.published_dir)
                except BaseException as cleanup_error:
                    error.__notes__ = [*getattr(error, '__notes__', ()),
                                      f'Auto job input cleanup failed: {cleanup_error}']
                else:
                    # Releasing this address ends our ownership. The failed
                    # caller must not later clean a newly prepared replacement.
                    item.published_dir = None
            raise
        if any(outcome is None for outcome in outcomes):
            raise RuntimeError("auto-job publish group lost an outcome")
        return outcomes
