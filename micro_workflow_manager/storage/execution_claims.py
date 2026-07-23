from __future__ import annotations

import json
import os
import time
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import datetime
from threading import get_ident
from typing import Any
from uuid import uuid4

from micro_workflow_manager.models import QUEUED, RUNNING


@dataclass(slots=True, frozen=True)
class RuntimeUpdate:
    node_name: str
    job_id: int
    generation: int
    execution_id: str | None
    state: str
    watch_id: str | None
    serialized: str


@dataclass(slots=True, frozen=True)
class ExecutionClaimBatch:
    node_name: str
    job_ids: tuple[int, ...]
    execution_ids: tuple[str, ...]
    started_at: str
    pid: int
    thread_id: int
    event_time: str

    @property
    def mutation_weight(self) -> int:
        return len(self.job_ids)


class JobExecutionClaimStorageMixin:
    """Runtime state plus execution-lease claiming and release."""

    def _init_job_execution_state(self) -> None:
        from threading import Lock

        self._claim_batch_lock = Lock()
        self._claim_batches: dict[
            tuple[str, int],
            list[tuple[int, str, Future[tuple[int, str]]]],
        ] = {}

    def read_job_runtime(self, node_name: str, job_id: int) -> dict[str, Any]:
        row = self.db_connection().execute(
            "SELECT runtime_json FROM jobs WHERE node_name=? AND job_id=?",
            (node_name, self.validate_job_id(job_id)),
        ).fetchone()
        if row is None or not row["runtime_json"]:
            return {}
        try:
            data = json.loads(row["runtime_json"])
        except json.JSONDecodeError:
            return {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _apply_runtime_updates(connection, updates: list[RuntimeUpdate]):
        if not updates:
            return []
        node_name = updates[0].node_name
        if any(update.node_name != node_name for update in updates):
            raise RuntimeError("runtime mutation group contains multiple nodes")

        # Runtime rows are observability metadata, not execution ownership. Fence
        # every queued write to the generation and execution that produced it so
        # an asynchronous API checkpoint/completion can never overwrite a later
        # restart. One executemany also avoids a savepoint and transaction unit per
        # job during 1k-10k API admission waves.
        # Keep only the latest queued observation for the same attempt. A fast
        # API call can enqueue both ``running`` and ``completed`` before the
        # writer reaches this priority class; persisting the intermediate value
        # adds work without improving inspection.
        latest_by_attempt: dict[tuple[int, int, str | None], RuntimeUpdate] = {}
        for update in updates:
            latest_by_attempt[(
                update.job_id,
                update.generation,
                update.execution_id,
            )] = update

        connection.executemany(
            "UPDATE jobs SET runtime_json=? WHERE node_name=? AND job_id=? "
            "AND generation=? "
            "AND ("
            "(? IS NULL AND active_execution_id IS NULL) "
            "OR active_execution_id=? "
            "OR ("
            "? IN ('timed_out','completed','failed','recovered') "
            "AND status IN ('done','failed','skipped','cancelled') "
            "AND json_valid(status_json)=1 "
            "AND json_extract(status_json, '$.execution_id')=?"
            ")"
            ") "
            "AND NOT ("
            "?='running' AND runtime_json IS NOT NULL "
            "AND json_valid(runtime_json)=1 "
            "AND json_extract(runtime_json, '$.watch_id')=? "
            "AND json_extract(runtime_json, '$.state') IN "
            "('timed_out','completed','failed','restarted','recovered')"
            ")",
            [
                (
                    update.serialized,
                    update.node_name,
                    update.job_id,
                    update.generation,
                    update.execution_id,
                    update.execution_id,
                    update.state,
                    update.execution_id,
                    update.state,
                    update.watch_id,
                )
                for update in latest_by_attempt.values()
            ],
        )
        return [(True, None) for _update in updates]

    def write_job_runtime(
        self,
        node_name: str,
        job_id: int,
        data: dict[str, Any],
        *,
        wait: bool = True,
        priority: int = 10,
    ):
        node_name = self.validate_node_name(node_name)
        job_id = self.validate_job_id(job_id)
        state = str(data.get("state") or "")
        try:
            generation = int(data.get("generation", 0) or 0)
        except (TypeError, ValueError):
            generation = 0
        execution_id = data.get("execution_id")
        if execution_id is not None:
            execution_id = str(execution_id)
        watch_id = data.get("watch_id")
        if watch_id is not None:
            watch_id = str(watch_id)
        update = RuntimeUpdate(
            node_name=node_name,
            job_id=job_id,
            generation=generation,
            execution_id=execution_id,
            state=state,
            watch_id=watch_id,
            serialized=json.dumps(data, ensure_ascii=False, separators=(",", ":")),
        )
        return self.submit_grouped_db_mutation(
            ("runtime", node_name),
            update,
            self._apply_runtime_updates,
            wait=wait,
            priority=priority,
            collect_seconds=0.001,
        )

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
                        "SELECT job_id, generation, status FROM jobs "
                        f"WHERE node_name=? AND job_id IN ({placeholders})",
                        [node_name, *chunk],
                    ).fetchall()
                )
            rows_by_node[node_name] = {int(row["job_id"]): row for row in rows}

        updates = []
        events = []
        outcomes = []
        for batch in batches:
            node_rows = rows_by_node[batch.node_name]
            missing = [job_id for job_id in batch.job_ids if job_id not in node_rows]
            if missing:
                outcomes.append((False, FileNotFoundError(
                    f"Job does not exist: {batch.node_name}/{missing[0]}"
                )))
                continue

            results = []
            for job_id, execution_id in zip(batch.job_ids, batch.execution_ids):
                row = node_rows[job_id]
                generation = int(row["generation"])
                previous_status = str(row["status"])
                status_extra = {
                    "started_at": batch.started_at,
                    "generation": generation,
                    "execution_id": execution_id,
                    "pid": batch.pid,
                }
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
                        json.dumps(
                            {
                                "previous_status": previous_status,
                                "status": RUNNING,
                                **status_extra,
                            },
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                    )
                )
                results.append((generation, execution_id))
            outcomes.append((True, results))

        if updates:
            connection.executemany(
                "UPDATE jobs SET active_execution_id=?, active_pid=?, "
                "active_thread_id=?, active_started_at=?, "
                "restart_requested_at=NULL, restart_requested_by_pid=NULL, "
                "restart_reason=NULL, runtime_json=NULL, status=?, status_json=? "
                "WHERE node_name=? AND job_id=? AND generation=?",
                updates,
            )
            connection.executemany(
                "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
                "VALUES(?, ?, ?, 'started', ?)",
                events,
            )
        return outcomes

    def claim_job_execution(
        self,
        node_name: str,
        job_id: int,
        *,
        started_at: str,
        priority: int = 10,
    ) -> tuple[int, str]:
        node_name = self.validate_node_name(node_name)
        job_id = self.validate_job_id(job_id)
        future: Future[tuple[int, str]] = Future()
        key = (node_name, priority)
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
        priority: int = 10,
    ) -> list[tuple[int, str]]:
        """Claim one preloaded API admission burst in one state mutation."""
        node_name = self.validate_node_name(node_name)
        normalized = [self.validate_job_id(job_id) for job_id in job_ids]
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
        )
        return self.submit_grouped_db_mutation(
            ("execution-claims", priority),
            batch,
            self._apply_execution_claim_batches,
            wait=True,
            priority=priority,
            collect_seconds=0.003,
        )

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
            self.notify_queue_change()
