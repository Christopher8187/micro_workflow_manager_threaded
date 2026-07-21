from __future__ import annotations

import json
import os
import time
from concurrent.futures import Future
from datetime import datetime
from threading import get_ident
from typing import Any
from uuid import uuid4

from micro_workflow_manager.models import QUEUED, RUNNING



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

    def write_job_runtime(
        self,
        node_name: str,
        job_id: int,
        data: dict[str, Any],
        *,
        wait: bool = True,
    ):
        job_id = self.validate_job_id(job_id)
        serialized = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        # One atomic conditional UPDATE replaces the old advisory-lock acquire,
        # runtime UPDATE, and advisory-lock release. The WHERE clause preserves
        # terminal runtime state when a late checkpoint from the same handler
        # races with timeout/restart completion.
        def update(connection):
            connection.execute(
                "UPDATE jobs SET runtime_json=? WHERE node_name=? AND job_id=? "
                "AND NOT ("
                "?='running' AND runtime_json IS NOT NULL "
                "AND json_valid(runtime_json)=1 "
                "AND json_extract(runtime_json, '$.watch_id')=? "
                "AND json_extract(runtime_json, '$.state') IN "
                "('timed_out','completed','failed','restarted','recovered')"
                ")",
                (
                    serialized,
                    node_name,
                    job_id,
                    data.get("state"),
                    data.get("watch_id"),
                ),
            )

        return self.submit_db_mutation(update, wait=wait)

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
        execution_ids = [uuid4().hex for _ in normalized]
        pid = os.getpid()
        thread_id = get_ident()
        event_time = datetime.now().isoformat(timespec="milliseconds")

        def claim(connection):
            rows = []
            for offset in range(0, len(normalized), 500):
                chunk = normalized[offset:offset + 500]
                placeholders = ",".join("?" for _ in chunk)
                rows.extend(
                    connection.execute(
                        "SELECT job_id, generation, status FROM jobs "
                        f"WHERE node_name=? AND job_id IN ({placeholders})",
                        [node_name, *chunk],
                    ).fetchall()
                )
            rows_by_id = {int(row["job_id"]): row for row in rows}
            missing = [job_id for job_id in normalized if job_id not in rows_by_id]
            if missing:
                raise FileNotFoundError(
                    f"Job does not exist: {node_name}/{missing[0]}"
                )

            updates = []
            events = []
            results = []
            for job_id, execution_id in zip(normalized, execution_ids):
                row = rows_by_id[job_id]
                generation = int(row["generation"])
                previous_status = str(row["status"])
                status_extra = {
                    "started_at": started_at,
                    "generation": generation,
                    "execution_id": execution_id,
                    "pid": pid,
                }
                updates.append(
                    (
                        execution_id,
                        pid,
                        thread_id,
                        started_at,
                        RUNNING,
                        json.dumps(
                            status_extra,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                        node_name,
                        job_id,
                        generation,
                    )
                )
                events.append(
                    (
                        node_name,
                        job_id,
                        event_time,
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
            return results

        return self.submit_db_mutation(claim, priority=priority)

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
