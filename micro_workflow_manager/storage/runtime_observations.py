from __future__ import annotations

import json
from concurrent.futures import Future
from dataclasses import dataclass, field
from threading import Lock
from typing import Any


@dataclass(slots=True, frozen=True)
class RuntimeUpdate:
    node_name: str
    job_id: int
    generation: int
    execution_id: str | None
    state: str
    watch_id: str | None
    serialized: str


@dataclass(slots=True)
class RuntimeUpdateSlot:
    """One queued runtime write whose not-yet-executing value is replaceable."""

    latest: RuntimeUpdate
    accepting: bool = True
    lock: Lock = field(default_factory=Lock)
    future: Future | None = None

    def take(self) -> RuntimeUpdate:
        with self.lock:
            self.accepting = False
            return self.latest


class JobRuntimeObservationStorageMixin:
    """Generation-fenced, coalescing checkpoint and attempt observations."""

    def _init_job_runtime_state(self) -> None:
        self._runtime_slot_lock = Lock()
        self._runtime_slots: dict[
            tuple[str, int, int, str | None, int],
            RuntimeUpdateSlot,
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
    def _apply_runtime_updates(connection, slots: list[RuntimeUpdateSlot]):
        if not slots:
            return []
        updates = [slot.take() for slot in slots]
        latest_by_attempt: dict[
            tuple[str, int, int, str | None], RuntimeUpdate
        ] = {}
        for update in updates:
            latest_by_attempt[(
                update.node_name,
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
        return [(True, None) for _slot in slots]

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
        slot = RuntimeUpdateSlot(update)
        if wait:
            return self.submit_grouped_db_mutation(
                ("runtime", node_name),
                slot,
                self._apply_runtime_updates,
                wait=True,
                priority=priority,
                collect_seconds=0.001,
            )

        key = (node_name, job_id, generation, execution_id, priority)
        with self._runtime_slot_lock:
            existing = self._runtime_slots.get(key)
            if existing is not None:
                with existing.lock:
                    if existing.accepting:
                        existing.latest = update
                        assert existing.future is not None
                        return existing.future
            self._runtime_slots[key] = slot
            future = self.submit_grouped_db_mutation(
                ("runtime", node_name),
                slot,
                self._apply_runtime_updates,
                wait=False,
                priority=priority,
                collect_seconds=0.001,
            )
            slot.future = future

        def clear(_completed) -> None:
            with self._runtime_slot_lock:
                if self._runtime_slots.get(key) is slot:
                    self._runtime_slots.pop(key, None)

        future.add_done_callback(clear)
        return future
