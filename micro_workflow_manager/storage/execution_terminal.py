from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from micro_workflow_manager.errors import JobRestartedError
from micro_workflow_manager.models import CANCELLED, DONE, FAILED, RUNNING, SKIPPED


from .priorities import TERMINAL_PRIORITY


TERMINAL_REFRESH_SECONDS = 0.001


@dataclass(slots=True, frozen=True)
class TerminalUpdate:
    node_name: str
    job_id: int
    generation: int
    execution_id: str
    status: str
    extra: dict[str, Any]


class JobTerminalStorageMixin:
    """Lease-fenced terminal publication and explicit output-backed recovery."""

    def finalize_job_execution(
        self,
        node_name: str,
        job_id: int,
        lease_generation: int,
        lease_execution_id: str,
        status: str,
        priority: int = TERMINAL_PRIORITY,
        **extra,
    ) -> None:
        """Publish one terminal update through the single SQLite writer.

        The writer groups related terminal records for at most one millisecond,
        then applies them with one bulk operation in its existing transaction.
        There is no intermediate terminal-status queue.
        """
        node_name = self.validate_node_name(node_name)
        job_id = self.validate_job_id(job_id)
        status = self.validate_status(status)
        if status not in {DONE, FAILED, CANCELLED, SKIPPED}:
            raise ValueError("finalize_job_execution requires a terminal status")
        update = TerminalUpdate(
            node_name=node_name,
            job_id=job_id,
            generation=int(lease_generation),
            execution_id=lease_execution_id,
            status=status,
            extra=dict(extra),
        )
        self.submit_grouped_db_mutation(
            ("terminal",),
            update,
            self._apply_terminal_updates,
            priority=priority,
            collect_seconds=TERMINAL_REFRESH_SECONDS,
        )

    def flush_terminal_updates(self) -> None:
        """Compatibility alias for the SQLite writer durability barrier."""
        self.db_mutation_barrier()

    @staticmethod
    def _terminal_restart_error(update: TerminalUpdate) -> JobRestartedError:
        return JobRestartedError(
            f"Job {update.node_name}/{update.job_id} generation "
            f"{update.generation} was restarted"
        )

    def _apply_terminal_updates(self, connection, updates: list[TerminalUpdate]):
        if not updates:
            return []

        requested_by_node: dict[str, list[int]] = {}
        for update in updates:
            requested_by_node.setdefault(update.node_name, []).append(update.job_id)
        rows = []
        for node_name, job_ids in requested_by_node.items():
            for offset in range(0, len(job_ids), 500):
                chunk = job_ids[offset:offset + 500]
                placeholders = ",".join("?" for _ in chunk)
                rows.extend(
                    connection.execute(
                        "SELECT node_name, job_id, status, generation, active_execution_id "
                        "FROM jobs "
                        f"WHERE node_name=? AND job_id IN ({placeholders})",
                        [node_name, *chunk],
                    ).fetchall()
                )
        rows_by_key = {
            (str(row["node_name"]), int(row["job_id"])): row
            for row in rows
        }
        event_time = datetime.now().isoformat(timespec="milliseconds")
        event_names = {
            DONE: "done",
            FAILED: "failed",
            CANCELLED: "cancelled",
            SKIPPED: "skipped",
        }
        outcomes = []
        status_updates = []
        events = []

        for update in updates:
            row = rows_by_key.get((update.node_name, update.job_id))
            if row is None or int(row["generation"]) != update.generation:
                outcomes.append((False, self._terminal_restart_error(update)))
                continue

            active_execution_id = row["active_execution_id"]
            if active_execution_id is None and str(row["status"]) == update.status:
                outcomes.append((True, None))
                continue
            if active_execution_id != update.execution_id:
                outcomes.append((False, self._terminal_restart_error(update)))
                continue

            previous_status = str(row["status"])
            status_updates.append((
                update.status,
                json.dumps(update.extra, ensure_ascii=False, separators=(",", ":")),
                update.node_name,
                update.job_id,
                update.generation,
                update.execution_id,
            ))
            events.append((
                update.node_name,
                update.job_id,
                event_time,
                event_names[update.status],
                json.dumps(
                    {
                        "previous_status": previous_status,
                        "status": update.status,
                        **update.extra,
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            ))
            outcomes.append((True, None))

        if status_updates:
            connection.executemany(
                "UPDATE jobs SET status=?, status_json=?, "
                "active_execution_id=NULL, active_pid=NULL, "
                "active_thread_id=NULL, active_started_at=NULL "
                "WHERE node_name=? AND job_id=? AND generation=? "
                "AND active_execution_id=?",
                status_updates,
            )
            connection.executemany(
                "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
                "VALUES(?, ?, ?, ?, ?)",
                events,
            )
        return outcomes

    def _submit_terminal_updates_batch(
        self,
        node_name: str,
        updates: list[TerminalUpdate],
        *,
        priority: int,
        wait: bool = True,
    ):
        """Submit a recovered batch without introducing another queue."""
        node_name = self.validate_node_name(node_name)
        futures = [
            self.submit_grouped_db_mutation(
                ("terminal",),
                update,
                self._apply_terminal_updates,
                priority=priority,
                collect_seconds=TERMINAL_REFRESH_SECONDS,
                wait=False,
            )
            for update in updates
        ]
        if not wait:
            return futures
        outcomes: list[BaseException | None] = []
        for future in futures:
            try:
                future.result()
            except BaseException as error:
                outcomes.append(error)
            else:
                outcomes.append(None)
        return outcomes

    def reconcile_terminal_outputs(
        self,
        node_names: list[str] | tuple[str, ...] | set[str] | None = None,
    ) -> int:
        """Recover output-backed terminal jobs whose SQLite update was lost.

        This is an explicit recovery operation. Normal execution does not scan
        output files; ``mwf resume`` and ``mwf resumefrom`` call it before they
        decide which stale running or failed jobs need a new generation.
        """
        self.db_mutation_barrier()
        params: list[object] = [RUNNING]
        sql = (
            "SELECT node_name, job_id, generation, active_execution_id, "
            "active_started_at FROM jobs WHERE status=? "
            "AND active_execution_id IS NOT NULL"
        )
        if node_names is not None:
            normalized = sorted({self.validate_node_name(name) for name in node_names})
            if not normalized:
                return 0
            placeholders = ",".join("?" for _ in normalized)
            sql += f" AND node_name IN ({placeholders})"
            params.extend(normalized)
        rows = self.db_connection().execute(sql, params).fetchall()
        recovered: dict[str, list[TerminalUpdate]] = {}
        for row in rows:
            node_name = str(row["node_name"])
            job_id = int(row["job_id"])
            output = self.read_json(self.output_file(node_name, job_id), default=None)
            if not isinstance(output, dict):
                continue
            status = output.get("status")
            if status not in {DONE, FAILED, CANCELLED, SKIPPED}:
                continue
            generation = int(row["generation"])
            output_generation = output.get("generation")
            if output_generation is not None:
                try:
                    if int(output_generation) != generation:
                        continue
                except (TypeError, ValueError):
                    continue
            path = self.output_file(node_name, job_id)
            try:
                finished_at = datetime.fromtimestamp(path.stat().st_mtime).isoformat(
                    timespec="milliseconds"
                )
            except OSError:
                finished_at = datetime.now().isoformat(timespec="milliseconds")
            recovered.setdefault(node_name, []).append(
                TerminalUpdate(
                    node_name=node_name,
                    job_id=job_id,
                    generation=generation,
                    execution_id=str(row["active_execution_id"]),
                    status=str(status),
                    extra={
                        "started_at": row["active_started_at"],
                        "finished_at": finished_at,
                        "generation": generation,
                        "execution_id": str(row["active_execution_id"]),
                        "recovered_from_output": True,
                    },
                )
            )

        count = 0
        for node_name, updates in recovered.items():
            outcomes = self._submit_terminal_updates_batch(
                node_name,
                updates,
                priority=0,
            )
            count += sum(outcome is None for outcome in outcomes)
        self.db_mutation_barrier()
        return count
