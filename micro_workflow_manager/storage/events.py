from __future__ import annotations

import json
from datetime import datetime
from typing import Any


class JobEventStorageMixin:
    """Append-only lifecycle history stored as indexed SQLite rows."""

    @staticmethod
    def _decode_event_data(raw: str | None) -> dict[str, Any] | None:
        try:
            value = json.loads(raw or "{}")
        except json.JSONDecodeError:
            return None
        return value if isinstance(value, dict) else None

    @staticmethod
    def _created_event_origin(data: dict[str, Any]) -> dict[str, Any]:
        """Return only the fields that define a job's durable origin."""
        producer = data.get("producer_component")
        return {
            "parent": data.get("parent"),
            "producer_component": list(producer) if isinstance(producer, (list, tuple)) else [],
            "job_kind": data.get("job_kind"),
        }

    @classmethod
    def insert_job_created_events(cls, connection, rows: list[tuple[str, int, str, str]]) -> None:
        """Insert created events and record preserved-origin changes.

        ``--keeptrace`` may intentionally leave event rows behind while a job row
        is deleted and later recreated with the same node/job identity.  The most
        recent prior ``created`` event is therefore the durable old origin.  When
        the recreated job has a different parent, append a distinct
        ``origin_changed`` event immediately before its new ``created`` event.
        """
        if not rows:
            return

        previous_by_key: dict[tuple[str, int], dict[str, Any]] = {}
        by_node: dict[str, list[int]] = {}
        for node_name, job_id, _event_time, _data_json in rows:
            by_node.setdefault(node_name, []).append(job_id)

        for node_name, job_ids in by_node.items():
            unique_ids = sorted(set(job_ids))
            for offset in range(0, len(unique_ids), 500):
                chunk = unique_ids[offset:offset + 500]
                placeholders = ",".join("?" for _ in chunk)
                prior_rows = connection.execute(
                    "SELECT job_id, data_json FROM job_events "
                    "WHERE node_name=? AND event='created' "
                    f"AND job_id IN ({placeholders}) ORDER BY event_id",
                    [node_name, *chunk],
                ).fetchall()
                for prior in prior_rows:
                    decoded = cls._decode_event_data(prior["data_json"])
                    if decoded is not None:
                        previous_by_key[(node_name, int(prior["job_id"]))] = decoded

        insert_rows: list[tuple[str, int, str, str, str]] = []
        for node_name, job_id, event_time, data_json in rows:
            current = cls._decode_event_data(data_json) or {}
            previous = previous_by_key.get((node_name, job_id))
            previous_origin = (
                cls._created_event_origin(previous) if previous is not None else None
            )
            current_origin = cls._created_event_origin(current)
            if previous_origin is not None and previous_origin != current_origin:
                insert_rows.append(
                    (
                        node_name,
                        job_id,
                        event_time,
                        "origin_changed",
                        json.dumps(
                            {
                                "previous_origin": previous_origin,
                                "current_origin": current_origin,
                                # Retain the parent-only keys for compatibility
                                # with any 0.5.0 prerelease trace readers.
                                "previous_parent": previous_origin.get("parent"),
                                "current_parent": current_origin.get("parent"),
                            },
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                    )
                )
            insert_rows.append((node_name, job_id, event_time, "created", data_json))

        connection.executemany(
            "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
            "VALUES(?, ?, ?, ?, ?)",
            insert_rows,
        )

    def append_job_created_event(self, node_name: str, job_id: int, **data: Any) -> None:
        job_id = self.validate_job_id(job_id)
        event_time = datetime.now().isoformat(timespec="milliseconds")
        data_json = json.dumps(data, ensure_ascii=False, separators=(",", ":"))

        def append(connection):
            self.insert_job_created_events(
                connection,
                [(node_name, job_id, event_time, data_json)],
            )

        self.submit_db_mutation(append)

    def clear_job_events(
        self,
        node_name: str,
        job_ids: list[int] | None = None,
    ) -> int:
        """Remove the durable trace journal for a node or selected jobs."""
        node_name = self.validate_node_name(node_name)
        normalized = None
        if job_ids is not None:
            normalized = sorted({self.validate_job_id(job_id) for job_id in job_ids})
            if not normalized:
                return 0

        with self.db_transaction() as connection:
            if normalized is None:
                count = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM job_events WHERE node_name=?",
                        (node_name,),
                    ).fetchone()[0]
                )
                connection.execute("DELETE FROM job_events WHERE node_name=?", (node_name,))
                return count

            count = 0
            for offset in range(0, len(normalized), 500):
                chunk = normalized[offset:offset + 500]
                placeholders = ",".join("?" for _ in chunk)
                args = [node_name, *chunk]
                count += int(
                    connection.execute(
                        f"SELECT COUNT(*) FROM job_events WHERE node_name=? "
                        f"AND job_id IN ({placeholders})",
                        args,
                    ).fetchone()[0]
                )
                connection.execute(
                    f"DELETE FROM job_events WHERE node_name=? "
                    f"AND job_id IN ({placeholders})",
                    args,
                )
            return count

    def clear_job_events_for_nodes(self, node_names: list[str]) -> int:
        total = 0
        for node_name in dict.fromkeys(node_names):
            total += self.clear_job_events(node_name)
        return total

    def clear_job_events_produced_by_components(
        self,
        producer_components: set[tuple[str, ...]],
    ) -> int:
        """Clear journals whose latest creation came from selected components.

        A prior ``--keeptrace`` run may have preserved a journal after deleting
        its job row.  A later default fresh run must still remove that orphaned
        history before the selected producer recreates the same node/job ID.
        The latest ``created`` event is the durable provenance record even when
        the current job no longer exists.
        """
        normalized = {tuple(component) for component in producer_components}
        if not normalized:
            return 0

        rows = self.db_connection().execute(
            "SELECT events.node_name, events.job_id, events.data_json "
            "FROM job_events AS events "
            "JOIN ("
            "  SELECT node_name, job_id, MAX(event_id) AS event_id "
            "  FROM job_events WHERE event='created' GROUP BY node_name, job_id"
            ") AS latest ON latest.event_id=events.event_id"
        ).fetchall()
        targets: dict[str, list[int]] = {}
        for row in rows:
            data = self._decode_event_data(row["data_json"])
            if data is None:
                continue
            producer = data.get("producer_component")
            if not isinstance(producer, list) or tuple(producer) not in normalized:
                continue
            targets.setdefault(str(row["node_name"]), []).append(int(row["job_id"]))

        return sum(
            self.clear_job_events(node_name, job_ids)
            for node_name, job_ids in targets.items()
        )

    def append_job_event(self, node_name: str, job_id: int, event: str, **data: Any):
        self.validate_job_id(job_id)
        event_time = datetime.now().isoformat(timespec="milliseconds")
        data_json = json.dumps(data, ensure_ascii=False, separators=(",", ":"))

        def append(connection):
            connection.execute(
                "INSERT INTO job_events(node_name, job_id, time, event, data_json) "
                "VALUES(?, ?, ?, ?, ?)",
                (
                    node_name,
                    job_id,
                    event_time,
                    str(event),
                    data_json,
                ),
            )

        self.submit_db_mutation(append)

    def read_job_events(self, node_name: str, job_id: int) -> list[dict[str, Any]]:
        rows = self.db_connection().execute(
            "SELECT time, event, data_json FROM job_events "
            "WHERE node_name=? AND job_id=? ORDER BY event_id",
            (node_name, self.validate_job_id(job_id)),
        ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            try:
                data = json.loads(row["data_json"] or "{}")
            except json.JSONDecodeError as error:
                data = {"error": str(error), "raw": row["data_json"]}
            if not isinstance(data, dict):
                data = {"value": data}
            result.append({"time": row["time"], "event": row["event"], **data})
        return result
