from __future__ import annotations

import json
from datetime import datetime
from typing import Any


class JobEventStorageMixin:
    """Append-only lifecycle history stored as indexed SQLite rows."""

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
