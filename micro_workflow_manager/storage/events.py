from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


class JobEventStorageMixin:
    """Append-only, per-job lifecycle history.

    Events are intentionally not part of scheduler state. A damaged or deleted
    events.jsonl never changes whether a job can run; it only reduces history.
    This keeps inspection useful without putting the hot scheduler path behind
    a large shared manifest.
    """

    def job_events_file(self, node_name: str, job_id: int) -> Path:
        return self.job_base_dir(node_name, job_id) / "events.jsonl"

    def append_job_event(self, node_name: str, job_id: int, event: str, **data: Any):
        self.validate_job_id(job_id)
        row = {
            "time": datetime.now().isoformat(timespec="milliseconds"),
            "event": str(event),
            **data,
        }
        line = json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
        # Only executions of the same job contend on this lock. There is no
        # node-wide or project-wide event log bottleneck.
        with self.interprocess_lock(f"job-{node_name}-{job_id}-events"):
            self.append_text(self.job_events_file(node_name, job_id), line)

    def read_job_events(self, node_name: str, job_id: int) -> list[dict[str, Any]]:
        path = self.job_events_file(node_name, job_id)
        if not path.exists():
            return []

        rows: list[dict[str, Any]] = []
        for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not raw.strip():
                continue
            try:
                value = json.loads(raw)
            except json.JSONDecodeError as error:
                rows.append({
                    "event": "invalid_event_line",
                    "line": line_number,
                    "error": str(error),
                    "raw": raw,
                })
                continue
            if isinstance(value, dict):
                rows.append(value)
        return rows
