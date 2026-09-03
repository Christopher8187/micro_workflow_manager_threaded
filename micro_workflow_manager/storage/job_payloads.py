from __future__ import annotations

import json
from pathlib import Path


class JobPayloadStorageMixin:
    """Persistent per-job parameter and terminal-return storage."""

    def write_output(self, node_name: str, job_id: int, data: dict):
        self.validate_job_id(job_id)
        self.atomic_write_json(self.output_file(node_name, job_id), data)

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
