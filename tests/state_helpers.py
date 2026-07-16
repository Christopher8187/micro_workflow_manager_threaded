from __future__ import annotations

from pathlib import Path
from typing import Any

from micro_workflow_manager.models import Job
from micro_workflow_manager.storage import FileStorage


def storage(root: Path) -> FileStorage:
    return FileStorage(root)


def seed_job(
    root: Path,
    node: str,
    job_id: int,
    status: str = "queued",
    *,
    params: dict[str, Any] | None = None,
    parent: dict[str, Any] | None = None,
    created_at: str = "test",
    status_extra: dict[str, Any] | None = None,
) -> FileStorage:
    state = FileStorage(root)
    if not state.job_exists(node, job_id):
        state.create_job(
            Job(
                job_id=job_id,
                node_name=node,
                params=dict(params or {}),
                parent=parent,
                created_at=created_at,
            )
        )
    if status != "queued" or status_extra:
        state.set_job_status(node, job_id, status, **dict(status_extra or {}))
    return state
