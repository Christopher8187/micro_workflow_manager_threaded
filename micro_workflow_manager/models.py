from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable


def now() -> str:
    return datetime.now().isoformat(timespec="seconds")


@dataclass
class Job:
    job_id: int
    node_name: str
    params: dict[str, Any]
    parent: dict[str, Any] | None = None
    created_at: str = field(default_factory=now)


@dataclass
class MountedTask:
    name: str
    handler: Callable
    allowed_params: set[str]
    required_params: set[str]
    retries: int = 0
    repeats: int = 1
    timeout: float | None = None


QUEUED = "queued"
RUNNING = "running"
DONE = "done"
FAILED = "failed"
CANCELLED = "cancelled"
SKIPPED = "skipped"

VALID_STATUSES = {
    QUEUED,
    RUNNING,
    DONE,
    FAILED,
    CANCELLED,
    SKIPPED,
}

# Jobs in these statuses are considered successful inputs for completing a node.
# CANCELLED is intentionally excluded: cancelling every job should not silently
# make downstream nodes run as if the work succeeded.
SUCCESSFUL_JOB_TERMINAL_STATUSES = {DONE, SKIPPED}

# Node-level completion is stricter and is used by dependency readiness checks.
NODE_COMPLETE_STATUSES = {DONE, SKIPPED}
