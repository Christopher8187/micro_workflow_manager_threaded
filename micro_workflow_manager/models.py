from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable


def now() -> str:
    return datetime.now().isoformat(timespec="milliseconds")


@dataclass
class Job:
    job_id: int
    node_name: str
    params: dict[str, Any]
    parent: dict[str, Any] | None = None
    producer_component: tuple[str, ...] | None = None
    job_kind: str | None = None
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
    checkpoint_timeout: float | None = None


QUEUED = "queued"
WAITING = "waiting"
RUNNING = "running"
DONE = "done"
FAILED = "failed"
CANCELLED = "cancelled"
SKIPPED = "skipped"

JOB_VALID_STATUSES = {
    QUEUED,
    RUNNING,
    DONE,
    FAILED,
    CANCELLED,
    SKIPPED,
}

NODE_VALID_STATUSES = JOB_VALID_STATUSES | {WAITING}

# Backward-compatible alias for callers that historically treated this as the
# complete lifecycle vocabulary. Storage paths distinguish jobs from nodes via
# the two explicit sets above.
VALID_STATUSES = NODE_VALID_STATUSES

# Jobs in these statuses are considered successful inputs for completing a node.
# CANCELLED is intentionally excluded: cancelling every job should not silently
# make downstream nodes run as if the work succeeded.
SUCCESSFUL_JOB_TERMINAL_STATUSES = {DONE, SKIPPED}

# Node-level completion is stricter and is used by dependency readiness checks.
NODE_COMPLETE_STATUSES = {DONE, SKIPPED}
