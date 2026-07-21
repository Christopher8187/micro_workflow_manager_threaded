from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from threading import Event
from time import monotonic
from uuid import uuid4


def _deadline_iso(seconds: float | None) -> str | None:
    if seconds is None:
        return None
    return (datetime.now().astimezone() + timedelta(seconds=seconds)).isoformat(
        timespec="milliseconds"
    )

def _validate_timeout(value: float | int | None, *, name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a positive number or None")
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive finite number or None")
    return value

def _validate_progress(value: float | int | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("progress must be a number from 0 to 1 or None")
    value = float(value)
    if not math.isfinite(value) or not 0 <= value <= 1:
        raise ValueError("progress must be a finite number from 0 to 1")
    return value

@dataclass
class AttemptWatch:
    """One scheduler-owned handler attempt and its progress deadline.

    A watch may be passive when neither timeout is configured. Passive watches
    do not start the supervisor thread or write runtime state until the task
    explicitly reports a checkpoint, preserving the old fast path for ordinary
    jobs that do not use progress reporting or timeout supervision.
    """

    node_name: str
    job_id: int
    task_name: str
    attempt: int
    repeat_index: int
    generation: int
    execution_id: str | None
    cancellation_event: Event
    total_timeout: float | None
    default_checkpoint_timeout: float | None
    force_abandonable: bool = False
    watch_id: str = field(default_factory=lambda: uuid4().hex)
    wake_event: Event = field(default_factory=Event)
    started_at: str = field(default_factory=lambda: datetime.now().astimezone().isoformat(timespec="milliseconds"))
    started_monotonic: float = field(default_factory=monotonic)
    total_deadline: float | None = None
    checkpoint_deadline: float | None = None
    checkpoint_timeout: float | None = None
    checkpoint_at: str | None = None
    checkpoint_name: str | None = None
    progress: float | None = None
    progress_detail: str | None = None
    revision: int = 0
    state: str = "active"
    timeout_kind: str | None = None
    timeout_message: str | None = None
    cancel_message: str | None = None
    runtime_written: bool = False
    external_wait_depth: int = 0
    external_wait_name: str | None = None
    external_wait_timeout: float | None = None
    external_wait_deadline: float | None = None

    @property
    def supervised(self) -> bool:
        """Whether a deadline/checkpoint requires scheduler supervision."""
        return self.total_timeout is not None or self.default_checkpoint_timeout is not None

    @property
    def abandonable(self) -> bool:
        """Whether the controller must isolate the handler in one daemon thread."""
        return self.force_abandonable or self.supervised

    @property
    def key(self) -> str:
        return self.watch_id
