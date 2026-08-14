from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Callable

import httpx


@dataclass(slots=True)
class ClientShard:
    client: httpx.AsyncClient
    shard_id: int = 0
    created_at: float = 0.0
    in_flight: int = 0
    peak_in_flight: int = 0
    requests_started: int = 0
    requests_completed: int = 0
    requests_failed: int = 0
    last_terminal_at: float = 0.0
    last_error: str | None = None
    retiring: bool = False
    retired_reason: str | None = None
    retired_at: float = 0.0
    cohort_stalls: int = 0


class NetworkFuture(Future):
    def __init__(self) -> None:
        super().__init__()
        self.submitted_at = time.monotonic()
        self.dispatched_at: float | None = None
        self.completed_at: float | None = None
        self.node_name: str | None = None
        self.job_id: int | None = None
        self.project_key: str | None = None
        self._task_lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task | None = None

    def bind_task(self, loop: asyncio.AbstractEventLoop, task: asyncio.Task) -> None:
        with self._task_lock:
            self._loop = loop
            self._task = task
            already_cancelled = self.cancelled()
        if already_cancelled:
            loop.call_soon_threadsafe(task.cancel)

    def cancel(self) -> bool:
        """Cancel both the public future and its live HTTP coroutine."""
        cancelled = super().cancel()
        with self._task_lock:
            loop, task = self._loop, self._task
        if loop is not None and task is not None and not task.done():
            loop.call_soon_threadsafe(task.cancel)
        return cancelled


@dataclass(slots=True)
class NetworkRequest:
    method: str
    url: str
    kwargs: dict[str, Any]
    future: NetworkFuture
    project_key: str | None = None
    node_name: str | None = None
    job_id: int | None = None
    expect_json: bool = False
    state_sink: Callable[[list[dict[str, Any]], float], None] | None = None
    attempt_callback: Callable[[int, str | None], None] | None = None


class CohortStreamStall(RuntimeError):
    """One stream stayed nonterminal while newer siblings kept completing."""


@dataclass(slots=True)
class NetworkCounters:
    submitted: int = 0
    dispatched: int = 0
    completed: int = 0
    failed: int = 0
    bytes_received: int = 0
    in_flight: int = 0
    peak_in_flight: int = 0
    max_ingress_delay_seconds: float = 0.0
    max_request_seconds: float = 0.0
    total_request_seconds: float = 0.0
    last_error: str | None = None

    def row(self, node_name: str) -> dict[str, Any]:
        return {
            "node_name": node_name,
            "submitted": self.submitted,
            "dispatched": self.dispatched,
            "completed": self.completed,
            "failed": self.failed,
            "bytes_received": self.bytes_received,
            "in_flight": self.in_flight,
            "peak_in_flight": self.peak_in_flight,
            "max_ingress_delay_seconds": self.max_ingress_delay_seconds,
            "max_request_seconds": self.max_request_seconds,
            "average_request_seconds": (
                self.total_request_seconds / self.completed if self.completed else 0.0
            ),
            "last_error": self.last_error,
        }
