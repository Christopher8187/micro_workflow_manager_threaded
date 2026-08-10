from __future__ import annotations

import hashlib
import json
import os
import shutil
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from threading import Event, Lock
from typing import Any, Iterable
from uuid import uuid4

from micro_workflow_manager.models import Job, QUEUED


class RefreshableQueuedJobSource:
    """Incrementally expose queued jobs that appear while a node pump is active.

    The cursor follows SQLite insertion order rather than ``job_id`` order. Batch
    producers reserve IDs before writing payloads and may commit those batches out
    of ID order; ``rowid`` still advances in commit order, so no late lower-ID batch
    is skipped. Existing jobs are included on the first pull, and newly inserted
    queued jobs become visible to later pulls.
    """

    def __init__(self, storage, node_name: str):
        self.storage = storage
        self.node_name = storage.validate_node_name(node_name)
        self.last_rowid = 0
        self.lock = Lock()

    def pull(self, max_items: int) -> list[int]:
        if type(max_items) is not int or max_items < 0:
            raise ValueError("max_items must be an integer >= 0")
        if max_items == 0:
            return []
        with self.lock:
            rows = self.storage.db_connection().execute(
                "SELECT rowid AS source_rowid, job_id FROM jobs "
                "WHERE node_name=? AND status=? AND rowid>? "
                "ORDER BY rowid LIMIT ?",
                (self.node_name, QUEUED, self.last_rowid, max_items),
            ).fetchall()
            if not rows:
                return []
            self.last_rowid = max(int(row["source_rowid"]) for row in rows)
        return [int(row["job_id"]) for row in rows]

    def remaining_hint(self) -> int:
        """Return a cheap startup-window hint for the current queue suffix."""
        with self.lock:
            row = self.storage.db_connection().execute(
                "SELECT COUNT(*) AS count FROM jobs "
                "WHERE node_name=? AND status=? AND rowid>?",
                (self.node_name, QUEUED, self.last_rowid),
            ).fetchone()
        return 0 if row is None else int(row["count"])

    def __iter__(self):
        while True:
            job_ids = self.pull(512)
            if not job_ids:
                return
            yield from job_ids



class LiveRefreshableQueuedJobObjectSource:
    """Keep one Hoeflein member attached to its durable queue until stopped.

    ``reserve``/``pull`` stay non-blocking so API runtimes can continue
    servicing already-active fibers.  Runners that reach an idle point call
    ``wait_for_change``; it sleeps on the project state-event broker and wakes
    when sibling component members publish feedback.  The component scheduler
    owns ``stop_event`` and sets it only for quiescence/failure, so a temporary
    empty queue never turns a Hoeflein member into a completed mini-DAG node.
    """

    refreshable = True

    def __init__(self, storage, source, stop_event: Event):
        self.storage = storage
        self.source = source
        self.stop_event = stop_event
        self.wake_event = Event()
        self.closed = False
        self.node_name = storage.validate_node_name(source.node_name)
        self.unsubscribe = storage.subscribe_queue_changes(
            self.wake_event.set, node_name=self.node_name
        )

    def reserve(self, max_items: int) -> list[int]:
        if self.closed or self.stop_event.is_set():
            return []
        reserve = getattr(self.source, "reserve", None)
        if not callable(reserve):
            raise TypeError("live queued source requires reserve()")
        return list(reserve(max_items))

    def load_reserved(self, job_ids: list[int]) -> list[Job]:
        if not job_ids:
            return []
        loader = getattr(self.source, "load_reserved", None)
        if not callable(loader):
            raise TypeError("live queued source requires load_reserved()")
        return list(loader(job_ids))

    def pull(self, max_items: int) -> list[Job]:
        return self.load_reserved(self.reserve(max_items))

    def remaining_hint(self) -> int:
        hint = getattr(self.source, "remaining_hint", None)
        return 0 if not callable(hint) else int(hint())

    def wait_for_change(self, timeout: float = 5.0) -> bool:
        """Wait until feedback/state changes or the component asks us to stop.

        The clear/recheck sequence closes the usual Event clear/wait race.
        ``True`` means the caller should probe the queue again; ``False`` means
        the component has stopped and the live pump should exit.
        """
        if self.closed or self.stop_event.is_set():
            return False
        self.wake_event.clear()
        if self.closed or self.stop_event.is_set():
            return False
        # A queue item may have committed immediately before clear(). Recheck
        # without consuming it so the following pull observes it normally.
        if self.remaining_hint() > 0:
            return True
        self.wake_event.wait(timeout)
        return not self.closed and not self.stop_event.is_set()

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.wake_event.set()
        self.unsubscribe()
        close = getattr(self.source, "close", None)
        if callable(close):
            close()



class PrefetchingQueuedJobObjectSource:
    """Bounded payload prefetch for queued job-object sources.

    Reservation and payload loading are deliberately split. A source that
    implements ``reserve``/``load_reserved`` reserves job IDs synchronously in
    a short critical section, then performs the filesystem payload reads in
    background workers. This keeps the threaded runner's shared source lock
    away from 64-file disk bursts while preserving reservation order.

    The default remains one worker/one batch for API admission. Threaded nodes
    opt into a small bounded multi-batch window so short local routing tasks do
    not drain the payload queue faster than a single loader can refill it.
    """

    def __init__(
        self,
        storage,
        source,
        *,
        prefetch_size: int = 64,
        prefetch_workers: int = 1,
        prefetch_batches: int | None = None,
    ):
        if type(prefetch_size) is not int or prefetch_size < 1:
            raise ValueError("prefetch_size must be an integer >= 1")
        if type(prefetch_workers) is not int or prefetch_workers < 1:
            raise ValueError("prefetch_workers must be an integer >= 1")
        if prefetch_batches is None:
            prefetch_batches = prefetch_workers
        if type(prefetch_batches) is not int or prefetch_batches < 1:
            raise ValueError("prefetch_batches must be an integer >= 1")
        self.storage = storage
        self.source = source
        self.prefetch_size = prefetch_size
        self.prefetch_workers = prefetch_workers
        self.prefetch_batches = max(prefetch_workers, prefetch_batches)
        self.executor = ThreadPoolExecutor(
            max_workers=prefetch_workers,
            thread_name_prefix="mwf-job-prefetch",
        )
        self.futures: deque[Future[list[Job]]] = deque()
        self.buffer: list[Job] = []
        self.closed = False
        self.exhausted = False
        self._schedule_until_full()

    def _load_reserved_background(self, reserved) -> list[Job]:
        try:
            loader = getattr(self.source, "load_reserved", None)
            if callable(loader):
                return list(loader(reserved))
            return list(reserved)
        finally:
            self.storage.close_thread_connection()

    def _pull_background(self, count: int) -> list[Job]:
        try:
            return list(self.source.pull(count))
        finally:
            self.storage.close_thread_connection()

    def _schedule_one(self) -> bool:
        if self.closed or self.exhausted:
            return False
        reserve = getattr(self.source, "reserve", None)
        if callable(reserve):
            reserved = reserve(self.prefetch_size)
            if not reserved:
                # An empty reservation is terminal only for snapshot sources. A
                # refreshable Hoeflein/API source may receive more jobs after the
                # current queue momentarily drains. Permanently exhausting it is
                # the 0.5.2 desynchronization regression.
                if not bool(getattr(self.source, "refreshable", False)):
                    self.exhausted = True
                return False
            self.futures.append(
                self.executor.submit(self._load_reserved_background, reserved)
            )
            return True
        # Generic pull sources are left at one outstanding pull unless callers
        # explicitly make them reservation-aware. Concurrent ``pull`` calls can
        # otherwise reorder a refreshable cursor.
        if self.futures:
            return False
        self.futures.append(
            self.executor.submit(self._pull_background, self.prefetch_size)
        )
        return True

    def _schedule_until_full(self) -> None:
        while len(self.futures) < self.prefetch_batches and not self.exhausted:
            if not self._schedule_one():
                break

    def pull(self, max_items: int) -> list[Job]:
        if type(max_items) is not int or max_items < 0:
            raise ValueError("max_items must be an integer >= 0")
        if max_items == 0 or self.closed:
            return []
        self._schedule_until_full()
        while len(self.buffer) < max_items and self.futures:
            future = self.futures.popleft()
            loaded = future.result()
            if not loaded and not callable(getattr(self.source, "reserve", None)):
                self.exhausted = True
            self.buffer.extend(loaded)
            self._schedule_until_full()
        result = self.buffer[:max_items]
        del self.buffer[:max_items]
        self._schedule_until_full()
        return result

    def remaining_hint(self) -> int | None:
        hint = getattr(self.source, "remaining_hint", None)
        if not callable(hint):
            return None
        return len(self.buffer) + int(hint())

    def wait_for_change(self, timeout: float = 5.0) -> bool:
        waiter = getattr(self.source, "wait_for_change", None)
        if not callable(waiter):
            return False
        return bool(waiter(timeout))

    def __iter__(self):
        while True:
            jobs = self.pull(self.prefetch_size)
            if jobs:
                yield from jobs
                continue
            if not self.wait_for_change():
                return

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.executor.shutdown(wait=True, cancel_futures=True)
        close_source = getattr(self.source, "close", None)
        if callable(close_source):
            close_source()


class QueuedJobObjectSource:
    """Load a queued-job snapshot with reservation separated from payload I/O."""

    refreshable = False

    def __init__(self, storage, node_name: str, job_ids: Iterable[int]):
        self.storage = storage
        self.node_name = storage.validate_node_name(node_name)
        self.job_ids = iter(job_ids)
        self.lock = Lock()

    def reserve(self, max_items: int) -> list[int]:
        if type(max_items) is not int or max_items < 0:
            raise ValueError("max_items must be an integer >= 0")
        if max_items == 0:
            return []
        reserved: list[int] = []
        with self.lock:
            for _ in range(max_items):
                try:
                    reserved.append(next(self.job_ids))
                except StopIteration:
                    break
        return reserved

    def load_reserved(self, job_ids: list[int]) -> list[Job]:
        if not job_ids:
            return []
        return self.storage.load_jobs_batch(self.node_name, job_ids)

    def pull(self, max_items: int) -> list[Job]:
        reserved = self.reserve(max_items)
        return self.load_reserved(reserved)

    def __iter__(self):
        while True:
            jobs = self.pull(64)
            if not jobs:
                return
            yield from jobs


class RefreshableQueuedJobObjectSource:
    """Refreshable queue cursor that preloads each admission burst."""

    refreshable = True

    def __init__(self, storage, node_name: str):
        self.storage = storage
        self.node_name = storage.validate_node_name(node_name)
        self.job_ids = RefreshableQueuedJobSource(storage, self.node_name)

    def reserve(self, max_items: int) -> list[int]:
        return self.job_ids.pull(max_items)

    def load_reserved(self, job_ids: list[int]) -> list[Job]:
        return self.storage.load_jobs_batch(self.node_name, job_ids)

    def pull(self, max_items: int) -> list[Job]:
        return self.load_reserved(self.reserve(max_items))

    def remaining_hint(self) -> int:
        return self.job_ids.remaining_hint()

    def __iter__(self):
        while True:
            jobs = self.pull(512)
            if not jobs:
                return
            yield from jobs
