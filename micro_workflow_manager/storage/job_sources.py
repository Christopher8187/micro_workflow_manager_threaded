from __future__ import annotations

import hashlib
import json
import os
import shutil
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from threading import Lock
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



class PrefetchingQueuedJobObjectSource:
    """Overlap the next payload batch with the current API admission slice.

    Only payload reads are prefetched. Jobs remain durably queued until the
    owning pump claims the returned batch, so a crash or sibling failure cannot
    create running jobs whose handlers never started.
    """

    def __init__(self, storage, source, *, prefetch_size: int = 64):
        if type(prefetch_size) is not int or prefetch_size < 1:
            raise ValueError("prefetch_size must be an integer >= 1")
        self.storage = storage
        self.source = source
        self.prefetch_size = prefetch_size
        self.executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="mwf-job-prefetch",
        )
        self.future: Future[list[Job]] | None = None
        self.buffer: list[Job] = []
        self.closed = False

    def _pull_background(self, count: int) -> list[Job]:
        try:
            return list(self.source.pull(count))
        finally:
            self.storage.close_thread_connection()

    def _schedule(self) -> None:
        if self.closed or self.future is not None:
            return
        self.future = self.executor.submit(
            self._pull_background,
            self.prefetch_size,
        )

    def pull(self, max_items: int) -> list[Job]:
        if type(max_items) is not int or max_items < 0:
            raise ValueError("max_items must be an integer >= 0")
        if max_items == 0 or self.closed:
            return []
        if not self.buffer:
            if self.future is None:
                self.buffer.extend(self.source.pull(max(max_items, self.prefetch_size)))
            else:
                future = self.future
                self.future = None
                self.buffer.extend(future.result())
        result = self.buffer[:max_items]
        del self.buffer[:max_items]
        if result and len(self.buffer) < self.prefetch_size:
            self._schedule()
        return result

    def remaining_hint(self) -> int | None:
        hint = getattr(self.source, "remaining_hint", None)
        if not callable(hint):
            return None
        return len(self.buffer) + int(hint())

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.executor.shutdown(wait=True, cancel_futures=True)


class QueuedJobObjectSource:
    """Load a snapshot of queued job payloads in database-sized bursts."""

    def __init__(self, storage, node_name: str, job_ids: Iterable[int]):
        self.storage = storage
        self.node_name = storage.validate_node_name(node_name)
        self.job_ids = iter(job_ids)

    def __iter__(self):
        while True:
            batch = []
            for _ in range(64):
                try:
                    batch.append(next(self.job_ids))
                except StopIteration:
                    break
            if not batch:
                return
            yield from self.storage.load_jobs_batch(self.node_name, batch)

class RefreshableQueuedJobObjectSource:
    """Refreshable queue cursor that preloads each admission burst."""

    def __init__(self, storage, node_name: str):
        self.storage = storage
        self.node_name = storage.validate_node_name(node_name)
        self.job_ids = RefreshableQueuedJobSource(storage, self.node_name)

    def pull(self, max_items: int) -> list[Job]:
        job_ids = self.job_ids.pull(max_items)
        return self.storage.load_jobs_batch(self.node_name, job_ids)

    def remaining_hint(self) -> int:
        return self.job_ids.remaining_hint()

    def __iter__(self):
        while True:
            jobs = self.pull(512)
            if not jobs:
                return
            yield from jobs
