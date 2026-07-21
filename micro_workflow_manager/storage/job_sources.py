from __future__ import annotations

import hashlib
import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
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

    def pull(self, max_items: int) -> list[int]:
        if type(max_items) is not int or max_items < 0:
            raise ValueError("max_items must be an integer >= 0")
        if max_items == 0:
            return []
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

    def __iter__(self):
        while True:
            job_ids = self.pull(512)
            if not job_ids:
                return
            yield from job_ids

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

    def __iter__(self):
        while True:
            jobs = self.pull(512)
            if not jobs:
                return
            yield from jobs
