from __future__ import annotations

from .job_batches import JobBatchStorageMixin
from .job_cleanup import JobCleanupStorageMixin
from .job_creation import JobCreationStorageMixin
from .job_identity import JobIdentityStorageMixin
from .job_payloads import JobPayloadStorageMixin
from .job_queries import JobQueryStorageMixin
from .job_sources import (
    PrefetchingQueuedJobObjectSource,
    QueuedJobObjectSource,
    RefreshableQueuedJobObjectSource,
    RefreshableQueuedJobSource,
)


class JobFileStorageMixin(
    JobPayloadStorageMixin,
    JobCleanupStorageMixin,
    JobQueryStorageMixin,
    JobBatchStorageMixin,
    JobCreationStorageMixin,
    JobIdentityStorageMixin,
):
    """Facade for SQLite job metadata and filesystem payload operations."""


__all__ = [
    "JobFileStorageMixin",
    "PrefetchingQueuedJobObjectSource",
    "QueuedJobObjectSource",
    "RefreshableQueuedJobObjectSource",
    "RefreshableQueuedJobSource",
]
