from __future__ import annotations

from .base import FileStorageBase
from .execution import JobExecutionStorageMixin
from .job_index import JobIndexStorageMixin
from .jobs import JobFileStorageMixin
from .nodes import NodeFileStorageMixin


class FileStorage(
    JobExecutionStorageMixin,
    JobFileStorageMixin,
    JobIndexStorageMixin,
    NodeFileStorageMixin,
    FileStorageBase,
):
    """Default file-backed storage adapter.

    The rest of the workflow manager depends on this public storage API rather
    than on one monolithic module. A future database adapter can implement the
    same API and be selected at construction time without forcing scheduler or
    CLI code to know where state is stored.
    """

    pass
