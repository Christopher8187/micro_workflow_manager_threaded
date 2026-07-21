from __future__ import annotations

from .base import FileStorageBase
from .execution import JobExecutionStorageMixin
from .events import JobEventStorageMixin
from .job_index import JobIndexStorageMixin
from .jobs import JobFileStorageMixin
from .nodes import NodeFileStorageMixin
from .runtime_config import RuntimeConfigStorageMixin
from .sqlite_state import SQLiteStateMixin


class FileStorage(
    RuntimeConfigStorageMixin,
    JobEventStorageMixin,
    JobExecutionStorageMixin,
    JobFileStorageMixin,
    JobIndexStorageMixin,
    NodeFileStorageMixin,
    SQLiteStateMixin,
    FileStorageBase,
):
    """Hybrid storage: user payload files plus SQLite framework state."""

    def __init__(self, project_dir):
        super().__init__(project_dir)
        self._init_sqlite_state()
        self._init_job_execution_state()
