from __future__ import annotations

from .sqlite.advisory import SQLiteAdvisoryLockMixin
from .sqlite.connection import SQLiteConnectionMixin
from .sqlite.schema import DATABASE_SCHEMA_VERSION, SQLiteSchemaMixin
from .clipboard_operation import ClipboardOperationStorageMixin
from .node_state_deletion import NodeStateDeletionStorageMixin


class SQLiteStateMixin(
    ClipboardOperationStorageMixin,
    NodeStateDeletionStorageMixin,
    SQLiteAdvisoryLockMixin,
    SQLiteSchemaMixin,
    SQLiteConnectionMixin,
):
    """Facade preserving the historical storage mixin import."""


__all__ = ["DATABASE_SCHEMA_VERSION", "SQLiteStateMixin"]
