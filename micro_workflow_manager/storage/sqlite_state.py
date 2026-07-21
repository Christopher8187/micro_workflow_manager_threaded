from __future__ import annotations

from .sqlite.advisory import SQLiteAdvisoryLockMixin
from .sqlite.connection import SQLiteConnectionMixin
from .sqlite.schema import DATABASE_SCHEMA_VERSION, SQLiteSchemaMixin
from .sqlite.transfer import SQLiteStateTransferMixin


class SQLiteStateMixin(
    SQLiteStateTransferMixin,
    SQLiteAdvisoryLockMixin,
    SQLiteSchemaMixin,
    SQLiteConnectionMixin,
):
    """Facade preserving the historical storage mixin import."""


__all__ = ["DATABASE_SCHEMA_VERSION", "SQLiteStateMixin"]
