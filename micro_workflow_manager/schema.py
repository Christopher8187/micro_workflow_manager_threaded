"""Versioning for MWF-owned persisted state.

The JSON schema applies to low-churn framework configuration. High-churn job,
queue, event, execution, checkpoint, idempotency, node-status, and advisory-lock
state has its own SQLite schema version. User inputs, outputs, and returned
files are deliberately excluded from both framework schemas.
"""

CURRENT_STATE_SCHEMA_VERSION = 2
STATE_SCHEMA_FIELD = "schema_version"
