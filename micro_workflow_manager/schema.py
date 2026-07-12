"""Versioning for MWF-owned file-backed state.

The schema version applies only to framework metadata. User inputs, task
outputs, returned files, and events.jsonl are deliberately excluded.
"""

CURRENT_STATE_SCHEMA_VERSION = 1
STATE_SCHEMA_FIELD = "schema_version"
