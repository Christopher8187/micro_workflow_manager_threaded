"""Shared SQLite mutation priority classes for durable workflow state."""

# Lower numeric values run first in SQLiteMutationWriter. Admission claims and
# terminal success/failure publication are both execution-critical: a queued job
# should become running promptly, and a completed/failed job should become
# terminal promptly. Keep both operations in the same top runtime priority class.
RUNTIME_CRITICAL_PRIORITY = 5
ADMISSION_PRIORITY = RUNTIME_CRITICAL_PRIORITY
TERMINAL_PRIORITY = RUNTIME_CRITICAL_PRIORITY
