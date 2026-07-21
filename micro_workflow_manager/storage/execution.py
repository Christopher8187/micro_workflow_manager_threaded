from __future__ import annotations

from .execution_claims import JobExecutionClaimStorageMixin
from .execution_restart import JobRestartStorageMixin
from .execution_terminal import JobTerminalStorageMixin


class JobExecutionStorageMixin(
    JobRestartStorageMixin,
    JobTerminalStorageMixin,
    JobExecutionClaimStorageMixin,
):
    """Facade for execution leases, terminal publication, and restarts."""
