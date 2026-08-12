from __future__ import annotations

from .execution_claims import JobExecutionClaimStorageMixin
from .execution_restart import JobRestartStorageMixin
from .execution_terminal import JobTerminalStorageMixin
from .runtime_observations import JobRuntimeObservationStorageMixin


class JobExecutionStorageMixin(
    JobRestartStorageMixin,
    JobTerminalStorageMixin,
    JobRuntimeObservationStorageMixin,
    JobExecutionClaimStorageMixin,
):
    """Facade for execution leases, terminal publication, and restarts."""

    def _init_job_execution_state(self) -> None:
        self._init_job_execution_claim_state()
        self._init_job_runtime_state()
