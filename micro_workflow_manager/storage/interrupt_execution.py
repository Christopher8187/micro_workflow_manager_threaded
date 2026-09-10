"""Storage surface for explicit interrupt execution."""

from .interrupt_admission import (
    InterruptAdmissionStorageMixin,
    read_interrupt_admission_conflicts,
)
from .interrupt_common import InterruptAdmissionPaused, read_session_parent_ids
from .interrupt_coordination import InterruptCoordinationStorageMixin
from .interrupt_observation import InterruptObservationStorageMixin
from .interrupt_settlement import InterruptSettlementStorageMixin


class InterruptExecutionStorageMixin(
    InterruptAdmissionStorageMixin,
    InterruptObservationStorageMixin,
    InterruptCoordinationStorageMixin,
    InterruptSettlementStorageMixin,
):
    """Combine explicit-interrupt admission, coordination, and settlement."""


__all__ = [
    "InterruptAdmissionPaused",
    "InterruptExecutionStorageMixin",
    "read_interrupt_admission_conflicts",
    "read_session_parent_ids",
]
