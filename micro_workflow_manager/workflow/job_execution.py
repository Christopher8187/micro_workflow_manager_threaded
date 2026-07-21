from __future__ import annotations

from .job_api import JobExecutionApiMixin
from .job_lifecycle import JobLifecycleMixin
from .task_execution import MountedTaskExecutionMixin


class JobExecutionMixin(
    JobExecutionApiMixin,
    MountedTaskExecutionMixin,
    JobLifecycleMixin,
):
    """Facade for job lifecycle, mounted-task execution, and public controls."""

    restart_poll_interval_seconds = 0.05
