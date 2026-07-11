class MicroWorkflowError(Exception):
    pass


class InvalidGraphError(MicroWorkflowError):
    pass


class InvalidJobError(MicroWorkflowError):
    pass


class JobFailedError(MicroWorkflowError):
    pass


class JobRestartedError(MicroWorkflowError):
    """Raised when an execution lease is superseded by a manual restart."""

    pass
