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


class JobTimeoutError(MicroWorkflowError):
    """Raised when a task exceeds its configured timeout."""

    pass


def safe_exception_repr(error: BaseException) -> str:
    """Render an exception without allowing broken display methods to escape."""
    try:
        return repr(error)
    except BaseException:
        try:
            message = str(error)
        except BaseException:
            message = "<unprintable>"
        return f"{type(error).__name__}({message!r})"
