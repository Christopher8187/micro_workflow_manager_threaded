class MicroWorkflowError(Exception):
    pass


class InvalidGraphError(MicroWorkflowError):
    pass


class InvalidJobError(MicroWorkflowError):
    pass


class JobFailedError(MicroWorkflowError):
    pass