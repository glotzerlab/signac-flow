class SubmitError(RuntimeError):
    "Indicates an error during cluster job submission."
    pass


class NoSchedulerError(AttributeError):
    "Indicates that there is no scheduler type defined for an environment class."
    pass
