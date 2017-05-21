class SubmitError(RuntimeError):
    pass


class NoSchedulerError(SubmitError):
    pass
