from functools import wraps


def decorate_with_job(func):
    """TODO: Add docs."""

    @wraps(func)
    def decorated(*jobs):
        with jobs[0] as job:
            if getattr(func, "_flow_cmd", False):
                return f'trap "cd $(pwd)" EXIT && cd {job.ws} && {func(job)}'
            else:
                return func(job)

    setattr(decorated, "_flow_with_job", True)
    return decorated
