from functools import wraps


def decorate_with_job(func):
    """Use ``arg`` as a context manager for ``func(arg)`` with this decorator.

    This decorator can only be used for operations that accept a single job as a parameter.

    If this function is an operation function defined by :class:`~.FlowProject`, it will
    be the same as using ``with job:``.

    .. note::

        This class is used by the :class:`~._FlowProjectClass` metaclass for the setup of
        operation functions and should not be used by users themselves.

    """

    @wraps(func)
    def decorated(job):
        with job:
            if getattr(func, "_flow_cmd", False):
                return f'trap "cd $(pwd)" EXIT && cd {job.ws} && {func(job)}'
            else:
                return func(job)

    setattr(decorated, "_flow_with_job", True)
    return decorated


__all__ = ["decorate_with_job"]
