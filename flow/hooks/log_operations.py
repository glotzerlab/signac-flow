# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Builtin execution hook for basic logging."""
import logging


class LogOperations:
    """:class:`~.LogOperations` logs information about the execution of operations to a log file.

    This hooks provides information, optionally, on the start, successful completion, and/or
    erroring of one or more operations in a `flow.FlowProject` instance. The logs are stored in a
    file given by the parameter ``fn_logfile``. This file will be appended to if it already exists.

    The default formating for the log provides the time, job id, log level, and log message.

    Examples
    --------
    The code block below provides an example of how install  :class:`~.LogOperations` to a
    instance of :class:`~.FlowProject`

    .. code-block:: python
        from flow import FlowProject
        from flow.hooks import LogOperations  # Import build


        class Project(FlowProject):
            pass


        # Do something


        if __name__ == "__main__":
            LogOperations.install_project_hooks(Project).main()


    Parameters
    ----------
    fn_logfile: log filename
        The name of the log file in the job workspace. Default is "execution-record.log".
    """

    def __init__(self, fn_logfile="execution-record.log"):
        self._fn_logfile = fn_logfile
        # getLogger keep its own cache. This just serves to reduce the time spent setting up loggers
        # by only doing it once.
        self._loggers = {}

    def on_start(self, operation, job):
        """Log the start of execution of a given job(s) operation pair."""
        self._get_logger(job).info(f"Starting execution of operation '{operation}'.")

    def on_success(self, operation, job):
        """Log the successful completion of a given job(s) operation pair."""
        self._get_logger(job).info(
            f"Successfully finished execution of operation '{operation}'."
        )

    def on_exception(self, operation, error, job):
        """Log the raising of an error in the execution of a given job(s) operation pair."""
        self._get_logger(job).info(
            f"Execution of operation '{operation}' failed with error '{error}'."
        )

    def _get_logger(self, job):
        if job not in self._loggers:
            self._loggers[job] = self._setup_logger(job)
        return self._loggers[job]

    def _setup_logger(self, job):
        logger = logging.getLogger(str(job))
        fh = logging.FileHandler(job.fn(self._fn_logfile))
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger

    def install_operation_hooks(self, op, project_cls=None):
        """Decorate operation to install log operation to one operation in a signac-flow project.

        Parameters
        ----------
        op : function or type
            An operation function to log or a subclass of `flow.FlowProject` if ``project_cls`` is
            ``None``.
        project_cls : type
            A subclass of `flow.FlowProject`.
        """
        if project_cls is None:
            return lambda func: self.install_operation_hooks(func, op)
        project_cls.operation_hooks.on_start(self.on_start)(op)
        project_cls.operation_hooks.on_success(self.on_success)(op)
        project_cls.operation_hooks.on_exception(self.on_exception)(op)
        return op

    def install_project_hooks(self, project):
        """Install log operation to all operations in a signac-flow project.

        Parameters
        ----------
        project : flow.FlowProject
            The project to install project wide hooks on.
        """
        project.project_hooks.on_start.append(self.on_start)
        project.project_hooks.on_success.append(self.on_success)
        project.project_hooks.on_exception.append(self.on_exception)
        return project
