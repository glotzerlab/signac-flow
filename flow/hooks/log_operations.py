# Copyright (c) 2023 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Builtin execution hook for basic logging."""
import logging


class LogOperations:
    """:class:`~.LogOperations` logs information about the execution of operations to a log file.

    This hook can provide information on the start, successful completion, and/or
    erroring of one or more operations in a `flow.FlowProject` instance. The logs are stored in a
    file given by the parameter ``fn_logfile``. This file will be appended to if it already exists.

    The default formating for the log provides the time, job id, log level, and log message.

    .. note::

        All logging is performed at the INFO level. To ensure outputs are captured in log files,
        use the `--debug` flag when running or submitting jobs, or specify
        `submit_options=--debug` in your directives (example shown below).


    Examples
    --------
    The following example will install :class:`~.LogOperations` at the operation level.
    Where the log will be stored in a file name `foo.log` in the job workspace.

    .. code-block:: python
        from flow import FlowProject
        from flow.hooks import LogOperations


        class Project(FlowProject):
            pass


        def install_operation_log_hook(operation_name, project_cls):
            log = LogOperation(f"{operation_name}.log")
            return lambda op: log.install_operation_hooks(op, project_cls)


        @install_operation_log_hook("foo", Project)
        @Project.operation(directives={
            "submit_options": "--debug"  # Always submit operation foo with the --debug flag
        })
        def foo(job):
            pass


    The code block below provides an example of how install :class:`~.LogOperations` to an
    instance of :class:`~.FlowProject`

    .. code-block:: python
        from flow import FlowProject
        from flow.hooks import LogOperations  # Import build


        class Project(FlowProject):
            pass


        # Project operation definitions


        if __name__ == "__main__":
            project = Project()
            project = LogOperations().install_project_hooks(project)
            project.main()

    Parameters
    ----------
    fn_logfile : str
        The name of the log file in the job workspace. Default is "operations.log".
    """

    def __init__(self, fn_logfile="operations.log"):
        self._fn_logfile = fn_logfile
        # getLogger keep its own cache. This reduces the time spent setting up loggers
        # by only doing it once.
        self._loggers = {}

    def on_start(self, operation, job):
        """Log the start of execution of an operation on a job."""
        self._get_logger(job).info(f"Operation '{operation}' started.")

    def on_success(self, operation, job):
        """Log the successful completion of a given job(s) operation pair."""
        self._get_logger(job).info(
            f"Operation '{operation}' finished without exception."
        )

    def on_exception(self, operation, error, job):
        """Log the raising of an error in the execution of a given job(s) operation pair."""
        self._get_logger(job).info(
            f"Operation '{operation}' failed with error '{error}'."
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
        """Decorate operation to log execution.

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
            The project to install hooks on.
        """
        project.project_hooks.on_start.append(self.on_start)
        project.project_hooks.on_success.append(self.on_success)
        project.project_hooks.on_exception.append(self.on_exception)
        return project
