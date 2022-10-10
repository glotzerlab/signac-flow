# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Builtin execution hook for basic logging."""
import logging


class LogOperations:
    """:class:`~.LogOperations` logs information about the execution of operations to a log file.

    #TODO: Add detail

    Parameters
    ----------
    fn_logfile: log filename
        #TODO.
        Default is ".operations.logs"
    """

    def __init__(self, fn_logfile=".operations.log"):
        self._fn_logfile = fn_logfile
        # getLogger keep its own cache. This just serves to reduce the time spent setting up loggers
        # by only doing it once.
        self._loggers = dict()

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

    def install_project_hooks(self, project):
        """Install log operation to all operations in a signac-flow project."""
        project.project_hooks.on_start.append(self.on_start)
        project.project_hooks.on_success.append(self.on_success)
        project.project_hooks.on_exception.append(self.on_exception)
        return project

    __call__ = install_project_hooks
