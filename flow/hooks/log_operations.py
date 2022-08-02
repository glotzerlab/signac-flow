# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Builtin execution hook for basic logging."""
import logging


class LogOperations:
    """:class:`~.LogOperations` logs basic information about the execution of operations to a log file.

    #TODO: Add detail

    Parameters
    ----------
    fn_logfile: log filename
        #TODO.
        Default is ".operations.logs"
    """

    def __init__(self, fn_logfile=".operations.log"):
        self._fn_logfile = fn_logfile
        self._loggers = dict()

    def log_operation(self, operation, job):
        """TODO: Add user-defined job operation to logger."""
        if str(job) in self._loggers:
            logger = self._loggers[str(job)]
        else:
            logger = self._loggers[str(job)] = logging.getLogger(str(job))
            logger.setLevel(logging.DEBUG)

            fh = logging.FileHandler(job.fn(self._fn_logfile))
            fh.setLevel(logging.DEBUG)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            fh.setFormatter(formatter)

            logger.addHandler(fh)
            logger.info(f"Executed operation '{operation}'.")

    def log_operation_on_exception(self, operation, error, job):
        """TODO: Add user-defined job operation to logger."""
        if str(job) in self._loggers:
            logger = self._loggers[str(job)]
        else:
            logger = self._loggers[str(job)] = logging.getLogger(str(job))
            logger.setLevel(logging.DEBUG)

            fh = logging.FileHandler(job.fn(self._fn_logfile))
            fh.setLevel(logging.DEBUG)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            fh.setFormatter(formatter)

            logger.addHandler(fh)

        if error is None:
            logger.info(f"Executed operation '{operation}'.")
        else:
            logger.info(
                "Execution of operation '{}' failed with "
                "error '{}'.".format(operation, error)
            )

    def install_hooks(self, project):
        """Install log operation to all operations in a signac project."""
        project.project_hooks.on_success.append(self.log_operation)
        project.project_hooks.on_exception.append(self.log_operation_on_exception)
        return project

    __call__ = install_hooks
