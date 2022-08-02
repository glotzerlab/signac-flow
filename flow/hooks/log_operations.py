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

    def log_operation(self, operation, error=None):
        """Add user-defined job operation to logger."""
        if str(operation.job) in self._loggers:
            logger = self._loggers[str(operation.job)]
        else:
            logger = self._loggers[str(operation.job)] = logging.getLogger(
                str(operation.job)
            )
            logger.setLevel(logging.DEBUG)

            fh = logging.FileHandler(operation.job.fn(self._fn_logfile))
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
        project.hooks.on_success.append(self.log_operation)
        project.hooks.on_exception.append(self.log_operation)
        return project

    __call__ = install_hooks
