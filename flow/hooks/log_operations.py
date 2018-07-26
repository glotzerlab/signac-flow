# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import logging


class LogOperations(object):

    def __init__(self, fn_logfile='operations.log'):
        self._fn_logfile = fn_logfile
        self._loggers = dict()

    def log_operation(self, operation, error=None):
        if str(operation.job) in self._loggers:
            logger = self._loggers[str(operation.job)]
        else:
            logger = self._loggers[str(operation.job)] = logging.getLogger(str(operation.job))
            logger.setLevel(logging.DEBUG)

            fh = logging.FileHandler(operation.job.fn(self._fn_logfile))
            fh.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)

            logger.addHandler(fh)

        if error is None:
            logger.info("Executed operation '{}'.".format(operation))
        else:
            logger.info("Execution of operation '{}' failed with "
                        "error '{}'.".format(operation, error))

    def install_hooks(self, project):
        project.hooks.on_success.append(self.log_operation)
        project.hooks.on_fail.append(self.log_operation)
        return project
