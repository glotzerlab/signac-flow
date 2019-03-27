# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Wrapper functions to detect and support deprecated APIs from previous versions."""
import logging

from . operations import JobsOperation


logger = logging.getLogger(__name__)


class JobsCursorWrapper(object):
    """Enables the execution of workflows on dynamic data spaces.

    Instead of storing a static list of jobs, we store the (optional)
    filters and evaluate them on each iteration.

    Note: This is the default behavior in upstream versions of signac core.
    """
    def __init__(self, project, filter=None, doc_filter=None):
        self._project = project
        self._filter = filter
        self._doc_filter = doc_filter

    def _find_ids(self):
        return self._project.find_job_ids(self._filter, self._doc_filter)

    def __iter__(self):
        return iter([self._project.open_job(id=_id) for _id in self._find_ids()])

    def __len__(self):
        return len(self._find_ids())


class JobOperation(JobsOperation):

    def __init__(self, name, job, cmd, directives=None):
        """This class represents the information needed to execute one operation for one job.

        An operation function in this context is a shell command, which should be a function
        of one and only one signac job.

        .. note::

            This class is used by the :class:`~.FlowProject` class for the execution and
            submission process and should not be instantiated by users themselves.

        .. versionchanged:: 0.6

        :param name:
            The name of this JobsOperation instance. The name is arbitrary,
            but helps to concisely identify the operation in various contexts.
        :type name:
            str
        :param jobs:
            The job instance associated with this operation.
        :type job:
            :py:class:`signac.Job`.
        :param cmd:
            The command that executes this operation.
        :type cmd:
            str
        :param directives:
            A dictionary of additional parameters that provide instructions on how
            to execute this operation, e.g., specifically required resources.
        :type directives:
            :class:`dict`
        """
        return super(JobOperation, self).__init__(
            name=name, jobs=[job], cmd=cmd, directives=directives)
