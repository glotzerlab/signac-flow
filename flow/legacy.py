# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Wrapper functions to detect and support deprecated APIs from previous versions."""
import logging


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
