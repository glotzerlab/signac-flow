# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Wrapper functions to detect and support deprecated APIs from previous versions."""
import logging
import functools
import warnings


logger = logging.getLogger(__name__)


def support_submit_operations_legacy_api(func):
    from .environment import ComputeEnvironment

    @functools.wraps(func)
    def wrapper(self, operations, _id=None, env=None, *args, **kwargs):
        if isinstance(operations, ComputeEnvironment) and isinstance(env, list):
            warnings.warn(
                "The FlowProject.submit_operations() signature has changed!", DeprecationWarning)
            env, operations = operations, env
        if kwargs.get('serial') is not None:
            warnings.warn(
                "The 'serial' argument for submit_operations() is deprecated and has been "
                "replaced by the 'parallel' argument as of version 0.7.", DeprecationWarning)
            kwargs['parallel'] = not kwargs.pop('serial')
        for key in 'nn', 'ppn':
            if key in kwargs:
                warnings.warn(
                    "The '{}' argument should be provided as part of the operation "
                    "directives as of version 0.7.".format(key), DeprecationWarning)
                for op in operations:
                    assert op.directives.setdefault(key, kwargs[key]) == kwargs[key]
        return func(self, operations=operations, _id=_id, env=env, *args, **kwargs)

    return wrapper


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
