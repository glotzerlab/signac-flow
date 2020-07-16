# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.

from copy import deepcopy
from itertools import zip_longest
from functools import partial

from signac.contrib.project import Project, JobsCursor


class _Aggregate:
    """Decorator for operation functions that are to be aggregated.

    :param aggregator:
        Information describing how to aggregate jobs. Is a callable that
        takes in a list of jobs and can return or yield subsets of jobs as list
        or single jobs. The default behavior is creating a single aggregate
        of all jobs.
    :type aggregator:
        callable
    :param select:
        Condition for filtering individual jobs. This is passed as the callable argument
        to `filter`. The default behaviour is no filtering.
    :type select:
        callable
    """

    def __init__(self, aggregator=None, select=None):
        if aggregator is None:
            def aggregator(jobs):
                return jobs

        if not callable(aggregator):
            raise TypeError("Expected callable for aggregator, got {}"
                            "".format(type(aggregator)))

        if select is not None and not callable(select):
            raise TypeError("Expected callable for select, got {}".format(type(select)))

        self._aggregator = aggregator
        self._select = None if select is None else partial(filter, select)

    @classmethod
    def groupsof(cls, num=1, select=None):
        # copied from: https://docs.python.org/3/library/itertools.html#itertools.zip_longest
        try:
            num = int(num)
            if num < 0:
                raise ValueError("The num parameter should be greater than 0")
        except Exception:
            raise TypeError("The num parameter should be an integer")

        def aggregator(jobs):
            args = [iter(jobs)] * num
            return zip_longest(*args)

        return cls(aggregator, select)

    def __call__(self, obj=None):
        if callable(obj):
            setattr(obj, '_flow_aggregate', self)
            return obj
        elif isinstance(obj, (list, Project, JobsCursor)):
            aggregated_jobs = list(obj)
            if self._select is not None:
                aggregated_jobs = list(self._select(aggregated_jobs))
            aggregated_jobs = self._aggregator([job for job in aggregated_jobs])
            aggregated_jobs = [[job for job in aggregates] for aggregates in aggregated_jobs]
            if not len(aggregated_jobs):
                return []
            return aggregated_jobs
        else:
            raise TypeError('Invalid argument passed while calling '
                            'the aggregate instance. Expected a callable '
                            'or an Iterable of Jobs, got {}.'.format(type(obj)))
