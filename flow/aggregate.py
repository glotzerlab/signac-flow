# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from collections.abc import Iterable
from copy import deepcopy
from itertools import groupby
from itertools import zip_longest
from functools import partial

import signac


class aggregate:
    """Decorator for operation functions that are to be aggregated.

    If this class is used for aggregation then by-default, if the aggregator
    parameter is not passed, an aggregate of all jobs will be created.

    .. code-block:: python

        example_aggregate = aggregate()
        @example_aggregate
        @FlowProject.operation
        def foo(*jobs):
            print(len(jobs))

    :param aggregator:
        Information on how to aggregate jobs. Takes in a list of
        jobs and can return or yield lists or single jobs instead.
        The default behavior is creating an aggregate of all jobs.
    :type aggregator:
        callable
    :param sort:
        Before aggregating, sort the jobs given by a statepoint parameter.
        The default value is None.
    :type sort:
        str or NoneType
    :param reverse:
        States if the jobs are to be sorted in reverse order.
        The default value is False.
    :type reverse:
        bool
    :param select:
        Condition for filtering jobs. This operates on a single job.
        The default value is None
    :type select:
        callable or NoneType
    """

    def __init__(self, aggregator=None, sort=None, reverse=False, select=None):
        if aggregator is None:
            def aggregator(jobs):
                return jobs

        if not callable(aggregator):
            raise TypeError("Expected callable aggregator function, got {}"
                            "".format(type(aggregator)))

        if sort is not None and not isinstance(sort, str):
            raise TypeError("Expected string sort parameter, got {}".format(type(sort)))

        if not isinstance(reverse, bool):
            raise TypeError("Expected bool reverse parameter got {}".format(type(reverse)))

        if select is not None and not callable(select):
            raise TypeError("Expected callable select parameter, got {}".format(type(select)))

        self._aggregator = aggregator
        self._sort = None if sort is None else partial(sorted,
                                                       key=lambda job: job.sp[sort],
                                                       reverse=reverse)
        self._select = None if select is None else partial(filter, select)

    @classmethod
    def groupsof(cls, num=1, sort=None, reverse=False, select=None):
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

        return cls(aggregator, sort, reverse, select)

    @classmethod
    def groupby(cls, key, default=None, sort=None, reverse=False, select=None):
        if isinstance(key, str):
            if default is None:
                def keyfunction(job):
                    return job.sp[key]
            else:
                def keyfunction(job):
                    return job.sp.get(key, default)

        elif isinstance(key, Iterable):
            keys = list(key)

            if default is None:
                def keyfunction(job):
                    return [job.sp[key] for key in keys]
            else:
                def keyfunction(job):
                    return [job.sp.get(key, default) for key in keys]

        elif callable(key):
            keyfunction = key

        else:
            raise TypeError("Invalid key argument. Expected either str, Iterable "
                            "or a callable, got {}".format(type(key)))

        def aggregator(jobs):
            for key, group in groupby(sorted(jobs, key=keyfunction), key=keyfunction):
                yield group

        return cls(aggregator, sort, reverse, select)

    def __call__(self, obj=None):
        if callable(obj):
            setattr(obj, '_flow_aggregate', self)
            return obj
        elif isinstance(obj, (list, signac.contrib.project.Project)):
            aggregated_jobs = deepcopy(list(obj))
            if self._select is not None:
                aggregated_jobs = list(self._select(aggregated_jobs))
            if self._sort is not None:
                aggregated_jobs = list(self._sort(aggregated_jobs))
            aggregated_jobs = self._aggregator([job for job in aggregated_jobs])
            aggregated_jobs = self._create_nested_aggregate_list(aggregated_jobs)
            if not len(aggregated_jobs):
                return []
            for i, job in enumerate(aggregated_jobs[-1]):
                if job is None:
                    del aggregated_jobs[-1][i:]
                    break
            return aggregated_jobs
        elif isinstance(obj, signac.contrib.job.Job):
            # For backwards compatibility
            return [obj]
        else:
            raise TypeError('Invalid argument passed while calling '
                            'the aggregate instance. Expected a callable '
                            'or an Iterable of Jobs, got {}.'.format(type(obj)))

    def _create_nested_aggregate_list(self, aggregated_jobs):
        # When jobs are aggregated, the user is required to specify an aggregator
        # function which either returns a nested list or nested iterable
        # in which the base element is an instance of `signac.contrib.job.Job`.
        # This method converts the returned object to a nested list.
        aggregated_jobs = list(aggregated_jobs)
        all_iterable = all_jobs = True
        nested_aggregates = []
        for aggregate in aggregated_jobs:
            if isinstance(aggregate, Iterable):
                all_jobs = False
                if not all_iterable:
                    # Got something like [Iterable, Iterable, Not an Iterable object]
                    raise ValueError("Invalid aggregator function provided by "
                                     "user.")
                if isinstance(aggregate, (list, tuple)):
                    nested_aggregates.append(list(aggregate))
                else:
                    nested_aggregates.append([job for job in aggregate])
            elif isinstance(aggregate, signac.contrib.job.Job):
                all_iterable = False
                if not all_jobs:
                    # Got something like [Job, Job, Not a Job object]
                    raise ValueError("Invalid aggregator function provided by "
                                     "user.")
                continue
            else:
                raise ValueError("Invalid aggregator function provided by "
                                 "user.")
        if all_iterable:
            return nested_aggregates
        elif all_jobs:
            return [aggregated_jobs]
