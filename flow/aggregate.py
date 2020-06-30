# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from collections.abc import Iterable
from itertools import zip_longest

class _aggregate:
    """Decorator for operation functions that needs to be aggregated.

    If this class is used for aggregation then by-default, if the aggregator
    parameter is not passed, an aggregate of all the jobs will be created.

    .. code-block:: python

        example_aggregate = aggregate()
        @example_aggregate
        @FlowProject.operation
        def foo(*jobs):
            print(len(jobs))

    :param aggregator:
        Information on how to aggregate jobs. Takes in a list of
        jobs and can return or yield lists or single jobs instead.
        The default behaviour is creating the aggregate of all jobs.
    :type grouper:
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
    """

    def __init__(self, aggregator=None, sort=None, reverse=False):
        if aggregator is None:
            def aggregator(jobs):
                yield jobs

        def key_sort(job, sort=sort):
            try:
                return job.sp[sort]
            except KeyError:
                raise KeyError("The key '{}' was not found in statepoint "
                               "parameters of the job {}.".format(sort, job))

        if not callable(aggregator):
            raise TypeError("Expected callable aggregator function, got {}".format(type(aggregator)))

        if sort is not None and not isinstance(sort, str):
            raise TypeError("Expected string sort parameter, got {}".format(type(sort)))

        if not isinstance(reverse, bool):
            raise TypeError("Expected bool reverse parameter got {}".format(type(reverse)))

        self._aggregator = aggregator
        self._sort = None if sort is None else functools.partial(sorted,
                                                                 key=key_sort,
                                                                 reverse=reverse)

    @classmethod
    def groupsof(cls, num=1, sort=None, reverse=False):
        # copied from: https://docs.python.org/3/library/itertools.html#itertools.zip_longest
        def aggregator(jobs):
            args = [iter(jobs)] * num
            return zip_longest(*args)

        return cls(aggregator, sort, reverse)

    @classmethod
    def groupby(cls, key, default=None, sort=None, reverse=False):
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
                    return [job.sp[k] for key in keys]
            else:
                def keyfunction(job):
                    return [job.sp.get(k, default) for key in keys]

        elif callable(key):
            keyfunction = key

        else:
            raise ValueError("Invalid key argument. Expected either str, Iterable "
                            "or a callable, got {}".format(type(key)))

        def aggregator(jobs):
            for key, group in groupby(sorted(jobs, key=keyfunction), key=keyfunction):
                yield group

        return cls(aggregator, sort, reverse)

    def __call__(self, func=None):
        if func is None:
            return (self._aggregator, self._sort)
        setattr(func, '_flow_aggregate', (self._aggregator, self._sort))
        return func


class _select:
    """Decorator for operation functions that will filter jobs
    according to the given condition.

    .. code-block:: python

        @select(filter=lambda job: job.sp.a>=5)
        @FlowProject.operation
        def foo(jobs):
            return len(jobs)

    :param filterby:
        Condition for filtering jobs. This operates on a single job.
        The default value is None
    :type filterby:
        callable or NoneType
    """

    def __init__(self, filterby=None):
        if filterby is not None and not callable(filterby):
            raise TypeError("Expected callable filterby function, got {}".format(type(filterby)))
        self._filter = functools.partial(filter, filterby)

    def __call__(self, func=None):
        if func is None:
            return self._filter
        setattr(func, '_flow_select', self._filter)

        return func
