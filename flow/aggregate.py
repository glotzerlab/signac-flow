# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from collections.abc import Iterable
from itertools import groupby
from itertools import zip_longest
from functools import partial


class Aggregate:
    """Decorator for operation functions that are to be aggregated.
    By default, if the aggregator parameter is not passed,
    an aggregate of all jobs will be created.
    .. code-block:: python
        example_aggregate = Aggregate()
        @example_aggregate
        @FlowProject.operation
        def foo(*jobs):
            print(len(jobs))
    :param aggregator:
        Information describing how to aggregate jobs. Is a callable that
        takes in a list of jobs and can return or yield subsets of jobs as
        an iterable. The default behavior is creating a single aggregate
        of all jobs
    :type aggregator:
        callable
    :param sort:
        Before aggregating, sort the jobs by a given statepoint parameter.
        The default behaviour is no sorting.
    :type sort:
        str or NoneType
    :param reverse:
        States if the jobs are to be sorted in reverse order.
        The default value is False.
    :type reverse:
        bool
    :param select:
        Condition for filtering individual jobs. This is passed as the
        callable argument to `filter`.
        The default behaviour is no filtering.
    :type select:
        callable or NoneType
    """

    def __init__(self, aggregator=None, sort=None, reverse=False, select=None):
        if aggregator is None:
            def aggregator(jobs):
                return [jobs]

        if not callable(aggregator):
            raise TypeError("Expected callable for aggregator, got {}"
                            "".format(type(aggregator)))

        if sort is not None and not isinstance(sort, str):
            raise TypeError("Expected string sort parameter, got {}"
                            "".format(type(sort)))

        if select is not None and not callable(select):
            raise TypeError("Expected callable for select, got {}"
                            "".format(type(select)))

        self._aggregator = aggregator
        self._sorter = sort
        self._reverse = reverse
        self._select = None if select is None else partial(filter, select)

        if aggregator.__name__ == 'groupsof_aggregator':
            try:
                self._is_aggregate = aggregator.__defaults__[0] != 1
            except Exception:
                self._is_aggregate = False
        else:
            self._is_aggregate = False

    @classmethod
    def groupsof(cls, num=1, sort=None, reverse=False, select=None):
        # copied from: https://docs.python.org/3/library/itertools.html#itertools.zip_longest
        try:
            num = int(num)
            if num < 0:
                raise ValueError("The num parameter should be greater than 0")
        except Exception:
            raise TypeError("The num parameter should be an integer")

        def groupsof_aggregator(jobs, num=num):
            args = [iter(jobs)] * num
            return zip_longest(*args)

        return cls(groupsof_aggregator, sort, reverse, select)

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
                if isinstance(default, Iterable):
                    if len(default) != len(keys):
                        raise ValueError("Expected length of default argument is {}, "
                                         "got {}.".format(len(keys), len(default)))
                else:
                    raise TypeError("Invalid default argument. Expected Iterable, "
                                    "got {}".format(type(default)))

                def keyfunction(job):
                    return [job.sp.get(key, default[i]) for i, key in enumerate(keys)]

        elif callable(key):
            keyfunction = key

        else:
            raise TypeError("Invalid key argument. Expected either str, Iterable "
                            "or a callable, got {}".format(type(key)))

        def groupby_aggregator(jobs):
            for key, group in groupby(sorted(jobs, key=keyfunction), key=keyfunction):
                yield group

        return cls(groupby_aggregator, sort, reverse, select)

    def __call__(self, func=None):
        if callable(func):
            setattr(func, '_flow_aggregate', MakeAggregate(self._aggregator, self._sorter,
                                                           self._reverse, self._select))
            return func
        else:
            raise TypeError('Invalid argument passed while calling '
                            'the aggregate instance. Expected a callable, '
                            'got {}.'.format(type(func)))


class MakeAggregate(Aggregate):
    r"""This class handles the creation of aggregates.
    .. note::
        This class should not be instantiated by users directly.
    :param \*args:
        Passed to the constructor of :py:class:`JobOperation`.
    """
    def __init__(self, *args):
        super(MakeAggregate, self).__init__(*args)
        self._sort = None if self._sorter is None else \
            partial(sorted,
                    key=lambda job: job.sp[self._sorter],
                    reverse=bool(self._reverse))

    def __call__(self, obj):
        "Return aggregated jobs"
        aggregated_jobs = list(obj)
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

    def _create_nested_aggregate_list(self, aggregated_jobs):
        # This method converts the returned subset of jobs as an Iterable
        # from an aggregator function to a subset of jobs as list.
        aggregated_jobs = list(aggregated_jobs)
        nested_aggregates = []
        for aggregate in aggregated_jobs:
            try:
                nested_aggregates.append([job for job in aggregate])
            except Exception:
                raise ValueError("Invalid aggregator function provided by "
                                 "the user.")
        return nested_aggregates
