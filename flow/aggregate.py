# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.

from itertools import zip_longest


class Aggregate:
    """Decorator for operation functions that are to be aggregated.

    :param aggregator:
        Information describing how to aggregate jobs. Is a callable that
        takes in a list of jobs and can return or yield subsets of jobs as list
        or single jobs. The default behavior is creating a single aggregate
        of all jobs.
    :type aggregator:
        callable
    """

    def __init__(self, aggregator=None):
        if aggregator is None:
            def aggregator(jobs):
                return [jobs]

        if not callable(aggregator):
            raise TypeError("Expected callable for aggregator, got {}"
                            "".format(type(aggregator)))

        self._aggregator = aggregator

    @classmethod
    def groupsof(cls, num=1):
        try:
            num = int(num)
            if num <= 0:
                raise ValueError("The num parameter should be greater than 0")
        except TypeError:
            raise TypeError("The num parameter should be an integer")

        def aggregator(jobs):
            args = [iter(jobs)] * num
            return zip_longest(*args)

        return cls(aggregator)

    def __call__(self, func=None):
        if callable(func):
            setattr(func, '_flow_aggregate', _MakeAggregate(self))
            return func
        else:
            raise TypeError('Invalid argument passed while calling '
                            'the aggregate instance. Expected a callable, '
                            'got {}.'.format(type(func)))


class _MakeAggregate:
    """This class handles the creation of aggregates.

    .. note::
        This class should not be instantiated by users directly.

    :param _aggregate:
        Aggregate object associated with an operation function
    :type _aggregate:
        :py:class:`Aggregate`
    """
    def __init__(self, _aggregate=Aggregate()):
        self._aggregate = _aggregate

    def __call__(self, list_of_jobs):
        "Return aggregated jobs"
        # Ensuring list
        list_of_jobs = list(list_of_jobs)
        aggregated_jobs = self._aggregate._aggregator([job for job in list_of_jobs])
        aggregated_jobs = [[job for job in aggregate] for aggregate in aggregated_jobs]
        if len(aggregated_jobs) == 0:
            return []
        for i, job in enumerate(aggregated_jobs[-1]):
            if job is None:
                del aggregated_jobs[-1][i:]
                break
        return aggregated_jobs
