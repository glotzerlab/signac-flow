# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from collections.abc import Iterable
from hashlib import md5
from hashlib import sha1
from itertools import groupby
from itertools import zip_longest


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
    :param sort_by:
        Before aggregating, sort the jobs by a given statepoint parameter.
        The default behavior is no sorting.
    :type sort_by:
        str or NoneType
    :param reverse_order:
        States if the jobs are to be sorted in reverse order.
        The default value is False.
    :type reverse_order:
        bool
    :param select:
        Condition for filtering individual jobs. This is passed as the
        callable argument to `filter`.
        The default behavior is no filtering.
    :type select:
        callable or NoneType
    """

    def __init__(self, aggregator=None, sort_by=None, reverse_order=False, select=None):
        if aggregator is None:
            def aggregator(jobs):
                return [jobs]

        if not callable(aggregator):
            raise TypeError(f"Expected callable for aggregator, got {type(aggregator)}")

        if sort_by is not None and not isinstance(sort_by, str):
            raise TypeError(f"Expected string sort_by parameter, got {type(sort_by)}")

        if select is not None and not callable(select):
            raise TypeError(f"Expected callable for select, got {type(select)}")

        if sort_by is None and select is None and not reverse_order:
            self._is_aggregate = getattr(aggregator, '_num', 0) != 1
        else:
            self._is_aggregate = True

        self._aggregator = aggregator
        self._sort_by = sort_by
        self._reverse_order = reverse_order
        self._select = select

    @classmethod
    def groupsof(cls, num=1, sort_by=None, reverse_order=False, select=None):
        # copied from: https://docs.python.org/3/library/itertools.html#itertools.zip_longest
        try:
            num = int(num)
            if num <= 0:
                raise ValueError('The num parameter should have a value greater than 0')
        except TypeError:
            raise TypeError('The num parameter should be an integer')

        def aggregator(jobs):
            args = [iter(jobs)] * num
            return zip_longest(*args)

        setattr(aggregator, '_num', num)

        return cls(aggregator, sort_by, reverse_order, select)

    @classmethod
    def groupby(cls, key, default=None, sort_by=None, reverse_order=False, select=None):
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
                        raise ValueError("Expected length of default argument is "
                                         f"{len(keys)}, got {len(default)}.")
                else:
                    raise TypeError("Invalid default argument. Expected Iterable, "
                                    f"got {type(default)}")

                def keyfunction(job):
                    return [job.sp.get(key, default_value)
                            for key, default_value in zip(keys, default)]

        elif callable(key):
            keyfunction = key

        else:
            raise TypeError("Invalid key argument. Expected either str, Iterable "
                            f"or a callable, got {type(key)}")

        def aggregator(jobs):
            for key, group in groupby(sorted(jobs, key=keyfunction), key=keyfunction):
                yield group

        return cls(aggregator, sort_by, reverse_order, select)

    def __hash__(self):
        blob_l = map(hash, [self._aggregator, self._sort_by,
                            self._reverse_order, self._select])
        blob = ','.join(str(attr) for attr in blob_l)

        return int(sha1(blob.encode('utf-8')).hexdigest(), 16)

    def _create_StoreAggregates(self):
        # We create the instance of _StoreAggregates or _StoreDefaultAggregates classes
        # to make sure that we don't explicitly store the aggregates in the FlowProject.
        # When iterated over these classes, an aggregate is yielded.
        if not self._is_aggregate:
            return _StoreDefaultAggregate()
        else:
            return _StoreAggregates(self)

    def __call__(self, func=None):
        if callable(func):
            setattr(func, '_flow_aggregate', self._create_StoreAggregates())
            return func
        else:
            raise TypeError('Invalid argument passed while calling '
                            'the aggregate instance. Expected a callable, '
                            f'got {type(func)}.')


class _StoreAggregates:
    """This class holds the information of all the aggregates associated with
    a :py:class:`Aggregate`.

    This is a callable class which, when called, generates all the aggregates.
    When iterated through it's instance, all the aggregates are yielded.

    :param aggregate:
        Aggregate object associated with this class.
    :type aggregate:
        :py:class:`Aggregate`
    """
    def __init__(self, aggregate):
        self._aggregate = aggregate
        self._aggregates = list()

    def __iter__(self):
        yield from self._aggregates

    def __call__(self, jobs):
        # If the instance of this class is called then we will
        # generate aggregates and store them in self._aggregates
        if isinstance(jobs, list):  # Got a tuple of jobs
            project = jobs[0]._project
        else:  # Got a project
            project = jobs

        aggregated_jobs = list(jobs)
        if self._aggregate._select is not None:
            aggregated_jobs = list(filter(self._aggregate._select, aggregated_jobs))
        if self._aggregate._sort_by is not None:
            aggregated_jobs = list(sorted(aggregated_jobs,
                                          key=lambda job: job.sp[self._aggregate._sort_by],
                                          reverse=bool(self._aggregate._reverse_order)))

        aggregated_jobs = self._aggregate._aggregator([job for job in aggregated_jobs])
        self._create_nested_aggregate_list(aggregated_jobs, project)

    def _create_nested_aggregate_list(self, aggregated_jobs, project):
        # This method converts the returned subset of jobs as an Iterable
        # from an aggregator function to a subset of jobs as tuple.
        aggregated_jobs = list(aggregated_jobs)
        nested_aggregates = []

        for aggregate in aggregated_jobs:
            try:
                filter_aggregate = []
                for job in aggregate:
                    if job is None:
                        continue
                    elif job not in project:
                        raise LookupError(f'The signac job {str(job)} not found in {project}')
                    filter_aggregate.append(job)
                filter_aggregate = tuple(filter_aggregate)
                nested_aggregates.append(filter_aggregate)
            except TypeError:  # aggregate is not iterable
                raise ValueError("Invalid aggregator function provided by "
                                 "the user.")

        self._aggregates = nested_aggregates

    def __hash__(self):
        blob = str(hash(self._aggregate))
        return int(sha1(blob.encode('utf-8')).hexdigest(), 16)


class _StoreDefaultAggregate:
    """This class holds the information of the project associated with
    an operation function aggregated by the default aggregates i.e.
    :py:class:`Aggregate.groupsof(1)`.

    When iterated through it's instance, it yields a tuple of a single job from
    the Project.
    """
    def __init__(self):
        self._project = None

    def __iter__(self):
        for job in self._project:
            yield (job,)

    def __call__(self, jobs):
        # We have to store self._project is a instance of this class
        # is called. This is because we will then iterate over that
        # project in order to return an aggregates of one.
        if isinstance(jobs, list):  # Got a tuple of jobs
            self._project = jobs[0]._project
        else:  # Got a project
            self._project = jobs

    def __hash__(self):
        blob = str(self._project)
        return int(sha1(blob.encode('utf-8')).hexdigest(), 16)


def get_aggregate_id(jobs):
    """Generate hashed id for an aggregate of jobs

    :param jobs:
        The signac job handles
    :type jobs:
        tuple
    """
    if len(jobs) == 1:
        return str(jobs[0])  # Return job id as it's already unique

    blob = ','.join((job.get_id() for job in jobs))
    return f'agg-{md5(blob.encode()).hexdigest()}'
