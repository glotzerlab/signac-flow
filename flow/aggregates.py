# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from collections.abc import Iterable
from hashlib import md5
from hashlib import sha1
from itertools import groupby
from itertools import zip_longest


class Aggregate:
    r"""Decorator for operation functions that are to be aggregated.
    By default, if the aggregator parameter is not passed,
    an aggregate of all jobs will be created.

    .. code-block:: python

        @Aggregate()
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
    :param \*\*kwargs:
        Additional information related to aggregator function.
    """

    def __init__(self, aggregator=None, sort_by=None, reverse_order=False, select=None,
                 **kwargs):
        if aggregator is None:
            def aggregator(jobs):
                return [jobs]

        if not callable(aggregator):
            raise TypeError(f"Expected callable for aggregator, got {type(aggregator)}")
        elif sort_by is not None and not isinstance(sort_by, str):
            raise TypeError(f"Expected string sort_by parameter, got {type(sort_by)}")
        elif select is not None and not callable(select):
            raise TypeError(f"Expected callable for select, got {type(select)}")

        # For "non-aggregate" functions we set the Aggregate object equals to
        # Aggregate.groupsof(1). If any other Aggregate object is associated with an
        # operation function then mark that as a "aggregate operation" else it's a
        # "non-aggregate" operation.
        if sort_by is None and select is None and not reverse_order:
            self._is_aggregate = getattr(aggregator, '_num', 0) != 1
        else:
            self._is_aggregate = True

        self._kwargs = kwargs
        self._aggregator = aggregator
        self._sort_by = sort_by
        self._reverse_order = reverse_order
        self._select = select

    @classmethod
    def groupsof(cls, num=1, sort_by=None, reverse_order=False, select=None):
        """signac jobs can be aggregated in groups of a number provided by an user.

        By default aggregate of a single job is created.

        If the number of jobs present in the project can't be divided by the number
        provided by the user then the last aggregate will have the maximum possible
        jobs in it.
        For instace, if 10 jobs are present in a project and they are aggregated in groups
        of 3 then the length of aggregates would be 3, 3, 3, 1.

        The below code-block provides an example on how jobs can be aggregated in
        groups of 2.

        .. code-block:: python

            @Aggregate.groupsof(num=2)
            @FlowProject.operation
            def foo(*jobs):
                print(len(jobs))

        :param num:
            Maximum possible length of an aggregate.
        :type num:
            int
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
        try:
            if num != int(num):
                raise ValueError('The num parameter should be an integer')
            num = int(num)
            if num <= 0:
                raise ValueError('The num parameter should have a value greater than 0')
        except TypeError:
            raise TypeError('The num parameter should be an integer')

        def aggregator(jobs):
            args = [iter(jobs)] * num
            return zip_longest(*args)

        setattr(aggregator, '_num', num)

        return cls(aggregator, sort_by, reverse_order, select, num=num)

    @classmethod
    def groupby(cls, key, default=None, sort_by=None, reverse_order=False, select=None):
        """signac jobs can be aggregated by a valid state point parameter.

        Users can aggregate jobs by passing a single or a list of state point parameters
        or a custom callable.
        If a user provides a list of state point parameters then the default value, if passed,
        expects a list of default values having length equal to the length of key passed.
        By default, an error is raised if the key passed is invalid.

        .. code-block:: python

            @Aggregate.groupby('sp', -1)
            @FlowProject.operation
            def foo(*jobs):
                print(len(jobs))

        :param key:
            Parameter which specifies how the jobs should be aggregated.
        :type key:
            str, Iterable, or callable
        :param key:
            Default values used for grouping if invalid key is passed
        :type key:
            str, Iterable, or callable
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

        return cls(aggregator, sort_by, reverse_order, select, key=key, default=default)

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        elif(
            self._sort_by != other._sort_by or
            self._reverse_order != other._reverse_order or
            self._kwargs != other._kwargs
        ):
            return False
        elif(
            self._select == other._select and
            self._aggregator == other._aggregator
        ):
            return True

        # Get unique id for _select attribute
        self_select = self._get_unique_function_id(self._select)
        other_select = self._get_unique_function_id(other._select)
        # Get unique id for _aggregator attribute
        self_aggregator = self._get_unique_function_id(self._aggregator)
        other_aggregator = self._get_unique_function_id(other._aggregator)

        if(
            self_select == other_select and
            self_aggregator == other_aggregator
        ):
            return True

        return False

    def __hash__(self):
        blob_l = list(map(hash, [self._sort_by, self._reverse_order]))
        # Get unique id for _aggregator and _select attributes
        blob_l += list(map(self._get_unique_function_id, [self._aggregator, self._select]))
        blob = ','.join(str(attr) for attr in blob_l)

        return int(sha1(blob.encode('utf-8')).hexdigest(), 16)

    def _get_unique_function_id(self, func):
        try:
            return func.__code__.co_code
        except Exception:  # Got partial function
            return str(hash(func))

    def _create_AggregatesStore(self, jobs):
        """Create the instance of _AggregatesStore or _DefaultAggregateStore classes
        to make sure that we don't explicitly store the aggregates in the FlowProject.

        When iterated over these classes, an aggregate is yielded.
        """
        if not self._is_aggregate:
            return _DefaultAggregateStore(jobs)
        else:
            return _AggregatesStore(self, jobs)

    def __call__(self, func=None):
        if callable(func):
            setattr(func, '_flow_aggregate', self)
            return func
        else:
            raise TypeError('Invalid argument passed while calling '
                            'the aggregate instance. Expected a callable, '
                            f'got {type(func)}.')


class _AggregatesStore:
    """This class holds the information of all the aggregates associated with
    a :py:class:`Aggregate`.

    This is a callable class which, when called, generates all the aggregates.
    When iterated through it's instance, all the aggregates are yielded.

    :param aggregate:
        Aggregate object associated with this class.
    :type aggregate:
        :py:class:`Aggregate`
    :param jobs:
        The signac job handles
    :type jobs:
        Iterator
    """
    def __init__(self, aggregate, jobs):
        self._aggregate = aggregate

        # We need to register the aggregates for this instance using the
        # jobs provided.
        self._aggregates = list()
        self._aggregate_ids = dict()
        self._register_aggregates(jobs)

    def __iter__(self):
        yield from self._aggregates

    def __getitem__(self, id):
        "Return an aggregate "
        try:
            return self._aggregate_ids[id]
        except KeyError:
            raise LookupError(f'Unable to find the aggregate having id {id} in '
                              'the FlowProject')

    def __contains__(self, aggregate):
        """Return whether an aggregate is stored in the this
        instance of :py:class:`_AggregateStore`

        :param aggregate:
            Aggregate of jobs
        :type jobs:
            tuple of :py:class:`signac.contrib.job.Job`
        """
        if aggregate in self._aggregates:
            return True
        else:
            return False

    def __len__(self):
        return len(self._aggregates)

    def __eq__(self, other):
        return type(self) == type(other) and \
               self._aggregates == other._aggregates and \
               self._aggregate == other._aggregate

    def __hash__(self):
        blob = str(hash(self._aggregate))
        return int(sha1(blob.encode('utf-8')).hexdigest(), 16)

    def _register_aggregates(self, jobs):
        """If the instance of this class is called then we will
        generate aggregates and store them in self._aggregates
        """
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
        """This method converts the returned subset of jobs as an Iterable
        from an aggregator function to a subset of jobs as tuple.
        """
        nested_aggregates = []

        for aggregate in aggregated_jobs:
            try:
                filter_aggregate = []
                for job in aggregate:
                    if job is None:
                        continue
                    elif job not in project:
                        raise LookupError(f'The signac job {job.get_id()} not found in {project}')
                    filter_aggregate.append(job)
                filter_aggregate = tuple(filter_aggregate)
                # Store aggregate in this instance
                nested_aggregates.append(filter_aggregate)
                # Store aggregate by their ids in order to search through id
                self._aggregate_ids[get_aggregate_id(filter_aggregate)] = filter_aggregate
            except TypeError:  # aggregate is not iterable
                raise ValueError("Invalid aggregator function provided by "
                                 "the user.")

        self._aggregates = nested_aggregates


class _DefaultAggregateStore:
    """This class holds the information of the project associated with
    an operation function aggregated by the default aggregates i.e.
    :py:class:`Aggregate.groupsof(1)`.

    When iterated through it's instance, it yields a tuple of a single job from
    the Project.

    :param jobs:
        The signac job handles
    :type jobs:
        Iterator
    """
    def __init__(self, jobs):
        self._project = None
        self._register_project(jobs)

    def __iter__(self):
        for job in self._project:
            yield (job,)

    def __getitem__(self, id):
        "Return a tuple of a single job via job id"
        try:
            return (self._project.open_job(id=id),)
        except KeyError:
            raise LookupError(f"Did not find job with id {id}.")

    def __contains__(self, job):
        """Return whether the job is present in the project associated with this
        instance of :py:class:`_DefaultAggregateStore`
        """
        if job in self._project:
            return True
        else:
            return False

    def __len__(self):
        return len(self._project)

    def __eq__(self, other):
        return type(self) == type(other) and \
               self._project == other._project

    def __hash__(self):
        blob = str(self._project)
        return int(sha1(blob.encode('utf-8')).hexdigest(), 16)

    def _generate_aggregates(self, jobs):
        """We have to store self._project when this method is invoked
        This is because we will then iterate over that project in
        order to return an aggregates of one.
        """
        self._register_project(jobs)

    def _register_project(self, jobs):
        if isinstance(jobs, list):  # Got a list of jobs
            self._project = jobs[0]._project
        else:  # Got a project
            self._project = jobs


def get_aggregate_id(jobs):
    """Generate hashed id for an aggregate of jobs

    :param jobs:
        The signac job handles
    :type jobs:
        tuple
    """
    if len(jobs) == 1:
        return jobs[0].get_id()  # Return job id as it's already unique

    blob = ','.join((job.get_id() for job in jobs))
    return f'agg-{md5(blob.encode()).hexdigest()}'
