# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Aggregation allows the definition of operations on multiple jobs.

Operations are each associated with an aggregator that determines how
jobs are grouped before being passed as arguments to the operation. The
default aggregator produces individual jobs.
"""
import itertools
from collections.abc import Iterable, Mapping
from hashlib import md5


class aggregator:
    """Decorator for operation functions that operate on aggregates.

    By default, if the ``aggregator_function`` is not passed,
    an aggregate of all jobs will be created.

    .. code-block:: python

        @aggregator()
        @FlowProject.operation
        def foo(*jobs):
            print(len(jobs))

    :param aggregator_function:
        A callable that performs aggregation of jobs. It takes in a list of
        jobs and can return or yield subsets of jobs as an iterable. The
        default behavior is creating a single aggregate of all jobs.
    :type aggregator_function:
        callable
    :param sort_by:
        Before aggregating, sort the jobs by a given statepoint parameter.
        The default behavior is no sorting.
    :type sort_by:
        str or NoneType
    :param sort_ascending:
        States if the jobs are to be sorted in ascending order.
        The default value is True.
    :type sort_ascending:
        bool
    :param select:
        Condition for filtering individual jobs. This is passed as the callable
        argument to :meth:`filter`. The default behavior is no filtering.
    :type select:
        callable or NoneType
    """

    def __init__(
        self, aggregator_function=None, sort_by=None, sort_ascending=True, select=None
    ):
        if aggregator_function is None:

            def aggregator_function(jobs):
                return (jobs,) if jobs else ()

        if not callable(aggregator_function):
            raise TypeError(
                "Expected callable for aggregator_function, got "
                f"{type(aggregator_function)}"
            )
        elif sort_by is not None and not isinstance(sort_by, str):
            raise TypeError(f"Expected string sort_by parameter, got {type(sort_by)}")
        elif select is not None and not callable(select):
            raise TypeError(f"Expected callable for select, got {type(select)}")

        # Set the `_is_aggregate` attribute to True by default. But if the "non-aggregate"
        # aggregator object i.e. aggregator.groupsof(1) is created using the class method,
        # then we explicitly set the `_is_aggregate` attribute to False.
        self._is_aggregate = True
        self._aggregator_function = aggregator_function
        self._sort_by = sort_by
        self._sort_ascending = bool(sort_ascending)
        self._select = select

    @classmethod
    def groupsof(cls, num=1, sort_by=None, sort_ascending=True, select=None):
        """Aggregate jobs into groupings of a given size.

        By default, creates aggregates consisting of a single job.

        If the number of jobs present in the project is not divisible by the
        number provided by the user, the last aggregate will be smaller and
        contain the remaining jobs. For instance, if 10 jobs are present in a
        project and they are aggregated in groups of 3, then the generated
        aggregates will have lengths 3, 3, 3, and 1.

        The code block below provides an example of how jobs can be aggregated
        in groups of 2.

        .. code-block:: python

            @aggregator.groupsof(num=2)
            @FlowProject.operation
            def foo(*jobs):
                print(len(jobs))

        :param num:
            The default size of aggregates excluding the final aggregate.
        :type num:
            int
        :param sort_by:
            Before aggregating, sort the jobs by a given statepoint parameter.
            The default behavior is no sorting.
        :type sort_by:
            str or NoneType
        :param sort_ascending:
            States if the jobs are to be sorted in ascending order.
            The default value is True.
        :type sort_ascending:
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
                raise ValueError("The num parameter should be an integer")
            num = int(num)
            if num <= 0:
                raise ValueError("The num parameter should have a value greater than 0")
        except TypeError:
            raise TypeError("The num parameter should be an integer")

        # This method is similar to the `grouper` method which can be found in the link below
        # https://docs.python.org/3/library/itertools.html#itertools.zip_longest
        def aggregator_function(jobs):
            args = [iter(jobs)] * num
            return itertools.zip_longest(*args)

        aggregator_obj = cls(aggregator_function, sort_by, sort_ascending, select)

        if num == 1 and sort_by == select is None and sort_ascending:
            aggregator_obj._is_aggregate = False

        return aggregator_obj

    @classmethod
    def groupby(cls, key, default=None, sort_by=None, sort_ascending=True, select=None):
        """Aggregate jobs according to matching state point values.

        The below code block provides an example of how to aggregate jobs having a
        common state point parameter 'sp' whose value, when not found, is replaced by a
        default value of -1.

        .. code-block:: python

            @aggregator.groupby(key='sp', default=-1)
            @FlowProject.operation
            def foo(*jobs):
                print(len(jobs))

        :param key:
            The method by which jobs are grouped. It may be a state point
            or a sequence of state points to group by specific state point
            keys. It may also be an arbitrary callable of :class:`signac.Job`
            when greater flexibility is needed.
        :type key:
            str, Iterable, or callable
        :param default:
            Default values used for grouping if invalid key is passed.
        :type default:
            str, Iterable, or callable
        :param sort_by:
            Before aggregating, sort the jobs by a given statepoint parameter.
            The default behavior is no sorting.
        :type sort_by:
            str or NoneType
        :param sort_ascending:
            States if the jobs are to be sorted in ascending order.
            The default value is True.
        :type sort_ascending:
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
                        raise ValueError(
                            "Expected length of default argument is "
                            f"{len(keys)}, got {len(default)}."
                        )
                else:
                    raise TypeError(
                        "Invalid default argument. Expected Iterable, "
                        f"got {type(default)}"
                    )

                def keyfunction(job):
                    return [
                        job.sp.get(key, default_value)
                        for key, default_value in zip(keys, default)
                    ]

        elif callable(key):
            keyfunction = key
        else:
            raise TypeError(
                "Invalid key argument. Expected either str, Iterable "
                f"or a callable, got {type(key)}"
            )

        def aggregator_function(jobs):
            for key, group in itertools.groupby(
                sorted(jobs, key=keyfunction), key=keyfunction
            ):
                yield group

        return cls(aggregator_function, sort_by, sort_ascending, select)

    def __eq__(self, other):
        return (
            type(self) == type(other)
            and not self._is_aggregate
            and not other._is_aggregate
        )

    def __hash__(self):
        return hash(
            (
                self._sort_by,
                self._sort_ascending,
                self._is_aggregate,
                self._get_unique_function_id(self._aggregator_function),
                self._get_unique_function_id(self._select),
            )
        )

    def _get_unique_function_id(self, func):
        """Generate unique id for the provided function.

        Hashing the bytecode rather than directly hashing the function allows
        for the comparison of internal functions like ``self._aggregator_function``
        or ``self._select`` that may have the same definitions but different
        hashes simply because they are distinct objects.

        It is possible for equivalent functions to have different ids if the
        bytecode is not identical.
        """
        try:
            return hash(func.__code__.co_code)
        except AttributeError:  # Cannot access function's compiled bytecode
            return hash(func)

    def _create_AggregatesStore(self, project):
        """Create the actual collections of jobs to be sent to aggregate operations.

        The :class:`aggregate` class is just a decorator that provides a signal for
        operation functions that should be treated as aggregate operations and
        information on how to perform the aggregation. This function generates
        the classes that actually hold sequences of jobs to which aggregate
        operations will be applied.

        :param project:
            A signac project used to fetch jobs for creating aggregates.
        :type project:
            :py:class:`flow.FlowProject` or :py:class:`signac.contrib.project.Project`
        """
        if not self._is_aggregate:
            return _DefaultAggregateStore(project)
        else:
            return _AggregatesStore(self, project)

    def __call__(self, func=None):
        """Add this aggregator to a provided operation.

        This call operator allows the class to be used as a decorator.

        :param func:
            The function to decorate.
        :type func:
            callable
        """
        if callable(func):
            setattr(func, "_flow_aggregate", self)
            return func
        else:
            raise TypeError(
                "Invalid argument passed while calling the aggregate "
                f"instance. Expected a callable, got {type(func)}."
            )


class _AggregatesStore(Mapping):
    """Class containing all aggregates associated with an :class:`aggregator`.

    This is a callable class which, when called, generates all the aggregates.
    Iterating over this object yields all aggregates.

    :param aggregator:
        aggregator object associated with this class.
    :type aggregator:
        :py:class:`aggregator`
    :param project:
        A signac project used to fetch jobs for creating aggregates.
    :type project:
        :py:class:`flow.FlowProject` or :py:class:`signac.contrib.project.Project`
    """

    def __init__(self, aggregator, project):
        self._aggregator = aggregator

        # We need to register the aggregates for this instance using the
        # project provided. After registering, we store the aggregates
        # mapped with the ids using the `get_aggregate_id` method.
        self._aggregate_per_id = {}
        self._register_aggregates(project)

    def __iter__(self):
        yield from self._aggregate_per_id

    def __getitem__(self, id):
        """Get the aggregate corresponding to the provided id."""
        try:
            return self._aggregate_per_id[id]
        except KeyError:
            raise LookupError(
                f"Unable to find the aggregate having id {id} in the FlowProject"
            )

    def __contains__(self, id):
        """Return whether this instance contains an aggregate (by aggregate id).

        :param id:
            The id of an aggregate of jobs.
        :type id:
            str
        """
        return id in self._aggregate_per_id

    def __len__(self):
        return len(self._aggregate_per_id)

    def __eq__(self, other):
        return type(self) == type(other) and self._aggregator == other._aggregator

    def __hash__(self):
        return hash(self._aggregator)

    def keys(self):
        return self._aggregate_per_id.keys()

    def values(self):
        return self._aggregate_per_id.values()

    def items(self):
        return self._aggregate_per_id.items()

    def _register_aggregates(self, project):
        """Register aggregates for a given project.

        This is called at instantiation to generate and store aggregates.
        """
        aggregated_jobs = self._generate_aggregates(project)
        self._create_nested_aggregate_list(aggregated_jobs, project)

    def _generate_aggregates(self, project):
        jobs = project
        if self._aggregator._select is not None:
            jobs = filter(self._aggregator._select, jobs)
        if self._aggregator._sort_by is None:
            jobs = list(jobs)
        else:
            jobs = sorted(
                jobs,
                key=lambda job: job.sp[self._aggregator._sort_by],
                reverse=not self._aggregator._sort_ascending,
            )
        yield from self._aggregator._aggregator_function(jobs)

    def _create_nested_aggregate_list(self, aggregated_jobs, project):
        """signac-flow internally assumes every aggregate to be a tuple of jobs.

        This method converts every aggregate in ``aggregated_jobs``, which may be of
        any type, returned from an aggregator_function using the instance of
        ``_MakeAggregates`` to an aggregate of jobs as tuple.
        """

        def _validate_and_filter_job(job):
            """Validate whether a job is eligible to be in an aggregate."""
            if job is None:
                return False
            elif job in project:
                return True
            else:
                raise LookupError(
                    f"The signac job {job.get_id()} not found in {project}"
                )

        for aggregate in aggregated_jobs:
            try:
                filter_aggregate = tuple(filter(_validate_and_filter_job, aggregate))
            except TypeError:  # aggregate is not iterable
                ValueError("Invalid aggregator_function provided by the user.")
            # Store aggregate by their ids in order to search through id
            self._aggregate_per_id[
                get_aggregate_id(filter_aggregate)
            ] = filter_aggregate


class _DefaultAggregateStore(Mapping):
    """Aggregate storage wrapper for the default aggregator.

    This class holds the information of the project associated with an
    operation function using the default aggregator, i.e.
    ``aggregator.groupsof(1)``.

    Iterating over this object yields tuples each containing one job from the project.

    :param project:
        A signac project used to fetch jobs for creating aggregates.
    :type project:
        :py:class:`flow.FlowProject` or :py:class:`signac.contrib.project.Project`
    """

    def __init__(self, project):
        self._project = project

    def __iter__(self):
        for job in self._project:
            yield job.get_id()

    def __getitem__(self, id):
        """Return an aggregate of one job from its job id."""
        try:
            return (self._project.open_job(id=id),)
        except KeyError:
            raise LookupError(f"Did not find aggregate with id {id}.")

    def __contains__(self, id):
        """Return whether this instance contains a job (by job id).

        :param id:
            The job id.
        :type id:
            str
        """
        try:
            self._project.open_job(id=id)
        except KeyError:
            return False
        except LookupError:
            raise
        else:
            return True

    def __len__(self):
        return len(self._project)

    def __eq__(self, other):
        return type(self) == type(other) and self._project == other._project

    def __hash__(self):
        return hash(repr(self._project))

    def keys(self):
        for job in self._project:
            yield job.get_id()

    def values(self):
        for job in self._project:
            yield (job,)

    def items(self):
        for job in self._project:
            yield (job.get_id(), (job,))

    def _register_aggregates(self, project):
        """Register aggregates for a given project.

        A reference to the project is stored on instantiation, and iterated
        over on-the-fly.
        """
        self._project = project


def get_aggregate_id(jobs):
    """Generate hashed id for an aggregate of jobs.

    :param jobs:
        The signac job handles
    :type jobs:
        tuple
    """
    if len(jobs) == 1:
        return jobs[0].get_id()  # Return job id as it's already unique

    blob = ",".join(job.get_id() for job in jobs)
    hash_ = md5(blob.encode("utf-8")).hexdigest()
    return f"agg-{hash_}"
