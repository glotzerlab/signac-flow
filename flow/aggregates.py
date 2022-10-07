# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Aggregation allows the definition of operations on multiple jobs.

Operations are each associated with an aggregator that determines how
jobs are grouped before being passed as arguments to the operation. The
default aggregator produces individual jobs.
"""
import itertools
from abc import abstractmethod
from collections.abc import Collection, Iterable, Mapping
from hashlib import md5

from .errors import FlowProjectDefinitionError


def _get_unique_function_id(func):
    """Generate unique id for the provided function.

    Hashing the bytecode rather than directly hashing the function allows for
    the comparison of internal functions like ``self._aggregator_function``
    or ``self._select`` that may have the same definitions but different
    hashes simply because they are distinct objects.

    It is possible for equivalent functions to have different ids if the
    bytecode is not identical.

    Parameters
    ----------
    func : callable
        The function to be hashed.

    Returns
    -------
    str
        The hash of the function's bytecode if possible, otherwise the hash
        of the function.

    """
    try:
        return hash(func.__code__.co_code)
    except AttributeError:  # Cannot access function's compiled bytecode
        return hash(func)


class aggregator:
    """Decorator for operation functions that operate on aggregates.

    By default, if the ``aggregator_function`` is ``None``, an aggregate of all
    jobs will be created.

    Examples
    --------
    The code block below defines a :class:`~.FlowOperation` that prints the
    total length of the provided aggregate of jobs.

    .. code-block:: python

        @aggregator()
        @FlowProject.operation
        def foo(*jobs):
            print(len(jobs))

    Parameters
    ----------
    aggregator_function : callable or None
        A callable that performs aggregation of jobs. It takes in a list of
        jobs and can return or yield subsets of jobs as an iterable. The
        default behavior is creating a single aggregate of all jobs.
    sort_by : str, callable, or None
        Before aggregating, sort the jobs by a given statepoint parameter. If
        the argument is a string, jobs are sorted by that state point key. If
        the argument is callable, this will be passed as the ``key`` argument to
        :func:`sorted`. If None, no sorting is performed (Default value = None).
    sort_ascending : bool
        True if the jobs are to be sorted in ascending order (Default value =
        True).
    select : callable or None
        Condition for filtering individual jobs. This is passed as the
        ``function`` argument to :func:`filter`. If None, no filtering is
        performed (Default value = None).

    """

    def __init__(
        self, aggregator_function=None, sort_by=None, sort_ascending=True, select=None
    ):
        if aggregator_function is None:

            def aggregator_function(jobs):
                yield tuple(jobs) if jobs else ()

        if not callable(aggregator_function):
            raise TypeError(
                "Expected aggregator_function to be callable, got "
                f"{type(aggregator_function)}"
            )
        if sort_by is not None and not (isinstance(sort_by, str) or callable(sort_by)):
            raise TypeError(
                f"Expected sort_by parameter to be str or callable, got {type(sort_by)}"
            )
        if select is not None and not callable(select):
            raise TypeError(
                f"Expected select parameter to be callable, got {type(select)}"
            )

        # Set the ``_is_default_aggregator`` attribute to False by default. But if
        # the "non-aggregate" aggregator object i.e. aggregator.groupsof(1) is
        # created using the class method, then we explicitly set the
        # ``_is_default_aggregator`` attribute to True.
        self._is_default_aggregator = False
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

        Examples
        --------
        The code block below shows how to aggregate jobs in groups of 2.

        .. code-block:: python

            @aggregator.groupsof(num=2)
            @FlowProject.operation
            def foo(*jobs):
                print(len(jobs))

        Parameters
        ----------
        num : int
            The default size of aggregates. The final aggregate contains the
            remaining jobs and may have fewer than ``num`` jobs.
        sort_by : str, callable, or None
            Before aggregating, sort the jobs by a given statepoint parameter. If
            the argument is a string, jobs are sorted by that state point key. If
            the argument is callable, this will be passed as the ``key`` argument to
            :func:`sorted`. If None, no sorting is performed (Default value = None).
        sort_ascending : bool
            True if the jobs are to be sorted in ascending order (Default value
            = True).
        select : callable or None
            Condition for filtering individual jobs. This is passed as the
            ``function`` argument to :func:`filter`. If None, no filtering is
            performed (Default value = None).

        Returns
        -------
        aggregator : :class:`~.aggregator`
            The :meth:`~.groupsof` aggregator.

        """
        try:
            if num != int(num):
                raise ValueError("The num parameter should be an integer")
            num = int(num)
            if num <= 0:
                raise ValueError("The num parameter should have a value greater than 0")
        except TypeError:
            raise TypeError("The num parameter should be an integer")

        # This method is similar to the `grouper` method documented here:
        # https://docs.python.org/3/library/itertools.html#itertools.zip_longest
        # However, this function does not have a fill value.
        # Source of this implementation: https://stackoverflow.com/a/31185097
        def aggregator_function(jobs):
            iterable = iter(jobs)
            return iter(lambda: tuple(itertools.islice(iterable, num)), tuple())

        aggregator_instance = cls(aggregator_function, sort_by, sort_ascending, select)

        if num == 1 and sort_by is None and select is None and sort_ascending:
            aggregator_instance._is_default_aggregator = True

        return aggregator_instance

    @classmethod
    def groupby(cls, key, default=None, sort_by=None, sort_ascending=True, select=None):
        """Aggregate jobs according to matching state point values.

        Examples
        --------
        The code block below provides an example of how to aggregate jobs
        by a state point parameter ``"key"``. If the state point does not
        contain the key ``"key"``, a default value of -1 is used.

        .. code-block:: python

            @aggregator.groupby(key="key", default=-1)
            @FlowProject.operation
            def foo(*jobs):
                print(len(jobs))

        Parameters
        ----------
        key : str, Iterable[str], or callable
            The method by which jobs are grouped. It may be a state point key
            or an iterable of state point keys whose values define the
            groupings. It may also be an arbitrary callable of
            :class:`~signac.contrib.job.Job` when greater flexibility is
            needed.
        default : Any
            Default value used for grouping if the key is missing or invalid.
            If ``key`` is an iterable, the default value must be a sequence
            of equal length. If ``key`` is a callable, this argument is
            ignored. If None, the provided keys must exist for all jobs
            (Default value = None).
        sort_by : str, callable, or None
            Before aggregating, sort the jobs by a given statepoint parameter. If
            the argument is a string, jobs are sorted by that state point key. If
            the argument is callable, this will be passed as the ``key`` argument to
            :func:`sorted`. If None, no sorting is performed (Default value = None).
        sort_ascending : bool
            True if the jobs are to be sorted in ascending order (Default value
            = True).
        select : callable or None
            Condition for filtering individual jobs. This is passed as the
            ``function`` argument to :func:`filter`. If None, no filtering is
            performed (Default value = None).

        Returns
        -------
        aggregator : :class:`~.aggregator`
            The :meth:`~.groupby` aggregator.

        """
        if isinstance(key, str):
            if default is None:

                def keyfunction(job):
                    return job.statepoint[key]

            else:

                def keyfunction(job):
                    return job.statepoint.get(key, default)

        elif isinstance(key, Iterable):
            keys = list(key)
            if default is None:

                def keyfunction(job):
                    return [job.statepoint[key] for key in keys]

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
                        job.statepoint.get(key, default_value)
                        for key, default_value in zip(keys, default)
                    ]

        elif callable(key):
            keyfunction = key
        else:
            raise TypeError(
                "Invalid key argument. Expected str, Iterable, "
                f"or a callable, got {type(key)}"
            )

        def aggregator_function(jobs):
            for key, group in itertools.groupby(
                sorted(jobs, key=keyfunction), key=keyfunction
            ):
                yield tuple(group)

        return cls(aggregator_function, sort_by, sort_ascending, select)

    def __eq__(self, other):
        """Test equality with another aggregator."""
        if not isinstance(other, type(self)):
            return NotImplemented
        # It is not possible to compare aggregators, even with equivalent
        # aggregator functions. Moreover, the code objects created by
        # _get_unique_function_id do not account for differences in the bound
        # parameters. Thus, the only meaningful comparison is whether both
        # aggregators are the default aggregator (and thus equivalent).
        return self._is_default_aggregator and other._is_default_aggregator

    def __hash__(self):
        """Hash this aggregator."""
        return hash(
            (
                self._is_default_aggregator,
                self._sort_ascending,
                _get_unique_function_id(self._sort_by),
                _get_unique_function_id(self._aggregator_function),
                _get_unique_function_id(self._select),
            )
        )

    def _create_AggregateStore(self, project):
        """Create the actual collections of jobs to be sent to aggregate operations.

        The :class:`aggregator` class is just a decorator that provides a
        signal for operation functions that should be treated as aggregate
        operations and information on how to perform the aggregation. This
        function generates the classes that actually hold the aggregates
        (tuples of jobs) to which aggregate operations will be applied.

        Parameters
        ----------
        project : :class:`signac.contrib.project.Project`
            A signac project used to fetch jobs for creating aggregates.

        Returns
        -------
        :class:`~._BaseAggregateStore`
            The aggregate store.

        """
        if self._is_default_aggregator:
            return _DefaultAggregateStore(project)
        else:
            return _AggregateStore(self, project)

    def __call__(self, func=None):
        """Add this aggregator to a provided operation.

        This call operator allows the class to be used as a decorator.

        Parameters
        ----------
        func : callable
            The function to decorate.

        """
        if not callable(func):
            raise FlowProjectDefinitionError(
                "Invalid argument passed while calling the aggregate "
                f"instance. Expected a callable, got {type(func)}."
            )
        if getattr(func, "_flow_with_job", False):
            raise FlowProjectDefinitionError(
                "The with_job option cannot be used with aggregation."
            )
        setattr(func, "_flow_aggregate", self)
        return func


class _BaseAggregateStore(Mapping):
    """Base abstract class for aggregate stores.

    An aggregate store is a mapping from aggregate ids to aggregates, where
    an aggregate is defined as a tuple of instances of
    :class:`signac.contrib.job.Job`.
    """

    def __init__(self, project):
        self._project = project

    def __iter__(self):
        yield from self.keys()


class _AggregateStore(_BaseAggregateStore):
    """Class containing all aggregates associated with an :class:`aggregator`.

    Iterating over this object yields aggregate ids, which can be used as
    indices to return the corresponding aggregates.

    Parameters
    ----------
    aggregator : :class:`aggregator`
        aggregator object used to generate aggregates for this store.
    project : :class:`flow.FlowProject` or :class:`signac.contrib.project.Project`
        A signac project containing the jobs that will be used to create
        aggregates.

    """

    def __init__(self, aggregator, project):
        self._aggregator = aggregator
        self._project = project

        # We need to register the aggregates for this instance using the
        # provided project. After registering, we store the aggregates mapped
        # with the ids using :func:`get_aggregate_id`.
        self._register_aggregates()

    def __getitem__(self, id):
        """Get the aggregate corresponding to the provided id."""
        try:
            return self._aggregates_by_id[id]
        except KeyError:
            raise KeyError(f"Aggregate id {id} could not be found.")

    def __contains__(self, id):
        """Return whether this instance contains an aggregate (by aggregate id).

        Parameters
        ----------
        id : str
            The id of an aggregate of jobs.

        Returns
        -------
        bool
            Whether this instance contains the aggregate.

        """
        return id in self._aggregates_by_id

    def __getstate__(self):
        state = {"_aggregator": self._aggregator}
        return state

    def __len__(self):
        return len(self._aggregates_by_id)

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._aggregator == other._aggregator

    def __hash__(self):
        return hash(self._aggregator)

    def keys(self):
        return self._aggregates_by_id.keys()

    def values(self):
        return self._aggregates_by_id.values()

    def items(self):
        return self._aggregates_by_id.items()

    def _register_aggregates(self):
        """Register aggregates from the project.

        This is called at instantiation to generate and store aggregates.

        Every aggregate is required to be a tuple of jobs.
        """
        # Initialize the internal mapping from id to aggregate
        self._aggregates_by_id = {}
        for aggregate in self._generate_aggregates():
            for job in aggregate:
                if job not in self._project:
                    raise LookupError(
                        f"The signac job {job.id} not found in {self._project}"
                    )
            try:
                stored_aggregate = tuple(aggregate)
            except TypeError:  # aggregate is not iterable
                raise ValueError("Invalid aggregator_function provided by the user.")
            # Store aggregate by id to allow searching by id
            self._aggregates_by_id[
                get_aggregate_id(stored_aggregate)
            ] = stored_aggregate

    def _generate_aggregates(self):
        jobs = self._project
        if self._aggregator._select is not None:
            jobs = filter(self._aggregator._select, jobs)
        if self._aggregator._sort_by is None:
            jobs = list(jobs)
        else:
            if callable(self._aggregator._sort_by):
                sort_function = self._aggregator._sort_by
            else:

                def sort_function(job):
                    return job.statepoint[self._aggregator._sort_by]

            jobs = sorted(
                jobs,
                key=sort_function,
                reverse=not self._aggregator._sort_ascending,
            )
        yield from self._aggregator._aggregator_function(jobs)


class _DefaultAggregateStore(_BaseAggregateStore):
    """Aggregate storage wrapper for the default aggregator.

    This class holds the information of the project associated with an
    operation function using the default aggregator, i.e.
    ``aggregator.groupsof(1)``.

    Iterating over this object yields tuples each containing one job from the project.

    Parameters
    ----------
    project : :class:`flow.FlowProject` or :class:`signac.contrib.project.Project`
        A signac project used to fetch jobs for creating aggregates.

    """

    def __init__(self, project):
        super().__init__(project)
        # Below, we store repr(project), which defines the hash and equality
        # operators of this class. This class must be hashable because it is
        # used as a dict key. However, when unpickling a FlowProject, this
        # object's hash must be computed *before* the FlowProject is fully
        # initialized. Thus, it is not possible to execute repr(project) when
        # hashing the instance at the time of unpickling. This means that this
        # class cannot be unpickled unless we pre-emptively compute and store
        # the repr.
        self._project_repr = repr(project)

    def __getitem__(self, id):
        """Return an aggregate of one job from its job id.

        Parameters
        ----------
        id : str
            The job id.

        """
        try:
            return (self._project.open_job(id=id),)
        except KeyError:
            raise KeyError(f"Aggregate id {id} could not be found.")

    def __contains__(self, id):
        """Return whether this instance contains a job (by job id).

        Parameters
        ----------
        id : str
            The job id.

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
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._project_repr == other._project_repr

    def __hash__(self):
        return hash(self._project_repr)

    def keys(self):
        for job in self._project:
            yield job.id

    def values(self):
        for job in self._project:
            yield (job,)

    def items(self):
        for job in self._project:
            yield (job.id, (job,))


def get_aggregate_id(aggregate):
    """Generate aggregate id for an aggregate of jobs.

    The aggregate id is a unique hash identifying a tuple of jobs. The
    aggregate id is sensitive to the order of the jobs in the aggregate. The
    id of an aggregate containing one job is that job's id (the hash of its
    state point).

    Parameters
    ----------
    aggregate : tuple of :class:`~signac.contrib.job.Job`
        Aggregate of signac jobs.

    Returns
    -------
    str
        The generated aggregate id.

    """
    if len(aggregate) == 1:
        # Return job id as it's already unique
        return aggregate[0].id

    id_string = ",".join(job.id for job in aggregate)
    hash_ = md5(id_string.encode("utf-8")).hexdigest()
    return f"agg-{hash_}"


class _AggregatesCursor(Collection):
    """Abstract class defining iterators over aggregates stored in a FlowProject.

    Parameters
    ----------
    project : :class:`~.FlowProject`
        A FlowProject whose jobs are aggregated.

    """

    @abstractmethod
    def __eq__(self, other):
        pass


class _AggregateStoresCursor(_AggregatesCursor):
    """Utility class to iterate over a collection of _AggregateStore instances.

    Parameters
    ----------
    project : :class:`~.FlowProject`
        A FlowProject whose jobs are aggregated.

    """

    def __init__(self, project):
        self._stores = project._group_to_aggregate_store.inverse.keys()

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._stores == other._stores

    def __contains__(self, aggregate):
        aggregate_id = get_aggregate_id(aggregate)
        return any(aggregate_id in aggregate_store for aggregate_store in self._stores)

    def __len__(self):
        # Return number of aggregates summed across all aggregate stores
        return sum(len(aggregate_store) for aggregate_store in self._stores)

    def __iter__(self):
        for aggregate_store in self._stores:
            yield from aggregate_store.values()


class _JobAggregateCursor(_AggregatesCursor):
    """Utility class to iterate over single-job aggregates in a FlowProject.

    Parameters
    ----------
    project : :class:`~.FlowProject`
        A FlowProject whose jobs are aggregated.
    filter : dict
        A mapping of key-value pairs that all indexed job state points are
        compared against (Default value = None).
    doc_filter : dict
        A mapping of key-value pairs that all indexed job documents are
        compared against (Default value = None).

    """

    def __init__(self, project, filter=None, doc_filter=None):
        self._cursor = project.find_jobs(filter, doc_filter)

    def __eq__(self, other):
        # Cursors cannot compare equal if one is over aggregates and the other
        # is over jobs.
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._cursor == other._cursor

    def __contains__(self, aggregate):
        return len(aggregate) == 1 and aggregate[0] in self._cursor

    def __len__(self):
        return len(self._cursor)

    def __iter__(self):
        for job in self._cursor:
            yield (job,)
