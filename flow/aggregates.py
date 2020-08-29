# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from collections.abc import Iterable
from hashlib import sha1
from itertools import groupby
from itertools import zip_longest


class aggregator:
    r"""Decorator for operation function that is to be aggregated.
    By default, if the ``aggregator_function`` is not passed,
    an aggregate of all jobs will be created.

    .. code-block:: python

        @aggregator()
        @FlowProject.operation
        def foo(*jobs):
            print(len(jobs))

    :param aggregator_function:
        Information describing how to aggregate jobs. Is a callable that
        takes in a list of jobs and can return or yield subsets of jobs as
        an iterable. The default behavior is creating a single aggregate
        of all jobs.
    :type aggregator_function:
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

    def __init__(self, aggregator_function=None, sort_by=None, reverse_order=False,
                 select=None, **kwargs):
        if aggregator_function is None:
            def aggregator_function(jobs):
                return [jobs]

        if not callable(aggregator_function):
            raise TypeError("Expected callable for aggregator_function, got "
                            f"{type(aggregator_function)}")
        elif sort_by is not None and not isinstance(sort_by, str):
            raise TypeError(f"Expected string sort_by parameter, got {type(sort_by)}")
        elif select is not None and not callable(select):
            raise TypeError(f"Expected callable for select, got {type(select)}")

        # For "non-aggregate" functions we set the aggregator object equals to
        # aggregator.groupsof(1). If any other aggregator object is associated with an
        # operation function then mark that as a "aggregate operation" else it's a
        # "non-aggregate" operation.
        if sort_by is None and select is None and not reverse_order:
            self._is_aggregate = getattr(aggregator_function, '_num', 0) != 1
        else:
            self._is_aggregate = True

        self._aggregator_function = aggregator_function
        self._sort_by = sort_by
        self._reverse_order = bool(reverse_order)
        self._select = select

    @classmethod
    def groupsof(cls, num=1, sort_by=None, reverse_order=False, select=None):
        """Aggregates jobs of a set group size.

        By default aggregate of a single job is created.

        If the number of jobs present in the project can't be divided by the number
        provided by the user then the last aggregate will have the maximum possible
        jobs in it.
        For instace, if 10 jobs are present in a project and they are aggregated in groups
        of 3 then the length of aggregates would be 3, 3, 3, 1.

        The below code-block provides an example on how jobs can be aggregated in
        groups of 2.

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

        def aggregator_function(jobs):
            args = [iter(jobs)] * num
            return zip_longest(*args)

        setattr(aggregator_function, '_num', num)

        return cls(aggregator_function, sort_by, reverse_order, select)

    @classmethod
    def groupby(cls, key, default=None, sort_by=None, reverse_order=False, select=None):
        """Aggregates jobs according to matching state point key values.

        The below code-block provides an example on how to aggregate jobs having
        common state point parameter 'sp' whose value, when not found, is replaced by a
        default value of -1.

        .. code-block:: python

            @aggregator.groupby(key='sp', default=-1)
            @FlowProject.operation
            def foo(*jobs):
                print(len(jobs))

        :param key:
            Parameter which specifies how to group jobs based on a single
            or a list of state point parameters or a custom callable.
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

        def aggregator_function(jobs):
            for key, group in groupby(sorted(jobs, key=keyfunction), key=keyfunction):
                yield group

        setattr(aggregator_function, '_key', key)
        setattr(aggregator_function, '_default', default)

        return cls(aggregator_function, sort_by, reverse_order, select)

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        elif(
            self._sort_by != other._sort_by or
            self._reverse_order != other._reverse_order or
            # Check equality of the _num attribute set in groupsof class method..
            getattr(self._aggregator_function, '_num', None) !=
            getattr(other._aggregator_function, '_num', None) or
            # Check equality of the _key attribute set in groupby class method.
            getattr(self._aggregator_function, '_key', None) !=
            getattr(other._aggregator_function, '_key', None) or
            # Check equality of the _default attribute set in groupby class method.
            getattr(self._aggregator_function, '_default', None) !=
            getattr(other._aggregator_function, '_default', None)
        ):
            return False
        elif(
            self._select == other._select and
            self._aggregator_function == other._aggregator_function
        ):
            return True

        # Get unique id for _select attribute
        self_select = self._get_unique_function_id(self._select)
        other_select = self._get_unique_function_id(other._select)
        # Get unique id for _aggregator_function attribute
        self_aggregator_function = self._get_unique_function_id(self._aggregator_function)
        other_aggregator_function = self._get_unique_function_id(other._aggregator_function)

        return self_select == other_select and \
            self_aggregator_function == other_aggregator_function

    def __hash__(self):
        blob_l = [
            hash(self._sort_by), hash(self._reverse_order),
            self._get_unique_function_id(self._aggregator_function),
            self._get_unique_function_id(self._select)
        ]
        blob = ','.join(str(attr) for attr in blob_l)

        return int(sha1(blob.encode('utf-8')).hexdigest(), 16)

    def _get_unique_function_id(self, func):
        """Generate unique id for the function passed. The id returned is used to generate
        hash and compare arbitrary types which are callable like ``self._aggregator_function``
        and ``self._select`` attributes.
        """
        try:
            return func.__code__.co_code
        except Exception:  # Got something other than a function
            return hash(func)

    def _create_AggregatesStore(self, project):
        """Create the instance of _AggregatesStore or _DefaultAggregateStore classes
        to make sure that we don't explicitly store the aggregates in the FlowProject.

        When iterated over these classes, an aggregate is yielded.

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
        if callable(func):
            setattr(func, '_flow_aggregate', self)
            return func
        else:
            raise TypeError('Invalid argument passed while calling '
                            'the aggregate instance. Expected a callable, '
                            f'got {type(func)}.')


class _AggregatesStore:
    """This class holds the information of all the aggregates associated with
    a :py:class:`aggregator`.

    This is a callable class which, when called, generates all the aggregates.
    When iterated through it's instance, all the aggregates are yielded.

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

        # We need to create a _MakeAggregate instance associated with this object
        # in order to generate aggregates whenever necessary.
        self._make_aggregates = _MakeAggregates.initialize_with_aggregator(aggregator)

        # We need to register the aggregates for this instance using the
        # project provided.
        self._aggregates = list()
        self._aggregate_ids = dict()
        self._register_aggregates(project)

    def __iter__(self):
        yield from self._aggregates

    def __getitem__(self, id):
        "Return an aggregate, if exists, using the id provided"
        try:
            return self._aggregate_ids[id]
        except KeyError:
            raise LookupError(f'Unable to find the aggregate having id {id} in '
                              'the FlowProject')

    def __contains__(self, aggregate):
        """Return whether an aggregate is stored in the this
        instance of :py:class:`_AggregateStore`

        :param aggregate:
            An aggregate of jobs.
        :type aggregate:
            tuple of :py:class:`signac.contrib.job.Job`
        """
        return get_aggregate_id(aggregate) in self._aggregate_ids

    def __len__(self):
        return len(self._aggregates)

    def __eq__(self, other):
        return type(self) == type(other) and \
               self._aggregate_ids.keys() == other._aggregate_ids.keys() and \
               self._aggregator == other._aggregator

    def __hash__(self):
        blob = str(hash(self._aggregator))
        return int(sha1(blob.encode('utf-8')).hexdigest(), 16)

    def _register_aggregates(self, project):
        """If the instance of this class is called then we will
        generate aggregates and store them in ``self._aggregates``.
        """
        # Since we should always clear the aggregates before registering
        # aggregates because we append the aggregates directly.
        self._aggregates.clear()
        self._aggregate_ids.clear()
        aggregated_jobs = self._make_aggregates(project)
        self._create_nested_aggregate_list(aggregated_jobs, project)

    def _create_nested_aggregate_list(self, aggregated_jobs, project):
        """signac-flow internally assumes every aggregate to be a tuple of jobs.

        This method converts every aggregate in ``aggregated_jobs``, which may be of
        any type, returned from an aggregator_function using the instance of
        ``_MakeAggregates`` to an aggregate of jobs as tuple.
        """
        def _validate_and_filter_job(job):
            "Validate whether a job is eligible to be a part of an aggregate or not."
            if job is None:
                return False
            elif job in project:
                return True
            else:
                raise LookupError(f'The signac job {job.get_id()} not found'
                                  f'in {project}')

        for aggregate in aggregated_jobs:
            try:
                filter_aggregate = tuple(filter(_validate_and_filter_job, aggregate))
            except TypeError:  # aggregate is not iterable
                ValueError("Invalid aggregator_function provided by the user.")
            # Store aggregate in this instance
            self._aggregates.append(filter_aggregate)
            # Store aggregate by their ids in order to search through id
            self._aggregate_ids[get_aggregate_id(filter_aggregate)] = filter_aggregate


class _DefaultAggregateStore:
    """This class holds the information of the project associated with
    an operation function aggregated by the default aggregates i.e.
    :py:class:`aggregator.groupsof(1)`.

    When iterated through it's instance, it yields a tuple of a single job from
    the Project.

    :param project:
        A signac project used to fetch jobs for creating aggregates.
    :type project:
        :py:class:`flow.FlowProject` or :py:class:`signac.contrib.project.Project`
    """
    def __init__(self, project):
        self._project = project

    def __iter__(self):
        for job in self._project:
            yield (job,)

    def __getitem__(self, id):
        "Return a tuple of a single job via job id."
        try:
            return (self._project.open_job(id=id),)
        except KeyError:
            raise LookupError(f"Did not find job with id {id}.")

    def __contains__(self, aggregate):
        """Return whether the job is present in the project associated with this
        instance of :py:class:`_DefaultAggregateStore`.
        """
        # signac-flow internally assumes every aggregate to be a tuple.
        # Hence this method will also get a tuple as an input.
        return len(aggregate) == 1 and aggregate[0] in self._project

    def __len__(self):
        return len(self._project)

    def __eq__(self, other):
        return type(self) == type(other) and \
               self._project == other._project

    def __hash__(self):
        blob = self._project.get_id()
        return int(sha1(blob.encode('utf-8')).hexdigest(), 16)

    def _register_aggregates(self, project):
        """We have to store self._project when this method is invoked
        This is because we will then iterate over that project in
        order to return an aggregates of one.
        """
        self._project = project


class _MakeAggregates:
    """This class acts as a funnctor which handles the creation of
    aggregates using all the jobs in a project.

    :param aggregator_function:
        The ``aggregator_function`` attribute associated with the
        :py:class:`aggregator`
    :type aggregator_function:
        callable
    :param sort_by:
        The ``sort_by`` attribute associated with the :py:class:`aggregator`
    :type sort_by:
        str or NoneType
    :param reverse_order:
        The ``reverse_order`` attribute associated with the :py:class:`aggregator`
    :type reverse_order:
        bool
    :param select:
        The ``select`` attribute associated with the :py:class:`aggregator`
    :type select:
        callable or NoneType
    """
    def __init__(self, aggregator_function, sort_by, reverse_order, select):
        self._aggregator_function = aggregator_function
        self._sort_by = sort_by
        self._reverse_order = bool(reverse_order)
        self._select = select

    def __call__(self, project):
        jobs = project
        if self._select is not None:
            jobs = filter(self._select, jobs)
        if self._sort_by is None:
            jobs = list(jobs)
        else:
            jobs = sorted(jobs,
                          key=lambda job: job.sp[self._sort_by],
                          reverse=self._reverse_order)
        yield from self._aggregator_function(jobs)

    @classmethod
    def initialize_with_aggregator(cls, aggregator):
        """Initialize this class using an aggregator object.

        :param aggregator:
            aggregator object associated with this class.
        :type aggregator:
            :py:class:`aggregator`
        """
        return cls(aggregator._aggregator_function, aggregator._sort_by,
                   aggregator._reverse_order, aggregator._select)


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
    return f'agg-{sha1(blob.encode()).hexdigest()}'
