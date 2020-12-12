# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Workflow definition with the FlowProject.

The FlowProject is a signac Project that allows the user to define a workflow.
"""
import argparse
import contextlib
import datetime
import functools
import inspect
import json
import logging
import multiprocessing
import os
import random
import re
import subprocess
import sys
import threading
import time
import traceback
import warnings
from collections import Counter, defaultdict
from copy import deepcopy
from enum import IntFlag
from hashlib import sha1
from itertools import chain, count, groupby, islice
from multiprocessing import Event, Pool, TimeoutError, cpu_count
from multiprocessing.pool import ThreadPool

import cloudpickle
import jinja2
import signac
from deprecation import deprecated
from jinja2 import TemplateNotFound as Jinja2TemplateNotFound
from signac.contrib.filterparse import parse_filter_arg
from signac.contrib.hashing import calc_id
from signac.contrib.project import JobsCursor
from tqdm.auto import tqdm

from .aggregates import _DefaultAggregateStore, aggregator, get_aggregate_id
from .environment import get_environment
from .errors import (
    ConfigKeyError,
    NoSchedulerError,
    SubmitError,
    TemplateError,
    UserConditionError,
    UserOperationError,
)
from .labels import _is_label_func, classlabel, label, staticlabel
from .render_status import Renderer as StatusRenderer
from .scheduling.base import ClusterJob, JobStatus
from .util import config as flow_config
from .util import template_filters
from .util.misc import (
    TrackGetItemDict,
    _cached_partial,
    _positive_int,
    _to_hashable,
    add_cwd_to_environment_pythonpath,
    roundrobin,
    switch_to_directory,
)
from .util.translate import abbreviate, shorten
from .version import __version__

logger = logging.getLogger(__name__)


# The TEMPLATE_HELP can be shown with the --template-help option available to all
# command line subcommands that use the templating system.
TEMPLATE_HELP = """Execution and submission scripts are generated with the jinja2 template files.
Standard files are shipped with the package, but maybe replaced or extended with
custom templates provided within a project.
The default template directory can be configured with the 'template_dir' configuration
variable, for example in the project configuration file. The current template directory is:
{template_dir}

All template variables can be placed within a template using the standard jinja2
syntax, e.g., the project root directory can be written like this: {{{{ project._rd }}}}.
The available template variables are:
{template_vars}

Filter functions can be used to format template variables in a specific way.
For example: {{{{ project.get_id() | capitalize }}}}.

The available filters are:
{filters}"""

_FMT_SCHEDULER_STATUS = {
    JobStatus.unknown: "U",
    JobStatus.registered: "R",
    JobStatus.inactive: "I",
    JobStatus.submitted: "S",
    JobStatus.held: "H",
    JobStatus.queued: "Q",
    JobStatus.active: "A",
    JobStatus.error: "E",
    JobStatus.placeholder: " ",
}


class IgnoreConditions(IntFlag):
    """Flags that determine which conditions are used to determine job eligibility."""

    # The __invert__ operator must be defined since IntFlag simply performs an
    # integer bitwise not on the underlying enum value, which is problematic in
    # two's-complement arithmetic. What we want is to only flip valid bits.

    def __invert__(self):
        # Compute the largest number of bits used to represent one of the flags
        # so that we can XOR the appropriate number.
        max_bits = len(bin(max([elem.value for elem in type(self)]))) - 2
        return self.__class__((2 ** max_bits - 1) ^ self._value_)

    NONE = 0
    """Check all conditions."""

    PRE = 1
    """Ignore preconditions."""

    POST = 2
    """Ignore postconditions."""

    ALL = PRE | POST
    """Ignore all conditions."""

    def __str__(self):
        return {
            IgnoreConditions.PRE: "pre",
            IgnoreConditions.POST: "post",
            IgnoreConditions.ALL: "all",
            IgnoreConditions.NONE: "none",
        }[self]


class _IgnoreConditionsConversion(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super().__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, getattr(IgnoreConditions, values.upper()))


class _condition:
    # This counter should be incremented each time a "never" condition
    # is created, and the value should be used as the tag for that
    # condition to ensure that no pair of "never" conditions
    # are found to be equal by the graph detection algorithm.
    current_arbitrary_tag = 0

    def __init__(self, condition, tag=None):
        """Add tag to differentiate built-in conditions during graph detection."""
        if tag is None:
            try:
                tag = condition.__code__.co_code
            except AttributeError:
                logger.warning(f"Condition {condition} could not autogenerate tag.")
        condition._flow_tag = tag
        self.condition = condition

    @classmethod
    def isfile(cls, filename):
        """Determine if the specified file exists for the job(s)."""

        def _isfile(*jobs):
            return all(job.isfile(filename) for job in jobs)

        return cls(_isfile, "isfile_" + filename)

    @classmethod
    def true(cls, key):
        """Evaluate if a document key is True for the job(s).

        Returns True if the specified key is present in the job document(s) and
        evaluates to True.
        """

        def _document(*jobs):
            return all(job.document.get(key, False) for job in jobs)

        return cls(_document, "true_" + key)

    @classmethod
    def false(cls, key):
        """Evaluate if a document key is False for the job(s).

        Returns True if the specified key is present in the job document(s) and
        evaluates to False.
        """

        def _no_document(*jobs):
            return all(not job.document.get(key, False) for job in jobs)

        return cls(_no_document, "false_" + key)

    @classmethod
    def never(cls, func):
        """Return False."""
        cls.current_arbitrary_tag += 1
        return cls(lambda _: False, str(cls.current_arbitrary_tag))(func)

    @classmethod
    def not_(cls, condition):
        """Return ``not condition(*jobs)`` for the provided condition function."""

        def _not(*jobs):
            return not condition(*jobs)

        return cls(_not, b"not_" + condition.__code__.co_code)


def _create_all_metacondition(condition_dict, *other_funcs):
    """Generate metacondition requiring all provided conditions to be true.

    This function generates an aggregate metaconditions that requires *all*
    provided conditions to be met. The resulting metacondition is constructed
    with appropriate information for graph detection.
    """
    condition_list = [c for f in other_funcs for c in condition_dict[f]]

    def _flow_metacondition(*jobs):
        return all(c(*jobs) for c in condition_list)

    _flow_metacondition._composed_of = condition_list
    return _flow_metacondition


def _make_bundles(operations, size=None):
    """Slice an iterable of operations into equally sized bundles.

    This utility function splits an iterable of operations into equally sized
    bundles. The final bundle may be smaller than the specified size.

    Parameters
    ----------
    operations : iterable
        Iterable of operations.
    size : int
        Size of bundles. (Default value = None)

    Yields
    ------
    list
        Bundles of operations with specified size.

    """
    if size == 0:
        size = None
    operations = iter(operations)
    while True:
        bundle = list(islice(operations, size))
        if bundle:
            yield bundle
        else:
            break


class _JobOperation:
    """Class containing execution information for one group and one job.

    The execution or submission of a :class:`~.FlowGroup` uses a passed-in command
    which can either be a string or function with no arguments that returns a shell
    executable command. The shell executable command won't be used if it is
    determined that the group can be executed without forking.

    .. note::

        This class is used by the :class:`~.FlowGroup` class for the execution and
        submission process and should not be instantiated by users themselves.

    Parameters
    ----------
    id : str
        The id of this _JobOperation instance. The id should be unique.
    name : str
        The name of the _JobOperation.
    jobs : tuple of :class:`~signac.contrib.job.Job`
        The jobs associated with this operation.
    cmd : callable or str
        The command that executes this operation. Can be a callable that when
        evaluated returns a string.
    directives : :class:`flow.directives._Directives`
        A :class:`flow.directives._Directives` object of additional parameters
        that provide instructions on how to execute this operation, e.g.,
        specifically required resources.

    """

    def __init__(self, id, name, jobs, cmd, directives=None):
        self._id = id
        self.name = name
        self._jobs = jobs
        if not (callable(cmd) or isinstance(cmd, str)):
            raise ValueError("cmd must be a callable or string.")
        self._cmd = cmd

        # Keys which were explicitly set by the user, but are not evaluated by the
        # template engine are cause for concern and might hint at a bug in the template
        # script or ill-defined directives. We are therefore keeping track of all
        # keys set by the user and check whether they have been evaluated by the template
        # script engine later.
        keys_set_by_user = set(directives._user_directives)

        # We use a special dictionary that tracks all keys that have been
        # evaluated by the template engine and compare them to those explicitly
        # set by the user. See also comment above.
        self.directives = TrackGetItemDict(directives)
        self.directives._keys_set_by_user = keys_set_by_user

    def __str__(self):
        assert len(self._jobs) > 0
        max_len = 3
        if len(self._jobs) > max_len:
            shown = self._jobs[: max_len - 2] + ("...",) + self._jobs[-1:]
        else:
            shown = self._jobs
        return (
            f"{self.name}[#{len(self._jobs)}]"
            f"({', '.join([str(element) for element in shown])})"
        )

    def __repr__(self):
        return "{type}(name='{name}', jobs='{jobs}', cmd={cmd}, directives={directives})".format(
            type=type(self).__name__,
            name=self.name,
            jobs="(" + ", ".join(map(repr, self._jobs)) + ")",
            cmd=repr(self.cmd),
            directives=self.directives,
        )

    def __hash__(self):
        return int(sha1(self.id.encode("utf-8")).hexdigest(), 16)

    def __eq__(self, other):
        return self.id == other.id

    @property
    def id(self):
        return self._id

    @property
    def cmd(self):
        if callable(self._cmd):
            # We allow cmd to be 'lazy' or an unevaluated function because
            # in cases where a user uses the Python API without specifying
            # a project entrypoint, running many operations is still valid.
            # If we need to fork this will fail to generate a command and
            # error, but not until then. If we don't fork then nothing errors,
            # and the user gets the expected result.
            return self._cmd()
        return self._cmd

    def set_status(self, value):
        """Store the operation's status."""
        # Since #324 doesn't include actual aggregation, it is guaranteed that the length
        # of self._jobs is equal to 1, hence we won't be facing the problem for lost
        # aggregates. #335 introduces the concept of storing aggregates which will
        # help retrieve the information of lost aggregates. The storage of aggregates
        # will be similar to bundles hence no change will be made to this method.
        # This comment should be removed after #335 gets merged.
        self._jobs[0]._project.document.setdefault("_status", {})[self.id] = int(value)

    def get_status(self):
        """Retrieve the operation's last known status."""
        try:
            return JobStatus(self._jobs[0]._project.document["_status"][self.id])
        except KeyError:
            return JobStatus.unknown


@deprecated(deprecated_in="0.11", removed_in="0.13", current_version=__version__)
class JobOperation(_JobOperation):
    """Class containing execution information for one group and one job.

    The execution or submission of a :class:`~.FlowGroup` uses a passed-in command
    which can either be a string or function with no arguments that returns a shell
    executable command.  The shell executable command won't be used if it is
    determined that the group can be executed without forking.

    .. note::

        This class is used by the :class:`~.FlowGroup` class for the execution and
        submission process and should not be instantiated by users themselves.

    Parameters
    ----------
    id : str
        The id of this JobOperation instance. The id should be unique.
    name : str
        The name of the JobOperation.
    job : :class:`~signac.contrib.job.Job`
        The job associated with this operation.
    cmd : callable or str
        The command that executes this operation. Can be a callable that when
        evaluated returns a string.
    directives : :class:`flow.directives._Directives`
        A :class:`flow.directives._Directives` object of additional parameters
        that provide instructions on how to execute this operation, e.g.,
        specifically required resources.

    """

    def __init__(self, id, name, job, cmd, directives=None):
        self._id = id
        self.name = name
        self._jobs = (job,)
        if not (callable(cmd) or isinstance(cmd, str)):
            raise ValueError("JobOperation cmd must be a callable or string.")
        self._cmd = cmd

        if directives is None:
            directives = job._project._environment._get_default_directives()
        else:
            directives = dict(directives)  # explicit copy

        # Keys which were explicitly set by the user, but are not evaluated by the
        # template engine are cause for concern and might hint at a bug in the template
        # script or ill-defined directives. We are therefore keeping track of all
        # keys set by the user and check whether they have been evaluated by the template
        # script engine later.
        keys_set_by_user = set(directives)

        # We use a special dictionary that tracks all keys that have been
        # evaluated by the template engine and compare them to those explicitly
        # set by the user. See also comment above.
        self.directives = TrackGetItemDict(
            {key: value for key, value in directives.items()}
        )
        self.directives._keys_set_by_user = keys_set_by_user

    @property
    def job(self):
        assert len(self._jobs) == 1
        return self._jobs[0]

    def __repr__(self):
        return "{type}(name='{name}', job='{job}', cmd={cmd}, directives={directives})".format(
            type=type(self).__name__,
            name=self.name,
            job=repr(self.job),
            cmd=repr(self.cmd),
            directives=self.directives,
        )


class _SubmissionJobOperation(_JobOperation):
    r"""Class containing submission information for one group and one job.

    This class extends :class:`_JobOperation` to include a set of groups
    that will be executed via the "run" command. These groups are known at
    submission time.

    Parameters
    ----------
    \*args
        Passed to the constructor of :class:`_JobOperation`.
    eligible_operations : list
        A list of :class:`_JobOperation` that will be executed when this
        submitted job is executed.
    operations_with_unmet_preconditions : list
        A list of :class:`_JobOperation` that will not be executed in the
        first pass of :meth:`FlowProject.run` due to unmet preconditions. These
        operations may be executed in subsequent iterations of the run loop.
    operations_with_met_postconditions : list
        A list of :class:`_JobOperation` that will not be executed in the
        first pass of :meth:`FlowProject.run` because all postconditions are
        met. These operations may be executed in subsequent iterations of the
        run loop.
    \*\*kwargs
        Passed to the constructor of :class:`_JobOperation`.

    """

    def __init__(
        self,
        *args,
        eligible_operations=None,
        operations_with_unmet_preconditions=None,
        operations_with_met_postconditions=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if eligible_operations is None:
            self.eligible_operations = []
        else:
            self.eligible_operations = eligible_operations

        if operations_with_unmet_preconditions is None:
            self.operations_with_unmet_preconditions = []
        else:
            self.operations_with_unmet_preconditions = (
                operations_with_unmet_preconditions
            )

        if operations_with_met_postconditions is None:
            self.operations_with_met_postconditions = []
        else:
            self.operations_with_met_postconditions = operations_with_met_postconditions


class _FlowCondition:
    """A _FlowCondition represents a condition as a function of a signac job.

    The ``__call__()`` method of a _FlowCondition object may return either True
    or False, representing whether the condition is met or not.  This can be
    used to build a graph of conditions and operations.

    Parameters
    ----------
    callback : callable
        A callable with one positional argument (the job).

    """

    def __init__(self, callback):
        self._callback = callback

    def __call__(self, jobs):
        try:
            return self._callback(*jobs)
        except Exception as error:
            assert len(jobs) == 1
            raise UserConditionError(
                "An exception was raised while evaluating the condition {name} "
                "for job {jobs}.".format(
                    name=self._callback.__name__, jobs=", ".join(map(str, jobs))
                )
            ) from error

    def __hash__(self):
        return hash(self._callback)

    def __eq__(self, other):
        return self._callback == other._callback


class BaseFlowOperation:
    """A :class:`~.BaseFlowOperation` represents a data space operation acting on any job.

    Every :class:`~.BaseFlowOperation` is associated with a specific command.

    Preconditions (pre) and postconditions (post) can be used to trigger an
    operation only when certain conditions are met. Conditions are unary
    callables, which expect an instance of job as their first and only
    positional argument and return either True or False.

    An operation is considered "eligible" for execution when all preconditions
    are met and when at least one of the postconditions is not met.
    Preconditions are always met when the list of preconditions is empty.
    Postconditions are never met when the list of postconditions is empty.

    .. note::
        This class should not be instantiated directly.

    Parameters
    ----------
    pre : sequence of callables
        List of preconditions.
    post : sequence of callables
        List of postconditions.

    """

    def __init__(self, pre=None, post=None):
        if pre is None:
            pre = []
        if post is None:
            post = []

        self._preconditions = [_FlowCondition(cond) for cond in pre]
        self._postconditions = [_FlowCondition(cond) for cond in post]

    def _eligible(self, jobs, ignore_conditions=IgnoreConditions.NONE):
        """Determine eligibility of jobs.

        Jobs are eligible when all preconditions are true and at least one
        postcondition is false, or corresponding conditions are ignored.

        Parameters
        ----------
        jobs : tuple
            The signac job handles.
        ignore_conditions : :class:`~.IgnoreConditions`
            Specify if preconditions and/or postconditions are to be ignored
            when determining eligibility. The default is
            :class:`IgnoreConditions.NONE`.

        Returns
        -------
        bool
            Whether the job is eligible.

        """
        if not isinstance(ignore_conditions, IgnoreConditions):
            raise ValueError(
                "The ignore_conditions argument of FlowProject.run() "
                "must be a member of class IgnoreConditions."
            )
        # len(self._preconditions) check for speed optimization
        met_preconditions = (
            (len(self._preconditions) == 0)
            or (ignore_conditions & IgnoreConditions.PRE)
            or all(cond(jobs) for cond in self._preconditions)
        )
        if met_preconditions and len(self._postconditions) > 0:
            unmet_postconditions = (ignore_conditions & IgnoreConditions.POST) or any(
                not cond(jobs) for cond in self._postconditions
            )
        else:
            unmet_postconditions = True
        return met_preconditions and unmet_postconditions

    @deprecated(deprecated_in="0.11", removed_in="0.13", current_version=__version__)
    def eligible(self, job, ignore_conditions=IgnoreConditions.NONE):
        """Determine eligibility of jobs.

        Jobs are eligible when all preconditions are true and at least one
        postcondition is false, or corresponding conditions are ignored.

        Parameters
        ----------
        job : :class:`~signac.contrib.job.Job`
            The signac job handle.
        ignore_conditions : :class:`~.IgnoreConditions`
            Specify if pre and/or postconditions check is to be ignored for
            eligibility check. The default is :class:`IgnoreConditions.NONE`.

        """
        return self._eligible((job,), ignore_conditions)

    def _complete(self, jobs):
        """Check if all postconditions are met."""
        if len(self._postconditions) > 0:
            return all(cond(jobs) for cond in self._postconditions)
        return False

    @deprecated(deprecated_in="0.11", removed_in="0.13", current_version=__version__)
    def complete(self, job):
        """Check if all postconditions are met."""
        return self._complete((job,))


class FlowCmdOperation(BaseFlowOperation):
    """An operation that executes a shell command.

    When an operation has the ``@cmd`` directive specified, it is instantiated
    as a FlowCmdOperation. The operation should be a function of
    :class:`~signac.contrib.job.Job`. The command (cmd) may
    either be a unary callable that expects an instance of
    :class:`~signac.contrib.job.Job` as its only positional argument and returns
    a string containing valid shell commands, or the string of commands itself.
    In either case, the resulting string may contain any attributes of the job placed
    in curly braces, which will then be substituted by Python string formatting.

    .. note::
        This class should not be instantiated directly.

    Parameters
    ----------
    cmd : str or callable
        The command to execute the operation. Callable values should be a
        function of ``job``. String values will be formatted with
        ``cmd.format(job=job)``.
    pre : sequence of callables
        List of preconditions.
    post : sequence of callables
        List of postconditions.

    """

    def __init__(self, cmd, pre=None, post=None):
        super().__init__(pre=pre, post=post)
        self._cmd = cmd

    def __str__(self):
        return f"{type(self).__name__}(cmd='{self._cmd}')"

    def __call__(self, *jobs, **kwargs):
        """Return the command formatted with the supplied job(s)."""
        job = kwargs.pop("job", None)
        if kwargs:
            raise ValueError(f"Invalid keyword arguments: {', '.join(kwargs)}")

        if job is not None:
            warnings.warn(
                "The job keyword argument is deprecated as of 0.11 and will be removed "
                "in 0.13",
                DeprecationWarning,
            )
        else:
            job = jobs[0] if len(jobs) == 1 else None

        if callable(self._cmd):
            return self._cmd(job).format(job=job)
        return self._cmd.format(job=job)


class FlowOperation(BaseFlowOperation):
    """An operation that executes a Python function.

    All operations without the ``@cmd`` directive use this class. The
    callable ``op_func`` should be a function of one or more instances of
    :class:`~signac.contrib.job.Job`.

    .. note::
        This class should not be instantiated directly.

    Parameters
    ----------
    op_func : callable
        A callable function of ``*jobs``.
    pre : sequence of callables
        List of preconditions.
    post : sequence of callables
        List of postconditions.

    """

    def __init__(self, op_func, pre=None, post=None):
        super().__init__(pre=pre, post=post)
        self._op_func = op_func

    def __str__(self):
        """Return string representing operation."""
        return f"{type(self).__name__}(op_func='{self._op_func}')"

    def __call__(self, *jobs):
        r"""Call the operation on the provided jobs.

        Parameters
        ----------
        \*jobs : One or more instances of :class:`~signac.contrib.job.Job`.
            The jobs passed to the operation.

        Returns
        -------
        object
            The result of the operation function.

        """
        return self._op_func(*jobs)


class FlowGroupEntry:
    """A FlowGroupEntry registers operations for inclusion into a :class:`FlowGroup`.

    Operation functions can be marked for inclusion into a :class:`FlowGroup`
    by decorating the functions with a corresponding :class:`FlowGroupEntry`.
    If the operation requires specific directives, :meth:`~.with_directives`
    accepts keyword arguments that are mapped to directives and returns a
    decorator that can be applied to the operation to mark it for inclusion in
    the group and indicate that it should be executed using the specified
    directives. This overrides the default directives specified by
    :meth:`flow.directives`.

    Parameters
    ----------
    name : str
        The name of the :class:`FlowGroup` to be created.
    options : str
        The :meth:`FlowProject.run` options to pass when submitting the group.
        These will be included in all submissions. Submissions use run
        commands to execute.
    aggregator : :class:`~.aggregator`
        aggregator object associated with the :class:`FlowGroup`

    """

    def __init__(self, name, options="", aggregator=aggregator.groupsof(1)):
        self.name = name
        self.options = options
        self.aggregator = aggregator

    def __call__(self, func):
        """Add the function into the group's operations.

        This call operator allows the class to be used as a decorator.

        Parameters
        ----------
        func : callable
            The function to decorate.

        Returns
        -------
        callable
            The decorated function.

        """
        if hasattr(func, "_flow_groups"):
            if self.name in func._flow_groups:
                raise ValueError(
                    f"Cannot register existing name {func} with group {self.name}"
                )
            func._flow_groups.append(self.name)
        else:
            func._flow_groups = [self.name]
        return func

    def _set_directives(self, func, directives):
        if hasattr(func, "_flow_group_operation_directives"):
            if self.name in func._flow_group_operation_directives:
                raise ValueError(
                    f"Cannot set directives because directives already exist "
                    f"for {func} in group {self.name}"
                )
            func._flow_group_operation_directives[self.name] = directives
        else:
            func._flow_group_operation_directives = {self.name: directives}

    def with_directives(self, directives):
        """Return a decorator that sets group specific directives to the operation.

        Parameters
        ----------
        directives : dict
            Directives to use for resource requests and running the operation
            through the group.

        Returns
        -------
        function
            A decorator which registers the function into the group with
            specified directives.

        """

        def decorator(func):
            self._set_directives(func, directives)
            return self(func)

        return decorator


class FlowGroup:
    """A :class:`~.FlowGroup` represents a subset of a workflow for a project.

    A :class:`FlowGroup` is associated with one or more instances of
    :class:`~.BaseFlowOperation`.

    Examples
    --------
    In the example below, the directives will be ``{'nranks': 4}`` for op1 and
    ``{'nranks': 2, 'executable': 'python3'}`` for op2.

    .. code-block:: python

        group = FlowProject.make_group(name='example_group')

        @group.with_directives(nranks=4)
        @FlowProject.operation
        @directives(nranks=2, executable="python3")
        def op1(job):
            pass

        @group
        @FlowProject.operation
        @directives(nranks=2, executable="python3")
        def op2(job):
            pass

    Parameters
    ----------
    name : str
        The name of the group to be used when calling from the command line.
    operations : dict
        A dictionary of operations where the keys are operation names and
        each value is a :class:`~.BaseFlowOperation`.
    operation_directives : dict
        A dictionary of additional parameters that provide instructions on how
        to execute a particular operation, e.g., specifically required
        resources. Operation names are keys and the dictionaries of directives
        are values. If an operation does not have directives specified, then
        the directives of the singleton group containing that operation are
        used. To prevent this, set the directives to an empty dictionary for
        that operation.
    options : str
        A string of options to append to the output of the object's call method.
        This allows options like ``--num_passes`` to be given to a group.

    """

    MAX_LEN_ID = 100

    def __init__(self, name, operations=None, operation_directives=None, options=""):
        self.name = name
        self.options = options
        # Requires Python >=3.6: dict must be ordered to ensure consistent
        # pretend submission output for templates.
        self.operations = {} if operations is None else dict(operations)
        if operation_directives is None:
            self.operation_directives = {}
        else:
            self.operation_directives = operation_directives

    def _set_entrypoint_item(self, entrypoint, directives, key, default, jobs):
        """Set a value (executable, path) for entrypoint in command.

        Order of priority is the operation directives specified and
        then the project specified value.
        """
        entrypoint[key] = directives.get(key, entrypoint.get(key, default))
        if callable(entrypoint[key]):
            entrypoint[key] = entrypoint[key](*jobs)

    def _determine_entrypoint(self, entrypoint, directives, jobs):
        """Get the entrypoint for creating a _JobOperation.

        If path cannot be determined, then raise a RuntimeError since we do not
        know where to point to.
        """
        entrypoint = entrypoint.copy()
        self._set_entrypoint_item(
            entrypoint, directives, "executable", sys.executable, jobs
        )

        # If a path is not provided, default to the path to the file where the
        # FlowProject (subclass) is defined.
        # We are assuming that all the jobs belong to the same project
        default_path = inspect.getfile(jobs[0]._project.__class__)
        self._set_entrypoint_item(entrypoint, directives, "path", default_path, jobs)
        return "{} {}".format(entrypoint["executable"], entrypoint["path"]).lstrip()

    def _resolve_directives(self, name, defaults, env):
        all_directives = env._get_default_directives()
        if name in self.operation_directives:
            all_directives.update(self.operation_directives[name])
        else:
            all_directives.update(defaults.get(name, {}))
        return all_directives

    def _submit_cmd(self, entrypoint, ignore_conditions, jobs=None):
        entrypoint = self._determine_entrypoint(entrypoint, {}, jobs)
        cmd = f"{entrypoint} run -o {self.name}"
        cmd = cmd if jobs is None else cmd + f" -j {get_aggregate_id(jobs)}"
        cmd = cmd if self.options is None else cmd + " " + self.options
        if ignore_conditions != IgnoreConditions.NONE:
            return cmd.strip() + " --ignore-conditions=" + str(ignore_conditions)
        return cmd.strip()

    def _run_cmd(self, entrypoint, operation_name, operation, directives, jobs):
        if isinstance(operation, FlowCmdOperation):
            return operation(*jobs).lstrip()
        entrypoint = self._determine_entrypoint(entrypoint, directives, jobs)
        return f"{entrypoint} exec {operation_name} {get_aggregate_id(jobs)}".lstrip()

    def __iter__(self):
        yield from self.operations.values()

    def __repr__(self):
        return (
            "{type}(name='{name}', operations='{operations}', "
            "operation_directives={directives}, options='{options}')".format(
                type=type(self).__name__,
                name=self.name,
                operations=" ".join(list(self.operations)),
                directives=self.operation_directives,
                options=self.options,
            )
        )

    def _eligible(self, jobs, ignore_conditions=IgnoreConditions.NONE):
        """Determine if at least one operation is eligible.

        A :class:`~.FlowGroup` is eligible for execution if at least one of
        its associated operations is eligible.

        Parameters
        ----------
        jobs : tuple
            The signac job handles.
        ignore_conditions : :class:`~.IgnoreConditions`
            Specify if preconditions and/or postconditions are to be ignored
            while checking eligibility. The default is
            :class:`IgnoreConditions.NONE`.

        Returns
        -------
        bool
            Whether the group is eligible.

        """
        return any(op._eligible(jobs, ignore_conditions) for op in self)

    def _complete(self, jobs):
        """Check if postconditions are met for all operations in the group.

        Parameters
        ----------
        jobs : tuple
            The signac job handles.

        Returns
        -------
        bool
            Whether the group is complete (all contained operations are
            complete).

        """
        return all(op._complete(jobs) for op in self)

    @deprecated(deprecated_in="0.11", removed_in="0.13", current_version=__version__)
    def eligible(self, job, ignore_conditions=IgnoreConditions.NONE):
        """Determine if at least one operation is eligible.

        A :class:`~.FlowGroup` is eligible for execution if at least one of
        its associated operations is eligible.

        Parameters
        ----------
        job : :class:`~signac.contrib.job.Job`
            A :class:`~signac.contrib.job.Job` from the signac workspace.
        ignore_conditions : :class:`~.IgnoreConditions`
            Specify if preconditions and/or postconditions are to be ignored
            while checking eligibility.  The default is
            :class:`IgnoreConditions.NONE`.

        Returns
        -------
        bool
            Whether the group is eligible.

        """
        return self._eligible((job,), ignore_conditions)

    @deprecated(deprecated_in="0.11", removed_in="0.13", current_version=__version__)
    def complete(self, job):
        """Check if all :class:`~.BaseFlowOperation` postconditions are met.

        Parameters
        ----------
        job : :class:`~signac.contrib.job.Job`
            A :class:`~signac.contrib.job.Job` from the signac workspace.

        Returns
        -------
        bool
            Whether the group is complete (all contained operations are
            complete).

        """
        return self._complete((job,))

    def add_operation(self, name, operation, directives=None):
        """Add an operation to the :class:`~.FlowGroup`.

        Parameters
        ----------
        name : str
            The name of the operation.
        operation : :class:`~.BaseFlowOperation`
            The workflow operation to add to the :class:`~.FlowGroup`.
        directives : dict
            The operation specific directives. (Default value = None)

        """
        self.operations[name] = operation
        if directives is not None:
            self.operation_directives[name] = directives

    def isdisjoint(self, group):
        """Return whether two groups are disjoint.

        Groups are disjoint if they do not share any common operations.

        Parameters
        ----------
        group : :class:`~.FlowGroup`
            The other :class:`~.FlowGroup` to compare to.

        Returns
        -------
        bool
            Returns ``True`` if ``group`` and ``self`` share no operations,
            otherwise returns ``False``.

        """
        return set(self).isdisjoint(set(group))

    def _generate_id(self, jobs, operation_name=None, index=0):
        """Generate a unique id which identifies this group and job(s).

        Parameters
        ----------
        jobs : sequence of :class:`signac.contrib.job.Job`
            Jobs defining the unique id.
        operation_name : str
            Operation name defining the unique id. (Default value = None)
        index : int
            Index for the :class:`~._JobOperation`. (Default value = 0)

        Returns
        -------
        str
            The unique id.

        """
        project = jobs[0]._project

        # The full name is designed to be truly unique for each job-group.
        if operation_name is None:
            op_string = "".join(sorted(list(self.operations)))
        else:
            op_string = operation_name

        full_name = "{}%{}%{}%{}".format(
            project.root_directory(), "-".join(map(str, jobs)), op_string, index
        )
        # The job_op_id is a hash computed from the unique full name.
        job_op_id = calc_id(full_name)

        # The actual job id is then constructed from a readable part and the job_op_id,
        # ensuring that the job-op is still somewhat identifiable, but guaranteed to
        # be unique. The readable name is based on the project id, job id, operation name,
        # and the index number. All names and the id itself are restricted in length
        # to guarantee that the id does not get too long.
        max_len = self.MAX_LEN_ID - len(job_op_id)
        if max_len < len(job_op_id):
            raise ValueError(f"Value for MAX_LEN_ID is too small ({self.MAX_LEN_ID}).")

        if len(jobs) > 1:
            concat_jobs_str = str(jobs[0])[0:8] + "-" + str(jobs[-1])[0:8]
        else:
            concat_jobs_str = str(jobs[0])[0:8]

        separator = getattr(project._environment, "JOB_ID_SEPARATOR", "/")
        readable_name = (
            "{project}{sep}{jobs}{sep}{op_string}{sep}{index:04d}{sep}".format(
                sep=separator,
                project=str(project)[:12],
                jobs=concat_jobs_str,
                op_string=op_string[:12],
                index=index,
            )[:max_len]
        )

        # By appending the unique job_op_id, we ensure that each id is truly unique.
        return readable_name + job_op_id

    def _get_status(self, jobs):
        """For a given job-aggregate, check the group's submission status."""
        try:
            return JobStatus(
                jobs[0]._project.document["_status"][self._generate_id(jobs)]
            )
        except KeyError:
            return JobStatus.unknown

    def _create_submission_job_operation(
        self,
        entrypoint,
        default_directives,
        jobs,
        ignore_conditions_on_execution=IgnoreConditions.NONE,
        index=0,
    ):
        """Create a _JobOperation object from the :class:`~.FlowGroup`.

        Creates a _JobOperation for use in submitting and scripting.

        Parameters
        ----------
        entrypoint : dict
            The path and executable, if applicable, to point to for execution.
        default_directives : dict
            The default directives to use for the operations. This is to allow
            for user specified groups to 'inherit' directives from
            ``default_directives``. If no defaults are desired, the argument
            can be set to an empty dictionary. This must be done explicitly,
            however.
        jobs : tuple of :class:`~signac.contrib.job.Job`
            The jobs that the :class:`~._JobOperation` is based on.
        ignore_conditions_on_execution : :class:`~.IgnoreConditions`
            Specify if preconditions and/or postconditions are to be ignored
            while checking eligibility during execution (after submission). The
            default is :class:`IgnoreConditions.NONE`.
        index : int
            Index for the :class:`~._JobOperation`. (Default value = 0)

        Returns
        -------
        :class:`_SubmissionJobOperation`
            Returns a :class:`~._SubmissionJobOperation` for submitting the
            group. The :class:`~._JobOperation` will have directives that have
            been collected appropriately from its contained operations.

        """
        unevaluated_cmd = _cached_partial(
            self._submit_cmd,
            entrypoint=entrypoint,
            jobs=jobs,
            ignore_conditions=ignore_conditions_on_execution,
        )

        def _get_run_ops(ignore_ops, additional_ignores_flag):
            """Get all runnable operations.

            Returns operations that match the combination of the conditions
            required by ``_create_submission_job_operation`` and the ignored
            flags, and remove operations in the ``ignore_ops`` list.

            Parameters
            ----------
            ignore_ops : iterable
                Operations to ignore.
            additional_ignores_flag : :class:`~.IgnoreConditions`
                An additional set of ignore flags combined with the ignore
                flags used for execution.

            Returns
            -------
            list of :class:`_JobOperation`
                Runnable operations.

            """
            return list(
                set(
                    self._create_run_job_operations(
                        entrypoint=entrypoint,
                        default_directives=default_directives,
                        jobs=jobs,
                        ignore_conditions=ignore_conditions_on_execution
                        | additional_ignores_flag,
                    )
                )
                - set(ignore_ops)
            )

        submission_directives = self._get_submission_directives(
            default_directives, jobs
        )
        eligible_operations = _get_run_ops([], IgnoreConditions.NONE)
        operations_with_unmet_preconditions = _get_run_ops(
            eligible_operations, IgnoreConditions.PRE
        )
        operations_with_met_postconditions = _get_run_ops(
            eligible_operations, IgnoreConditions.POST
        )

        submission_job_operation = _SubmissionJobOperation(
            self._generate_id(jobs, index=index),
            self.name,
            jobs,
            cmd=unevaluated_cmd,
            directives=submission_directives,
            eligible_operations=eligible_operations,
            operations_with_unmet_preconditions=operations_with_unmet_preconditions,
            operations_with_met_postconditions=operations_with_met_postconditions,
        )
        return submission_job_operation

    def _create_run_job_operations(
        self,
        entrypoint,
        default_directives,
        jobs,
        ignore_conditions=IgnoreConditions.NONE,
        index=0,
    ):
        """Create _JobOperation object(s) from the :class:`~.FlowGroup`.

        Yields a _JobOperation for each contained operation given proper
        conditions are met.

        Parameters
        ----------
        entrypoint : dict
            The path and executable, if applicable, to point to for execution.
        default_directives : dict
            The default directives to use for the operations. This is to allow
            for user-specified groups to inherit directives from
            ``default_directives``. If no defaults are desired, the argument
            must be explicitly set to an empty dictionary.
        jobs : tuple of :class:`~signac.contrib.job.Job`
            The jobs that the :class:`~._JobOperation` is based on.
        ignore_conditions : :class:`~.IgnoreConditions`
            Specify if preconditions and/or postconditions are to be ignored
            when determining eligibility check. The default is
            :class:`IgnoreConditions.NONE`.
        index : int
            Index for the :class:`~._JobOperation`. (Default value = 0)

        Returns
        -------
        Iterator[_JobOperation]
            Iterator of eligible instances of :class:`~._JobOperation`.

        """
        # Assuming all the jobs belong to the same FlowProject
        env = jobs[0]._project._environment
        for operation_name, operation in self.operations.items():
            if operation._eligible(jobs, ignore_conditions):
                directives = self._resolve_directives(
                    operation_name, default_directives, env
                )
                directives.evaluate(jobs)
                # Return an unevaluated command to make evaluation lazy and
                # reduce side effects in callable FlowCmdOperations.
                unevaluated_cmd = _cached_partial(
                    self._run_cmd,
                    entrypoint=entrypoint,
                    operation_name=operation_name,
                    operation=operation,
                    directives=directives,
                    jobs=jobs,
                )
                job_op = _JobOperation(
                    self._generate_id(jobs, operation_name, index=index),
                    operation_name,
                    jobs,
                    cmd=unevaluated_cmd,
                    directives=deepcopy(directives),
                )
                # Get the prefix, and if it's non-empty, set the fork directive
                # to True since we must launch a separate process. Override
                # the command directly.
                prefix = jobs[0]._project._environment.get_prefix(job_op)
                if prefix != "":
                    job_op.directives["fork"] = True
                    job_op._cmd = f"{prefix} {job_op.cmd}"
                yield job_op

    def _get_submission_directives(self, default_directives, jobs):
        """Get the combined resources for submission.

        No checks are done to mitigate inappropriate aggregation of operations.
        This can lead to poor utilization of computing resources.
        """
        env = jobs[0]._project._environment
        operation_names = list(self.operations.keys())
        # The first operation's directives are evaluated, then all other
        # operations' directives are applied as updates with aggregate=True
        directives = self._resolve_directives(
            operation_names[0], default_directives, env
        )
        for name in operation_names[1:]:
            # get directives for operation
            directives.update(
                self._resolve_directives(name, default_directives, env),
                aggregate=True,
                jobs=jobs,
            )
        return directives


class _FlowProjectClass(type):
    """Metaclass for the FlowProject class."""

    def __new__(metacls, name, bases, namespace):
        cls = type.__new__(metacls, name, bases, dict(namespace))

        # All operation functions are registered with the operation()
        # classmethod, which is intended to be used as a decorator function.
        # _OPERATION_FUNCTIONS is a list of tuples of the operation name and
        # the operation function. In addition, preconditions and postconditions
        # are registered with the class.

        cls._OPERATION_FUNCTIONS = []
        cls._OPERATION_PRE_CONDITIONS = defaultdict(list)
        cls._OPERATION_POST_CONDITIONS = defaultdict(list)

        # All label functions are registered with the label() classmethod,
        # which is intended to be used as decorator function. The
        # _LABEL_FUNCTIONS dict contains the function as key and the label name
        # as value, or None to use the default label name.
        cls._LABEL_FUNCTIONS = {}

        # Give the class a preconditions and post class that are aware of the
        # class they are in.
        cls.pre = cls._setup_pre_conditions_class(parent_class=cls)
        cls.post = cls._setup_post_conditions_class(parent_class=cls)

        # All groups are registered with the function returned by the
        # make_group classmethod. In contrast to operations and labels, the
        # make_group classmethod does not serve as the decorator, the functor
        # it returns does. The _GROUPS list records the groups created and
        # their passed parameters for later initialization. The _GROUP_NAMES
        # set stores whether a group name has already been used.
        cls._GROUPS = []
        cls._GROUP_NAMES = set()

        return cls

    @staticmethod
    def _setup_pre_conditions_class(parent_class):
        class pre(_condition):
            """Define and evaluate preconditions for operations.

            A precondition is a function accepting one or more jobs as
            positional arguments (``*jobs``) that must evaluate to True for
            this operation to be eligible for execution. For example:

            .. code-block:: python

                @Project.operation
                @Project.pre(lambda job: not job.doc.get('hello'))
                def hello(job):
                    print('hello', job)
                    job.doc.hello = True

            The *hello* operation would only execute if the 'hello' key in the
            job document does not evaluate to True.

            An optional tag may be associated with the condition. These tags
            are used by :meth:`~.detect_operation_graph` when comparing
            conditions for equality. The tag defaults to the bytecode of the
            function.
            """

            _parent_class = parent_class

            def __call__(self, func):
                operation_functions = [
                    operation[1]
                    for operation in self._parent_class._collect_operations()
                ]
                if self.condition in operation_functions:
                    raise ValueError(
                        "Operation functions cannot be used as preconditions."
                    )
                self._parent_class._OPERATION_PRE_CONDITIONS[func].insert(
                    0, self.condition
                )
                return func

            @classmethod
            def copy_from(cls, *other_funcs):
                """Copy preconditions from other operation(s).

                True if and only if all pre conditions of other operation
                function(s) are met.
                """
                return cls(
                    _create_all_metacondition(
                        cls._parent_class._collect_pre_conditions(), *other_funcs
                    )
                )

            @classmethod
            def after(cls, *other_funcs):
                """Precondition to run an operation after other operations.

                True if and only if all postconditions of other operation
                function(s) are met.
                """
                operation_functions = [
                    operation[1]
                    for operation in cls._parent_class._collect_operations()
                ]
                if not all(
                    condition in operation_functions for condition in other_funcs
                ):
                    raise ValueError("The arguments to pre.after must be operations.")
                return cls(
                    _create_all_metacondition(
                        cls._parent_class._collect_post_conditions(), *other_funcs
                    )
                )

        return pre

    @staticmethod
    def _setup_post_conditions_class(parent_class):
        class post(_condition):
            """Define and evaluate postconditions for operations.

            A postcondition is a function accepting one or more jobs as
            positional arguments (``*jobs``) that must evaluate to True for
            this operation to be considered complete. For example:

            .. code-block:: python

                @Project.operation
                @Project.post(lambda job: job.doc.get('bye'))
                def bye(job):
                    print('bye', job)
                    job.doc.bye = True

            The *bye* operation would be considered complete and therefore no
            longer eligible for execution once the 'bye' key in the job
            document evaluates to True.

            An optional tag may be associated with the condition. These tags
            are used by :meth:`~.detect_operation_graph` when comparing
            conditions for equality. The tag defaults to the bytecode of the
            function.
            """

            _parent_class = parent_class

            def __call__(self, func):
                operation_functions = [
                    operation[1]
                    for operation in self._parent_class._collect_operations()
                ]
                if self.condition in operation_functions:
                    raise ValueError(
                        "Operation functions cannot be used as postconditions."
                    )
                self._parent_class._OPERATION_POST_CONDITIONS[func].insert(
                    0, self.condition
                )
                return func

            @classmethod
            def copy_from(cls, *other_funcs):
                """Copy postconditions from other operation(s).

                True if and only if all postconditions of other operation
                function(s) are met.
                """
                return cls(
                    _create_all_metacondition(
                        cls._parent_class._collect_post_conditions(), *other_funcs
                    )
                )

        return post


class FlowProject(signac.contrib.Project, metaclass=_FlowProjectClass):
    """A signac project class specialized for workflow management.

    This class is used to define, execute, and submit workflows based on
    operations and conditions.

    Users typically interact with this class through its command line interface.

    This is a typical example of how to use this class:

    .. code-block:: python

        @FlowProject.operation
        def hello(job):
            print('hello', job)

        FlowProject().main()

    Parameters
    ----------
    config : :class:`signac.contrib.project._ProjectConfig`
        A signac configuration, defaults to the configuration loaded
        from the current directory.
    environment : :class:`flow.environment.ComputeEnvironment`
        An environment to use for scheduler submission. If ``None``, the
        environment is automatically identified. The default is ``None``.
    entrypoint : dict
        A dictionary with two possible keys: ``'executable'`` and ``'path'``.
        The path represents the location of the script file (the
        script file must call :meth:`FlowProject.main`). The executable
        represents the location of the Python interpreter used for the
        execution of :class:`~.BaseFlowOperation` that are Python functions.

    """

    def __init__(self, config=None, environment=None, entrypoint=None):
        super().__init__(config=config)

        # Associate this class with a compute environment.
        self._environment = environment or get_environment()

        # Assign variables that give script location information
        self._entrypoint = {} if entrypoint is None else entrypoint

        # The standard local template directory is a directory called 'templates' within
        # the project root directory. This directory may be specified with the 'template_dir'
        # configuration variable.
        self._template_dir = os.path.join(
            self.root_directory(), self._config.get("template_dir", "templates")
        )
        self._template_environment_ = {}

        # Register all label functions with this project instance.
        self._label_functions = {}
        self._register_labels()

        # Register all operation functions with this project instance.
        self._operations = {}
        self._register_operations()

        # Register all groups with this project instance.
        self._groups = {}
        self._aggregator_per_group = {}
        self._register_groups()

        # Register all aggregates which are created for this project
        self._stored_aggregates = {}
        self._register_aggregates()

    def _setup_template_environment(self):
        """Set up the jinja2 template environment.

        The templating system is used to generate templated scripts for the
        script() and _submit_operations() / submit() function and the
        corresponding command line subcommands.
        """
        if self._config.get("flow") and self._config["flow"].get("environment_modules"):
            envs = self._config["flow"].as_list("environment_modules")
        else:
            envs = []

        # Templates are searched in the local template directory first, then in additionally
        # installed packages, then in the main package 'templates' directory.
        extra_packages = []
        for env in envs:
            try:
                extra_packages.append(jinja2.PackageLoader(env, "templates"))
            except ImportError as error:
                logger.warning(f"Unable to load template from package '{error.name}'.")

        load_envs = (
            [jinja2.FileSystemLoader(self._template_dir)]
            + extra_packages
            + [jinja2.PackageLoader("flow", "templates")]
        )

        template_environment = jinja2.Environment(
            loader=jinja2.ChoiceLoader(load_envs),
            trim_blocks=True,
            extensions=[TemplateError],
        )

        # Setup standard filters that can be used to format context variables.
        template_environment.filters[
            "format_timedelta"
        ] = template_filters.format_timedelta
        template_environment.filters["identical"] = template_filters.identical
        template_environment.filters["with_np_offset"] = template_filters.with_np_offset
        template_environment.filters["calc_tasks"] = template_filters.calc_tasks
        template_environment.filters["calc_num_nodes"] = template_filters.calc_num_nodes
        template_environment.filters[
            "check_utilization"
        ] = template_filters.check_utilization
        template_environment.filters[
            "homogeneous_openmp_mpi_config"
        ] = template_filters.homogeneous_openmp_mpi_config
        template_environment.filters["get_config_value"] = flow_config.get_config_value
        template_environment.filters[
            "require_config_value"
        ] = flow_config.require_config_value
        template_environment.filters[
            "get_account_name"
        ] = template_filters.get_account_name
        template_environment.filters["print_warning"] = template_filters.print_warning
        if "max" not in template_environment.filters:  # for jinja2 < 2.10
            template_environment.filters["max"] = max
        if "min" not in template_environment.filters:  # for jinja2 < 2.10
            template_environment.filters["min"] = min

        return template_environment

    def _template_environment(self, environment=None):
        if environment is None:
            environment = self._environment
        if environment not in self._template_environment_:
            template_environment = self._setup_template_environment()

            # Add environment-specific custom filters:
            for name, member in inspect.getmembers(environment):
                if getattr(member, "_flow_template_filter", False):
                    template_environment.filters[name] = member

            self._template_environment_[environment] = template_environment
        return self._template_environment_[environment]

    def _get_standard_template_context(self):
        """Return the standard templating context for run and submission scripts."""
        context = {}
        context["project"] = self
        return context

    def _show_template_help_and_exit(self, template_environment, context):
        """Print all context variables and filters to screen and exit."""
        from textwrap import TextWrapper

        wrapper = TextWrapper(width=90, break_long_words=False)
        print(
            TEMPLATE_HELP.format(
                template_dir=self._template_dir,
                template_vars="\n".join(wrapper.wrap(", ".join(sorted(context)))),
                filters="\n".join(
                    wrapper.wrap(", ".join(sorted(template_environment.filters)))
                ),
            )
        )
        sys.exit(2)

    @classmethod
    def label(cls, label_name_or_func=None):
        """Designate a function as a label function for this class.

        For example, we can define a label function like this:

        .. code-block:: python

            @FlowProject.label
            def foo_label(job):
                if job.document.get('foo', False):
                    return 'foo-label-text'

        The ``foo-label-text`` label will now show up in the status view for
        each job, where the ``foo`` key evaluates true.

        If the label functions returns any type other than ``str``, the label
        name will be the name of the function if and only if the return value
        evaluates to ``True``, for example:

        .. code-block:: python

            @FlowProject.label
            def foo_label(job):
                return job.document.get('foo', False)

        Finally, specify a label name by providing it as the first argument
        to the ``label()`` decorator.

        Parameters
        ----------
        label_name_or_func : str or callable
            A label name or callable. (Default value = None)

        Returns
        -------
        callable
            A decorator for the label function.

        """
        if callable(label_name_or_func):
            # This handles the case where no label name is given, as in
            # @FlowProject.label. label_name_or_func is a function.
            cls._LABEL_FUNCTIONS[label_name_or_func] = None
            return label_name_or_func

        def label_func(func):
            # This handles the case where a label name is given, as in
            # @FlowProject.label("label_name"). label_name_or_func is a string.
            cls._LABEL_FUNCTIONS[func] = label_name_or_func
            return func

        return label_func

    def detect_operation_graph(self):
        """Determine the directed acyclic graph given by operation conditions.

        In general, executing a given operation registered with a FlowProject
        just involves checking the operation's preconditions and postconditions
        to determine eligibility. More generally, however, the preconditions
        and postconditions define a directed acyclic graph that governs the
        execution of all operations. Visualizing this graph can be useful for
        finding logic errors in the specified conditions, and having this graph
        computed also enables additional execution modes. For example, using
        this graph it is possible to determine exactly what operations need to
        be executed in order to make the operation eligible so that the task of
        executing all necessary operations can be automated.

        The graph is determined by iterating over all pairs of operations and
        checking for equality of preconditions and postconditions. The
        algorithm builds an adjacency matrix based on whether the preconditions
        for one operation match the postconditions for another. The comparison
        of operations is conservative; by default, conditions must be composed
        of identical code to be identified as equal (technically, they must
        have equivalent bytecode, i.e. ``cond1.__code__.co_code ==
        cond2.__code__.co_code``). Users can specify that conditions should be
        treated as equal by providing tags to the operations.

        Given a :class:`~.FlowProject` subclass defined in a module
        ``project.py``, the output graph could be visualized using Matplotlib
        and NetworkX with the following code:

        .. code-block:: python

            import numpy as np
            import networkx as nx
            from matplotlib import pyplot as plt

            from project import Project

            project = Project()
            ops = project.operations.keys()
            adj = np.asarray(project.detect_operation_graph())

            plt.figure()
            g = nx.DiGraph(adj)
            pos = nx.spring_layout(g)
            nx.draw(g, pos)
            nx.draw_networkx_labels(
                g, pos,
                labels={key: name for (key, name) in
                        zip(range(len(ops)), [o for o in ops])})

            plt.show()

        Returns
        -------
        list of lists of int
            The adjacency matrix of operation dependencies. A zero indicates no
            dependency, and a 1 indicates dependency. This can be converted to
            a graph using NetworkX.

        Raises
        ------
        :class:`RuntimeError`
            If a condition does not have a tag. This can occur when using
            ``functools.partial``, and a manually specified condition tag has
            not been set.

        """

        def to_callbacks(conditions):
            """Get the actual callables associated with FlowConditions."""
            return [condition._callback for condition in conditions]

        def unpack_conditions(condition_functions):
            """Create a set of callbacks from condition functions.

            Metaconditions in the list of condition functions are reduced into
            the functions that they are composed of. All condition functions
            must have a `_flow_tag` attribute defined.
            """
            callbacks = set()
            for condition_function in condition_functions:
                # The condition function may not have a __name__ attribute in
                # cases where functools is used for condition creation.
                if (
                    hasattr(condition_function, "__name__")
                    and condition_function.__name__ == "_flow_metacondition"
                ):
                    callbacks = callbacks.union(
                        unpack_conditions(condition_function._composed_of)
                    )
                else:
                    if condition_function._flow_tag is None:
                        raise RuntimeError(
                            f"Condition {condition_function} was not tagged. "
                            "To create a graph, ensure each base condition "
                            "has a ``__code__`` attribute or manually "
                            "specified tag."
                        )
                    callbacks.add(condition_function._flow_tag)

            return callbacks

        operations = list(self.operations.values())
        mat = [[0 for _ in range(len(operations))] for _ in range(len(operations))]

        for i, operation_i in enumerate(operations):
            for j, operation_j in enumerate(operations[i:]):
                postconditions_i = unpack_conditions(
                    to_callbacks(operation_i._postconditions)
                )
                postconditions_j = unpack_conditions(
                    to_callbacks(operation_j._postconditions)
                )
                preconditions_i = unpack_conditions(
                    to_callbacks(operation_i._preconditions)
                )
                preconditions_j = unpack_conditions(
                    to_callbacks(operation_j._preconditions)
                )
                if postconditions_i.intersection(preconditions_j):
                    mat[i][j + i] = 1
                elif preconditions_i.intersection(postconditions_j):
                    mat[j + i][i] = 1
        return mat

    def _register_class_labels(self):
        """Register all label functions which are part of the class definition.

        To register a class method or function as a label function, use the
        :meth:`~.FlowProject.label()` class method as a decorator.
        """

        def predicate(member):
            return inspect.ismethod(member) or inspect.isfunction(member)

        class_label_functions = {}
        for name, function in inspect.getmembers(type(self), predicate=predicate):
            if _is_label_func(function):
                class_label_functions[name] = function

        for name in sorted(class_label_functions):
            self._label_functions[class_label_functions[name]] = None

    def _register_labels(self):
        """Register all label functions registered with this class and its parent classes."""
        self._register_class_labels()

        for cls in type(self).__mro__:
            self._label_functions.update(getattr(cls, "_LABEL_FUNCTIONS", {}))

    ALIASES = {
        str(status).replace("JobStatus.", ""): symbol
        for status, symbol in _FMT_SCHEDULER_STATUS.items()
        if status != JobStatus.placeholder
    }
    """Default aliases used within the status output."""

    @classmethod
    def _alias(cls, name):
        """Use alias if specified.

        Parameters
        ----------
        name : str
            Long name to abbreviate.

        Returns
        -------
        str
            Abbreviation if it exists, otherwise the input name.

        """
        try:
            return abbreviate(name, cls.ALIASES.get(name, name))
        except TypeError:
            return name

    def _fn_bundle(self, bundle_id):
        """Return the canonical name to store bundle information."""
        return os.path.join(self.root_directory(), ".bundles", bundle_id)

    def _store_bundled(self, operations):
        """Store operation-ids as part of a bundle and return bundle id.

        The operation identifiers are stored in a text file whose name is
        determined by the _fn_bundle() method. This may be used to identify
        the status of individual operations from the bundle id. A single
        operation will not be stored, but instead the operation's id is
        directly returned.

        Parameters
        ----------
        operations : A sequence of instances of :class:`._JobOperation`
            The operations to bundle.

        Returns
        -------
        str
            The bundle id.

        """
        if len(operations) == 1:
            return operations[0].id
        sep = getattr(self._environment, "JOB_ID_SEPARATOR", "/")
        _id = sha1(".".join(op.id for op in operations).encode("utf-8")).hexdigest()
        bundle_id = f"{self}{sep}bundle{sep}{_id}"
        fn_bundle = self._fn_bundle(bundle_id)
        os.makedirs(os.path.dirname(fn_bundle), exist_ok=True)
        with open(fn_bundle, "w") as file:
            for operation in operations:
                file.write(operation.id + "\n")
        return bundle_id

    def _expand_bundled_jobs(self, scheduler_jobs):
        """Expand jobs which were submitted as part of a bundle."""
        sep = getattr(self._environment, "JOB_ID_SEPARATOR", "/")
        bundle_prefix = f"{self}{sep}bundle{sep}"
        if scheduler_jobs is None:
            return
        for job in scheduler_jobs:
            if job.name().startswith(bundle_prefix):
                with open(self._fn_bundle(job.name())) as file:
                    for line in file:
                        yield ClusterJob(line.strip(), job.status())
            else:
                yield job

    def scheduler_jobs(self, scheduler):
        """Fetch jobs from the scheduler.

        This function will fetch all scheduler jobs from the scheduler and also
        expand bundled jobs automatically.

        However, this function will not automatically filter scheduler jobs
        which are not associated with this project.

        Parameters
        ----------
        scheduler : :class:`~.Scheduler`
            The scheduler instance.

        Yields
        ------
        :class:`~.ClusterJob`:
            All cluster jobs fetched from the scheduler.

        """
        yield from self._expand_bundled_jobs(scheduler.jobs())

    def _get_operations_status(self, jobs, cached_status):
        """Return a dict with information about job-operations for this aggregate.

        Parameters
        ----------
        jobs : :class:`~signac.contrib.job.Job` or aggregate of jobs
            The signac job or aggregate.
        cached_status : dict
            Dictionary of cached status information. The keys are uniquely
            generated ids for each group and job. The values are instances of
            :class:`~.JobStatus`.

        Yields
        ------
        str
            Operation name.
        dict
            Operation status dictionary.

        """
        starting_dict = functools.partial(dict, scheduler_status=JobStatus.unknown)
        status_dict = defaultdict(starting_dict)
        operation_names = list(self.operations.keys())
        groups = [self._groups[name] for name in operation_names]
        for group in groups:
            if get_aggregate_id(jobs) in self._get_aggregate_store(group.name):
                completed = group._complete(jobs)
                eligible = not completed and group._eligible(jobs)
                scheduler_status = cached_status.get(
                    group._generate_id(jobs), JobStatus.unknown
                )
                for operation in group.operations:
                    if scheduler_status >= status_dict[operation]["scheduler_status"]:
                        status_dict[operation] = {
                            "scheduler_status": scheduler_status,
                            "eligible": eligible,
                            "completed": completed,
                        }

        yield from sorted(status_dict.items())

    def get_job_status(self, job, ignore_errors=False, cached_status=None):
        """Return status information about a job.

        Parameters
        ----------
        job : :class:`~signac.contrib.job.Job`
            The signac job.
        ignore_errors : bool
            Whether to ignore exceptions raised during status check. (Default value = False)
        cached_status : dict
            Dictionary of cached status information. The keys are uniquely
            generated ids for each group and job. The values are instances of
            :class:`~.JobStatus`. (Default value = None)

        Returns
        -------
        dict
            A dictionary containing job status for all jobs.

        """
        # TODO: Add support for aggregates for this method.
        result = {}
        result["job_id"] = str(job)
        try:
            if cached_status is None:
                try:
                    cached_status = self.document["_status"]._as_dict()
                except KeyError:
                    cached_status = {}
            result["operations"] = dict(
                self._get_operations_status((job,), cached_status)
            )
            result["_operations_error"] = None
        except Exception as error:
            logger.debug(
                "Error while getting operations status for job '%s': '%s'.", job, error
            )
            if ignore_errors:
                result["operations"] = {}
                result["_operations_error"] = str(error)
            else:
                raise
        try:
            result["labels"] = sorted(set(self.labels(job)))
            result["_labels_error"] = None
        except Exception as error:
            logger.debug(
                "Error while determining labels for job '%s': '%s'.", job, error
            )
            if ignore_errors:
                result["labels"] = []
                result["_labels_error"] = str(error)
            else:
                raise
        return result

    def _fetch_scheduler_status(self, jobs=None, file=None, ignore_errors=False):
        """Update the status docs.

        Parameters
        ----------
        jobs : sequence of :class:`~signac.contrib.job.Job` or aggregates of jobs
            The signac job or aggregate. (Default value = None)
        file : file-like object
            File where status information is printed. If ``None``,
            ``sys.stderr`` is used. The default is ``None``.
        ignore_errors : bool
            Whether to ignore exceptions raised during status check. (Default value = False)

        """
        if file is None:
            file = sys.stderr
        try:
            scheduler = self._environment.get_scheduler()

            self.document.setdefault("_status", {})
            scheduler_info = {
                scheduler_job.name(): scheduler_job.status()
                for scheduler_job in self.scheduler_jobs(scheduler)
            }
            status = {}
            print("Query scheduler...", file=file)
            for group in tqdm(
                self._groups.values(),
                desc="Fetching operation status",
                total=len(self._groups),
                file=file,
            ):
                aggregate_store = self._get_aggregate_store(group.name)
                for aggregate in tqdm(
                    aggregate_store.values(),
                    total=len(aggregate_store),
                    desc="Fetching aggregate info for aggregate",
                    leave=False,
                    file=file,
                ):
                    if self._is_selected_aggregate(aggregate, jobs):
                        submit_id = group._generate_id(aggregate)
                        status[submit_id] = int(
                            scheduler_info.get(submit_id, JobStatus.unknown)
                        )
            self.document._status.update(status)
        except NoSchedulerError:
            logger.debug("No scheduler available.")
        except RuntimeError as error:
            logger.warning("Error occurred while querying scheduler: '%s'.", error)
            if not ignore_errors:
                raise
        else:
            logger.info("Updated job status cache.")

    def _get_group_status(self, group_name, ignore_errors=False, cached_status=None):
        """Return status information about a group.

        Status information is fetched for all jobs/aggregates associated with
        this group and returned as a dict.

        Parameters
        ----------
        group_name : str
            Group name.
        ignore_errors : bool
            Whether to ignore exceptions raised during status check. (Default value = False)
        cached_status : dict
            Dictionary of cached status information. The keys are uniquely
            generated ids for each group and job. The values are instances of
            :class:`~.JobStatus`. (Default value = None)

        Returns
        -------
        dict
            A dictionary containing job status for all jobs.

        """
        group = self._groups[group_name]
        status_dict = {}
        errors = {}
        aggregate_store = self._get_aggregate_store(group.name)
        for aggregate_id, aggregate in tqdm(
            aggregate_store.items(),
            desc=f"Collecting aggregate status info for operation {group.name}",
            total=len(aggregate_store),
            leave=False,
        ):
            errors.setdefault(aggregate_id, "")
            scheduler_status = JobStatus.unknown
            completed = False
            eligible = False
            try:
                job_op_id = group._generate_id(aggregate)
                scheduler_status = cached_status.get(job_op_id, JobStatus.unknown)
                completed = group._complete(aggregate)
                eligible = not completed and group._eligible(aggregate)
            except Exception as error:
                msg = (
                    "Error while getting operation status for aggregate "
                    f"'{aggregate_id}': '{error}'."
                )
                logger.debug(msg)
                if ignore_errors:
                    errors[aggregate_id] += str(error) + "\n"
                else:
                    raise
            finally:
                status_dict[aggregate_id] = {
                    "scheduler_status": scheduler_status,
                    "eligible": eligible,
                    "completed": completed,
                }

        return {
            "operation_name": group_name,
            "job_status_details": status_dict,
            "_operation_error_per_job": errors,
        }

    def _get_job_labels(self, job, ignore_errors=False):
        """Return a dict with information about the labels of a job.

        Parameters
        ----------
        job : :class:`signac.contrib.job.Job`
            Job handle.
        ignore_errors : bool
            Whether to ignore errors raised while fetching labels. (Default value = False)

        Returns
        -------
        dict
            Dictionary with keys ``job_id``, ``labels``, and ``_labels_error``.

        """
        result = {}
        result["job_id"] = str(job)
        try:
            result["labels"] = sorted(set(self.labels(job)))
        except Exception as error:
            logger.debug(
                "Error while determining labels for job '%s': '%s'.", job, error
            )
            if ignore_errors:
                result["labels"] = []
                result["_labels_error"] = str(error)
            else:
                raise
        else:
            result["_labels_error"] = None
        return result

    def _fetch_status(
        self,
        aggregates,
        distinct_jobs,
        err,
        ignore_errors,
        status_parallelization="thread",
    ):
        """Fetch status for the provided aggregates / jobs.

        Parameters
        ----------
        aggregates : list
            The aggregates for which a user requested to fetch status.
        distinct_jobs : list of :class:`~signac.contrib.job.Job`
            Distinct jobs fetched from the ids provided in the ``jobs``
            argument.  This is used for fetching labels for a job because a
            label is not associated with an aggregate.
        err : file-like object
            File where status information is printed.
        ignore_errors : bool
            Fetch status even if querying the scheduler fails.
        status_parallelization : str
            Parallelization mode for fetching the status. By default, thread
            parallelism is used.

        Returns
        -------
        list
            A list of dictionaries containing job ids,
            operations, labels, and any errors caught.

        """
        # The argument status_parallelization is used so that _fetch_status method
        # gets to know whether the deprecated argument no_parallelization passed
        # while calling print_status is True or False. This can also be done by
        # setting self.config['flow']['status_parallelization']='none' if the argument
        # is True. But the later functionality will last the rest of the session but in order
        # to do proper deprecation, it is not required for now.

        # Update the project's status cache
        self._fetch_scheduler_status(aggregates, err, ignore_errors)
        # Get status dict for all selected aggregates
        try:
            cached_status = self.document["_status"]._as_dict()
        except KeyError:
            cached_status = {}

        get_job_labels = functools.partial(
            self._get_job_labels, ignore_errors=ignore_errors
        )
        get_group_status = functools.partial(
            self._get_group_status,
            ignore_errors=ignore_errors,
            cached_status=cached_status,
        )

        operation_names = list(self.operations.keys())

        with self._potentially_buffered():
            try:
                if status_parallelization == "thread":
                    with contextlib.closing(ThreadPool()) as pool:
                        # First attempt at parallelized status determination.
                        # This may fail on systems that don't allow threads.
                        label_results = list(
                            tqdm(
                                iterable=pool.imap(get_job_labels, distinct_jobs),
                                desc="Collecting job label info",
                                total=len(distinct_jobs),
                                file=err,
                            )
                        )
                        group_results = list(
                            tqdm(
                                iterable=pool.imap(get_group_status, operation_names),
                                desc="Collecting operation status",
                                total=len(operation_names),
                                file=err,
                            )
                        )
                elif status_parallelization == "process":
                    with contextlib.closing(Pool()) as pool:
                        try:
                            (
                                label_results,
                                group_results,
                            ) = self._fetch_status_in_parallel(
                                pool,
                                distinct_jobs,
                                operation_names,
                                ignore_errors,
                                cached_status,
                            )
                        except self._PickleError as error:
                            raise RuntimeError(
                                "Unable to parallelize execution due to a pickling "
                                f"error: {error}."
                            )
                        label_results = list(
                            tqdm(
                                iterable=label_results,
                                desc="Collecting job label info",
                                total=len(distinct_jobs),
                                file=err,
                            )
                        )
                        group_results = list(
                            tqdm(
                                iterable=group_results,
                                desc="Collecting operation status",
                                total=len(operation_names),
                                file=err,
                            )
                        )
                elif status_parallelization == "none":
                    label_results = list(
                        tqdm(
                            iterable=map(get_job_labels, distinct_jobs),
                            desc="Collecting job label info",
                            total=len(distinct_jobs),
                            file=err,
                        )
                    )
                    group_results = list(
                        tqdm(
                            iterable=map(get_group_status, operation_names),
                            desc="Collecting operation status",
                            total=len(operation_names),
                            file=err,
                        )
                    )
                else:
                    raise RuntimeError(
                        "Configuration value status_parallelization is invalid. "
                        "Valid choices are 'thread', 'parallel', or 'none'."
                    )
            except RuntimeError as error:
                if "can't start new thread" not in error.args:
                    raise  # unrelated error

                def print_status(iterable, fetch_status, description):
                    num_itr = len(iterable)
                    results = []
                    start_time = time.time()
                    i = 0
                    for i, itr in enumerate(iterable):
                        results.append(fetch_status(itr))
                        # The status interval 0.2 seconds is used since we expect the
                        # status for an aggregate to be fetched within that interval
                        if time.time() - start_time > 0.2:
                            tqdm.update(
                                f"{description}: {i+1}/{num_itr}", end="\r", file=err
                            )
                            start_time = time.time()
                    # Always print the completed progressbar.
                    print(f"{description}: {i+1}/{num_itr}", file=err)
                    return results

                label_results = print_status(
                    distinct_jobs, get_job_labels, "Collecting job label info"
                )
                group_results = print_status(
                    operation_names, get_group_status, "Collecting operation status"
                )

        results = []
        index = {}
        for i, job in enumerate(distinct_jobs):
            results_entry = {}
            results_entry["job_id"] = job.get_id()
            results_entry["operations"] = {}
            results_entry["_operations_error"] = None
            results_entry["labels"] = []
            results_entry["_labels_error"] = None
            results.append(results_entry)
            index[job.get_id()] = i

        for op_result in group_results:
            for aggregate_id, aggregate_status in op_result[
                "job_status_details"
            ].items():
                aggregate = self._get_aggregate_from_id(aggregate_id)
                if not self._is_selected_aggregate(aggregate, aggregates):
                    continue
                error = op_result["_operation_error_per_job"].get(aggregate_id, None)
                for job in aggregate:
                    results[index[job.get_id()]]["operations"][
                        op_result["operation_name"]
                    ] = aggregate_status
                    results[index[job.get_id()]]["_operations_error"] = error

        for label_result in label_results:
            results[index[label_result["job_id"]]]["labels"] = label_result["labels"]
            results[index[label_result["job_id"]]]["_labels_error"] = label_result[
                "_labels_error"
            ]

        return results

    def _fetch_status_in_parallel(
        self, pool, jobs, groups, ignore_errors, cached_status
    ):
        try:
            serialized_project = cloudpickle.dumps(self)
            serialized_tasks_labels = [
                (
                    cloudpickle.loads,
                    serialized_project,
                    job.get_id(),
                    ignore_errors,
                    "fetch_labels",
                )
                for job in jobs
            ]
            serialized_tasks_groups = [
                (
                    cloudpickle.loads,
                    serialized_project,
                    group,
                    ignore_errors,
                    cached_status,
                    "fetch_status",
                )
                for group in groups
            ]
        except Exception as error:  # Masking all errors since they must be pickling related.
            raise self._PickleError(error)

        label_results = pool.starmap(_serializer, serialized_tasks_labels)
        group_results = pool.starmap(_serializer, serialized_tasks_groups)

        return label_results, group_results

    PRINT_STATUS_ALL_VARYING_PARAMETERS = True
    """This constant can be used to signal that the print_status() method is supposed
    to automatically show all varying parameters."""

    def print_status(
        self,
        jobs=None,
        overview=True,
        overview_max_lines=None,
        detailed=False,
        parameters=None,
        param_max_width=None,
        expand=False,
        all_ops=False,
        only_incomplete=False,
        dump_json=False,
        unroll=True,
        compact=False,
        pretty=False,
        file=None,
        err=None,
        ignore_errors=False,
        no_parallelize=False,
        template=None,
        profile=False,
        eligible_jobs_max_lines=None,
        output_format="terminal",
    ):
        """Print the status of the project.

        Parameters
        ----------
        jobs : Sequence of instances of :class:`~signac.contrib.job.Job`.
            Only execute operations for the given jobs, or all if the argument
            is omitted. (Default value = None)
        overview : bool
            Aggregate an overview of the project' status. (Default value = True)
        overview_max_lines : int
            Limit the number of overview lines. (Default value = None)
        detailed : bool
            Print a detailed status of each job. (Default value = False)
        parameters : list of str
            Print the value of the specified parameters. (Default value = None)
        param_max_width : int
            Limit the number of characters of parameter columns. (Default value = None)
        expand : bool
            Present labels and operations in two separate tables. (Default value = False)
        all_ops : bool
            Include operations that are not eligible to run. (Default value = False)
        only_incomplete : bool
            Only show jobs that have eligible operations. (Default value = False)
        dump_json : bool
            Output the data as JSON instead of printing the formatted output.
            (Default value = False)
        unroll : bool
            Separate columns for jobs and the corresponding operations. (Default value = True)
        compact : bool
            Print a compact version of the output. (Default value = False)
        pretty : bool
            Prettify the output. (Default value = False)
        file : str
            Redirect all output to this file, defaults to sys.stdout.
        err : str
            Redirect all error output to this file, defaults to sys.stderr.
        ignore_errors : bool
            Print status even if querying the scheduler fails. (Default value = False)
        template : str
            User provided Jinja2 template file. (Default value = None)
        profile : bool
            Show profile result. (Default value = False)
        eligible_jobs_max_lines : int
            Limit the number of operations and its eligible job count printed
            in the overview. (Default value = None)
        output_format : str
            Status output format, supports:
            'terminal' (default), 'markdown' or 'html'.
        no_parallelize : bool
            Disable parallelization. (Default value = False)

        Returns
        -------
        :class:`~.Renderer`
            A Renderer class object that contains the rendered string.

        """
        if file is None:
            file = sys.stdout
        if err is None:
            err = sys.stderr

        aggregates = self._convert_aggregates_from_jobs(jobs)
        if aggregates is not None:
            # Fetch all the distinct jobs from all the jobs or aggregate passed by the user.
            distinct_jobs = set()
            for aggregate in aggregates:
                for job in aggregate:
                    distinct_jobs.add(job)
        else:
            distinct_jobs = self

        if eligible_jobs_max_lines is None:
            eligible_jobs_max_lines = flow_config.get_config_value(
                "eligible_jobs_max_lines"
            )

        if no_parallelize:
            print(
                "WARNING: "
                "The no_parallelize argument is deprecated as of 0.10 "
                "and will be removed in 0.12. "
                "Instead, set the status_parallelization configuration value to 'none'. "
                "In order to do this from the CLI, execute "
                "`signac config set flow.status_parallelization 'none'`\n",
                file=sys.stderr,
            )
            status_parallelization = "none"
        else:
            status_parallelization = self.config["flow"]["status_parallelization"]

        # initialize jinja2 template environment and necessary filters
        template_environment = self._template_environment()

        context = self._get_standard_template_context()

        # get job status information
        if profile:
            try:
                import pprofile
            except ImportError:
                raise ImportError(
                    "Profiling requires the pprofile package. "
                    "Install with `pip install pprofile`."
                )
            prof = pprofile.StatisticalProfile()

            fn_filter = [
                inspect.getfile(threading),
                inspect.getfile(multiprocessing),
                inspect.getfile(Pool),
                inspect.getfile(ThreadPool),
                inspect.getfile(tqdm),
            ]

            with prof(single=False):
                fetched_status = self._fetch_status(
                    aggregates,
                    distinct_jobs,
                    err,
                    ignore_errors,
                    status_parallelization,
                )

            prof._mergeFileTiming()

            # Unrestricted
            total_impact = 0
            hits = [
                hit
                for fn, file_timing in prof.merged_file_dict.items()
                if fn not in fn_filter
                for hit in file_timing.iterHits()
            ]
            sorted_hits = reversed(sorted(hits, key=lambda hit: hit[2]))
            total_num_hits = sum([hit[2] for hit in hits])

            profiling_results = ["# Profiling:\n"]

            profiling_results.extend(
                ["Rank Impact Code object", "---- ------ -----------"]
            )
            for i, (line, code, hits, duration) in enumerate(sorted_hits):
                impact = hits / total_num_hits
                total_impact += impact
                rank = i + 1
                profiling_results.append(
                    f"{rank:>4} {impact:>6.0%} {code.co_filename}:"
                    f"{code.co_firstlineno}:{code.co_name}"
                )
                if i > 10 or total_impact > 0.8:
                    break

            for module_fn in prof.merged_file_dict:
                if re.match(profile, module_fn):
                    file_timing = prof.merged_file_dict[module_fn]
                else:
                    continue

                total_hits = file_timing.getTotalHitCount()
                total_impact = 0

                profiling_results.append(f"\nHits by line for '{module_fn}':")
                profiling_results.append("-" * len(profiling_results[-1]))

                hits = list(sorted(file_timing.iterHits(), key=lambda h: 1 / h[2]))
                for line, code, hits, duration in hits:
                    impact = hits / total_hits
                    total_impact += impact
                    profiling_results.append(f"{module_fn}:{line} ({impact:2.0%}):")
                    try:
                        lines, start = inspect.getsourcelines(code)
                    except OSError:
                        continue
                    hits_ = [
                        file_timing.getHitStatsFor(line)[0]
                        for line in range(start, start + len(lines))
                    ]
                    profiling_results.extend(
                        [
                            f"{h:>5} {lineno:>4}: {l.rstrip()}"
                            for lineno, (l, h) in enumerate(zip(lines, hits_), start)
                        ]
                    )
                    profiling_results.append("")
                    if total_impact > 0.8:
                        break

            profiling_results.append(f"Total runtime: {int(prof.total_time)}s")
            if prof.total_time < 20:
                profiling_results.append(
                    "Warning: Profiler ran only for a short time, "
                    "results may be highly inaccurate."
                )

        else:
            fetched_status = self._fetch_status(
                aggregates, distinct_jobs, err, ignore_errors, status_parallelization
            )
            profiling_results = None

        operations_errors = {
            status_entry["_operations_error"] for status_entry in fetched_status
        }
        labels_errors = {
            status_entry["_labels_error"] for status_entry in fetched_status
        }
        errors = list(filter(None, operations_errors.union(labels_errors)))

        if errors:
            logger.warning(
                "Some job status updates did not succeed due to errors. Number "
                "of unique errors: %i. Use --debug to list all errors.",
                len(errors),
            )
            for i, error in enumerate(errors):
                logger.debug("Status update error #%i: '%s'", i + 1, error)

        if only_incomplete:
            # Remove jobs with no eligible operations from the status info

            def _incomplete(status_entry):
                return any(
                    operation["eligible"]
                    for operation in status_entry["operations"].values()
                )

            fetched_status = list(filter(_incomplete, fetched_status))

        statuses = {
            status_entry["job_id"]: status_entry for status_entry in fetched_status
        }

        # If the dump_json variable is set, just dump all status info
        # formatted in JSON to screen.
        if dump_json:
            print(json.dumps(statuses, indent=4), file=file)
            return None

        if overview:
            # get overview info:
            progress = defaultdict(int)
            for status in statuses.values():
                for label in status["labels"]:
                    progress[label] += 1
            progress_sorted = list(
                islice(
                    sorted(progress.items(), key=lambda x: (x[1], x[0]), reverse=True),
                    overview_max_lines,
                )
            )

        # Optionally expand parameters argument to all varying parameters.
        if parameters is self.PRINT_STATUS_ALL_VARYING_PARAMETERS:
            parameters = list(
                sorted(
                    {
                        key
                        for job in distinct_jobs
                        for key in job.sp.keys()
                        if len(
                            {_to_hashable(job.sp().get(key)) for job in distinct_jobs}
                        )
                        > 1
                    }
                )
            )

        if parameters:
            # get parameters info

            def _add_parameters(status):
                statepoint = self.open_job(id=status["job_id"]).statepoint()

                def dotted_get(mapping, key):
                    """Fetch a value from a nested mapping using a dotted key."""
                    if mapping is None:
                        return None
                    tokens = key.split(".")
                    if len(tokens) > 1:
                        return dotted_get(mapping.get(tokens[0]), ".".join(tokens[1:]))
                    return mapping.get(key)

                status["parameters"] = {}
                for parameter in parameters:
                    status["parameters"][parameter] = shorten(
                        str(self._alias(dotted_get(statepoint, parameter))),
                        param_max_width,
                    )

            for status in statuses.values():
                _add_parameters(status)

            for i, parameter in enumerate(parameters):
                parameters[i] = shorten(self._alias(str(parameter)), param_max_width)

        if detailed:
            # get detailed view info
            status_legend = " ".join(f"[{v}]:{k}" for k, v in self.ALIASES.items())

            if compact:
                num_operations = len(self._operations)

            if pretty:
                OPERATION_STATUS_SYMBOLS = {
                    "ineligible": "\u25cb",  # open circle
                    "eligible": "\u25cf",  # black circle
                    "active": "\u25b9",  # open triangle
                    "running": "\u25b8",  # black triangle
                    "completed": "\u2714",  # check mark
                }
                # Pretty (unicode) symbols denoting the execution status of operations.
            else:
                OPERATION_STATUS_SYMBOLS = {
                    "ineligible": "-",
                    "eligible": "+",
                    "active": "*",
                    "running": ">",
                    "completed": "X",
                }
                # ASCII symbols denoting the execution status of operations.
            operation_status_legend = " ".join(
                f"[{v}]:{k}" for k, v in OPERATION_STATUS_SYMBOLS.items()
            )

        context["jobs"] = list(statuses.values())
        context["overview"] = overview
        context["detailed"] = detailed
        context["all_ops"] = all_ops
        context["parameters"] = parameters
        context["compact"] = compact
        context["pretty"] = pretty
        context["unroll"] = unroll
        if overview:
            context["progress_sorted"] = progress_sorted
        if detailed:
            context["alias_bool"] = {True: "Y", False: "N"}
            context["scheduler_status_code"] = _FMT_SCHEDULER_STATUS
            context["status_legend"] = status_legend
            if compact:
                context["extra_num_operations"] = max(num_operations - 1, 0)
            if not unroll:
                context["operation_status_legend"] = operation_status_legend
                context["operation_status_symbols"] = OPERATION_STATUS_SYMBOLS

        def _add_placeholder_operation(job):
            job["operations"][""] = {
                "completed": False,
                "eligible": False,
                "scheduler_status": JobStatus.placeholder,
            }

        for job in context["jobs"]:
            has_eligible_ops = any([v["eligible"] for v in job["operations"].values()])
            if not has_eligible_ops and not context["all_ops"]:
                _add_placeholder_operation(job)

        op_counter = Counter()
        for job in context["jobs"]:
            for key, value in job["operations"].items():
                if key != "" and value["eligible"]:
                    op_counter[key] += 1
        context["op_counter"] = op_counter.most_common(eligible_jobs_max_lines)
        num_omitted_operations = len(op_counter) - len(context["op_counter"])
        if num_omitted_operations > 0:
            context["op_counter"].append(
                (f"[{num_omitted_operations} more operations omitted]", "")
            )

        status_renderer = StatusRenderer()

        # We have to make a deep copy of the template environment if we're
        # using a process Pool for parallelism. Somewhere in the process of
        # manually pickling and dispatching tasks to individual processes
        # Python's reference counter loses track of the environment and ends up
        # destructing the template environment. This causes subsequent calls to
        # print_status to fail (although _fetch_status calls will still
        # succeed).
        template_environment_copy = (
            deepcopy(template_environment)
            if status_parallelization == "process"
            else template_environment
        )
        render_output = status_renderer.render(
            template,
            template_environment_copy,
            context,
            detailed,
            expand,
            unroll,
            compact,
            output_format,
        )

        print(render_output, file=file)

        # Show profiling results (if enabled)
        if profiling_results:
            print("\n" + "\n".join(profiling_results), file=file)

        return status_renderer

    def _run_operations(
        self, operations=None, pretend=False, np=None, timeout=None, progress=False
    ):
        """Execute the next operations as specified by the project's workflow.

        See also: :meth:`~.run`.

        Parameters
        ----------
        operations : Sequence of instances of :class:`._JobOperation`
            The operations to execute (optional). (Default value = None)
        pretend : bool
            Do not actually execute the operations, but show the commands that
            would have been executed. (Default value = False)
        np : int
            The number of processors to use for each operation. (Default value = None)
        timeout : int
            An optional timeout for each operation in seconds after which
            execution will be cancelled. Use -1 to indicate no timeout (the
            default).
        progress : bool
            Show a progress bar during execution. (Default value = False)

        """
        if timeout is not None and timeout < 0:
            timeout = None
        if operations is None:
            operations = list(self._get_pending_operations())
        else:
            operations = list(operations)  # ensure list

        if np is None or np == 1 or pretend:
            if progress:
                operations = tqdm(operations)
            for operation in operations:
                self._execute_operation(operation, timeout, pretend)
        else:
            logger.debug("Parallelized execution of %i operation(s).", len(operations))
            with contextlib.closing(
                Pool(processes=cpu_count() if np < 0 else np)
            ) as pool:
                logger.debug(
                    "Parallelized execution of %i operation(s).", len(operations)
                )
                try:
                    self._run_operations_in_parallel(
                        pool, operations, progress, timeout
                    )
                except self._PickleError as error:
                    raise RuntimeError(
                        "Unable to parallelize execution due to a pickling "
                        f"error: {error}."
                    )

    @deprecated(deprecated_in="0.11", removed_in="0.13", current_version=__version__)
    def run_operations(
        self, operations=None, pretend=False, np=None, timeout=None, progress=False
    ):
        """Execute the next operations as specified by the project's workflow.

        See also: :meth:`~.run`

        Parameters
        ----------
        operations : Sequence of instances of :class:`~.JobOperation`
            The operations to execute (optional). (Default value = None)
        pretend : bool
            Do not actually execute the operations, but show the commands that
            would have been executed. (Default value = False)
        np : int
            The number of processors to use for each operation. (Default value = None)
        timeout : int
            An optional timeout for each operation in seconds after which
            execution will be cancelled. Use -1 to indicate no timeout (the
            default).
        progress : bool
            Show a progress bar during execution. (Default value = False)

        """
        return self._run_operations(operations, pretend, np, timeout, progress)

    class _PickleError(Exception):
        """Indicates a pickling error while trying to parallelize the execution of operations."""

    @staticmethod
    def _job_operation_to_tuple(operation):
        return (
            operation.id,
            operation.name,
            [job.get_id() for job in operation._jobs],
            operation.cmd,
            operation.directives,
        )

    def _job_operation_from_tuple(self, data):
        id, name, job_ids, cmd, directives = data
        jobs = tuple(self.open_job(id=job_id) for job_id in job_ids)
        all_directives = jobs[0]._project._environment._get_default_directives()
        all_directives.update(directives)
        return _JobOperation(id, name, jobs, cmd, all_directives)

    def _run_operations_in_parallel(self, pool, operations, progress, timeout):
        """Execute operations in parallel.

        This function executes the given list of operations with the provided
        process pool.

        Since pickling of the project instance is likely to fail, we manually
        pickle the project instance and the operations before submitting them
        to the process pool.
        """
        try:
            serialized_project = cloudpickle.dumps(self)
            serialized_tasks = [
                (
                    cloudpickle.loads,
                    serialized_project,
                    self._job_operation_to_tuple(operation),
                    "run_operations",
                )
                for operation in tqdm(
                    operations, desc="Serialize tasks", file=sys.stderr
                )
            ]
        except Exception as error:  # Masking all errors since they must be pickling related.
            raise self._PickleError(error)

        results = [pool.apply_async(_serializer, task) for task in serialized_tasks]
        for result in tqdm(results) if progress else results:
            result.get(timeout=timeout)

    def _execute_operation(self, operation, timeout=None, pretend=False):
        if pretend:
            print(operation.cmd)
            return None

        logger.info("Execute operation '%s'...", operation)
        # Check if we need to fork for operation execution...
        if (
            # The 'fork' directive was provided and evaluates to True:
            operation.directives.get("fork", False)
            # Separate process needed to cancel with timeout:
            or timeout is not None
            # The operation function is of an instance of FlowCmdOperation:
            or isinstance(self._operations[operation.name], FlowCmdOperation)
            # The specified executable is not the same as the interpreter instance:
            or operation.directives.get("executable", sys.executable) != sys.executable
        ):
            # ... need to fork:
            logger.debug(
                "Forking to execute operation '%s' with cmd '%s'.",
                operation,
                operation.cmd,
            )
            subprocess.run(operation.cmd, shell=True, timeout=timeout, check=True)
        else:
            # ... executing operation in interpreter process as function:
            logger.debug(
                "Executing operation '%s' with current interpreter process (%s).",
                operation,
                os.getpid(),
            )
            try:
                self._operations[operation.name](*operation._jobs)
            except Exception as error:
                assert len(operation._jobs) == 1
                raise UserOperationError(
                    f"An exception was raised during operation {operation.name} "
                    f"for job or aggregate with id {get_aggregate_id(operation._jobs)}."
                ) from error

    def _get_default_directives(self):
        return {
            name: self.groups[name].operation_directives.get(name, {})
            for name in self.operations
        }

    def run(
        self,
        jobs=None,
        names=None,
        pretend=False,
        np=None,
        timeout=None,
        num=None,
        num_passes=1,
        progress=False,
        order=None,
        ignore_conditions=IgnoreConditions.NONE,
    ):
        """Execute all pending operations for the given selection.

        This function will run in an infinite loop until all pending operations
        are executed, unless it reaches the maximum number of passes per
        operation or the maximum number of executions.

        By default there is no limit on the total number of executions, but a
        specific operation will only be executed once per job. This is to avoid
        accidental infinite loops when no or faulty postconditions are
        provided.

        Parameters
        ----------
        jobs : iterable of :class:`~signac.contrib.job.Job` or aggregates of jobs
            Only execute operations for the given jobs or aggregates of jobs,
            or all if the argument is omitted. (Default value = None)
        names : iterable of :class:`str`
            Only execute operations that are in the provided set of names, or
            all if the argument is omitted. (Default value = None)
        pretend : bool
            Do not actually execute the operations, but show the commands that
            would have been executed. (Default value = False)
        np : int
            Parallelize to the specified number of processors. Use -1 to
            parallelize to all available processing units. (Default value = None)
        timeout : int
            An optional timeout for each operation in seconds after which
            execution will be cancelled. Use -1 to indicate no timeout (the
            default).
        num : int
            The total number of operations that are executed will not exceed
            this argument if provided. (Default value = None)
        num_passes : int or None
            The total number of executions of one specific job-operation pair
            will not exceed this argument. The default is 1, there is no limit
            if this argument is None.
        progress : bool
            Show a progress bar during execution. (Default value = False)
        order : str, callable, or None
            Specify the order of operations. Possible values are:

            * 'none' or None (no specific order)
            * 'by-job' (operations are grouped by job)
            * 'by-op' (operations are grouped by operation)
            * 'cyclic' (order operations cyclic by job)
            * 'random' (shuffle the execution order randomly)
            * callable (a callable returning a comparison key for an
              operation used to sort operations)

            The default value is ``'none'``, which is equivalent to ``'by-op'``
            in the current implementation.

            .. note::

                Users are advised to not rely on a specific execution order as
                a substitute for defining the workflow in terms of
                preconditions and postconditions. However, a specific execution
                order may be more performant in cases where operations need to
                access and potentially lock shared resources.

        ignore_conditions : :class:`~.IgnoreConditions`
            Specify if preconditions and/or postconditions are to be ignored
            when determining eligibility. The default is
            :class:`IgnoreConditions.NONE`.

        """
        aggregates = self._convert_aggregates_from_jobs(jobs)

        # Get all matching FlowGroups
        if isinstance(names, str):
            raise ValueError(
                "The names argument of FlowProject.run() must be a sequence of strings, "
                "not a string."
            )
        if names is None:
            names = list(self.operations)

        flow_groups = self._gather_flow_groups(names)

        # Get default directives
        default_directives = self._get_default_directives()

        # Negative values for the execution limits, means 'no limit'.
        if num_passes and num_passes < 0:
            num_passes = None
        if num and num < 0:
            num = None

        if not isinstance(ignore_conditions, IgnoreConditions):
            raise ValueError(
                "The ignore_conditions argument of FlowProject.run() "
                "must be a member of class IgnoreConditions."
            )

        messages = []

        def log(msg, lvl=logging.INFO):
            messages.append((msg, lvl))

        reached_execution_limit = Event()

        def select(operation):
            if not self._is_selected_aggregate(operation._jobs, aggregates):
                return False

            if num is not None and select.total_execution_count >= num:
                reached_execution_limit.set()
                raise StopIteration  # Reached total number of executions

            # Check whether the operation was executed more than the total number of allowed
            # passes *per operation* (default=1).
            if (
                num_passes is not None
                and select.num_executions.get(operation, 0) >= num_passes
            ):
                log(
                    f"Operation '{operation}' exceeds max. # of allowed "
                    f"passes ({num_passes})."
                )

                # Warn if an operation has no postconditions set.
                has_post_conditions = len(
                    self.operations[operation.name]._postconditions
                )
                if not has_post_conditions:
                    log(
                        f"Operation '{operation.name}' has no postconditions!",
                        logging.WARNING,
                    )

                return False  # Reached maximum number of passes for this operation.

            # Increase execution counters for this operation.
            select.num_executions[operation] += 1
            select.total_execution_count += 1
            return True

        # Keep track of all executed job-operations; the number of executions
        # of each individual job-operation cannot exceed num_passes.
        select.num_executions = defaultdict(int)

        # Keep track of the total execution count, it may not exceed the value given by
        # num, if not None.
        # Note: We are not using sum(select.num_execution.values()) for efficiency.
        select.total_execution_count = 0

        for i_pass in count(1):
            if reached_execution_limit.is_set():
                logger.warning(
                    "Reached the maximum number of operations that can be executed, but "
                    "there are still operations pending."
                )
                break
            try:
                # Change groups to available run _JobOperation(s)
                with self._potentially_buffered():
                    operations = []
                    for flow_group in flow_groups:
                        for aggregate in self._get_aggregate_store(
                            flow_group.name
                        ).values():
                            operations.extend(
                                flow_group._create_run_job_operations(
                                    self._entrypoint,
                                    default_directives,
                                    aggregate,
                                    ignore_conditions,
                                )
                            )
                    operations = list(filter(select, operations))
            finally:
                if messages:
                    for msg, level in set(messages):
                        logger.log(level, msg)
                    del messages[:]  # clear
            if not operations:
                break  # No more pending operations or execution limits reached.

            def key_func_by_job(operation):
                # In order to group the aggregates in a by-job manner, we need
                # to first sort the aggregates using their aggregate id.
                return get_aggregate_id(operation._jobs)

            # Optionally re-order operations for execution if order argument is provided:
            if callable(order):
                operations = list(sorted(operations, key=order))
            elif order == "random":
                random.shuffle(operations)
            elif order in ("by-job", "cyclic"):
                groups = [
                    list(group)
                    for _, group in groupby(
                        sorted(operations, key=key_func_by_job), key=key_func_by_job
                    )
                ]
                if order == "cyclic":
                    operations = list(roundrobin(*groups))
                else:
                    operations = list(chain(*groups))
            elif order is None or order in ("none", "by-op"):
                pass  # by-op is the default order
            else:
                raise ValueError(
                    "Invalid value for the 'order' argument, valid arguments are "
                    "'none', 'by-op', 'by-job', 'cyclic', 'random', None, or a callable."
                )

            logger.info(
                "Executing %i operation(s) (Pass #%02d)...",
                len(operations),
                i_pass,
            )
            self._run_operations(
                operations, pretend=pretend, np=np, timeout=timeout, progress=progress
            )

    def _gather_flow_groups(self, names=None):
        r"""Grabs :class:`~.FlowGroup`\ s that match any of a set of names."""
        operations = {}
        # if no names are selected try all singleton groups
        if names is None:
            names = self._operations.keys()
        for name in names:
            if name in operations:
                continue
            groups = [
                group
                for gname, group in self.groups.items()
                if re.fullmatch(name, gname)
            ]
            if len(groups) > 0:
                for group in groups:
                    operations[group.name] = group
            else:
                continue
        operations = list(operations.values())
        if not FlowProject._verify_group_compatibility(operations):
            raise ValueError(
                "Cannot specify groups or operations that "
                "will be included twice when using the"
                " -o/--operation option."
            )
        return operations

    def _get_submission_operations(
        self,
        aggregates,
        default_directives,
        names=None,
        ignore_conditions=IgnoreConditions.NONE,
        ignore_conditions_on_execution=IgnoreConditions.NONE,
    ):
        r"""Grabs eligible :class:`~._JobOperation`\ s from :class:`~.FlowGroup`s."""
        for group in self._gather_flow_groups(names):
            for aggregate in self._get_aggregate_store(group.name).values():
                if (
                    group._eligible(aggregate, ignore_conditions)
                    and self._eligible_for_submission(group, aggregate)
                    and self._is_selected_aggregate(aggregate, aggregates)
                ):
                    yield group._create_submission_job_operation(
                        entrypoint=self._entrypoint,
                        default_directives=default_directives,
                        jobs=aggregate,
                        index=0,
                        ignore_conditions_on_execution=ignore_conditions_on_execution,
                    )

    def _get_pending_operations(
        self, jobs=None, operation_names=None, ignore_conditions=IgnoreConditions.NONE
    ):
        """Get all pending operations for the given selection."""
        assert not isinstance(operation_names, str)
        for operation in self._next_operations(jobs, ignore_conditions):
            # Return operations with names that match the provided list of
            # regular expressions, or all operations if no names are specified.
            if operation_names is None or any(
                re.fullmatch(operation_name, operation.name)
                for operation_name in operation_names
            ):
                yield operation

    @classmethod
    def _verify_group_compatibility(cls, groups):
        """Verify that all selected groups can be submitted together."""
        return all(a.isdisjoint(b) for a in groups for b in groups if a != b)

    def _aggregate_is_in_project(self, aggregate):
        """Verify that the aggregate belongs to this project."""
        return any(
            get_aggregate_id(aggregate) in aggregates
            for aggregates in self._stored_aggregates
        )

    @staticmethod
    def _is_selected_aggregate(aggregate, jobs):
        """Verify whether the aggregate is present in the provided jobs.

        Providing ``jobs=None`` indicates that no specific job is provided by
        the user and hence ``aggregate`` is eligible for further evaluation.

        Always returns True if jobs is None.
        """
        return (jobs is None) or (aggregate in jobs)

    def _get_aggregate_from_id(self, id):
        # Iterate over all the instances of stored aggregates and search for the
        # aggregate in those instances.
        for aggregate_store in self._stored_aggregates:
            try:
                # Assume the id exists and skip the __contains__ check for
                # performance. If the id doesn't exist in this aggregate_store,
                # it will raise an exception that can be ignored.
                return aggregate_store[id]
            except KeyError:
                pass
        # Raise error as didn't find the id in any of the stored objects
        raise LookupError(f"Did not find aggregate with id {id} in the project")

    def _convert_aggregates_from_jobs(self, jobs):
        # The jobs parameter in public methods like ``run``, ``submit``, ``status`` may
        # accept either a signac job or an aggregate. We convert that job / aggregate
        # (which may be of any type (e.g. list)) to an aggregate of type ``tuple``.
        if jobs is not None:
            # aggregates must be a set to prevent duplicate entries
            aggregates = set()
            for aggregate in jobs:
                # User can still pass signac jobs.
                if isinstance(aggregate, signac.contrib.job.Job):
                    if aggregate not in self:
                        raise LookupError(
                            f"Did not find job {aggregate} in the project"
                        )
                    aggregates.add((aggregate,))
                else:
                    try:
                        aggregate = tuple(aggregate)
                    except TypeError as error:
                        raise TypeError(
                            "Invalid jobs argument. Please provide a valid "
                            "signac job or aggregate of jobs."
                        ) from error
                    else:
                        if not self._aggregate_is_in_project(aggregate):
                            raise LookupError(
                                f"Did not find aggregate {aggregate} in the project"
                            )
                        aggregates.add(aggregate)  # An aggregate provided by the user
            return list(aggregates)
        return None

    @contextlib.contextmanager
    def _potentially_buffered(self):
        """Enable the use of buffered mode for certain functions."""
        if self.config["flow"].as_bool("use_buffered_mode"):
            logger.debug("Entering buffered mode...")
            with signac.buffered():
                yield
            logger.debug("Exiting buffered mode.")
        else:
            yield

    def _script(
        self, operations, parallel=False, template="script.sh", show_template_help=False
    ):
        """Generate a run script to execute given operations.

        Parameters
        ----------
        operations : Sequence of instances of :class:`._JobOperation`
            The operations to execute.
        parallel : bool
            Execute all operations in parallel (default is False).
        template : str
            The name of the template to use to generate the script. (Default value = "script.sh")
        show_template_help : bool
            Show help related to the templating system and then exit. (Default value = False)

        Returns
        -------
        str
            Rendered template of run script.

        """
        template_environment = self._template_environment()
        template = template_environment.get_template(template)
        context = self._get_standard_template_context()
        # For script generation we do not need the extra logic used for
        # generating cluster job scripts.
        context["base_script"] = "base_script.sh"
        context["operations"] = list(operations)
        context["parallel"] = parallel
        if show_template_help:
            self._show_template_help_and_exit(template_environment, context)
        return template.render(**context)

    @deprecated(deprecated_in="0.11", removed_in="0.13", current_version=__version__)
    def script(
        self, operations, parallel=False, template="script.sh", show_template_help=False
    ):
        """Generate a run script to execute given operations.

        Parameters
        ----------
        operations : Sequence of instances of :class:`~.JobOperation`
            The operations to execute.
        parallel : bool
            Execute all operations in parallel (default is False).
        template : str
            The name of the template to use to generate the script. (Default value = "script.sh")
        show_template_help : bool
            Show help related to the templating system and then exit. (Default value = False)

        Returns
        -------
        str
            Rendered template of run script.

        """
        return self._script(operations, parallel, template, show_template_help)

    def _generate_submit_script(
        self, _id, operations, template, show_template_help, env, **kwargs
    ):
        """Generate submission script to submit the execution of operations to a scheduler."""
        if template is None:
            template = env.template
        assert _id is not None

        template_environment = self._template_environment(env)
        template = template_environment.get_template(template)
        context = self._get_standard_template_context()
        # The flow 'script.sh' file simply extends the base script
        # provided. The choice of base script is dependent on the
        # environment, but will default to the 'base_script.sh' provided
        # with signac-flow unless additional environment information is
        # detected.

        logger.info("Use environment '%s'.", env)
        logger.info("Set 'base_script=%s'.", env.template)
        context["base_script"] = env.template
        context["environment"] = env
        context["id"] = _id
        context["operations"] = list(operations)
        context.update(kwargs)
        if show_template_help:
            self._show_template_help_and_exit(template_environment, context)
        return template.render(**context)

    def _submit_operations(
        self,
        operations,
        _id=None,
        env=None,
        parallel=False,
        flags=None,
        force=False,
        template="script.sh",
        pretend=False,
        show_template_help=False,
        **kwargs,
    ):
        r"""Submit a sequence of operations to the scheduler.

        Parameters
        ----------
        operations : A sequence of instances of :class:`~._JobOperation`
            The operations to submit.
        _id : str
            The _id to be used for this submission. (Default value = None)
        env : :class:`~.ComputeEnvironment`
            The environment to use for submission. Uses the environment defined
            by the :class:`~.FlowProject` if None (Default value = None).
        parallel : bool
            Execute all bundled operations in parallel. (Default value = False)
        flags : list
            Additional options to be forwarded to the scheduler. (Default value = None)
        force : bool
            Ignore all warnings or checks during submission, just submit. (Default value = False)
        template : str
            The name of the template file to be used to generate the submission
            script. (Default value = "script.sh")
        pretend : bool
            Do not actually submit, but only print the submission script to screen. Useful
            for testing the submission workflow. (Default value = False)
        show_template_help : bool
            Show information about available template variables and filters and
            exit. (Default value = False)
        \*\*kwargs
            Additional keyword arguments forwarded to :meth:`~.ComputeEnvironment.submit`.

        Returns
        -------
        :class:`~.JobStatus` or None
            Returns the submission status after successful submission or None.

        """
        if _id is None:
            _id = self._store_bundled(operations)
        if env is None:
            env = self._environment
        else:
            warnings.warn(
                "The env argument is deprecated as of 0.10 and will be removed in 0.12. "
                "Instead, set the environment when constructing a FlowProject.",
                DeprecationWarning,
            )

        print(f"Submitting cluster job '{_id}':", file=sys.stderr)

        def _msg(group):
            print(f" - Group: {group}", file=sys.stderr)
            return group

        try:
            script = self._generate_submit_script(
                _id=_id,
                operations=map(_msg, operations),
                template=template,
                show_template_help=show_template_help,
                env=env,
                parallel=parallel,
                force=force,
                **kwargs,
            )
        except ConfigKeyError as error:
            key = str(error)
            raise SubmitError(
                f"Unable to submit, because of a configuration error.\n"
                f"The following key is missing: {key}.\n"
                f"Add the key to the configuration by executing:\n\n"
                f"  $ signac config --global set {key} VALUE\n"
            )
        else:
            # Keys which were explicitly set by the user, but are not evaluated by the
            # template engine are cause for concern and might hint at a bug in the template
            # script or ill-defined directives. Here we check whether all directive keys that
            # have been explicitly set by the user were actually evaluated by the template
            # engine and warn about those that have not been.
            keys_unused = {
                key
                for op in operations
                for key in op.directives._keys_set_by_user.difference(
                    op.directives.keys_used
                )
                if key not in ("fork", "nranks", "omp_num_threads")  # ignore list
            }
            if keys_unused:
                logger.warning(
                    "Some of the keys provided as part of the directives were not used by "
                    "the template script, including: %s",
                    ", ".join(sorted(keys_unused)),
                )
            if pretend:
                print(script)

            else:
                return env.submit(_id=_id, script=script, flags=flags, **kwargs)

    @deprecated(deprecated_in="0.11", removed_in="0.13", current_version=__version__)
    def submit_operations(
        self,
        operations,
        _id=None,
        env=None,
        parallel=False,
        flags=None,
        force=False,
        template="script.sh",
        pretend=False,
        show_template_help=False,
        **kwargs,
    ):
        r"""Submit a sequence of operations to the scheduler.

        Parameters
        ----------
        operations : A sequence of instances of :class:`~.JobOperation`
            The operations to submit.
        _id : str
            The _id to be used for this submission. (Default value = None)
        env : :class:`~.ComputeEnvironment`
            The environment to use for submission. Uses the environment defined
            by the :class:`~.FlowProject` if None (Default value = None).
        parallel : bool
            Execute all bundled operations in parallel. (Default value = False)
        flags : list
            Additional options to be forwarded to the scheduler. (Default value = None)
        force : bool
            Ignore all warnings or checks during submission, just submit. (Default value = False)
        template : str
            The name of the template file to be used to generate the submission
            script. (Default value = "script.sh")
        pretend : bool
            Do not actually submit, but only print the submission script to screen. Useful
            for testing the submission workflow. (Default value = False)
        show_template_help : bool
            Show information about available template variables and filters and
            exit. (Default value = False)
        \*\*kwargs
            Additional keyword arguments forwarded to :meth:`~.ComputeEnvironment.submit`.

        Returns
        -------
        :class:`~.JobStatus` or None
            Returns the submission status after successful submission or None.

        """
        return self._submit_operations(
            operations,
            _id,
            env,
            parallel,
            flags,
            force,
            template,
            pretend,
            show_template_help,
            **kwargs,
        )

    def submit(
        self,
        bundle_size=1,
        jobs=None,
        names=None,
        num=None,
        parallel=False,
        force=False,
        walltime=None,
        env=None,
        ignore_conditions=IgnoreConditions.NONE,
        ignore_conditions_on_execution=IgnoreConditions.NONE,
        **kwargs,
    ):
        r"""Submit function for the project's main submit interface.

        Parameters
        ----------
        bundle_size : int
            Specify the number of operations to be bundled into one submission, defaults to 1.
        jobs : Sequence of instances :class:`~signac.contrib.job.Job`.
            Only submit operations associated with the provided jobs. Defaults to all jobs.
        names : Sequence of :class:`str`
            Only submit operations with any of the given names, defaults to all names.
        num : int
            Limit the total number of submitted operations, defaults to no limit.
        parallel : bool
            Execute all bundled operations in parallel. (Default value = False)
        force : bool
            Ignore all warnings or checks during submission, just submit. (Default value = False)
        walltime : :class:`datetime.timedelta`
            Specify the walltime in hours or as instance of
            :class:`datetime.timedelta`. (Default value = None)
        ignore_conditions : :class:`~.IgnoreConditions`
            Specify if preconditions and/or postconditions are to be ignored
            when determining eligibility. The default is
            :class:`IgnoreConditions.NONE`.
        ignore_conditions_on_execution : :class:`~.IgnoreConditions`
            Specify if preconditions and/or postconditions are to be ignored
            when determining eligibility after submitting. The default is
            :class:`IgnoreConditions.NONE`.
        env : :class:`~.ComputeEnvironment`
            The environment to use for submission. Uses the environment defined
            by the :class:`~.FlowProject` if None (Default value = None).
        \*\*kwargs
            Additional keyword arguments forwarded to :meth:`~.ComputeEnvironment.submit`.

        """
        aggregates = self._convert_aggregates_from_jobs(jobs)

        # Regular argument checks and expansion
        if isinstance(names, str):
            raise ValueError(
                "The 'names' argument must be a sequence of strings, however "
                f"a single string was provided: {names}."
            )
        if env is None:
            env = self._environment
        else:
            warnings.warn(
                "The env argument is deprecated as of 0.10 and will be removed in 0.12. "
                "Instead, set the environment when constructing a FlowProject.",
                DeprecationWarning,
            )
        if walltime is not None:
            try:
                walltime = datetime.timedelta(hours=walltime)
            except TypeError as error:
                if (
                    str(error) != "unsupported type for timedelta "
                    "hours component: datetime.timedelta"
                ):
                    raise
        if not isinstance(ignore_conditions, IgnoreConditions):
            raise ValueError(
                "The ignore_conditions argument of FlowProject.run() "
                "must be a member of class IgnoreConditions."
            )

        # Gather all pending operations.
        with self._potentially_buffered():
            default_directives = self._get_default_directives()
            # The generator must be used *inside* the buffering context manager
            # for performance reasons.
            operation_generator = self._get_submission_operations(
                aggregates,
                default_directives,
                names,
                ignore_conditions,
                ignore_conditions_on_execution,
            )
            # islice takes the first "num" elements from the generator, or all
            # items if num is None.
            operations = list(islice(operation_generator, num))

        # Bundle them up and submit.
        with self._potentially_buffered():
            for bundle in _make_bundles(operations, bundle_size):
                status = self._submit_operations(
                    operations=bundle,
                    env=env,
                    parallel=parallel,
                    force=force,
                    walltime=walltime,
                    **kwargs,
                )
                if status is not None:
                    # Operations were submitted, store status
                    for operation in bundle:
                        operation.set_status(status)

    @classmethod
    def _add_submit_args(cls, parser):
        """Add arguments to submit subcommand to parser."""
        parser.add_argument(
            "flags", type=str, nargs="*", help="Flags to be forwarded to the scheduler."
        )
        parser.add_argument(
            "--pretend",
            action="store_true",
            help="Do not really submit, but print the submission script to screen.",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Ignore all warnings and checks, just submit.",
        )
        parser.add_argument(
            "--test",
            action="store_true",
            help="Do not interact with the scheduler, implies --pretend.",
        )
        parser.add_argument(
            "--ignore-conditions",
            type=str,
            choices=["none", "pre", "post", "all"],
            default=IgnoreConditions.NONE,
            action=_IgnoreConditionsConversion,
            help="Specify conditions to ignore for eligibility check.",
        )
        parser.add_argument(
            "--ignore-conditions-on-execution",
            type=str,
            choices=["none", "pre", "post", "all"],
            default=IgnoreConditions.NONE,
            action=_IgnoreConditionsConversion,
            help="Specify conditions to ignore after submitting. May be useful "
            "for conditions that cannot be checked once scheduled.",
        )

        cls._add_operation_selection_arg_group(parser)
        cls._add_operation_bundling_arg_group(parser)
        cls._add_template_arg_group(parser)

    @classmethod
    def _add_script_args(cls, parser):
        cls._add_operation_selection_arg_group(parser)
        execution_group = parser.add_argument_group("execution")
        execution_group.add_argument(
            "-p",
            "--parallel",
            action="store_true",
            help="Execute all operations in parallel.",
        )
        cls._add_template_arg_group(parser)

    @classmethod
    def _add_template_arg_group(cls, parser, default="script.sh"):
        """Add argument group to parser for template handling."""
        template_group = parser.add_argument_group(
            "templating",
            "The execution and submission scripts are always generated from a script "
            f"which is by default called '{default}' and located within the default "
            "template directory. The system uses a default template if none is provided. "
            "The default template extends from a base template, which may be different "
            "depending on the local compute environment, e.g., 'slurm.sh' for an environment "
            "with SLURM scheduler. The name of the base template is provided with the "
            "'base_script' template variable.",
        )
        template_group.add_argument(
            "--template",
            type=str,
            default=default,
            help="The name of the template file within the template directory. "
            "The standard template directory is '${{project_root}}/templates' and "
            "can be configured with the 'template_dir' configuration variable. "
            f"Default: '{default}'.",
        )
        template_group.add_argument(
            "--template-help",
            dest="show_template_help",
            action="store_true",
            help="Show information about the template context, including available variables "
            "and filter functions; then exit.",
        )

    @classmethod
    def _add_job_selection_args(cls, parser):
        parser.add_argument(
            "-j",
            "--job-id",
            type=str,
            nargs="+",
            help="Only select jobs that match the given id(s).",
        )
        parser.add_argument(
            "-f",
            "--filter",
            type=str,
            nargs="+",
            help="Only select jobs that match the given state point filter.",
        )
        parser.add_argument(
            "--doc-filter",
            type=str,
            nargs="+",
            help="Only select jobs that match the given document filter.",
        )

    @classmethod
    def _add_operation_selection_arg_group(cls, parser):
        """Add argument group to parser for job-operation selection."""
        selection_group = parser.add_argument_group(
            "job-operation selection",
            "By default, all eligible operations for all jobs are selected. Use "
            "the options in this group to reduce this selection.",
        )
        cls._add_job_selection_args(selection_group)
        selection_group.add_argument(
            "-o",
            "--operation",
            dest="operation_name",
            nargs="+",
            help="Only select operation or groups that match the given "
            "operation/group name(s).",
        )
        selection_group.add_argument(
            "-n",
            "--num",
            type=int,
            help="Limit the total number of operations/groups to be selected. A group is "
            "considered to be one operation even if it consists of multiple operations.",
        )

    @classmethod
    def _add_operation_bundling_arg_group(cls, parser):
        """Add argument group to parser for operation bundling."""
        bundling_group = parser.add_argument_group(
            "bundling",
            "Bundle multiple operations for execution, e.g., to submit them "
            "all together to a cluster job, or execute them in parallel within "
            "an execution script.",
        )
        bundling_group.add_argument(
            "-b",
            "--bundle",
            type=int,
            nargs="?",
            const=0,
            default=1,
            dest="bundle_size",
            help="Bundle multiple operations for execution in a single "
            "scheduler job. When this option is provided without argument, "
            " all pending operations are aggregated into one bundle.",
        )
        bundling_group.add_argument(
            "-p",
            "--parallel",
            action="store_true",
            help="Execute all operations in a single bundle in parallel.",
        )

    def export_job_statuses(self, collection, statuses):
        """Export the job statuses to a :class:`signac.Collection`."""
        for status in statuses:
            job = self.open_job(id=status["job_id"])
            status["statepoint"] = job.statepoint()
            collection.update_one(
                {"_id": status["job_id"]}, {"$set": status}, upsert=True
            )

    @classmethod
    def _add_print_status_args(cls, parser):
        """Add arguments to parser for the :meth:`~.print_status` method."""
        cls._add_job_selection_args(parser)
        view_group = parser.add_argument_group(
            "view", "Specify how to format the status display."
        )
        view_group.add_argument(
            "--json",
            dest="dump_json",
            action="store_true",
            help="Do not format the status display, but dump all data formatted in JSON.",
        )
        view_group.add_argument(
            "-d",
            "--detailed",
            action="store_true",
            help="Show a detailed view of all jobs and their labels and operations.",
        )
        view_group.add_argument(
            "-a",
            "--all-operations",
            dest="all_ops",
            action="store_true",
            help="Show information about all operations, not just active or eligible ones.",
        )
        view_group.add_argument(
            "--only-incomplete-operations",
            dest="only_incomplete",
            action="store_true",
            help="Only show information for jobs with incomplete operations.",
        )
        view_group.add_argument(
            "--stack",
            action="store_false",
            dest="unroll",
            help="Show labels and operations in separate rows.",
        )
        view_group.add_argument(
            "-1",
            "--one-line",
            dest="compact",
            action="store_true",
            help="Show only one line per job.",
        )
        view_group.add_argument(
            "-e",
            "--expand",
            action="store_true",
            help="Display job labels and job operations in two separate tables.",
        )
        view_group.add_argument("--pretty", action="store_true")
        view_group.add_argument(
            "--full",
            action="store_true",
            help="Show all available information (implies --detailed --all-operations).",
        )
        view_group.add_argument(
            "--no-overview",
            action="store_false",
            dest="overview",
            help="Do not print an overview.",
        )
        view_group.add_argument(
            "-m",
            "--overview-max-lines",
            type=_positive_int,
            help="Limit the number of lines in the overview.",
        )
        view_group.add_argument(
            "-p",
            "--parameters",
            type=str,
            nargs="*",
            help="Display select parameters of the job's "
            "statepoint with the detailed view.",
        )
        view_group.add_argument(
            "--param-max-width", type=int, help="Limit the width of each parameter row."
        )
        view_group.add_argument(
            "--eligible-jobs-max-lines",
            type=_positive_int,
            help="Limit the number of eligible jobs that are shown.",
        )
        parser.add_argument(
            "--ignore-errors",
            action="store_true",
            help="Ignore errors that might occur when querying the scheduler.",
        )
        parser.add_argument(
            "--no-parallelize",
            action="store_true",
            help="Do not parallelize the status determination. "
            "The '--no-parallelize' argument is deprecated. "
            "Please use the status_parallelization configuration "
            "instead (see above).",
        )
        view_group.add_argument(
            "-o",
            "--output-format",
            type=str,
            default="terminal",
            help="Set status output format: terminal, markdown, or html.",
        )

    def labels(self, job):
        """Yield all labels for the given ``job``.

        See also: :meth:`~.label`.

        Parameters
        ----------
        job : :class:`signac.contrib.job.Job`
            Job handle.

        Yields
        ------
        str
            Label value.

        """
        for label_func, label_name in self._label_functions.items():
            if label_name is None:
                label_name = getattr(
                    label_func,
                    "_label_name",
                    getattr(label_func, "__name__", type(label_func).__name__),
                )
            try:
                label_value = label_func(job)
            except TypeError:
                try:
                    label_value = label_func(self, job)
                except Exception:
                    label_func = getattr(self, label.__func__.__name__)
                    label_value = label_func(job)

            assert label_name is not None
            if isinstance(label_value, str):
                yield label_value
            elif bool(label_value) is True:
                yield label_name

    def add_operation(self, name, cmd, pre=None, post=None, **kwargs):
        r"""Add an operation to the workflow.

        This method will add an instance of :class:`~.FlowOperation` to the
        operations of this project.

        .. seealso::

            A Python function may be defined as an operation function directly
            using the :meth:`~.operation` decorator.

        Any FlowOperation is associated with a specific command, which should
        be a function of :class:`~signac.contrib.job.Job`. The command (cmd)
        can be stated as function, either by using str-substitution based on a
        job's attributes, or by providing a unary callable, which expects an
        instance of job as its first and only positional argument.

        For example, if we wanted to define a command for a program called
        'hello', which expects a job id as its first argument, we could
        construct the following two equivalent operations:

        .. code-block:: python

            op = FlowOperation('hello', cmd='hello {job._id}')
            op = FlowOperation('hello', cmd=lambda 'hello {}'.format(job._id))

        Here are some more useful examples for str-substitutions:

        .. code-block:: python

            # Substitute job state point parameters:
            op = FlowOperation('hello', cmd='cd {job.ws}; hello {job.sp.a}')

        Preconditions (pre) and postconditions (post) can be used to trigger an
        operation only when certain conditions are met. Conditions are unary
        callables, which expect an instance of job as their first and only
        positional argument and return either True or False.

        An operation is considered "eligible" for execution when all
        preconditions are met and when at least one of the postconditions is
        not met. Preconditions are always met when the list of preconditions
        is empty. Postconditions are never met when the list of postconditions
        is empty.

        Please note, eligibility in this contexts refers only to the workflow
        pipeline and not to other contributing factors, such as whether the
        job-operation is currently running or queued.

        Parameters
        ----------
        name : str
            A unique identifier for this operation, which may be freely chosen.
        cmd : str or callable
            The command to execute operation; should be a function of job.
        pre : sequence of callables
            List of preconditions. (Default value = None)
        post : sequence of callables
            List of postconditions. (Default value = None)
        \*\*kwargs
            Keyword arguments passed as directives.

        """
        if name in self.operations:
            raise KeyError("An operation with this identifier is already added.")
        op = self.operations[name] = FlowCmdOperation(cmd=cmd, pre=pre, post=post)
        if name in self._groups:
            raise KeyError("A group with this identifier already exists.")
        self._groups[name] = FlowGroup(
            name, operations={name: op}, operation_directives=dict(name=kwargs)
        )

    def completed_operations(self, job):
        """Determine which operations have been completed for job.

        Parameters
        ----------
        job : :class:`~signac.contrib.job.Job`
            The signac job handle.

        Yields
        ------
        str
            The names of the operations that are complete.

        """
        for name, op in self._operations.items():
            if op._complete((job,)):
                yield name

    def _next_operations(self, jobs=None, ignore_conditions=IgnoreConditions.NONE):
        """Determine the next eligible operations for aggregates.

        Parameters
        ----------
        jobs : tuple of :class:`~signac.contrib.job.Job`
            The signac job handles. By default all the aggregates are evaluated
            to get the next operation associated.
        ignore_conditions : :class:`~.IgnoreConditions`
            Specify if preconditions and/or postconditions are to be ignored
            when determining eligibility. The default is
            :class:`IgnoreConditions.NONE`.

        Yields
        ------
        :class:`~._JobOperation`
            All eligible operations for the provided jobs.

        """
        for name in self.operations:
            group = self._groups[name]
            for aggregate in self._get_aggregate_store(group.name).values():
                if not self._is_selected_aggregate(aggregate, jobs):
                    continue
                yield from group._create_run_job_operations(
                    entrypoint=self._entrypoint,
                    default_directives={},
                    jobs=aggregate,
                    ignore_conditions=ignore_conditions,
                )

    @deprecated(deprecated_in="0.11", removed_in="0.13", current_version=__version__)
    def next_operations(self, *jobs, ignore_conditions=IgnoreConditions.NONE):
        r"""Determine the next eligible operations for provided job(s).

        Parameters
        ----------
        \*jobs : One or more instances of :class:`.Job`.
            Jobs.
        ignore_conditions : :class:`~.IgnoreConditions`
            Specify if preconditions and/or postconditions are to be ignored
            while checking eligibility. The default is
            :class:`IgnoreConditions.NONE`.

        Yields
        ------
        :class:`~.JobOperation`
            Eligible job operation.

        """
        for name in self.operations:
            group = self._groups[name]
            aggregate_store = self._get_aggregate_store(group.name)

            # Only yield JobOperation instances from the default aggregates
            if not isinstance(aggregate_store, _DefaultAggregateStore):
                continue

            for aggregate in aggregate_store.values():
                # JobOperation handles a single job and not an aggregate of
                # jobs. Hence the single job in that aggregate should be
                # present in the jobs passed by a user.
                if aggregate[0] not in jobs:
                    continue

                for operation in group._create_run_job_operations(
                    entrypoint=self._entrypoint,
                    jobs=aggregate,
                    default_directives={},
                    ignore_conditions=ignore_conditions,
                    index=0,
                ):
                    yield JobOperation(
                        operation.id,
                        operation.name,
                        operation._jobs[0],
                        operation._cmd,
                        operation.directives,
                    )

    @classmethod
    def operation(cls, func, name=None):
        """Add an operation function to the class workflow definition.

        This function is designed to be used as a decorator, for example:

        .. code-block:: python

            @FlowProject.operation
            def hello(job):
                print('Hello', job)

        See also: :meth:`~.flow.FlowProject.add_operation`.

        Parameters
        ----------
        func : callable
            The function to add to the workflow.
        name : str
            The operation name. Uses the name of the function if None.
             (Default value = None)

        Returns
        -------
        callable
            The operation function.

        """
        if isinstance(func, str):
            return lambda op: cls.operation(op, name=func)

        if name is None:
            name = func.__name__

        if (name, func) in cls._OPERATION_FUNCTIONS:
            raise ValueError(f"An operation with name '{name}' is already registered.")
        if name in cls._GROUP_NAMES:
            raise ValueError(f"A group with name '{name}' is already registered.")

        signature = inspect.signature(func)
        for i, parameter_value in enumerate(signature.parameters.values()):
            if i > 0 and parameter_value.default is inspect.Parameter.empty:
                raise ValueError(
                    "Only the first argument in an operation argument may not have "
                    f"a default value! ({name})"
                )
        if not getattr(func, "_flow_aggregate", False):
            func._flow_aggregate = aggregator.groupsof(1)

        # Append the name and function to the class registry
        cls._OPERATION_FUNCTIONS.append((name, func))
        cls._GROUPS.append(
            FlowGroupEntry(name=name, options="", aggregator=func._flow_aggregate)
        )
        if hasattr(func, "_flow_groups"):
            func._flow_groups.append(name)
        else:
            func._flow_groups = [name]
        return func

    @classmethod
    def _collect_operations(cls):
        """Collect all operations added with the ``@FlowProject.operation`` decorator."""
        operations = []
        for parent_class in cls.__mro__:
            operations.extend(getattr(parent_class, "_OPERATION_FUNCTIONS", []))
        return operations

    @classmethod
    def _collect_conditions(cls, attr):
        """Collect conditions from attr using the mro hierarchy."""
        ret = defaultdict(list)
        for parent_class in cls.__mro__:
            for func, conds in getattr(parent_class, attr, {}).items():
                ret[func].extend(conds)
        return ret

    @classmethod
    def _collect_pre_conditions(cls):
        """Collect all preconditions added with the ``@FlowProject.pre`` decorator."""
        return cls._collect_conditions("_OPERATION_PRE_CONDITIONS")

    @classmethod
    def _collect_post_conditions(cls):
        """Collect all postconditions added with the ``@FlowProject.post`` decorator."""
        return cls._collect_conditions("_OPERATION_POST_CONDITIONS")

    def _register_operations(self):
        """Register all operation functions registered with this class and its parent classes."""
        operations = self._collect_operations()
        pre_conditions = self._collect_pre_conditions()
        post_conditions = self._collect_post_conditions()

        for name, func in operations:
            if name in self._operations:
                raise ValueError(f"Repeat definition of operation with name '{name}'.")

            # Extract preconditions/postconditions and directives from function:
            params = {
                "pre": pre_conditions.get(func, None),
                "post": post_conditions.get(func, None),
            }

            # Construct FlowOperation:
            if getattr(func, "_flow_cmd", False):
                self._operations[name] = FlowCmdOperation(cmd=func, **params)
            else:
                self._operations[name] = FlowOperation(op_func=func, **params)

    def _register_aggregates(self):
        """Generate aggregates for every operation or group in a FlowProject."""
        stored_aggregates = {}
        for _aggregator, groups in self._aggregator_per_group.items():
            stored_aggregates[_aggregator._create_AggregatesStore(self)] = groups
        self._stored_aggregates = stored_aggregates

    @classmethod
    def make_group(cls, name, options=""):
        r"""Make a :class:`~.FlowGroup` named ``name`` and return a decorator to make groups.

        A :class:`~.FlowGroup` is used to group operations together for
        running and submitting :class:`~.JobOperation`\ s.

        Examples
        --------
        The code below creates a group and adds an operation to that group.

        .. code-block:: python

            example_group = FlowProject.make_group('example')

            @example_group
            @FlowProject.operation
            def foo(job):
                return "hello world"

        Parameters
        ----------
        name : str
            The name of the :class:`~.FlowGroup`.
        options : str
            A string to append to submissions. Can be any valid
            :meth:`FlowOperation.run` option. (Default value = "")

        Returns
        -------
        :class:`~.FlowGroupEntry`
            The created group.

        """
        if name in cls._GROUP_NAMES:
            raise ValueError(f"Repeat definition of group with name '{name}'.")
        cls._GROUP_NAMES.add(name)
        group_entry = FlowGroupEntry(name, options, aggregator.groupsof(1))
        cls._GROUPS.append(group_entry)
        return group_entry

    def _register_groups(self):
        """Register all groups and add the correct operations to each."""
        group_entries = []
        # Gather all groups from class and parent classes.
        for cls in type(self).__mro__:
            group_entries.extend(getattr(cls, "_GROUPS", []))

        aggregators = defaultdict(list)
        # Initialize all groups without operations.
        # Also store the aggregates we need to store all the groups associated
        # with each aggregator.
        for entry in group_entries:
            self._groups[entry.name] = FlowGroup(entry.name, options=entry.options)
            aggregators[entry.aggregator].append(entry.name)
        self._aggregator_per_group = dict(aggregators)

        # Add operations and directives to group
        for operation_name, operation in self._operations.items():
            if isinstance(operation, FlowCmdOperation):
                func = operation._cmd
            else:
                func = operation._op_func

            if hasattr(func, "_flow_groups"):
                op_directives = getattr(func, "_flow_group_operation_directives", {})
                for group_name in func._flow_groups:
                    directives = op_directives.get(group_name)
                    self._groups[group_name].add_operation(
                        operation_name, operation, directives
                    )

            # For singleton groups add directives
            directives = getattr(func, "_flow_directives", {})
            self._groups[operation_name].operation_directives[
                operation_name
            ] = directives

    @property
    def operations(self):
        """Get the dictionary of operations that have been added to the workflow."""
        return self._operations

    @property
    def groups(self):
        """Get the dictionary of groups that have been added to the workflow."""
        return self._groups

    def _get_aggregate_store(self, group):
        """Return aggregate store associated with the :class:`~.FlowGroup`.

        Parameters
        ----------
        group : str
            The name of the :class:`~.FlowGroup` whose aggregate store will be returned.

        Returns
        -------
        :class:`_DefaultAggregateStore`
            Aggregate store containing aggregates associated with the provided :class:`~.FlowGroup`.

        """
        for aggregate_store, groups in self._stored_aggregates.items():
            if group in groups:
                return aggregate_store
        return {}

    def _eligible_for_submission(self, flow_group, jobs):
        """Check flow_group eligibility for submission with a job-aggregate.

        By default, an operation is eligible for submission when it
        is not considered active, that means already queued or running.
        """
        if flow_group is None or jobs is None:
            return False
        if flow_group._get_status(jobs) >= JobStatus.submitted:
            return False
        group_ops = set(flow_group)
        for other_group in self._groups.values():
            if group_ops & set(other_group):
                if other_group._get_status(jobs) >= JobStatus.submitted:
                    return False
        return True

    def _main_status(self, args):
        """Print status overview."""
        aggregates = self._select_jobs_from_args(args)
        if args.compact and not args.unroll:
            logger.warning(
                "The -1/--one-line argument is incompatible with "
                "'--stack' and will be ignored."
            )
        show_traceback = args.debug or args.show_traceback
        args = {
            key: val
            for key, val in vars(args).items()
            if key
            not in [
                "func",
                "verbose",
                "debug",
                "show_traceback",
                "job_id",
                "filter",
                "doc_filter",
            ]
        }
        if args.pop("full"):
            args["detailed"] = args["all_ops"] = True

        start = time.time()
        try:
            self.print_status(jobs=aggregates, **args)
        except Exception as error:
            if show_traceback:
                logger.error(
                    f"Error during status update: {str(error)}\nUse '--ignore-errors' to "
                    "complete the update anyways."
                )
            else:
                logger.error(
                    f"Error during status update: {str(error)}\nUse '--ignore-errors' to "
                    "complete the update anyways or '--show-traceback' to show "
                    "the full traceback."
                )
                error = error.__cause__  # Always show the user traceback cause.
            traceback.print_exception(type(error), error, error.__traceback__)
        else:
            if aggregates is None:
                length_jobs = sum(
                    len(aggregate_store) for aggregate_store in self._stored_aggregates
                )
            else:
                length_jobs = len(aggregates)
            # Use small offset to account for overhead with few jobs
            delta_t = (time.time() - start - 0.5) / max(length_jobs, 1)
            config_key = "status_performance_warn_threshold"
            warn_threshold = flow_config.get_config_value(config_key)
            if not args["profile"] and delta_t > warn_threshold >= 0:
                print(
                    "WARNING: "
                    f"The status compilation took more than {warn_threshold}s per job. "
                    "Consider using `--profile` to determine bottlenecks "
                    "within the project workflow definition.\n"
                    f"Execute `signac config set flow.{config_key} VALUE` to specify "
                    "the warning threshold in seconds.\n"
                    "To speed up the compilation, try executing "
                    "`signac config set flow.status_parallelization 'process'` to set "
                    "the status_parallelization config value to process."
                    "Use -1 to completely suppress this warning.\n",
                    file=sys.stderr,
                )

    def _main_next(self, args):
        """Determine the jobs that are eligible for a specific operation."""
        for operation in self._next_operations():
            if args.name in operation.name:
                print(get_aggregate_id(operation._jobs))

    def _main_run(self, args):
        """Run all (or select) job operations."""
        # Select jobs:
        aggregates = self._select_jobs_from_args(args)

        # Setup partial run function, because we need to call this either
        # inside some context managers or not based on whether we need
        # to switch to the project root directory or not.
        run = functools.partial(
            self.run,
            jobs=aggregates,
            names=args.operation_name,
            pretend=args.pretend,
            np=args.parallel,
            timeout=args.timeout,
            num=args.num,
            num_passes=args.num_passes,
            progress=args.progress,
            order=args.order,
            ignore_conditions=args.ignore_conditions,
        )

        if args.switch_to_project_root:
            with add_cwd_to_environment_pythonpath():
                with switch_to_directory(self.root_directory()):
                    run()
        else:
            run()

    def _main_script(self, args):
        """Generate a script for the execution of operations."""
        print(
            "WARNING: "
            "The script argument is deprecated as of 0.12 "
            "and will be removed in 0.14. "
            'Use "submit --pretend" instead.',
            file=sys.stderr,
        )

        # Select jobs:
        aggregates = self._select_jobs_from_args(args)

        # Gather all pending operations or generate them based on a direct command...
        with self._potentially_buffered():
            names = args.operation_name if args.operation_name else None
            default_directives = self._get_default_directives()
            operations = self._get_submission_operations(
                aggregates,
                default_directives,
                names,
                args.ignore_conditions,
                args.ignore_conditions_on_execution,
            )
            operations = list(islice(operations, args.num))

        # Generate the script and print to screen.
        print(
            self._script(
                operations=operations,
                parallel=args.parallel,
                template=args.template,
                show_template_help=args.show_template_help,
            )
        )

    def _main_submit(self, args):
        """Submit jobs to a scheduler."""
        if args.test:
            args.pretend = True
        kwargs = vars(args)

        # Select jobs:
        aggregates = self._select_jobs_from_args(args)

        # Fetch the scheduler status.
        if not args.test:
            self._fetch_scheduler_status(aggregates)

        names = args.operation_name if args.operation_name else None
        self.submit(jobs=aggregates, names=names, **kwargs)

    def _main_exec(self, args):
        aggregates = self._select_jobs_from_args(args)
        try:
            operation = self._operations[args.operation]

            if isinstance(operation, FlowCmdOperation):

                def operation_function(job):
                    cmd = operation(job).format(job=job)
                    subprocess.run(cmd, shell=True, check=True)

            else:
                operation_function = operation

        except KeyError:
            raise KeyError(f"Unknown operation '{args.operation}'.")

        for aggregate in self._get_aggregate_store(args.operation).values():
            if self._is_selected_aggregate(aggregate, aggregates):
                operation_function(*aggregate)

    def _select_jobs_from_args(self, args):
        """Select jobs with the given command line arguments ('-j/-f/--doc-filter/--job-id')."""
        if (
            not args.func == self._main_exec
            and args.job_id
            and (args.filter or args.doc_filter)
        ):
            raise ValueError(
                "Cannot provide both -j/--job-id and -f/--filter or --doc-filter in combination."
            )

        if args.job_id:
            # aggregates must be a set to prevent duplicate entries
            aggregates = set()
            for id in args.job_id:
                # TODO: We need to add support for aggregation id parameter
                # for the -j flag ('agg-...')
                try:
                    aggregates.add((self.open_job(id=id),))
                except KeyError as error:
                    raise LookupError(f"Did not find job with id {error}.")
            return list(aggregates)
        elif "filter" in args or "doc_filter" in args:
            filter_ = parse_filter_arg(args.filter)
            doc_filter = parse_filter_arg(args.doc_filter)
            return JobsCursor(self, filter_, doc_filter)
        else:
            return None

    def main(self, parser=None):
        """Call this function to use the main command line interface.

        In most cases one would want to call this function as part of the
        class definition:

        .. code-block:: python

            # my_project.py
            from flow import FlowProject

            class MyProject(FlowProject):
                pass

            if __name__ == '__main__':
                MyProject().main()

        The project can then be executed on the command line:

        .. code-block:: bash

            $ python my_project.py --help

        Parameters
        ----------
        parser : :class:`argparse.ArgumentParser`
            The argument parser used to implement the command line interface.
            If None, a new parser is constructed. (Default value = None)

        """
        # Find file that main is called in. When running through the command
        # line interface, we know exactly what the entrypoint path should be:
        # it's the file where main is called, which we can pull off the stack.
        self._entrypoint.setdefault(
            "path", os.path.realpath(inspect.stack()[-1].filename)
        )

        if parser is None:
            parser = argparse.ArgumentParser()

        base_parser = argparse.ArgumentParser(add_help=False)

        # The argparse module does not automatically merge options shared between the main
        # parser and the subparsers. We therefore assign different destinations for each
        # option and then merge them manually below.
        for prefix, _parser in (("main_", parser), ("", base_parser)):
            _parser.add_argument(
                "-v",
                "--verbose",
                dest=prefix + "verbose",
                action="count",
                default=0,
                help="Increase output verbosity.",
            )
            _parser.add_argument(
                "--show-traceback",
                dest=prefix + "show_traceback",
                action="store_true",
                help="Show the full traceback on error.",
            )
            _parser.add_argument(
                "--debug",
                dest=prefix + "debug",
                action="store_true",
                help="This option implies `-vv --show-traceback`.",
            )

        subparsers = parser.add_subparsers()

        parser_status = subparsers.add_parser(
            "status",
            parents=[base_parser],
            description="Parallelization of the status command can be "
            "controlled by setting the flow.status_parallelization config "
            "value to 'thread' (default), 'none', or 'process'. To do this, "
            "execute `signac config set flow.status_parallelization VALUE`.",
        )
        self._add_print_status_args(parser_status)
        parser_status.add_argument(
            "--profile",
            const=inspect.getsourcefile(inspect.getmodule(self)),
            nargs="?",
            help="Collect statistics to determine code paths that are responsible "
            "for the majority of runtime required for status determination. "
            "Optionally provide a filename pattern to select for what files "
            "to show result for. Defaults to the main module. "
            "(requires pprofile)",
        )
        parser_status.set_defaults(func=self._main_status)

        parser_next = subparsers.add_parser(
            "next",
            parents=[base_parser],
            description="Determine jobs that are eligible for a specific operation.",
        )
        parser_next.add_argument("name", type=str, help="The name of the operation.")
        parser_next.set_defaults(func=self._main_next)

        parser_run = subparsers.add_parser(
            "run",
            parents=[base_parser],
        )
        self._add_operation_selection_arg_group(parser_run)

        execution_group = parser_run.add_argument_group("execution")
        execution_group.add_argument(
            "--pretend",
            action="store_true",
            help="Do not actually execute commands, just show them.",
        )
        execution_group.add_argument(
            "--progress",
            action="store_true",
            help="Display a progress bar during execution.",
        )
        execution_group.add_argument(
            "--num-passes",
            type=int,
            default=1,
            help="Specify how many times a particular job-operation may be executed within one "
            "session (default=1). This is to prevent accidental infinite loops, "
            "where operations are executed indefinitely, because postconditions "
            "were not properly set. Use -1 to allow for an infinite number of passes.",
        )
        execution_group.add_argument(
            "-t",
            "--timeout",
            type=int,
            help="A timeout in seconds after which the execution of one operation is canceled.",
        )
        execution_group.add_argument(
            "--switch-to-project-root",
            action="store_true",
            help="Temporarily add the current working directory to the python search path and "
            "switch to the root directory prior to execution.",
        )
        execution_group.add_argument(
            "-p",
            "--parallel",
            type=int,
            nargs="?",
            const="-1",
            help="Specify the number of cores to parallelize to. Defaults to all available "
            "processing units if argument is omitted.",
        )
        execution_group.add_argument(
            "--order",
            type=str,
            choices=["none", "by-op", "by-job", "cyclic", "random"],
            default=None,
            help="Specify the execution order of operations for each execution pass.",
        )
        execution_group.add_argument(
            "--ignore-conditions",
            type=str,
            choices=["none", "pre", "post", "all"],
            default=IgnoreConditions.NONE,
            action=_IgnoreConditionsConversion,
            help="Specify conditions to ignore for eligibility check.",
        )
        parser_run.set_defaults(func=self._main_run)

        parser_script = subparsers.add_parser(
            "script",
            parents=[base_parser],
        )
        parser_script.add_argument(
            "--ignore-conditions",
            type=str,
            choices=["none", "pre", "post", "all"],
            default=IgnoreConditions.NONE,
            action=_IgnoreConditionsConversion,
            help="Specify conditions to ignore for eligibility check.",
        )
        parser_script.add_argument(
            "--ignore-conditions-on-execution",
            type=str,
            choices=["none", "pre", "post", "all"],
            default=IgnoreConditions.NONE,
            action=_IgnoreConditionsConversion,
            help="Specify conditions to ignore after submitting. May be useful "
            "for conditions that cannot be checked once scheduled.",
        )
        self._add_script_args(parser_script)
        parser_script.set_defaults(func=self._main_script)

        parser_submit = subparsers.add_parser(
            "submit",
            parents=[base_parser],
            conflict_handler="resolve",
        )
        self._add_submit_args(parser_submit)
        env_group = parser_submit.add_argument_group(
            f"{self._environment.__name__} options"
        )
        self._environment.add_args(env_group)
        parser_submit.set_defaults(func=self._main_submit)
        print(
            "Using environment configuration:",
            self._environment.__name__,
            file=sys.stderr,
        )

        parser_exec = subparsers.add_parser(
            "exec",
            parents=[base_parser],
        )
        parser_exec.add_argument(
            "operation",
            type=str,
            choices=list(sorted(self._operations)),
            help="The operation to execute.",
        )
        parser_exec.add_argument(
            "job_id",
            type=str,
            nargs="*",
            help="The job ids, as registered in the signac project. "
            "Omit to default to all statepoints.",
        )
        parser_exec.set_defaults(func=self._main_exec)

        args = parser.parse_args()
        if not hasattr(args, "func"):
            parser.print_usage()
            sys.exit(2)

        # Manually 'merge' the various global options defined for both the main parser
        # and the parent parser that are shared by all subparsers:
        for dest in ("verbose", "show_traceback", "debug"):
            setattr(args, dest, getattr(args, "main_" + dest) or getattr(args, dest))
            delattr(args, "main_" + dest)

        # Read the config file and set the internal flag.
        # Do not overwrite with False if not present in config file
        if flow_config.get_config_value("show_traceback"):
            args.show_traceback = True

        if args.debug:  # Implies '-vv' and '--show-traceback'
            args.verbose = max(2, args.verbose)
            args.show_traceback = True

        # Support print_status argument alias
        if args.func == self._main_status and args.full:
            args.detailed = args.all_ops = True

        # Empty parameters argument on the command line means: show all varying parameters.
        if hasattr(args, "parameters"):
            if args.parameters is not None and len(args.parameters) == 0:
                args.parameters = self.PRINT_STATUS_ALL_VARYING_PARAMETERS

        # Set verbosity level according to the `-v` argument.
        logging.basicConfig(level=max(0, logging.WARNING - 10 * args.verbose))

        def _show_traceback_and_exit(error):
            if args.show_traceback:
                traceback.print_exception(type(error), error, error.__traceback__)
            elif isinstance(error, (UserOperationError, UserConditionError)):
                # Always show the user traceback cause.
                error = error.__cause__
                traceback.print_exception(type(error), error, error.__traceback__)
                print(
                    "Execute with '--show-traceback' or '--debug' to show the "
                    "full traceback.",
                    file=sys.stderr,
                )
            else:
                print(
                    "Execute with '--show-traceback' or '--debug' to get more "
                    "information.",
                    file=sys.stderr,
                )
            sys.exit(1)

        try:
            args.func(args)
        except NoSchedulerError as error:
            print(
                f"ERROR: {error}",
                "Consider to use the 'script' command to generate an execution script instead.",
                file=sys.stderr,
            )
            _show_traceback_and_exit(error)
        except SubmitError as error:
            print("Submission error:", error, file=sys.stderr)
            _show_traceback_and_exit(error)
        except (TimeoutError, subprocess.TimeoutExpired) as error:
            print(
                "Error: Failed to complete execution due to "
                f"timeout ({args.timeout}s).",
                file=sys.stderr,
            )
            _show_traceback_and_exit(error)
        except Jinja2TemplateNotFound as error:
            print(f"Did not find template script '{error}'.", file=sys.stderr)
            _show_traceback_and_exit(error)
        except AssertionError as error:
            if not args.show_traceback:
                print(
                    "ERROR: Encountered internal error during program execution.",
                    file=sys.stderr,
                )
            _show_traceback_and_exit(error)
        except (UserOperationError, UserConditionError) as error:
            if str(error):
                print(f"ERROR: {error}\n", file=sys.stderr)
            else:
                print(
                    "ERROR: Encountered error during program execution.\n",
                    file=sys.stderr,
                )
            _show_traceback_and_exit(error)
        except Exception as error:
            if str(error):
                print(
                    "ERROR: Encountered error during program execution: "
                    f"'{error}'\n",
                    file=sys.stderr,
                )
            else:
                print(
                    "ERROR: Encountered error during program execution.\n",
                    file=sys.stderr,
                )
            _show_traceback_and_exit(error)


def _serializer(loads, project, *args):
    project = loads(project)
    if args[-1] == "run_operations":
        operation_data = args[0]
        project._execute_operation(project._job_operation_from_tuple(operation_data))
    elif args[-1] == "fetch_labels":
        job = project.open_job(id=args[0])
        ignore_errors = args[1]
        return project._get_job_labels(job, ignore_errors=ignore_errors)
    elif args[-1] == "fetch_status":
        group = args[0]
        ignore_errors = args[1]
        cached_status = args[2]
        return project._get_group_status(group, ignore_errors, cached_status)
    return None


__all__ = [
    "FlowProject",
    "FlowOperation",
    "label",
    "staticlabel",
    "classlabel",
]
