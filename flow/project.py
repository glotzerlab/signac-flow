# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Workflow definition with the FlowProject.

The FlowProject is a signac Project that allows the user to define a workflow.
"""
import argparse
import contextlib
import functools
import inspect
import json
import logging
import multiprocessing
import os
import random
import re
import shlex
import subprocess
import sys
import textwrap
import threading
import time
import warnings
from collections import Counter, defaultdict
from copy import deepcopy
from enum import IntFlag
from hashlib import md5, sha1
from itertools import chain, count, groupby, islice
from multiprocessing import Event, Pool, TimeoutError, cpu_count
from multiprocessing.pool import ThreadPool

import cloudpickle
import jinja2
import jsonschema
import signac
from jinja2 import TemplateNotFound as Jinja2TemplateNotFound
from signac.contrib.filterparse import parse_filter_arg

from .aggregates import (
    _AggregatesCursor,
    _AggregateStore,
    _AggregateStoresCursor,
    _JobAggregateCursor,
    aggregator,
    get_aggregate_id,
)
from .directives import _document_directive
from .environment import ComputeEnvironment, get_environment, registered_environments
from .errors import (
    ConfigKeyError,
    FlowProjectDefinitionError,
    NoSchedulerError,
    SubmitError,
    TemplateError,
    UserConditionError,
    UserOperationError,
)
from .hooks import _Hooks
from .labels import _is_label_func, classlabel, label, staticlabel
from .operations import cmd as _cmd
from .operations import with_job as _with_job
from .render_status import _render_status
from .scheduling.base import ClusterJob, JobStatus
from .util import config as flow_config
from .util import template_filters
from .util.misc import (
    _add_cwd_to_environment_pythonpath,
    _bidict,
    _cached_partial,
    _get_parallel_executor,
    _positive_int,
    _roundrobin,
    _switch_to_directory,
    _to_hashable,
    _TrackGetItemDict,
    tqdm,
)
from .util.translate import abbreviate, shorten

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
syntax, e.g., the project root directory can be written as: {{{{ project.root_directory() }}}}.
The available template variables are:
{template_vars}

Filter functions can be used to format template variables in a specific way.
For example: {{{{ project.id | capitalize }}}}.

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
    JobStatus.group_registered: "GR",
    JobStatus.group_inactive: "GI",
    JobStatus.group_submitted: "GS",
    JobStatus.group_held: "GH",
    JobStatus.group_queued: "GQ",
    JobStatus.group_active: "GA",
    JobStatus.group_error: "GE",
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
        max_bits = len(bin(max(elem.value for elem in type(self)))) - 2
        return self.__class__((2**max_bits - 1) ^ self._value_)

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
    directives : dict
        A `dict` object of additional parameters that provide instructions on
        how to execute this operation, e.g., specifically required resources.
    user_directives : set
        Keys in ``directives`` that correspond to user-specified directives
        that are not part of the environment's standard directives.
    """

    def __init__(self, id, name, jobs, cmd, directives, user_directives):
        self._id = id
        self.name = name
        self._jobs = jobs
        if not (callable(cmd) or isinstance(cmd, str)):
            raise ValueError("cmd must be a callable or string.")
        self._cmd = cmd

        # We use a special dictionary that tracks all keys that have been
        # evaluated by the template engine and compare them to those explicitly
        # set by the user. See also comment below.
        self.directives = _TrackGetItemDict(directives)

        # Keys which were explicitly set by the user, but are not evaluated by
        # the template engine are cause for concern and might hint at a bug in
        # the template script or ill-defined directives. We are therefore
        # keeping track of all keys set by the user and check whether they have
        # been evaluated by the template script engine later.
        self.directives._keys_set_by_user = user_directives

    def __str__(self):
        aggregate_id = get_aggregate_id(self._jobs)
        return f"{self.name}({aggregate_id})"

    def __repr__(self):
        return "{type}(name='{name}', jobs='{jobs}', cmd={cmd}, directives={directives})".format(
            type=type(self).__name__,
            name=self.name,
            jobs="(" + ", ".join(map(repr, self._jobs)) + ")",
            cmd=repr(self.cmd),
            directives=self.directives,
        )

    def __hash__(self):
        return hash(self.id)

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
            eligible_operations = []
        self.eligible_operations = eligible_operations

        if operations_with_unmet_preconditions is None:
            operations_with_unmet_preconditions = []
        self.operations_with_unmet_preconditions = operations_with_unmet_preconditions

        if operations_with_met_postconditions is None:
            operations_with_met_postconditions = []
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

    def _eligible(self, aggregate, ignore_conditions=IgnoreConditions.NONE):
        """Determine eligibility of an aggregate.

        An aggregate is eligible when all preconditions are true and at least
        one postcondition is false, or corresponding conditions are ignored.

        Parameters
        ----------
        aggregate : tuple of :class:`~signac.contrib.job.Job`
            The aggregate of signac jobs.
        ignore_conditions : :class:`~.IgnoreConditions`
            Specify if preconditions and/or postconditions are to be ignored
            when determining eligibility. The default is
            :class:`IgnoreConditions.NONE`.

        Returns
        -------
        bool
            Whether the aggregate is eligible.

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
            or all(cond(aggregate) for cond in self._preconditions)
        )
        if met_preconditions and len(self._postconditions) > 0:
            unmet_postconditions = (ignore_conditions & IgnoreConditions.POST) or any(
                not cond(aggregate) for cond in self._postconditions
            )
        else:
            unmet_postconditions = True
        return met_preconditions and unmet_postconditions

    def _complete(self, jobs):
        """Check if all postconditions are met."""
        if len(self._postconditions) > 0:
            return all(cond(jobs) for cond in self._postconditions)
        return False


class FlowCmdOperation(BaseFlowOperation):
    """An operation that executes a shell command.

    When an operation has the ``FlowProject.operation(cmd=True)`` directive specified, it is
    instantiated as a :class:`~.FlowCmdOperation`. The operation should be a
    function of one or more positional arguments that are instances of
    :class:`~signac.contrib.job.Job`. The command (cmd) may either be a
    callable that expects one or more instances of
    :class:`~signac.contrib.job.Job` as positional arguments and returns a
    string containing valid shell commands, or the string of commands itself.
    In either case, the resulting string may contain any attributes of the
    job (or jobs) placed in curly braces, which will then be substituted by
    Python string formatting.

    .. note::
        This class should not be instantiated directly.

    Parameters
    ----------
    cmd : str or callable
        The command to execute the operation. Callable values will be
        provided one or more positional arguments that are
        instances of :class:`~signac.contrib.job.Job`. String values will be
        formatted with ``cmd.format(jobs=jobs)`` where ``jobs`` is a tuple of
        :class:`~signac.contrib.job.Job`, or ``cmd.format(jobs=jobs,
        job=job)`` if only one job is provided.
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

    def __call__(self, *jobs):
        """Return the command formatted with the supplied job(s)."""
        cmd = self._cmd(*jobs) if callable(self._cmd) else self._cmd
        # The logic below will be removed after version 0.23.0. This is only to temporary fix an
        # issue in supporting the formatting of cmd operation in the interim.
        format_arguments = {}
        if not callable(self._cmd):
            format_arguments["jobs"] = jobs
            if len(jobs) == 1:
                format_arguments["job"] = jobs[0]
            formatted_cmd = cmd.format(**format_arguments)
        else:
            argspec = inspect.getfullargspec(self._cmd)
            signature = inspect.signature(self._cmd)
            args = {
                k: v for k, v in signature.parameters.items() if k != argspec.varargs
            }

            # get all named positional/keyword arguments with individual names.
            for i, arg_name in enumerate(args):
                try:
                    format_arguments[arg_name] = jobs[i]
                except IndexError:
                    format_arguments[arg_name] = args[arg_name].default

            # capture any remaining variable positional arguments.
            if argspec.varargs:
                format_arguments[argspec.varargs] = jobs[len(args) :]

            # Capture old behavior which assumes job or jobs in the format string. We need to test
            # the truthiness of the key versus the inclusion because in the case of with_job the
            # above logic results in format_arguments["jobs"] = ().
            if not any(format_arguments.get(key, False) for key in ("jobs", "job")):
                if match := re.search("{.*(jobs?).*}", cmd):
                    # Saves in key jobs or job based on regex match.
                    format_arguments[match.group(1)] = jobs
            formatted_cmd = cmd.format(**format_arguments)
        if formatted_cmd != cmd:
            warnings.warn(
                "Returning format strings in a cmd operation is deprecated as of version 0.22.0 "
                "and will be removed in  0.23.0. Users should format the command string.",
                FutureWarning,
            )
        return formatted_cmd


class FlowOperation(BaseFlowOperation):
    """An operation that executes a Python function.

    All operations without the ``FlowProject.operation(cmd=True)`` directive use this class. The
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

    Application developers should not directly instantiate this class, but
    use :meth:`~.FlowProject.make_group` instead.

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
    project : flow.FlowProject
        The project the group is associated with.
    submit_options : str
        The :meth:`FlowProject.run` options to pass when submitting the group.
        These will be included in all submissions. Submissions use run
        commands to execute.
    run_options : str
        The options to pass to ``entrypoint exec`` when running the group. Specifying this will
        cause the operation to be forked even if it otherwise would run in the current Python
        interpreter.
    group_aggregator : :class:`~.aggregator`
        aggregator object associated with the :class:`FlowGroup` (Default value = None).
    """

    def __init__(
        self, name, project, submit_options="", run_options="", group_aggregator=None
    ):
        self.name = name
        self._project = project
        self.submit_options = submit_options
        self.run_options = run_options
        # We register aggregators associated with operation functions in
        # `_register_groups` and we do not set the aggregator explicitly.
        # We delay setting the aggregator because we do not restrict the
        # decorator placement in terms of `@FlowGroupEntry`, `@aggregator`, or
        # `@operation`.
        self.group_aggregator = group_aggregator

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
        if not any(
            func == op_func for _, op_func in self._project._OPERATION_FUNCTIONS
        ):
            raise FlowProjectDefinitionError(
                f"Cannot add function '{func}' to group without making the function an "
                f"operation. Add @MyProjectClass.operation below group decorator."
            )

        if self.name in func._flow_groups[self._project]:
            raise FlowProjectDefinitionError(
                f"Cannot reregister operation '{func}' with the group '{self.name}'."
            )
        func._flow_groups[self._project].add(self.name)
        return func

    def _set_directives(self, func, directives):
        if hasattr(func, "_flow_group_operation_directives"):
            if self.name in func._flow_group_operation_directives:
                raise FlowProjectDefinitionError(
                    "Cannot set directives because directives already exist "
                    f"for operation '{func}' in group '{self.name}'."
                )
            func._flow_group_operation_directives[self.name] = directives
        else:
            func._flow_group_operation_directives = {self.name: directives}

    def with_directives(self, directives):
        """Decorate an operation to provide additional execution directives for this group.

        Directives can be used to provide information about required resources
        such as the number of processors required for execution of parallelized
        operations. For a list of supported directives, see
        :meth:`.FlowProject.operation.with_directives`. For more information,
        see :ref:`signac-docs:cluster_submission_directives`.

        The directives specified in this decorator are only applied when
        executing the operation through the :class:`FlowGroup`.
        To apply directives to an individual operation executed outside of the
        group, see :meth:`.FlowProject.operation.with_directives`.

        Parameters
        ----------
        directives : dict
            Directives to use for resource requests and execution.

        Returns
        -------
        function
            A decorator which registers the operation with the group using the
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

        @group.with_directives({"nranks": 4})
        @FlowProject.operation.with_directives({"nranks": 2, "executable": "python3"})
        def op1(job):
            pass

        @group
        @FlowProject.operation.with_directives({"nranks": 2, "executable": "python3"})
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
    submit_options : str
        The :meth:`FlowProject.run` options to pass when submitting the group. These will be
        included in all submissions. Submissions use run commands to execute.
    run_options : str
        The options to pass to ``entrypoint exec`` when running the group. Specifying this will
        cause the operation to be forked even if it otherwise would run in the current Python
        interpreter.
    """

    MAX_LEN_ID = 100

    def __init__(
        self,
        name,
        operations=None,
        operation_directives=None,
        submit_options="",
        run_options="",
    ):
        self.name = name
        self.submit_options = submit_options
        self.run_options = run_options
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

    def _submit_cmd(self, entrypoint, ignore_conditions, jobs):
        entrypoint = self._determine_entrypoint(entrypoint, {}, jobs)
        cmd = f"{entrypoint} run -o {self.name}"
        cmd = cmd if jobs is None else cmd + f" -j {get_aggregate_id(jobs)}"
        options = self.submit_options
        if ignore_conditions != IgnoreConditions.NONE:
            options += " --ignore-conditions=" + str(ignore_conditions)
        return " ".join((cmd, options)).strip()

    def _run_cmd(self, entrypoint, operation_name, operation, directives, jobs):
        if isinstance(operation, FlowCmdOperation):
            return operation(*jobs).lstrip()
        entrypoint = self._determine_entrypoint(entrypoint, directives, jobs)
        cmd = f"{entrypoint} exec {operation_name} {get_aggregate_id(jobs)} {self.run_options}"
        return cmd.strip()

    def __iter__(self):
        yield from self.operations.values()

    def __repr__(self):
        return (
            f"{type(self).__name__}(name={repr(self.name)}, operations='"
            f"{' '.join(list(self.operations))}',"
            f"operation_directives={self.operation_directives}, "
            f"submit_options={repr(self.submit_options)}, run_options={repr(self.run_options)})"
        )

    def _eligible(self, aggregate, ignore_conditions=IgnoreConditions.NONE):
        """Determine if at least one operation is eligible.

        A :class:`~.FlowGroup` is eligible for execution if at least one of
        its associated operations is eligible.

        Parameters
        ----------
        aggregate : tuple of :class:`~signac.contrib.job.Job`
            The aggregate of signac jobs.
        ignore_conditions : :class:`~.IgnoreConditions`
            Specify if preconditions and/or postconditions are to be ignored
            while checking eligibility. The default is
            :class:`IgnoreConditions.NONE`.

        Returns
        -------
        bool
            Whether the group is eligible.

        """
        return any(op._eligible(aggregate, ignore_conditions) for op in self)

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

    def _generate_id(self, aggregate, operation_name=None):
        """Generate a unique id which identifies this group and job(s).

        The generated value is used to identify interactions with the
        scheduler.

        Parameters
        ----------
        aggregate : tuple of :class:`signac.contrib.job.Job`
            The aggregate of signac jobs.
        operation_name : str
            Operation name defining the unique id. (Default value = None)

        Returns
        -------
        str
            The unique id.

        """
        project = aggregate[0]._project

        # The full name is designed to be truly unique for each job-group.
        if operation_name is None:
            op_string = "".join(sorted(list(self.operations)))
        else:
            op_string = operation_name

        root_directory = project.root_directory()
        aggregate_id = get_aggregate_id(aggregate)
        full_name = f"{root_directory}%{aggregate_id}%{op_string}"
        # The job_op_id is a hash computed from the unique full name.
        job_op_id = md5(full_name.encode("utf-8")).hexdigest()

        # The actual job id is then constructed from a readable part and the
        # job_op_id, ensuring that the job-op is still somewhat identifiable,
        # but guaranteed to be unique. The readable name is based on the
        # project id, aggregate id, and operation name. All names and the id
        # itself are restricted in length to guarantee that the id does not get
        # too long.
        max_len = self.MAX_LEN_ID - len(job_op_id)
        if max_len < len(job_op_id):
            raise ValueError(f"Value for MAX_LEN_ID is too small ({self.MAX_LEN_ID}).")

        separator = getattr(project._environment, "JOB_ID_SEPARATOR", "/")
        readable_name = "{project}{sep}{aggregate_id}{sep}{op_string}{sep}".format(
            sep=separator,
            project=str(project)[:12],
            aggregate_id=aggregate_id,
            op_string=op_string[:12],
        )[:max_len]

        # By appending the unique job_op_id, we ensure that each id is truly unique.
        return readable_name + job_op_id

    def _create_submission_job_operation(
        self,
        entrypoint,
        default_directives,
        jobs,
        ignore_conditions_on_execution=IgnoreConditions.NONE,
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

        Returns
        -------
        :class:`~._SubmissionJobOperation`
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
            ignore_ops = set(ignore_ops)
            return [
                op
                for op in self._create_run_job_operations(
                    entrypoint=entrypoint,
                    default_directives=default_directives,
                    jobs=jobs,
                    ignore_conditions=ignore_conditions_on_execution
                    | additional_ignores_flag,
                )
                if op not in ignore_ops
            ]

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
            self._generate_id(jobs),
            self.name,
            jobs,
            cmd=unevaluated_cmd,
            directives=dict(submission_directives),
            user_directives=set(submission_directives.user_keys),
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
                    self._generate_id(jobs, operation_name),
                    operation_name,
                    jobs,
                    cmd=unevaluated_cmd,
                    directives=dict(directives),
                    user_directives=set(directives.user_keys),
                )
                # Get the prefix, and if it's non-empty, set the fork directive
                # to True since we must launch a separate process. Override
                # the command directly.
                prefix = jobs[0]._project._environment.get_prefix(job_op)
                if prefix != "" or self.run_options != "":
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
        directives.evaluate(jobs)
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
        cls._OPERATION_PRECONDITIONS = defaultdict(list)
        cls._OPERATION_POSTCONDITIONS = defaultdict(list)
        cls._OPERATION_HOOK_REGISTRY = defaultdict(lambda: defaultdict(list))

        # All label functions are registered with the label() classmethod,
        # which is intended to be used as decorator function. The
        # _LABEL_FUNCTIONS dict contains the function as key and the label name
        # as value, or None to use the default label name.
        cls._LABEL_FUNCTIONS = {}

        # Give the class a preconditions and postconditions class that are
        # aware of the class they are in.
        cls.pre = cls._setup_preconditions_class(parent_class=cls)
        cls.post = cls._setup_postconditions_class(parent_class=cls)

        # Give the class an operation register object
        cls.operation = cls._setup_operation_object(parent_class=cls)

        # All groups are registered with the function returned by the
        # make_group classmethod. In contrast to operations and labels, the
        # make_group classmethod does not serve as the decorator, the functor
        # it returns does. The _GROUPS list records the groups created and
        # their passed parameters for later initialization. The _GROUP_NAMES
        # set stores whether a group name has already been used.
        cls._GROUPS = []
        cls._GROUP_NAMES = set()

        cls.operation_hooks = cls._setup_hooks_object(parent_class=cls)

        return cls

    @staticmethod
    def _setup_preconditions_class(parent_class):
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

                @Project.operation
                @aggregator()
                @Project.pre(lambda *jobs: all("hi_all" not in job.doc for job in jobs))
                def hi_all(*jobs):
                    print('hi', jobs)
                    for job in jobs:
                        job.doc.hi_all = True

            The *hello* operation would only execute if the 'hello' key in the
            job document does not evaluate to True. Similarly, the *hi_all* operation
            would execute only if the 'hi_all' key is not present in all of the jobs passed.

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
                    raise FlowProjectDefinitionError(
                        "Operation functions cannot be used as preconditions."
                    )
                self._parent_class._OPERATION_PRECONDITIONS[func].insert(
                    0, self.condition
                )
                return func

            @classmethod
            def copy_from(cls, *other_funcs):
                """Copy preconditions from other operation(s).

                True if and only if all preconditions of other operation
                function(s) are met.
                """
                return cls(
                    _create_all_metacondition(
                        cls._parent_class._collect_preconditions(), *other_funcs
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
                    raise FlowProjectDefinitionError(
                        "The arguments to pre.after must be operations."
                    )
                return cls(
                    _create_all_metacondition(
                        cls._parent_class._collect_postconditions(), *other_funcs
                    )
                )

        return pre

    @staticmethod
    def _setup_postconditions_class(parent_class):
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

                @Project.operation
                @aggregator()
                @Project.post(lambda *jobs: all("bye_all" in job.doc for job in jobs))
                def bye_all(*jobs):
                    print('bye', jobs)
                    for job in jobs:
                        job.doc.bye_all = True

            The *bye* operation would be considered complete and therefore no
            longer eligible for execution once the 'bye' key in the job
            document evaluates to True. Similarly, the *bye_all* operation
            would be considered complete and therefore no longer eligible for execution
            only if the 'bye_all' key is present in all of the jobs passed.

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
                    raise FlowProjectDefinitionError(
                        "Operation functions cannot be used as postconditions."
                    )
                self._parent_class._OPERATION_POSTCONDITIONS[func].insert(
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
                        cls._parent_class._collect_postconditions(), *other_funcs
                    )
                )

        return post

    @staticmethod
    def _setup_operation_object(parent_class):
        class OperationRegister:
            """Add operation functions to the class workflow definition.

            This object is designed to be used as a decorator, for example:

            .. code-block:: python

                @FlowProject.operation
                def hello(job):
                    print('Hello', job)

            Directives can also be specified by using :meth:`FlowProject.operation.with_directives`.

            .. code-block:: python

                @FlowProject.operation.with_directives({"nranks": 4})
                def mpi_hello(job):
                    print("hello")

            Parameters
            ----------
            func : callable
                The function to add to the workflow.
            name : str
                The operation name. Uses the name of the function if None.
                (Default value = None)
            cmd : bool, optional, keyword-only
                Whether the decorated function returns a shell executable string or not. When
                ``True``, the returned string is executed by the shell. Defaults to ``False``.
            with_job : bool, optional, keyword-only
                Whether to change directories to the job workspace when running the job. Defaults to
                ``False``.
            directives : dict, optional, keyword-only
                Directives to use for resource requests and execution.

            Returns
            -------
            callable
                The operation function.
            """

            _parent_class = parent_class

            def __call__(
                self,
                func=None,
                name=None,
                *,
                cmd=False,
                with_job=False,
                directives=None,
            ):
                if isinstance(func, str):
                    return lambda op: self._internal_call(
                        op, name=func, cmd=cmd, with_job=with_job, directives=directives
                    )
                if func is None:
                    return lambda op: self._internal_call(
                        op, name=name, cmd=cmd, with_job=with_job, directives=directives
                    )
                return self._internal_call(
                    func, name=name, cmd=cmd, with_job=with_job, directives=directives
                )

            def _internal_call(self, func, name, *, cmd, with_job, directives):
                if func in chain(
                    *self._parent_class._OPERATION_PRECONDITIONS.values(),
                    *self._parent_class._OPERATION_POSTCONDITIONS.values(),
                ):
                    raise FlowProjectDefinitionError(
                        "A condition function cannot be used as an operation."
                    )

                # Handle cmd and with_job options. Use the deprecated decorators internally until
                # the decorators are removed. These must be done first for now as with_job actually
                # wraps the original function meaning that any other labels we apply will be masked
                # if we do this later or not even captured it not added to _OPERATION_FUNCTIONS.
                with warnings.catch_warnings():
                    warnings.simplefilter(action="ignore", category=FutureWarning)
                    if cmd:
                        _cmd(func)
                    if with_job:
                        func = _with_job(func)

                # Store directives
                if directives is not None:
                    func._flow_directives = directives

                if name is None:
                    name = func.__name__

                for (
                    registered_name,
                    registered_func,
                ) in self._parent_class._OPERATION_FUNCTIONS:
                    if name == registered_name:
                        raise FlowProjectDefinitionError(
                            f"An operation with name '{name}' is already registered."
                        )
                    if func is registered_func:
                        raise FlowProjectDefinitionError(
                            "An operation with this function is already registered."
                        )
                if name in self._parent_class._GROUP_NAMES:
                    raise FlowProjectDefinitionError(
                        f"A group with name '{name}' is already registered."
                    )

                if not getattr(func, "_flow_aggregate", False):
                    func._flow_aggregate = aggregator.groupsof(1)

                # Append the name and function to the class registry
                self._parent_class._OPERATION_FUNCTIONS.append((name, func))
                # We register aggregators associated with operation functions in
                # `_register_groups` and we do not set the aggregator explicitly.  We
                # delay setting the aggregator because we do not restrict the decorator
                # placement in terms of `@FlowGroupEntry`, `@aggregator`, or
                # `@operation`.
                self._parent_class._GROUPS.append(
                    FlowGroupEntry(name=name, project=self._parent_class)
                )
                if not hasattr(func, "_flow_groups"):
                    func._flow_groups = {}
                func._flow_groups[self._parent_class] = {name}
                return func

            def with_directives(self, directives, name=None):
                """Decorate a function to make it an operation with additional execution directives.

                Directives can be used to provide information about required
                resources such as the number of processors required for
                execution of parallelized operations. For more information, see
                :ref:`signac-docs:cluster_submission_directives`. To apply
                directives to an operation that is part of a group, use
                :meth:`.FlowGroupEntry.with_directives`.

                Parameters
                ----------
                directives : dict
                    Directives to use for resource requests and execution.
                name : str
                    The operation name. Uses the name of the function if None
                    (Default value = None).

                Returns
                -------
                function
                    A decorator which registers the function with the provided
                    name and directives as an operation of the
                    :class:`~.FlowProject` subclass.
                """
                warnings.warn(
                    "@FlowProject.operation.with_directives has been deprecated as of 0.22.0 and "
                    "will be removed in 0.23.0. Use @FlowProject.operation(directives={...}) "
                    "instead.",
                    FutureWarning,
                )

                def add_operation_with_directives(function):
                    function._flow_directives = directives
                    return self(function, name)

                return add_operation_with_directives

            _directives_to_document = (
                ComputeEnvironment._get_default_directives()._directive_definitions.values()
            )
            with_directives.__doc__ += textwrap.indent(
                "\n\n**Supported Directives:**\n\n"
                + "\n\n".join(
                    _document_directive(directive)
                    for directive in _directives_to_document
                ),
                " " * 16,
            )

        return OperationRegister()

    @staticmethod
    def _setup_hooks_object(parent_class):
        class _HooksRegister:
            """Add hooks to an operation.

            This object is designed to be used as a decorator. The example
            below shows an operation level decorator that prints the
            operation name and job id at the start of the operation
            execution.

            .. code-block:: python

                def start_hook(operation_name, job):
                    print(f"Starting operation {operation_name} on job {job.id}.")

                @FlowProject.operation_hooks.on_start(start_hook)
                @FlowProject.operation
                def foo(job):
                    pass

            A hook is a function that is called at specific points during
            the execution of a job operation. In the example above, the
            ``start_hook`` hook function is executed before the operation
            **foo** runs. Hooks can also run after an operation finishes,
            when an operation exits with error, or when an operation exits
            without error.

            The available triggers are ``on_start``, ``on_exit``, ``on_exception``, and
            ``on_success`` which run when the operation starts, completes, fails, and
            succeeds respectively.

            Parameters
            ----------
            hook_func : callable
                The function that will be executed at a specified point.
            trigger : string
                The point when a hook operation is executed.
            """

            _parent_class = parent_class

            def __init__(self, hook_func, trigger):
                self._hook_func = hook_func
                self._hook_trigger = trigger

            @classmethod
            def on_start(cls, hook_func):
                """Add a hook function triggered before an operation starts."""
                return cls(hook_func, trigger="on_start")

            @classmethod
            def on_exit(cls, hook_func):
                """Add a hook function triggered after the operation exits.

                The hook is triggered regardless of whether the operation exits
                with or without an error.
                """
                return cls(hook_func, trigger="on_exit")

            @classmethod
            def on_success(cls, hook_func):
                """Add a hook function triggered after the operation exits without error."""
                return cls(hook_func, trigger="on_success")

            @classmethod
            def on_exception(cls, hook_func):
                """Add a hook function triggered after the operation exits with an error."""
                return cls(hook_func, trigger="on_exception")

            def __call__(self, func):
                """Add the decorated function to the operation hook registry.

                Parameters
                ----------
                func : callable
                    The operation function associated with the hook function.
                """
                self._parent_class._OPERATION_HOOK_REGISTRY[func][
                    self._hook_trigger
                ].append(self._hook_func)
                return func

        return _HooksRegister


def _config_value_as_bool(value):
    # Function to interpret a configobj bool-like value as a boolean.
    if isinstance(value, str):
        if value.lower() in {"true", "on", "yes", "1"}:
            return True
        elif value.lower() in {"false", "off", "no", "0"}:
            return False
        else:
            raise ValueError("Invalid boolean config value.")
    return bool(value)


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

        # Initialize the local config.
        # TODO: In signac 2.0 we will not allow config modification after
        # project initialization. The flow config is already effectively
        # immutable since it is an internal variable that is not exposed to the
        # user in any way. Once signac no longer relies on configobj and stops
        # supporting in-place config modification, flow can make use of the
        # signac Project config dictionary directly and that can be updated by
        # any flow config APIs. For now, we store the flow config separately to
        # avoid any side effects associated with modifying instances of
        # signac.contrib._ProjectConfig.
        self._flow_config = {
            **flow_config._FLOW_CONFIG_DEFAULTS,
            **self._config.get("flow", {}),
        }
        self._flow_config["eligible_jobs_max_lines"] = int(
            self._flow_config["eligible_jobs_max_lines"]
        )
        self._flow_config["status_performance_warn_threshold"] = float(
            self._flow_config["status_performance_warn_threshold"]
        )
        jsonschema.validate(
            self._flow_config,
            flow_config._FLOW_SCHEMA,
            format_checker=jsonschema.draft7_format_checker,
        )

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

        # Setup execution hooks
        self._project_hooks = _Hooks()
        self._operation_hooks = defaultdict(_Hooks)

        # Register all label functions with this project instance.
        self._label_functions = {}
        self._register_labels()

        # Register all operation functions with this project instance.
        self._operations = {}
        self._register_operations()

        # Register all groups and aggregates with this project instance.
        self._groups = {}
        self._group_to_aggregate_store = _bidict()
        self._register_groups()

    def _setup_template_environment(self):
        """Set up the jinja2 template environment.

        The templating system is used to generate templated scripts for the
        script() and _submit_operations() / submit() function and the
        corresponding command line subcommands.
        """
        environment_modules = [
            cls.__module__
            for cls in registered_environments()
            if not cls.__module__.startswith("flow.environments")
        ]

        # Templates are searched in the local template directory first, then in additionally
        # installed packages, then in the main package 'templates' directory.
        extra_packages = []
        for env in environment_modules:
            try:
                extra_packages.append(jinja2.PackageLoader(env, "templates"))
            except ImportError as error:
                logger.warning(f"Unable to load template from package '{error.name}'.")
            except ValueError as error:
                logger.warning(
                    f"Unable to load template from package. Original Error '{error}'."
                )

        load_envs = (
            [jinja2.FileSystemLoader(self._template_dir)]
            + extra_packages
            + [jinja2.PackageLoader("flow", "templates")]
        )

        template_environment = jinja2.Environment(
            loader=jinja2.ChoiceLoader(load_envs),
            trim_blocks=True,
            lstrip_blocks=True,
            extensions=[TemplateError],
        )

        # Setup standard filters that can be used to format context variables.
        template_environment.filters[
            "format_timedelta"
        ] = template_filters.format_timedelta
        template_environment.filters["format_memory"] = template_filters.format_memory
        template_environment.filters["identical"] = template_filters.identical
        template_environment.filters["with_np_offset"] = template_filters.with_np_offset
        template_environment.filters["calc_tasks"] = template_filters.calc_tasks
        template_environment.filters["calc_num_nodes"] = template_filters.calc_num_nodes
        template_environment.filters["calc_walltime"] = template_filters.calc_walltime
        template_environment.filters["calc_memory"] = template_filters.calc_memory
        template_environment.filters[
            "check_utilization"
        ] = template_filters.check_utilization
        template_environment.filters[
            "homogeneous_openmp_mpi_config"
        ] = template_filters.homogeneous_openmp_mpi_config
        template_environment.filters["get_config_value"] = flow_config.get_config_value
        template_environment.filters[
            "get_account_name"
        ] = template_filters.get_account_name
        template_environment.filters["print_warning"] = template_filters.print_warning
        template_environment.filters["quote_argument"] = shlex.quote

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

    @property
    def project_hooks(self):
        """:class:`.hooks.Hooks` defined for all project operations.

        Project-wide hooks are added to an *instance* of the FlowProject, not
        the class. For example:

        .. code-block:: python

            def finish_hook(operation_name, job):
                print(f"Finished operation {operation_name} on job {job.id}")

            if __name__ == "__main__":
                project = FlowProject()
                project.project_hooks.on_exit.append(finish_hook)
                project.main()
        """
        return self._project_hooks

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

    def _get_cached_scheduler_status(self):
        """Fetch all status information.

        The project document key ``_status`` is returned as a plain dict, or an
        empty dict if no status information is present.

        Returns
        -------
        dict
            Dictionary of cached status information. The keys are uniquely
            generated ids for each group and job. The values are instances of
            :class:`~.JobStatus`.

        """
        try:
            return self.document["_status"]()
        except KeyError:
            return {}

    @contextlib.contextmanager
    def _update_cached_scheduler_status(self):
        """Context manager used to update cached project status.

        When entered, this context manager yields an empty dictionary. The
        keys in this dictionary are unique generated ids for a given group
        and aggregate, and the value is an instance of :class:`~.JobStatus`.
        When the context exits, this dictionary is used to update the project
        document's cached status.

        Yields
        ------
        dict
            Empty dictionary where status information should be stored.

        """
        status_update = {}
        try:
            yield status_update
        finally:
            # This "finally" block cannot include "return", "break", or
            # "continue", or else saved exceptions raised in the context
            # manager will be lost and not be seen by the user.
            # https://docs.python.org/3/reference/compound_stmts.html#the-try-statement
            if status_update:
                status_update = {
                    key: int(value) for key, value in status_update.items()
                }
                if "_status" in self.document:
                    disk_status = self.document["_status"]()
                else:
                    disk_status = {}
                disk_status.update(status_update)

                # Filter out JobStatus.unknown before writing to disk, to save
                # space and reduce the write time.
                disk_status = {
                    key: value
                    for key, value in disk_status.items()
                    if value != int(JobStatus.unknown)
                }

                self.document["_status"] = disk_status

    def _generate_selected_aggregate_groups(
        self,
        selected_aggregates=None,
        selected_groups=None,
        tqdm_kwargs=None,
    ):
        """Yield selected aggregates and groups.

        Parameters
        ----------
        selected_aggregates : sequence of tuples of :class:`~signac.contrib.job.Job`
            Aggregates to select.
        selected_groups : set of :class:`~.FlowGroup`
            Groups to select.
        tqdm_kwargs : dict or None
            A dict of keyword arguments to the tqdm progress bar used for
            iterating over aggregates. If None, no progress bar will be
            shown. (Default value = None)

        Yields
        ------
        aggregate_id : str
            Selected aggregate id.
        aggregate : tuple of :class:`~signac.contrib.job.Job`
            Selected aggregate.
        group : :class:`~.FlowGroup`
            Selected group.

        """

        def aggregate_progress_wrapper(aggregates):
            """Show progress bar if keyword arguments are provided."""
            if tqdm_kwargs is not None:
                return tqdm(
                    aggregates,
                    total=len(aggregates),
                    **tqdm_kwargs,
                )
            else:
                return aggregates

        if (
            selected_groups is not None
            and len(selected_groups) >= 0
            and len(self._group_to_aggregate_store.inverse) > 1
        ):
            # Use only aggregate stores for the selected groups.
            aggregate_stores_of_selected_groups = {
                self._group_to_aggregate_store[group] for group in selected_groups
            }
            aggregate_stores = {
                aggregate_store: self._group_to_aggregate_store.inverse[aggregate_store]
                for aggregate_store in aggregate_stores_of_selected_groups
            }
        else:
            # Use all aggregate stores.
            aggregate_stores = self._group_to_aggregate_store.inverse

        for (
            aggregate_store,
            aggregate_groups,
        ) in aggregate_stores.items():
            if selected_groups is not None:
                # Filter out groups that are not selected. The order of
                # aggregate_groups must be preserved. Using an intersection of
                # ordered sets would be preferable but would require a
                # dependency.
                matching_groups = [
                    group for group in aggregate_groups if group in selected_groups
                ]
                if len(matching_groups) == 0:
                    # Skip aggregate store if no groups are selected
                    continue
            else:
                matching_groups = aggregate_groups
            if selected_aggregates is not None:
                # Use selected aggregates in the aggregate store
                for aggregate in aggregate_progress_wrapper(selected_aggregates):
                    aggregate_id = get_aggregate_id(aggregate)
                    if aggregate_id in aggregate_store:
                        for group in matching_groups:
                            yield aggregate_id, aggregate, group
            else:
                # Use all aggregates in the aggregate store
                for aggregate_id, aggregate in aggregate_progress_wrapper(
                    aggregate_store.items()
                ):
                    for group in matching_groups:
                        yield aggregate_id, aggregate, group

    def _generate_selected_aggregate_groups_with_status(
        self,
        scheduler_info=None,
        *args,
        **kwargs,
    ):
        r"""Yield selected aggregates and groups while updating cached status.

        After the iterator is exhausted (or if an exception is raised during
        iteration), the project's cached status will be updated for all
        aggregates and groups that have been yielded by this generator.

        Parameters
        ----------
        scheduler_info : dict or None
            A dict of the form returned by :meth:`~._query_scheduler_status`,
            with keys corresponding to scheduler job names and values that
            are instances of :class:`~.JobStatus`. If None, all jobs will
            have unknown status. (Default value = None)
        \*args :
            Arguments forwarded to
            :meth:`~._generate_selected_aggregate_groups`.
        \*\*kwargs :
            Keyword arguments forwarded to
            :meth:`~._generate_selected_aggregate_groups`.

        Yields
        ------
        scheduler_id : str
            Unique identifier for this aggregate and group.
        scheduler_status : :class:`~.JobStatus`
            The scheduler status of this aggregate and group.
        aggregate_id : str
            Selected aggregate id.
        aggregate : tuple of :class:`~signac.contrib.job.Job`
            Selected aggregate.
        group : :class:`~.FlowGroup`
            Selected group.

        """
        if scheduler_info is None:
            scheduler_info = {}
        with self._update_cached_scheduler_status() as status_update:
            for (
                aggregate_id,
                aggregate,
                group,
            ) in self._generate_selected_aggregate_groups(*args, **kwargs):
                scheduler_id = group._generate_id(aggregate)
                scheduler_status = scheduler_info.get(scheduler_id, JobStatus.unknown)
                status_update[scheduler_id] = scheduler_status
                yield scheduler_id, scheduler_status, aggregate_id, aggregate, group

    def _get_aggregate_group_status(self, aggregate, cached_status):
        """Fetch group status for this aggregate.

        Parameters
        ----------
        aggregate : tuple of :class:`~signac.contrib.job.Job`
            Aggregate for which status information is fetched.
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

        for aggregate_id, aggregate, group, in self._generate_selected_aggregate_groups(
            selected_aggregates=[aggregate],
        ):
            completed = group._complete(aggregate)
            # If the group is not completed, it is sufficient to determine
            # eligibility while ignoring postconditions (we know at least
            # one postcondition is not met).
            eligible = not completed and group._eligible(
                aggregate, ignore_conditions=IgnoreConditions.POST
            )
            scheduler_status = cached_status.get(
                group._generate_id(aggregate), JobStatus.unknown
            )
            for operation in group.operations:
                if scheduler_status >= status_dict[operation]["scheduler_status"]:
                    status_dict[operation] = {
                        "scheduler_status": scheduler_status,
                        "eligible": eligible,
                        "completed": completed,
                    }

        yield from sorted(status_dict.items())

    def _get_aggregate_status(self, aggregate, cached_status, ignore_errors=False):
        """Return status information about an aggregate.

        Parameters
        ----------
        aggregate : tuple of :class:`~signac.contrib.job.Job`
            Aggregate for which status information is fetched.
        cached_status : dict
            Dictionary of cached status information. The keys are uniquely
            generated ids for each group and job. The values are instances of
            :class:`~.JobStatus`. (Default value = None)
        ignore_errors : bool
            Whether to ignore exceptions raised during status check. (Default value = False)

        Returns
        -------
        dict
            A dictionary containing job status for all jobs.

        """
        aggregate_id = get_aggregate_id(aggregate)
        result = {
            "job_id": aggregate_id,
            "operations": {},
            "_operations_error": None,
        }
        try:
            result["operations"] = dict(
                self._get_aggregate_group_status(aggregate, cached_status)
            )
        except Exception as error:
            logger.debug(
                "Error while getting operations status for job '%s': '%s'.",
                aggregate_id,
                error,
            )
            if ignore_errors:
                result["_operations_error"] = str(error)
            else:
                raise
        return result

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
            A dictionary containing status for the requested job.

        """
        if cached_status is None:
            cached_status = self._get_cached_scheduler_status()
        result = self._get_aggregate_status(
            aggregate=(job,),
            ignore_errors=ignore_errors,
            cached_status=cached_status,
        )
        labels_result = self._get_job_labels(job, ignore_errors=ignore_errors)
        result["labels"] = labels_result["labels"]
        result["_labels_error"] = labels_result["_labels_error"]
        return result

    def _query_scheduler_status(self, err=None, ignore_errors=False):
        """Query the scheduler for job status.

        Parameters
        ----------
        err : file-like object
            File where status information is printed.
        ignore_errors : bool
            Whether to ignore exceptions raised during status check.

        Returns
        -------
        dict :
            A dictionary of scheduler job information. The keys are scheduler
            job names and the values are instances of :class:`~.JobStatus`.
            If the scheduler cannot be found or an error occurs with
            ``ignore_errors=True``, an empty dic is returned.

        """
        if err is None:
            err = sys.stderr
        try:
            scheduler = self._environment.get_scheduler()
            print("Querying scheduler...", file=err)
            return {
                scheduler_job.name(): scheduler_job.status()
                for scheduler_job in self.scheduler_jobs(scheduler)
            }
        except NoSchedulerError:
            logger.debug("No scheduler available.")
        except RuntimeError as error:
            logger.warning("Error occurred while querying scheduler: '%s'.", error)
            if not ignore_errors:
                raise
        return {}

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
        result = {
            "job_id": job.id,
            "labels": [],
            "_labels_error": None,
        }
        try:
            result["labels"] = sorted(set(self.labels(job)))
        except Exception as error:
            logger.debug(
                "Error while determining labels for job '%s': '%s'.", job, error
            )
            if ignore_errors:
                result["_labels_error"] = str(error)
            else:
                raise
        return result

    def _fetch_status(
        self,
        aggregates,
        err,
        ignore_errors,
        status_parallelization="none",
    ):
        """Fetch status for the provided aggregates / jobs.

        Parameters
        ----------
        aggregates : sequence of aggregates
            The aggregates for which a user requested to fetch status.
        err : file-like object
            File where status information is printed.
        ignore_errors : bool
            Fetch status even if querying the scheduler fails.
        status_parallelization : str
            Parallelization mode for fetching the status. Allowed values are
            "thread", "process", or "none". (Default value = "none")

        Returns
        -------
        status_results : list
            A list of dictionaries containing keys ``aggregate_id``,
            ``groups``, and ``_error``.
        job_labels : list
            A list of dictionaries containing keys ``job_id``, ``labels``,
            and ``_labels_error``.
        individual_jobs : list of :class:`~signac.contrib.job.Job`
            List of jobs, filtered from aggregates containing one job. This
            is used internally to generate labels (labels are only supported
            by individual jobs) and the calling code in
            :meth:`~.print_status` also needs this information. This is
            returned from this method so that iteration over all aggregates
            only has to occur one time.

        """
        if status_parallelization not in ("thread", "process", "none"):
            raise RuntimeError(
                "Configuration value status_parallelization is invalid. "
                "Valid choices are 'thread', 'process', or 'none'."
            )

        parallel_executor = _get_parallel_executor(status_parallelization)

        # Update the project's status cache
        scheduler_info = self._query_scheduler_status(
            err=err, ignore_errors=ignore_errors
        )

        def compute_status(data):
            scheduler_id, scheduler_status, aggregate_id, aggregate, group = data

            status = {}
            error_text = None
            try:
                status["scheduler_status"] = scheduler_status
                completed = group._complete(aggregate)
                status["completed"] = completed
                if len(group.operations) > 1:
                    status["eligible"] = group._eligible(aggregate)
                else:
                    # For groups with only one operation, if the group is not
                    # complete, it is sufficient to determine eligibility while
                    # ignoring postconditions (we know at least one
                    # postcondition is not met).
                    status["eligible"] = not completed and group._eligible(
                        aggregate, ignore_conditions=IgnoreConditions.POST
                    )
            except Exception as error:
                logger.debug(
                    "Error while getting operations status for job '%s': '%s'.",
                    aggregate_id,
                    error,
                )
                if not ignore_errors:
                    raise
                error_text = str(error)
                status["completed"] = False
                status["eligible"] = False
            result = [
                {
                    "aggregate_id": aggregate_id,
                    "group_name": group.name,
                    "status": status,
                    "_error": error_text,
                }
            ]
            if (
                group.name not in self._operations
                and scheduler_status != JobStatus.unknown
            ):
                operation_status = {
                    **status,
                    "scheduler_status": JobStatus._to_group(scheduler_status),
                }
                for op_name in group.operations:
                    result.append(
                        {
                            "aggregate_id": aggregate_id,
                            "group_name": op_name,
                            "status": operation_status,
                            "_error": error_text,
                        }
                    )
            return result

        with self._buffered():
            aggregate_groups = list(
                self._generate_selected_aggregate_groups_with_status(
                    scheduler_info=scheduler_info,
                    selected_aggregates=aggregates,
                )
            )
            status_results = []
            for result in parallel_executor(
                compute_status,
                aggregate_groups,
                desc="Fetching status",
                file=err,
            ):
                status_results.extend(result)
            # aggregate_groups is a list of tuples containing scheduler,
            # aggregate, and group information. To compute labels, we fetch the
            # unique jobs from the aggregates containing only one job.
            if len(self._groups) > 0:
                individual_jobs = {
                    aggregate_group[3][0]
                    for aggregate_group in aggregate_groups
                    if len(aggregate_group[3]) == 1
                }
            else:
                # If no operations exist, use all jobs in the project.
                individual_jobs = set(self)
            compute_labels = functools.partial(
                self._get_job_labels,
                ignore_errors=ignore_errors,
            )
            job_labels = parallel_executor(
                compute_labels,
                individual_jobs,
                desc="Fetching labels",
                file=err,
            )

        def combine_group_and_operation_status(aggregate_status_results):
            group_statuses = {}
            # Iterate over all status results for singleton groups and all user created groups.
            # Given group job statuses being exclusive with respect to standard statuses we can
            # store only the group status (e.g. Group Active) in group submission.
            for status_result in aggregate_status_results:
                group_name = status_result["group_name"]
                # Only true when both a user group and singleton group report a status. We use the
                # one that is not unknown, so we don't store this status and continue.
                if (
                    group_name in group_statuses
                    and status_result["status"]["scheduler_status"] == JobStatus.unknown
                ):
                    continue
                group_statuses[group_name] = status_result["status"]
            return group_statuses

        status_results_combined = []
        for aggregate_id, aggregate_results in groupby(
            sorted(status_results, key=lambda result: result["aggregate_id"]),
            key=lambda result: result["aggregate_id"],
        ):
            aggregate_results = list(aggregate_results)
            # Collect all errors that occurred while evaluating status of
            # groups for this aggregate
            error_message = None
            error_messages = [result["_error"] for result in aggregate_results]
            if any(error_messages):
                error_message = f"{len(error_messages)} error(s): " + ", ".join(
                    error_messages
                )
            status_results_combined.append(
                {
                    "aggregate_id": aggregate_id,
                    "groups": combine_group_and_operation_status(aggregate_results),
                    "_error": error_message,
                }
            )

        return status_results_combined, job_labels, individual_jobs

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
        template=None,
        profile=False,
        eligible_jobs_max_lines=None,
        output_format="terminal",
    ):
        """Print the status of the project.

        Parameters
        ----------
        jobs : iterable of :class:`~signac.contrib.job.Job` or aggregates
            If ``None``, print status for all jobs/aggregates. If not
            ``None``, only print status for the given jobs or aggregates
            (Default value = None).
        overview : bool
            Display an overview of the project status. (Default value = True)
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

        """
        if file is None:
            file = sys.stdout
        if err is None:
            err = sys.stderr

        aggregates = self._convert_jobs_to_aggregates(jobs)

        if eligible_jobs_max_lines is None:
            eligible_jobs_max_lines = self._flow_config["eligible_jobs_max_lines"]

        status_parallelization = self._flow_config["status_parallelization"]

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
                status_results, job_labels, individual_jobs = self._fetch_status(
                    aggregates=aggregates,
                    err=err,
                    ignore_errors=ignore_errors,
                    status_parallelization=status_parallelization,
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
            total_num_hits = sum(hit[2] for hit in hits)

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
            status_results, job_labels, individual_jobs = self._fetch_status(
                aggregates=aggregates,
                err=err,
                ignore_errors=ignore_errors,
                status_parallelization=status_parallelization,
            )
            profiling_results = None

        operations_errors = {status_entry["_error"] for status_entry in status_results}
        labels_errors = {status_entry["_labels_error"] for status_entry in job_labels}
        errors = list(filter(None, operations_errors.union(labels_errors)))

        if errors:
            logger.warning(
                "Some job status updates did not succeed due to errors. Number "
                "of unique errors: %i. Use --debug to list all errors.",
                len(errors),
            )
            for i, error in enumerate(errors):
                logger.debug("Status update error #%i: '%s'", i + 1, error)

        # Get the total number of statuses before removing those with no
        # eligible groups.
        total_num_jobs_or_aggregates = len(status_results)

        def _has_any_eligible_group(status_entry):
            return any(group["eligible"] for group in status_entry["groups"].values())

        if only_incomplete:
            # Remove jobs with no eligible groups from the status info.
            status_results = list(filter(_has_any_eligible_group, status_results))
            total_num_eligible_jobs_or_aggregates = len(status_results)
        else:
            total_num_eligible_jobs_or_aggregates = sum(
                1 for _ in filter(_has_any_eligible_group, status_results)
            )

        def display_group_name(group_name):
            """Return the operation name or group name with number of operations."""
            # If the name is from a group that is not an operation, we append the
            # number of operations to its name in the status.
            if group_name not in self._operations:
                num_operations = len(self._groups[group_name].operations)
                return f"{group_name} ({num_operations} ops)"
            return group_name

        statuses = {}
        # We store the name for display in statuses[aggregate_id][group_name]["display_name"] to
        # prevent the need for a Jinja filter. We store this as an additional parameter as multiple
        # places in the templates need this value including the statuses dictionary itself.
        for status_entry in status_results:
            statuses[status_entry["aggregate_id"]] = status_entry
            group_statuses = status_entry["groups"]
            for group_name, group_status in group_statuses.items():
                group_status["display_name"] = display_group_name(group_name)

        # Add labels to the status information.
        for job_label_data in job_labels:
            job_id = job_label_data["job_id"]
            # There is no status information if the project has no operations.
            # If no status information exists for this job, we need to set
            # default values.
            if job_id in statuses:
                # Don't create label entries for job ids that were removed by
                # --only-incomplete-operations.
                statuses[job_id].setdefault("groups", {})
                statuses[job_id]["labels"] = job_label_data["labels"]

        # If the dump_json variable is set, just dump all status info
        # formatted in JSON to screen.
        if dump_json:
            print(json.dumps(statuses, indent=4), file=file)
            return None

        if overview:
            # get overview info:
            progress = defaultdict(int)
            for status in job_labels:
                for label in status["labels"]:
                    progress[label] += 1
            # Sort the label progress by amount complete (descending), then
            # alphabetically.
            progress_sorted = list(
                islice(
                    sorted(progress.items(), key=lambda x: (-x[1], x[0])),
                    overview_max_lines,
                )
            )

        # Optionally expand parameters argument to all varying parameters.
        if parameters is self.PRINT_STATUS_ALL_VARYING_PARAMETERS:
            parameters = list(
                sorted(
                    {
                        key
                        for job in individual_jobs
                        for key in job.statepoint.keys()
                        if len(
                            {
                                _to_hashable(job.statepoint().get(key))
                                for job in individual_jobs
                            }
                        )
                        > 1
                    }
                )
            )

        if parameters:
            # get parameters info

            def _add_parameters(status):
                aggregate_id = status["aggregate_id"]
                if aggregate_id.startswith("agg-"):
                    # TODO: Fill parameters with empty values (or shared values?)
                    raise ValueError("Cannot show parameters for aggregates.")
                job = self.open_job(id=aggregate_id)
                # Cache the job state point and document if used to render status parameters.
                statepoint = None
                document = None

                def dotted_get(mapping, key):
                    """Fetch a value from a nested mapping using a dotted key."""
                    tokens = key.split(".")
                    v = mapping
                    for token in tokens:
                        if v is None:
                            return None
                        v = v.get(token)
                    return v

                status["parameters"] = {}
                for parameter in parameters:
                    if not parameter.startswith("doc."):
                        if parameter.startswith("sp."):
                            parameter_name = parameter[3:]
                        else:
                            parameter_name = parameter
                        if statepoint is None:
                            statepoint = job.statepoint()
                        status["parameters"][parameter] = shorten(
                            str(self._alias(dotted_get(statepoint, parameter_name))),
                            param_max_width,
                        )
                    else:
                        parameter_name = parameter[4:]
                        if document is None:
                            document = job.document()
                        status["parameters"][parameter] = shorten(
                            str(self._alias(dotted_get(document, parameter_name))),
                            param_max_width,
                        )

            for status in statuses.values():
                _add_parameters(status)

            for i, parameter in enumerate(parameters):
                parameters[i] = shorten(self._alias(str(parameter)), param_max_width)

        if detailed:
            # get detailed view info

            if compact:
                num_operations = len(self._groups)

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

        status_legend = " ".join(f"[{v}]:{k}" for k, v in self.ALIASES.items())
        context["jobs"] = list(statuses.values())
        context["total_num_jobs_or_aggregates"] = total_num_jobs_or_aggregates
        context[
            "total_num_eligible_jobs_or_aggregates"
        ] = total_num_eligible_jobs_or_aggregates
        context["total_num_job_labels"] = len(job_labels)
        context["overview"] = overview
        context["detailed"] = detailed
        context["all_ops"] = all_ops
        context["parameters"] = parameters
        context["compact"] = compact
        context["pretty"] = pretty
        context["unroll"] = unroll
        context["status_legend"] = status_legend
        if overview:
            context["progress_sorted"] = progress_sorted
        if detailed:
            context["alias_bool"] = {True: "Y", False: "N"}
            context["scheduler_status_code"] = _FMT_SCHEDULER_STATUS
            if compact:
                context["extra_num_operations"] = max(num_operations - 1, 0)
            if not unroll:
                context["operation_status_legend"] = operation_status_legend
                context["operation_status_symbols"] = OPERATION_STATUS_SYMBOLS

        def _add_placeholder_operation(job):
            job["groups"][""] = {
                "completed": False,
                "eligible": False,
                "scheduler_status": JobStatus.placeholder,
            }

        for job in context["jobs"]:
            has_eligible_ops = any([v["eligible"] for v in job["groups"].values()])
            if not has_eligible_ops and not context["all_ops"]:
                _add_placeholder_operation(job)

        op_counter = Counter()
        op_submission_status_counter = defaultdict(Counter)
        for job in context["jobs"]:
            for group_name, group_status in job["groups"].items():
                # Exclude placeholder operations, which have no display name.
                if group_name != "":
                    display_name = group_status["display_name"]
                    if group_status["eligible"]:
                        op_counter[display_name] += 1
                    op_submission_status_counter[display_name][
                        group_status["scheduler_status"]
                    ] += 1

        def _op_submission_summary(counter):
            """Generate string of statuses and counts, sorted by status."""
            return ", ".join(
                f"[{_FMT_SCHEDULER_STATUS[status]}]: {count}"
                for status, count in sorted(counter.items())
            )

        op_counter_status = [
            [
                group_name,
                group_count,
                _op_submission_summary(op_submission_status_counter[group_name]),
            ]
            for group_name, group_count in op_counter.most_common(
                eligible_jobs_max_lines
            )
        ]
        context["op_counter"] = op_counter_status
        num_omitted_operations = len(op_counter) - len(context["op_counter"])
        if num_omitted_operations > 0:
            context["op_counter"].append(
                (f"[{num_omitted_operations} more operations omitted]", "", "")
            )

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
        render_output = _render_status(
            template=template,
            template_environment=template_environment_copy,
            context=context,
            detailed=detailed,
            expand=expand,
            unroll=unroll,
            compact=compact,
            output_format=output_format,
        )

        print(render_output, file=file)

        # Show profiling results (if enabled)
        if profiling_results:
            print("\n" + "\n".join(profiling_results), file=file)

    def _run_operations(
        self, operations, pretend=False, np=None, timeout=None, progress=False
    ):
        """Execute the next operations as specified by the project's workflow.

        See also: :meth:`~.run`.

        Parameters
        ----------
        operations : Sequence of instances of :class:`_JobOperation`
            The operations to execute.
        pretend : bool
            Do not actually execute the operations, but show the commands that
            would have been executed (Default value = False).
        np : int
            Parallelize to the specified number of processors. Use -1 to
            parallelize over all available processors. The value None uses one
            processor (Default value = None).
        timeout : float
            An optional timeout for each operation in seconds after which
            execution will be cancelled. Use None to indicate no timeout
            (Default value = None).
        progress : bool
            Show a progress bar during execution (Default value = False).

        """
        if timeout is not None and timeout < 0:
            timeout = None
        operations = list(operations)  # ensure list

        if np is None or np == 1 or pretend:
            if progress:
                operations = tqdm(operations)
            for operation in operations:
                self._execute_operation(operation, timeout, pretend)
        else:
            logger.debug("Parallelized execution of %i operation(s).", len(operations))
            with Pool(processes=cpu_count() if np < 0 else np) as pool:
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

    class _PickleError(Exception):
        """Indicates a pickling error while trying to parallelize the execution of operations."""

    @staticmethod
    def _job_operation_to_tuple(operation):
        return (
            operation.id,
            operation.name,
            [job.id for job in operation._jobs],
            operation.cmd,
            operation.directives,
        )

    def _job_operation_from_tuple(self, data):
        id, name, job_ids, cmd, directives = data
        jobs = tuple(self.open_job(id=job_id) for job_id in job_ids)
        return _JobOperation(
            id, name, jobs, cmd, directives, directives._keys_set_by_user
        )

    def _run_operations_in_parallel(self, pool, operations, progress, timeout):
        """Execute operations in parallel.

        This function executes the given list of operations with the provided
        process pool.

        Since pickling of the project instance is likely to fail, we manually
        pickle the project instance and the operations before submitting them
        to the process pool.

        Parameters
        ----------
        pool : :class:`multiprocessing.Pool`
            Process pool.
        operations : Sequence of instances of :class:`_JobOperation`
            The operations to execute.
        progress : bool
            Show a progress bar during execution.
        timeout : float
            A timeout for each operation in seconds after which
            execution will be cancelled. Use None to indicate no timeout
            (Default value = None).

        """
        try:
            serialized_project = cloudpickle.dumps(self)
            serialized_tasks = [
                (
                    cloudpickle.loads,
                    serialized_project,
                    self._job_operation_to_tuple(operation),
                )
                for operation in tqdm(
                    operations, desc="Serialize tasks", file=sys.stderr
                )
            ]
        except Exception as error:  # Masking all errors since they must be pickling related.
            raise self._PickleError(error)

        results = [
            pool.apply_async(_deserialize_and_run_operation, task)
            for task in serialized_tasks
        ]
        if progress:
            results = tqdm(results)
        for result in results:
            result.get(timeout=timeout)

    @contextlib.contextmanager
    def _run_with_hooks(self, operation):
        name = operation.name
        jobs = operation._jobs

        # Determine operation hooks
        operation_hooks = self._operation_hooks.get(name, _Hooks())

        self.project_hooks.on_start(name, *jobs)
        operation_hooks.on_start(name, *jobs)
        try:
            yield
        except Exception as error:
            self.project_hooks.on_exception(name, error, *jobs)
            operation_hooks.on_exception(name, error, *jobs)
            raise
        else:
            self.project_hooks.on_success(name, *jobs)
            operation_hooks.on_success(name, *jobs)
        finally:
            self.project_hooks.on_exit(name, *jobs)
            operation_hooks.on_exit(name, *jobs)

    def _execute_operation(self, operation, timeout=None, pretend=False):
        """Execute an operation.

        Parameters
        ----------
        operation : :class:`_JobOperation`
            The operation to execute.
        timeout : float
            An optional timeout for each operation in seconds after which
            execution will be cancelled. Use None to indicate no timeout
            (Default value = None).
        pretend : bool
            Do not actually execute the operations, but show the commands that
            would have been executed (Default value = False).

        """
        if pretend:
            print(operation.cmd)
            return None

        logger.info("Execute operation '%s'...", operation)

        execution_error_message = (
            f"An exception was raised during operation {operation.name} for "
            f"job or aggregate with id {get_aggregate_id(operation._jobs)}."
        )

        # Check if we need to fork for operation execution...
        if (
            # The 'fork' directive was provided and evaluates to True:
            operation.directives.get("fork", False)
            # A separate process is needed to cancel with timeout:
            or timeout is not None
            # The operation function is an instance of FlowCmdOperation:
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
            try:
                with self._run_with_hooks(operation):
                    subprocess.run(
                        operation.cmd, shell=True, timeout=timeout, check=True
                    )
            except subprocess.CalledProcessError as error:
                raise UserOperationError(execution_error_message) from error
        else:
            # ... executing operation in interpreter process as function:
            logger.debug(
                "Executing operation '%s' with current interpreter process (%s).",
                operation,
                os.getpid(),
            )
            try:
                with self._run_with_hooks(operation):
                    self._operations[operation.name](*operation._jobs)
            except Exception as error:
                raise UserOperationError(execution_error_message) from error

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
        """Execute all eligible operations for the given selection.

        This function will run in an infinite loop until all eligible
        operations are executed, unless it reaches the maximum number of
        passes per operation or the maximum number of executions.

        By default there is no limit on the total number of executions, but a
        specific operation will only be executed once per job. This is to avoid
        accidental infinite loops when no or faulty postconditions are
        provided.

        Parameters
        ----------
        jobs : iterable of :class:`~signac.contrib.job.Job` or aggregates
            If ``None``, execute operations for all eligible jobs/aggregates.
            If not ``None``, only execute operations for the given jobs or
            aggregates (Default value = None).
        names : iterable of :class:`str`
            Only execute operations that match the provided set of names
            (interpreted as regular expressions), or all if the argument is
            None. (Default value = None)
        pretend : bool
            Do not actually execute the operations, but show the commands that
            would have been executed. (Default value = False)
        np : int
            Parallelize to the specified number of processors. Use -1 to
            parallelize over all available processors. The value None uses one
            processor (Default value = None).
        timeout : float
            An optional timeout for each operation in seconds after which
            execution will be cancelled. Use None to indicate no timeout
            (Default value = None).
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
            * 'cyclic' (order operations cyclic by job)
            * 'random' (shuffle the execution order randomly)
            * callable (a callable returning a comparison key for an
              operation used to sort operations)

            The default value is ``'none'``, which is equivalent to ``'by-job'``
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
        aggregates = self._convert_jobs_to_aggregates(jobs)

        # Get all matching FlowGroups
        if isinstance(names, str):
            raise ValueError(
                "The names argument of FlowProject.run() must be a sequence of strings, "
                "not a string."
            )
        if names is None:
            names = list(self.operations)
        else:
            absent_ops = (set(self._groups.keys()) ^ set(names)) & set(names)
            if absent_ops:
                print(
                    f"Unrecognized flow operation(s): {', '.join(absent_ops)}",
                    file=sys.stderr,
                )

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
            if operation._jobs not in aggregates:
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
                has_postconditions = (
                    len(self.operations[operation.name]._postconditions) > 0
                )
                if not has_postconditions:
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
                    "there are still eligible operations."
                )
                break
            try:
                # Generate _JobOperation instances for selected groups and aggregates.
                with self._buffered():
                    operations = []
                    run_groups = set(self._gather_flow_groups(names))
                    for (
                        aggregate_id,
                        aggregate,
                        group,
                    ) in self._generate_selected_aggregate_groups(
                        selected_aggregates=aggregates,
                        selected_groups=run_groups,
                    ):
                        operations.extend(
                            group._create_run_job_operations(
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
                break  # No more eligible operations or execution limits reached.

            def key_func_by_job(operation):
                # In order to group the aggregates in a by-job manner, we need
                # to first sort the aggregates using their aggregate id.
                return get_aggregate_id(operation._jobs)

            # Optionally re-order operations for execution if order argument is provided:
            if callable(order):
                operations = list(sorted(operations, key=order))
            elif order == "cyclic":
                groups = [
                    list(group)
                    for _, group in groupby(
                        sorted(operations, key=key_func_by_job), key=key_func_by_job
                    )
                ]
                operations = list(_roundrobin(*groups))
            elif order == "random":
                random.shuffle(operations)
            elif order is None or order in ("none", "by-job"):
                # by-job is the default order
                pass
            else:
                raise ValueError(
                    "Invalid value for the 'order' argument, valid arguments are "
                    "'none', 'by-job', 'cyclic', 'random', None, or a callable."
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
        r"""Grabs :class:`~.FlowGroup`\ s that match any of a set of names.

        The provided names can be any regular expressions that fully match a group name.

        Parameters
        ----------
        names : iterable of :class:`str`
            Only select operations that match the provided set of names
            (interpreted as regular expressions), or all if the argument is
            None. (Default value = None)

        Returns
        -------
        list
            List of groups matching the provided names.

        """
        if names is None:
            # If no names are selected, use all singleton groups
            operations = [self._groups[name] for name in self.operations]
        else:
            operations = {}
            for name in names:
                if name in operations:
                    continue
                groups = [
                    group
                    for group_name, group in self.groups.items()
                    if re.fullmatch(name, group_name)
                ]
                if len(groups) > 0:
                    for group in groups:
                        operations[group.name] = group
                else:
                    continue
            operations = list(operations.values())
        if not FlowProject._verify_group_compatibility(operations):
            raise ValueError(
                "Cannot specify groups or operations that will be included "
                "twice when using the -o/--operation option."
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
        r"""Grabs eligible :class:`~._JobOperation`\ s from :class:`~.FlowGroup`\ s.

        Parameters
        ----------
        aggregates : sequence of tuples of :class:`~signac.contrib.job.Job`
            The aggregates to consider for submission.
        default_directives : dict
            The default directives to use for the operations. This is to allow
            for user specified groups to 'inherit' directives from
            ``default_directives``. If no defaults are desired, the argument
            can be set to an empty dictionary. This must be done explicitly,
            however.
        names : iterable of :class:`str`
            Only select operations that match the provided set of names
            (interpreted as regular expressions), or all if the argument is
            None. (Default value = None)
        ignore_conditions : :class:`~.IgnoreConditions`
            Specify if preconditions and/or postconditions are to be ignored
            when determining eligibility. The default is
            :class:`IgnoreConditions.NONE`.
        ignore_conditions_on_execution : :class:`~.IgnoreConditions`
            Specify if preconditions and/or postconditions are to be ignored
            when determining eligibility after submitting. The default is
            :class:`IgnoreConditions.NONE`.

        Yields
        ------
        :class:`~._SubmissionJobOperation`
            Returns a :class:`~._SubmissionJobOperation` for submitting the
            group. The :class:`~._JobOperation` will have directives that have
            been collected appropriately from its contained operations.

        """
        submission_groups = set(self._gather_flow_groups(names))

        # Fetch scheduler status
        scheduler_info = self._query_scheduler_status()
        for (
            scheduler_id,
            scheduler_status,
            aggregate_id,
            aggregate,
            group,
        ) in self._generate_selected_aggregate_groups_with_status(
            scheduler_info=scheduler_info,
            selected_aggregates=aggregates,
            selected_groups=submission_groups,
        ):
            if group._eligible(
                aggregate=aggregate, ignore_conditions=ignore_conditions
            ) and self._eligible_for_submission(
                group, aggregate, scheduler_status, scheduler_info
            ):
                yield group._create_submission_job_operation(
                    entrypoint=self._entrypoint,
                    default_directives=default_directives,
                    jobs=aggregate,
                    ignore_conditions_on_execution=ignore_conditions_on_execution,
                )

    @classmethod
    def _verify_group_compatibility(cls, groups):
        """Verify that all selected groups can be submitted together."""
        return all(a.isdisjoint(b) for a in groups for b in groups if a != b)

    def _get_aggregate_from_id(self, id, check_abbrevations=True):
        # The logic in this function slightly resembles that of signac's
        # Project.open_job.
        # We exit early if the id belongs to a single job.
        if not id.startswith("agg-"):
            try:
                return (self.open_job(id=id),)
            except LookupError:
                raise LookupError(f"Did not find job with id {repr(id)}.")
        # Next, attempt to find the id as a direct match by
        # iterating over all the aggregate stores and trying to access the
        # aggregate ids from those instances.
        for aggregate_store in self._group_to_aggregate_store.inverse:
            try:
                # Assume the id exists and skip the __contains__ check for
                # performance. If the id doesn't exist in this aggregate_store,
                # it will raise an exception that can be ignored.
                return aggregate_store[id]
            except KeyError:
                pass
        if check_abbrevations:
            # No direct match was found, so check for abbreviated ids. Requires
            # iteration over all elements of all aggregate stores.
            matches = set()
            for aggregate_store in self._group_to_aggregate_store.inverse:
                for full_id in aggregate_store:
                    if full_id.startswith(id):
                        matches.add(aggregate_store[full_id])
            if len(matches) == 1:
                return next(iter(matches))
            elif len(matches) > 1:
                raise LookupError(f"Did not find aggregate with id {repr(id)}.")
            # By elimination, len(matches) == 0
        raise KeyError(f"Did not find aggregate with id {repr(id)}.")

    def _convert_jobs_to_aggregates(self, jobs):
        """Convert sequences of signac jobs to aggregates.

        The ``jobs`` parameter in public methods like :meth:`~.run`,
        :meth:`~.submit`, and :meth:`~.print_status` accepts a sequence of
        signac jobs. This method converts that sequence into a sequence of
        aggregates (tuples containing single signac jobs).
        """
        if jobs is None:
            return _AggregateStoresCursor(self)
        elif isinstance(jobs, _AggregatesCursor):
            return jobs

        # Handle user-provided jobs/aggregates
        aggregates = []
        for aggregate in jobs:
            if isinstance(aggregate, signac.contrib.job.Job):
                # aggregate is a single signac job.
                if aggregate not in self:
                    raise LookupError(f"Did not find job {aggregate} in the project")
                aggregates.append((aggregate,))
            else:
                try:
                    aggregate = tuple(aggregate)
                    assert all(
                        isinstance(job, signac.contrib.job.Job) for job in aggregate
                    )
                except (AssertionError, TypeError) as error:
                    raise TypeError(
                        "Invalid jobs argument. Please provide a valid "
                        "signac job or aggregate of jobs."
                    ) from error
                else:
                    # aggregate is a tuple of signac jobs.
                    # Ensure that the aggregate exists in one of the aggregate
                    # stores associated with this project. This will raise an
                    # error if not.
                    aggregate_from_id = self._get_aggregate_from_id(
                        get_aggregate_id(aggregate), check_abbrevations=False
                    )
                    aggregates.append(aggregate_from_id)
        return aggregates

    @contextlib.contextmanager
    def _buffered(self):
        """Enable the use of buffered mode for certain functions."""
        logger.debug("Entering buffered mode.")
        with signac.buffered():
            yield
        logger.debug("Exiting buffered mode.")

    def _generate_submit_script(
        self, _id, operations, template, show_template_help, **kwargs
    ):
        """Generate submission script to submit the execution of operations to a scheduler."""
        if template is None:
            template = self._environment.template
        assert _id is not None

        template_environment = self._template_environment(self._environment)
        template = template_environment.get_template(template)
        context = self._get_standard_template_context()
        # The flow 'script.sh' file simply extends the base script
        # provided. The choice of base script is dependent on the
        # environment, but will default to the 'base_script.sh' provided
        # with signac-flow unless additional environment information is
        # detected.

        logger.info("Set 'base_script=%s'.", self._environment.template)
        logger.info("Use environment '%s'.", self._environment)
        context["base_script"] = self._environment.template
        context["environment"] = self._environment
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
        parallel=False,
        flags=None,
        force=False,
        template="script.sh",
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
        parallel : bool
            Execute all bundled operations in parallel. (Default value = False)
        flags : list
            Additional options to be forwarded to the scheduler. (Default value = None)
        force : bool
            Ignore all warnings or checks during submission, just submit. (Default value = False)
        template : str
            The name of the template file to be used to generate the submission
            script. (Default value = "script.sh")
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
            return self._environment.submit(
                _id=_id, script=script, flags=flags, **kwargs
            )

    def submit(
        self,
        bundle_size=1,
        jobs=None,
        names=None,
        num=None,
        parallel=False,
        force=False,
        ignore_conditions=IgnoreConditions.NONE,
        ignore_conditions_on_execution=IgnoreConditions.NONE,
        **kwargs,
    ):
        r"""Submit function for the project's main submit interface.

        Parameters
        ----------
        bundle_size : int
            Specify the number of operations to be bundled into one submission, defaults to 1.
        jobs : iterable of :class:`~signac.contrib.job.Job` or aggregates
            If ``None``, submit operations for all eligible jobs/aggregates.
            If not ``None``, only submit operations for the given jobs or
            aggregates (Default value = None).
        names : iterable of :class:`str`
            Only submit operations that match the provided set of names
            (interpreted as regular expressions), or all if the argument is
            None. (Default value = None)
        num : int
            Limit the total number of submitted operations, defaults to no limit.
        parallel : bool
            Execute all bundled operations in parallel. (Default value = False)
        force : bool
            Ignore all warnings or checks during submission, just submit. (Default value = False)
        ignore_conditions : :class:`~.IgnoreConditions`
            Specify if preconditions and/or postconditions are to be ignored
            when determining eligibility. The default is
            :class:`IgnoreConditions.NONE`.
        ignore_conditions_on_execution : :class:`~.IgnoreConditions`
            Specify if preconditions and/or postconditions are to be ignored
            when determining eligibility after submitting. The default is
            :class:`IgnoreConditions.NONE`.
        \*\*kwargs
            Additional keyword arguments forwarded to :meth:`~.ComputeEnvironment.submit`.

        """
        if names is not None:
            absent_ops = (set(self._groups.keys()) ^ set(names)) & set(names)
            if absent_ops:
                print(
                    f"Unrecognized flow operation(s): {', '.join(absent_ops)}",
                    file=sys.stderr,
                )

        aggregates = self._convert_jobs_to_aggregates(jobs)

        # Regular argument checks and expansion
        if isinstance(names, str):
            raise ValueError(
                "The 'names' argument must be a sequence of strings, however "
                f"a single string was provided: {names}."
            )

        if not isinstance(ignore_conditions, IgnoreConditions):
            raise ValueError(
                "The ignore_conditions argument of FlowProject.run() "
                "must be a member of class IgnoreConditions."
            )

        # Gather all eligible operations.
        with self._buffered():
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
        with self._buffered():
            with self._update_cached_scheduler_status() as status_update:
                for bundle in _make_bundles(operations, bundle_size):
                    status = self._submit_operations(
                        operations=bundle,
                        parallel=parallel,
                        force=force,
                        **kwargs,
                    )
                    if status is not None:
                        # Operations were submitted, store status
                        for operation in bundle:
                            status_update[operation.id] = status

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
            "scheduler job. When this option is provided without an argument, "
            "all eligible operations are combined into one bundle.",
        )
        bundling_group.add_argument(
            "-p",
            "--parallel",
            action="store_true",
            help="Execute all operations in a single bundle in parallel.",
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
            help="Display select parameters of the job state point "
            "(with optional prefix 'sp.') or job document (by using prefix 'doc.') "
            "in the detailed view.",
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

    def _next_operations(
        self, jobs=None, operation_names=None, ignore_conditions=IgnoreConditions.NONE
    ):
        """Determine the next eligible operations for aggregates.

        Parameters
        ----------
        jobs : tuple of :class:`~signac.contrib.job.Job`
            The signac job handles. By default all the aggregates are evaluated
            to get the next operation associated.
        operation_names : iterable of :class:`str`
            Only select operations that match the provided set of names
            (interpreted as regular expressions), or all single operation
            groups if the argument is None. (Default value = None)
        ignore_conditions : :class:`~.IgnoreConditions`
            Specify if preconditions and/or postconditions are to be ignored
            when determining eligibility. The default is
            :class:`IgnoreConditions.NONE`.

        Yields
        ------
        :class:`~._JobOperation`
            All eligible operations for the provided jobs.

        """
        if jobs is None:
            jobs = _AggregateStoresCursor(self)
        if operation_names is None:
            selected_groups = {self._groups[name] for name in self.operations}
        else:
            selected_groups = set(self._gather_flow_groups(operation_names))
        for aggregate_id, aggregate, group, in self._generate_selected_aggregate_groups(
            selected_aggregates=jobs,
            selected_groups=selected_groups,
        ):
            yield from group._create_run_job_operations(
                entrypoint=self._entrypoint,
                default_directives={},
                jobs=aggregate,
                ignore_conditions=ignore_conditions,
            )

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
    def _collect_preconditions(cls):
        """Collect all preconditions added with the ``@FlowProject.pre`` decorator."""
        return cls._collect_conditions("_OPERATION_PRECONDITIONS")

    @classmethod
    def _collect_postconditions(cls):
        """Collect all postconditions added with the ``@FlowProject.post`` decorator."""
        return cls._collect_conditions("_OPERATION_POSTCONDITIONS")

    def _register_operations(self):
        """Register all operation functions registered with this class and its parent classes."""
        operations = self._collect_operations()
        preconditions = self._collect_preconditions()
        postconditions = self._collect_postconditions()

        for name, func in operations:
            if name in self._operations:
                raise FlowProjectDefinitionError(
                    f"Repeat definition of operation with name '{name}'."
                )

            # Extract preconditions/postconditions and directives from function:
            params = {
                "pre": preconditions.get(func, None),
                "post": postconditions.get(func, None),
            }

            # Update operation hooks
            self._operation_hooks[name].update(
                _Hooks(**self._OPERATION_HOOK_REGISTRY[func])
            )

            # Construct FlowOperation:
            if getattr(func, "_flow_cmd", False):
                self._operations[name] = FlowCmdOperation(cmd=func, **params)
            else:
                self._operations[name] = FlowOperation(op_func=func, **params)

    @classmethod
    def make_group(cls, name, submit_options="", run_options="", group_aggregator=None):
        r"""Make a :class:`~.FlowGroup` named ``name`` and return a decorator to make groups.

        A :class:`~.FlowGroup` is used to group operations together for
        running and submitting :class:`~._JobOperation`\ s.

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
        submit_options : str
            The :meth:`FlowProject.run` options to pass when submitting the group. These will be
            included in all submissions. Submissions use run commands to execute.
        run_options : str
            The options to pass to ``entrypoint exec`` when running the group. Specifying this will
            cause the operation to be forked even if it otherwise would run in the current Python
            interpreter.
        group_aggregator : :class:`~.aggregator`
            An instance of :class:`~flow.aggregator` to associate with the :class:`FlowGroup`.
            If None, no aggregation takes place (Default value = None).

        Returns
        -------
        :class:`~.FlowGroupEntry`
            The created group.

        """
        if name in cls._GROUP_NAMES:
            raise FlowProjectDefinitionError(
                f"Repeat definition of group with name '{name}'."
            )
        if any(
            name == operation_name for operation_name, _ in cls._OPERATION_FUNCTIONS
        ):
            raise FlowProjectDefinitionError(
                f"Cannot create a group with the same name as the existing operation '{name}'."
            )
        cls._GROUP_NAMES.add(name)
        group_entry = FlowGroupEntry(
            name=name,
            project=cls,
            submit_options=submit_options,
            run_options=run_options,
            group_aggregator=group_aggregator,
        )
        cls._GROUPS.append(group_entry)
        return group_entry

    def _register_groups(self):
        """Register all groups.

        Operations are assigned to each group.

        Aggregators are created for each group and tracked in a bidirectional mapping.

        """
        group_entries = []
        # Gather all groups from class and parent classes.
        for cls in type(self).__mro__:
            group_entries.extend(getattr(cls, "_GROUPS", []))

        # Initialize all groups without operations. Also store the aggregators
        # associated with each group. The aggregate stores are cached so that
        # equivalent aggregators only generate once.
        created_aggregate_stores = {}
        for entry in group_entries:
            group = FlowGroup(
                entry.name,
                submit_options=entry.submit_options,
                run_options=entry.run_options,
            )
            self._groups[entry.name] = group
            # Handle unset aggregators
            if entry.group_aggregator is None:
                # Use the operation's aggregator for singleton groups
                # corresponding to single operations
                if entry.name in self._operations:
                    operation = self._operations[entry.name]
                    if isinstance(operation, FlowCmdOperation):
                        entry.group_aggregator = operation._cmd._flow_aggregate
                    else:
                        entry.group_aggregator = operation._op_func._flow_aggregate
                # The default group aggregator just iterates over jobs
                else:
                    entry.group_aggregator = aggregator.groupsof()
            if entry.group_aggregator not in created_aggregate_stores:
                created_aggregate_stores[
                    entry.group_aggregator
                ] = entry.group_aggregator._create_AggregateStore(self)
            # Associate the group with its aggregate store
            self._group_to_aggregate_store[group] = created_aggregate_stores[
                entry.group_aggregator
            ]

        # Add operations and directives to group
        for operation_name, operation in self._operations.items():
            if isinstance(operation, FlowCmdOperation):
                func = operation._cmd
            else:
                func = operation._op_func

            op_directives = getattr(func, "_flow_group_operation_directives", {})
            for cls in self.__class__.__mro__:
                # Need to use `get` since we don't know which class in the
                # hierarchy this function was registered to.
                for group_name in func._flow_groups.get(cls, []):
                    directives = op_directives.get(group_name)
                    self._groups[group_name].add_operation(
                        operation_name, operation, directives
                    )

            # For singleton groups add directives
            directives = getattr(func, "_flow_directives", {})
            self._groups[operation_name].operation_directives[
                operation_name
            ] = directives

    def _reregister_aggregates(self):
        """Re-register the aggregates present in this :class:`~.FlowProject`."""
        # TODO: This method could be consolidated with the code in _register_groups.
        # For now, we will not put it into the public API.
        for group in self._groups.values():
            aggregator = self._group_to_aggregate_store[group]
            if isinstance(aggregator, _AggregateStore):
                aggregator._register_aggregates()
                self._group_to_aggregate_store[group] = aggregator

    @property
    def operations(self):
        """Get the dictionary of operations that have been added to the workflow."""
        return self._operations

    @property
    def groups(self):
        """Get the dictionary of groups that have been added to the workflow."""
        return self._groups

    def _eligible_for_submission(
        self, flow_group, jobs, scheduler_status, cached_status
    ):
        """Check group eligibility for submission with an aggregate.

        By default, a group is eligible for submission when it is not
        considered active, that means already queued or running.

        Parameters
        ----------
        flow_group : :class:`~.FlowGroup`
            The FlowGroup used to determine eligibility.
        aggregate : tuple of :class:`~signac.contrib.job.Job`
            The aggregate of signac jobs.
        scheduler_status : :class:`~.JobStatus`
            The status of the provided group and aggregate (this should be
            known by the calling code and is re-used instead of fetching from
            the ``cached_status`` for efficiency).
        cached_status : dict
            Dictionary of status information. The keys are uniquely
            generated ids for each group and aggregate. The values are instances of
            :class:`~.JobStatus`.

        Returns
        -------
        bool
            Whether the group is eligible for submission with the provided aggregate.

        """

        def _group_is_submitted(flow_group):
            """Check if group has been submitted for the provided jobs."""
            group_id = flow_group._generate_id(jobs)
            job_status = JobStatus(cached_status.get(group_id, JobStatus.unknown))
            return job_status >= JobStatus.submitted

        if scheduler_status >= JobStatus.submitted:
            return False

        # Check if any other groups containing an operation from this group
        # have been submitted. Submitting both groups might cause conflicts.
        for other_group in self._groups.values():
            if not flow_group.isdisjoint(other_group) and _group_is_submitted(
                other_group
            ):
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
        args = {
            key: val
            for key, val in vars(args).items()
            if key
            not in [
                "func",
                "verbose",
                "debug",
                "job_id",
                "filter",
                "doc_filter",
                "show_traceback",
            ]
        }
        if args.pop("full"):
            args["detailed"] = args["all_ops"] = True

        start = time.time()
        try:
            self.print_status(jobs=aggregates, **args)
        except Exception as error:
            logger.error(
                f"Error during status update: {str(error)}\nUse '--ignore-errors' to "
                "complete the update anyways."
            )
            raise error
        else:
            if aggregates is None:
                length_jobs = sum(
                    len(aggregate_store)
                    for aggregate_store in self._group_to_aggregate_store.inverse
                )
            else:
                length_jobs = len(aggregates)
            # Use small offset to account for overhead with few jobs
            delta_t = (time.time() - start - 0.5) / max(length_jobs, 1)
            config_key = "status_performance_warn_threshold"
            warn_threshold = self._flow_config[config_key]
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
        if args.name not in self.operations:
            print(
                f"The requested flow operation '{args.name}' does not exist.",
                file=sys.stderr,
            )
        else:
            for operation in self._next_operations():
                # This filter cannot use the operation_names parameter to
                # _next_operations because it must be an exact match, not a
                # regex match.
                if args.name == operation.name:
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
            with _add_cwd_to_environment_pythonpath():
                with _switch_to_directory(self.root_directory()):
                    run()
        else:
            run()

    def _main_submit(self, args):
        """Submit jobs to a scheduler."""
        kwargs = vars(args)

        # Select jobs:
        aggregates = self._select_jobs_from_args(args)
        names = args.operation_name if args.operation_name else None
        self.submit(jobs=aggregates, names=names, **kwargs)

    def _main_exec(self, args):
        aggregates = self._select_jobs_from_args(args)
        try:
            operation = self._operations[args.operation]

            if isinstance(operation, FlowCmdOperation):

                def operation_function(job):
                    cmd = operation(job)
                    subprocess.run(cmd, shell=True, check=True)

            else:
                operation_function = operation

        except KeyError:
            raise KeyError(f"Unknown operation '{args.operation}'.")

        for aggregate_id, aggregate, group in self._generate_selected_aggregate_groups(
            selected_aggregates=aggregates,
            selected_groups={self._groups[args.operation]},
        ):
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
            for job_id in args.job_id:
                try:
                    aggregates.add(
                        self._get_aggregate_from_id(job_id, check_abbrevations=True)
                    )
                except KeyError as error:
                    raise LookupError(error)
            return list(aggregates)
        elif args.func == self._main_exec:
            # exec command does not support filters, so we must exit early.
            return _AggregateStoresCursor(self)
        elif args.filter or args.doc_filter:
            # filter or doc_filter provided. Filters can only be used to select
            # single jobs and not aggregates of multiple jobs.
            filter_ = parse_filter_arg(args.filter)
            doc_filter = parse_filter_arg(args.doc_filter)
            return _JobAggregateCursor(self, filter_, doc_filter)
        else:
            # Use all aggregates
            return _AggregateStoresCursor(self)

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
                dest="show_traceback",
                action="store_true",
                help="No op. Exists to be backwards comaptible with signac-flow version <= 0.21.",
            )
            _parser.add_argument(
                "--debug",
                dest=prefix + "debug",
                action="store_true",
                help="This option implies `-vv`.",
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
            type=float,
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
            "processing units.",
        )
        execution_group.add_argument(
            "--order",
            type=str,
            choices=["none", "by-job", "cyclic", "random"],
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
            help="The job ids or aggregate ids in the FlowProject. "
            "Defaults to all jobs and aggregates.",
        )
        parser_exec.set_defaults(func=self._main_exec)

        args = parser.parse_args()
        if not hasattr(args, "func"):
            parser.print_usage()
            sys.exit(2)

        if args.show_traceback:
            warnings.warn(
                "--show-traceback is deprecated and to be removed in signac-flow version 0.23.",
                FutureWarning,
            )

        # Manually 'merge' the various global options defined for both the main parser
        # and the parent parser that are shared by all subparsers:
        for dest in ("verbose", "debug"):
            setattr(args, dest, getattr(args, "main_" + dest) or getattr(args, dest))
            delattr(args, "main_" + dest)

        if args.debug:  # Implies '-vv'
            args.verbose = max(2, args.verbose)

        # Support print_status argument alias
        if args.func == self._main_status and args.full:
            args.detailed = args.all_ops = True

        # Empty parameters argument on the command line means: show all varying parameters.
        if hasattr(args, "parameters"):
            if args.parameters is not None and len(args.parameters) == 0:
                args.parameters = self.PRINT_STATUS_ALL_VARYING_PARAMETERS

        # Set verbosity level according to the `-v` argument.
        logging.basicConfig(level=max(0, logging.WARNING - 10 * args.verbose))

        try:
            args.func(args)
        except (TimeoutError, subprocess.TimeoutExpired) as error:
            print(
                "Error: Failed to complete execution due to "
                f"timeout ({args.timeout} seconds).",
                file=sys.stderr,
            )
            raise error
        except Jinja2TemplateNotFound as error:
            print(f"Did not find template script '{error}'.", file=sys.stderr)
            raise error


def _deserialize_and_run_operation(loads, project, operation_data):
    project = loads(project)
    project._execute_operation(project._job_operation_from_tuple(operation_data))
    return None


__all__ = [
    "FlowProject",
    "FlowOperation",
    "label",
    "staticlabel",
    "classlabel",
]
