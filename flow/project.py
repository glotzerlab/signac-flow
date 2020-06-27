# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Workflow definition with the FlowProject.

The FlowProject is a signac Project that allows the user to define a workflow.
"""
import sys
import os
import re
import logging
import argparse
import time
import datetime
import json
import inspect
import functools
import contextlib
import random
import subprocess
import traceback
import warnings
from deprecation import deprecated
from collections import defaultdict
from collections import OrderedDict
from collections import Counter
from copy import deepcopy
from itertools import islice
from itertools import count
from itertools import groupby
from hashlib import sha1
import multiprocessing
import threading
from multiprocessing import Pool
from multiprocessing import cpu_count
from multiprocessing import TimeoutError
from multiprocessing.pool import ThreadPool
from multiprocessing import Event
import jinja2
from jinja2 import TemplateNotFound as Jinja2TemplateNotFound
from tqdm import tqdm

import signac
from signac.contrib.hashing import calc_id
from signac.contrib.filterparse import parse_filter_arg
from signac.contrib.project import JobsCursor

from enum import IntEnum

from .environment import get_environment
from .scheduling.base import ClusterJob
from .scheduling.base import JobStatus
from .scheduling.status import update_status
from .errors import SubmitError
from .errors import ConfigKeyError
from .errors import NoSchedulerError
from .errors import UserConditionError
from .errors import UserOperationError
from .errors import TemplateError
from .util.misc import _positive_int
from .util.misc import roundrobin
from .util.misc import to_hashable
from .util import template_filters as tf
from .util.misc import add_cwd_to_environment_pythonpath
from .util.misc import switch_to_directory
from .util.misc import TrackGetItemDict
from .util.translate import abbreviate
from .util.translate import shorten
from .labels import label
from .labels import staticlabel
from .labels import classlabel
from .labels import _is_label_func
from .util import config as flow_config
from .render_status import Renderer as StatusRenderer
from .version import __version__


logger = logging.getLogger(__name__)


# The TEMPLATE_HELP can be shown with the --template-help option available to all
# command line sub commands that use the templating system.
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
    JobStatus.unknown: 'U',
    JobStatus.registered: 'R',
    JobStatus.inactive: 'I',
    JobStatus.submitted: 'S',
    JobStatus.held: 'H',
    JobStatus.queued: 'Q',
    JobStatus.active: 'A',
    JobStatus.error: 'E',
    JobStatus.dummy: ' ',
}


# TODO: When we drop Python 3.5 support, change this to inherit from IntFlag
# instead of IntEnum so that we can remove the operator overloads.
class IgnoreConditions(IntEnum):
    """Flags that determine which conditions are used to determine job eligibility.

    The options include:
        * IgnoreConditions.NONE: check all conditions
        * IgnoreConditions.PRE: ignore pre conditions
        * IgnoreConditions.POST: ignore post conditions
        * IgnoreConditions.ALL: ignore all conditions
    """
    # The following three functions can be removed once we drop Python 3.5
    # support, they are implemented by the IntFlag class in Python > 3.6.
    def __or__(self, other):
        if not isinstance(other, (self.__class__, int)):
            return NotImplemented
        result = self.__class__(self._value_ | self.__class__(other)._value_)
        return result

    def __and__(self, other):
        if not isinstance(other, (self.__class__, int)):
            return NotImplemented
        return self.__class__(self._value_ & self.__class__(other)._value_)

    def __xor__(self, other):
        if not isinstance(other, (self.__class__, int)):
            return NotImplemented
        return self.__class__(self._value_ ^ self.__class__(other)._value_)

    # This operator must be defined since IntFlag simply performs an integer
    # bitwise not on the underlying enum value, which is problematic in
    # twos-complement arithmetic. What we want is to only flip valid bits.
    def __invert__(self):
        # Compute the largest number of bits use to represent one of the flags
        # so that we can XOR the appropriate number.
        max_bits = len(bin(max([elem.value for elem in type(self)]))) - 2
        return self.__class__((2**max_bits - 1) ^ self._value_)

    NONE = 0
    PRE = 1
    POST = 2
    ALL = PRE | POST

    def __str__(self):
        return {IgnoreConditions.PRE: 'pre', IgnoreConditions.POST: 'post',
                IgnoreConditions.ALL: 'all', IgnoreConditions.NONE: 'none'}[self]


class _IgnoreConditionsConversion(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super(_IgnoreConditionsConversion, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, getattr(IgnoreConditions, values.upper()))


class _condition(object):
    # This counter should be incremented each time an "always" or "never"
    # condition is created, and the value should be used as the tag for that
    # condition to ensure that no pair of "always" and "never" conditions
    # are found to be equal by the graph detection algorithm.
    current_arbitrary_tag = 0

    def __init__(self, condition, tag=None):
        """Add tag to differentiate built-in conditions during graph detection."""

        if tag is None:
            try:
                tag = condition.__code__.co_code
            except AttributeError:
                logger.warning("Condition {} could not autogenerate tag.".format(condition))
        condition._flow_tag = tag
        self.condition = condition

    @classmethod
    def isfile(cls, filename):
        "True if the specified file exists for this job."
        return cls(lambda job: job.isfile(filename), 'isfile_' + filename)

    @classmethod
    def true(cls, key):
        """True if the specified key is present in the job document and
        evaluates to True."""
        return cls(lambda job: job.document.get(key, False), 'true_' + key)

    @classmethod
    def false(cls, key):
        """True if the specified key is present in the job document and
        evaluates to False."""
        return cls(lambda job: not job.document.get(key, False), 'false_' + key)

    @classmethod
    @deprecated(
        deprecated_in="0.9", removed_in="0.11", current_version=__version__,
        details="This condition decorator is obsolete.")
    def always(cls, func):
        "Returns True."
        cls.current_arbitrary_tag += 1
        return cls(lambda _: True, str(cls.current_arbitrary_tag))(func)

    @classmethod
    def never(cls, func):
        "Returns False."
        cls.current_arbitrary_tag += 1
        return cls(lambda _: False, str(cls.current_arbitrary_tag))(func)

    @classmethod
    def not_(cls, condition):
        "Returns ``not condition(job)`` for the provided condition function."
        return cls(lambda job: not condition(job),
                   'not_'.encode() + condition.__code__.co_code)


def _create_all_metacondition(condition_dict, *other_funcs):
    """Standard function for generating aggregate metaconditions that require
    *all* provided conditions to be met. The resulting metacondition is
    constructed with appropriate information for graph detection."""
    condition_list = [c for f in other_funcs for c in condition_dict[f]]

    def _flow_metacondition(job):
        return all(c(job) for c in condition_list)

    _flow_metacondition._composed_of = condition_list
    return _flow_metacondition


def _make_bundles(operations, size=None):
    """Utility function for the generation of bundles.

    This function splits an iterable of operations into equally
    sized bundles and a possibly smaller final bundle.
    """
    n = None if size == 0 else size
    operations = iter(operations)
    while True:
        b = list(islice(operations, n))
        if b:
            yield b
        else:
            break


@deprecated(deprecated_in="0.9", removed_in="0.11", current_version=__version__)
def make_bundles(operations, size=None):
    """Utility function for the generation of bundles.

    This function splits an iterable of operations into equally
    sized bundles and a possibly smaller final bundle.
    """
    return _make_bundles(operations, size)


class JobOperation(object):
    """This class represents the information needed to execute one group for one job.

    The execution or submission of a :py:class:`FlowGroup` uses a passed in command
    which can either be a string or function with no arguments that returns a shell
    executable command.  This won't be used if it is determined that the group can be
    executed without forking.

    .. note::

        This class is used by the :class:`~.FlowGroup` class for the execution and
        submission process and should not be instantiated by users themselves.

    :param id:
        The id of this JobOperation instance. The id should be unique.
    :type id:
        str
    :param name:
        The name of the JobOperation.
    :type name:
        str
    :param job:
        The job instance associated with this operation.
    :type job:
        :py:class:`signac.Job`.
    :param cmd:
        The command that executes this operation. Can be a function that when
        evaluated returns a string.
    :type cmd:
        callable or str
    :param directives:
        A dictionary of additional parameters that provide instructions on how
        to execute this operation, e.g., specifically required resources.
    :type directives:
        :class:`dict`
    """

    def __init__(self, id, name, job, cmd, directives=None):
        self._id = id
        self.name = name
        self.job = job
        if not (callable(cmd) or isinstance(cmd, str)):
            raise ValueError("JobOperation cmd must be a callable or string.")
        self._cmd = cmd

        if directives is None:
            directives = dict()  # default argument
        else:
            directives = dict(directives)  # explicit copy

        # Keys which were explicitly set by the user, but are not evaluated by the
        # template engine are cause for concern and might hint at a bug in the template
        # script or ill-defined directives. We are therefore keeping track of all
        # keys set by the user and check whether they have been evaluated by the template
        # script engine later.
        keys_set_by_user = set(directives)

        # We use a special dictionary that allows us to track all keys that have been
        # evaluated by the template engine and compare them to those explicitly set
        # by the user. See also comment above.
        self.directives = TrackGetItemDict(
            {key: value for key, value in directives.items()})
        self.directives._keys_set_by_user = keys_set_by_user

    def __str__(self):
        return "{}({})".format(self.name, self.job)

    def __repr__(self):
        return "{type}(name='{name}', job='{job}', cmd={cmd}, directives={directives})".format(
                   type=type(self).__name__,
                   name=self.name,
                   job=str(self.job),
                   cmd=repr(self.cmd),
                   directives=self.directives)

    def __hash__(self):
        return int(sha1(self.id.encode('utf-8')).hexdigest(), 16)

    def __eq__(self, other):
        return self.id == other.id

    @deprecated(deprecated_in="0.9", removed_in="0.11", current_version=__version__)
    def get_id(self):
        return self._id

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
        else:
            return self._cmd

    def set_status(self, value):
        "Store the operation's status."
        self.job._project.document.setdefault('_status', dict())[self.id] = int(value)

    def get_status(self):
        "Retrieve the operation's last known status."
        try:
            return JobStatus(self.job._project.document['_status'][self.id])
        except KeyError:
            return JobStatus.unknown


class SubmissionJobOperation(JobOperation):
    R"""This class represents the information needed to submit one group for one job.

    This class extends :py:class:`JobOperation` to include a set of groups
    that will be executed via the "run" command. These groups are known at
    submission time.

    :param \*args:
        Passed to the constructor of :py:class:`JobOperation`.
    :param eligible_operations:
        A list of :py:class:`JobOperation` that will be executed when this
        submitted job is executed.
    :type eligible_operations:
        list
    :param operations_with_unmet_preconditions:
        A list of :py:class:`JobOperation` that will not be executed in the
        first pass of :meth:`FlowProject.run` due to unmet preconditions. These
        operations may be executed in subsequent iterations of the run loop.
    :type operations_with_unmet_preconditions:
        list
    :param operations_with_met_postconditions:
        A list of :py:class:`JobOperation` that will not be executed in the
        first pass of :meth:`FlowProject.run` because all postconditions are
        met. These operations may be executed in subsequent iterations of the
        run loop.
    :type operations_with_met_postconditions:
        list
    :param \*\*kwargs:
        Passed to the constructor of :py:class:`JobOperation`.
    """

    def __init__(
        self,
        *args,
        eligible_operations=None,
        operations_with_unmet_preconditions=None,
        operations_with_met_postconditions=None,
        **kwargs
    ):
        super(SubmissionJobOperation, self).__init__(*args, **kwargs)

        if eligible_operations is None:
            self.eligible_operations = []
        else:
            self.eligible_operations = eligible_operations

        if operations_with_unmet_preconditions is None:
            self.operations_with_unmet_preconditions = []
        else:
            self.operations_with_unmet_preconditions = operations_with_unmet_preconditions

        if operations_with_met_postconditions is None:
            self.operations_with_met_postconditions = []
        else:
            self.operations_with_met_postconditions = operations_with_met_postconditions


class FlowCondition(object):
    """A FlowCondition represents a condition as a function of a signac job.

    The __call__() function of a FlowCondition object may return either True
    or False, representing whether the condition is met or not.
    This can be used to build a graph of conditions and operations.

    :param callback:
        A callable with one positional argument (the job).
    :type callback:
        callable
    """

    def __init__(self, callback):
        self._callback = callback

    def __call__(self, job):
        if self._callback is None:
            return True
        try:
            return self._callback(job)
        except Exception as e:
            raise UserConditionError(
                'An exception was raised while evaluating the condition {name} '
                'for job {job}.'.format(name=self._callback.__name__, job=job)) from e

    def __hash__(self):
        return hash(self._callback)

    def __eq__(self, other):
        return self._callback == other._callback


class BaseFlowOperation(object):
    """A BaseFlowOperation represents a data space operation, operating on any job.

    Every BaseFlowOperation is associated with a specific command.

    Pre-requirements (pre) and post-conditions (post) can be used to
    trigger an operation only when certain conditions are met. Conditions are unary
    callables, which expect an instance of job as their first and only positional
    argument and return either True or False.

    An operation is considered "eligible" for execution when all pre-requirements
    are met and when at least one of the post-conditions is not met.
    Requirements are always met when the list of requirements is empty and
    post-conditions are never met when the list of post-conditions is empty.

    .. note::
        This class should not be instantiated directly.

    :param cmd:
        The command to execute operation; should be a function of job.
    :type cmd:
        str or callable
    :param pre:
        required conditions
    :type pre:
        sequence of callables
    :param post:
        post-conditions to determine completion
    :type post:
        sequence of callables
    """

    def __init__(self, pre=None, post=None):
        if pre is None:
            pre = []
        if post is None:
            post = []

        self._prereqs = [FlowCondition(cond) for cond in pre]
        self._postconds = [FlowCondition(cond) for cond in post]

    def __str__(self):
        return "{type}(cmd='{cmd}')".format(type=type(self).__name__, cmd=self._cmd)

    def eligible(self, job, ignore_conditions=IgnoreConditions.NONE):
        """Eligible, when all pre-conditions are true and at least one post-condition is false,
        or corresponding conditions are ignored.
        :param job:
            The signac job handles.
        :type job:
            :class:`~signac.contrib.job.Job`
        :param ignore_conditions:
            Specify if pre and/or post conditions check is to be ignored for eligibility check.
            The default is :py:class:`IgnoreConditions.NONE`.
        :type ignore_conditions:
            :py:class:`~.IgnoreConditions`
        """
        if type(ignore_conditions) != IgnoreConditions:
            raise ValueError(
                "The ignore_conditions argument of FlowProject.run() "
                "must be a member of class IgnoreConditions")
        # len(self._prereqs) check for speed optimization
        pre = (not len(self._prereqs)) or (ignore_conditions & IgnoreConditions.PRE) \
            or all(cond(job) for cond in self._prereqs)
        if pre and len(self._postconds):
            post = (ignore_conditions & IgnoreConditions.POST) \
                or any(not cond(job) for cond in self._postconds)
        else:
            post = True
        return pre and post

    def complete(self, job):
        "True when all post-conditions are met."
        if len(self._postconds):
            return all(cond(job) for cond in self._postconds)
        else:
            return False


class FlowCmdOperation(BaseFlowOperation):
    """A BaseFlowOperation that holds a shell executable command.

    When an operation has the ``@cmd`` directive specified, it is instantiated
    as a FlowCmdOperation. The operation should be a function of
    :py:class:`~signac.contrib.job.Job`. The command (cmd) may
    either be a unary callable that expects an instance of
    :class:`~signac.contrib.job.Job` as its only positional argument and returns
    a string containing valid shell commands, or the string of commands itself.
    In either case, the resulting string may contain any attributes of the job placed
    in curly braces, which will then be substituted by Python string formatting.

    .. note::
        This class should not be instantiated directly.
    """

    def __init__(self, cmd, pre=None, post=None):
        super(FlowCmdOperation, self).__init__(pre=pre, post=post)
        self._cmd = cmd

    def __str__(self):
        return "{type}(cmd='{cmd}')".format(type=type(self).__name__, cmd=self._cmd)

    def __call__(self, job=None):
        if callable(self._cmd):
            return self._cmd(job).format(job=job)
        else:
            return self._cmd.format(job=job)


class FlowOperation(BaseFlowOperation):
    """FlowOperation holds a Python function that does not return a shell executable string.

    All operations without the ``@cmd`` directive use this class. No command can be specified, and
    the function is not stored internally (but in an external list) stored by the
    :class:`FlowProject`.

    .. note::
        This class should not be instantiated directly.
    """

    def __init__(self, name, pre=None, post=None):
        super(FlowOperation, self).__init__(pre=pre, post=post)
        self.name = name

    def __str__(self):
        return "{type}(name='{name}')".format(type=type(self).__name__, name=self.name)


class FlowGroupEntry(object):
    """A FlowGroupEntry registers operations for inclusion into a :py:class:`FlowGroup`.

    Has two methods for adding operations: ``self()`` and ``with_directives``.
    Using ``with_directives`` takes one argument, ``directives``, which are
    applied to the operation when running through the group. This overrides
    the default directives specified by ``@flow.directives``. Calling the object
    directly will then use the default directives.

    :param name:
        The name of the :py:class:`FlowGroup` to be created.
    :type name:
        str
    :param options:
        The :py:meth:`FlowProject.run` options to pass when submitting the group.
        These will be included in all submissions. Submissions use run
        commands to execute.
    :type options:
        str
    """
    def __init__(self, name, options=""):
        self.name = name
        self.options = options

    def __call__(self, func):
        """Decorator that adds the function into the group's operations.

        :param func:
            The function to decorate.
        :type func:
            callable
        """
        if hasattr(func, '_flow_groups'):
            if self.name in func._flow_groups:
                raise ValueError("Cannot register existing name {} with group {}"
                                 "".format(func, self.name))
            else:
                func._flow_groups.append(self.name)
        else:
            func._flow_groups = [self.name]
        return func

    def _set_directives(self, func, directives):
        if hasattr(func, '_flow_group_operation_directives'):
            if self.name in func._flow_group_operation_directives:
                raise ValueError("Cannot set directives because directives already exist for {} "
                                 "in group {}".format(func, self.name))
            else:
                func._flow_group_operation_directives[self.name] = directives
        else:
            func._flow_group_operation_directives = {self.name: directives}

    def with_directives(self, directives):
        """Returns a decorator that sets group specific directives to the operation.

        :param directives:
            Directives to use for resource requests and running the operation
            through the group.
        :type directives:
            dict
        :returns:
            A decorator which registers the function into the group with
            specified directives.
        :rtype:
            function
        """

        def decorator(func):
            self._set_directives(func, directives)
            return self(func)

        return decorator


class FlowGroup(object):
    """A FlowGroup represents a subset of a workflow for a project.

    Any :py:class:`FlowGroup` is associated with one or more instances of
    :py:class:`BaseFlowOperation`.

    In the example below, the directives will be {'nranks': 4} for op1 and
    {'nranks': 2, 'executable': 'python3'} for op2

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

    :param name:
        The name of the group to be used when calling from the command line.
    :type name:
        str
    :param operations:
        A dictionary of operations where the keys are operation names and
        each value is a :py:class:`BaseFlowOperation`.
    :type operations:
        dict
    :param operation_directives:
        A dictionary of additional parameters that provide instructions on how
        to execute a particular operation, e.g., specifically required
        resources. Operation names are keys and the dictionaries of directives are
        values. If an operation does not have directives specified, then the
        directives of the singleton group containing that operation are used. To
        prevent this, set the directives to an empty dictionary for that
        operation.
    :type operation_directives:
        dict
    :param options:
        A string of options to append to the output of the object's call method.
        This lets options like ``--num_passes`` to be given to a group.
    :type options:
        str
    """

    MAX_LEN_ID = 100

    def __init__(self, name, operations=None, operation_directives=None,
                 options=""):
        self.name = name
        self.options = options
        # An OrderedDict is not necessary here, but is used to ensure
        # consistent ordering of pretend submission output for templates.
        self.operations = OrderedDict() if operations is None else OrderedDict(operations)
        if operation_directives is None:
            self.operation_directives = dict()
        else:
            self.operation_directives = operation_directives

    def _set_entrypoint_item(self, entrypoint, directives, key, default, job):
        """Set a value (executable, path) for entrypoint in command.

        Order of priority is the operation directives specified and
        then the project specified value.
        """
        entrypoint[key] = directives.get(key, entrypoint.get(key, default))
        if callable(entrypoint[key]):
            entrypoint[key] = entrypoint[key](job)

    def _determine_entrypoint(self, entrypoint, directives, job):
        """Get the entrypoint for creating a JobOperation.

        If path cannot be determined, then raise a RuntimeError since we do not
        know where to point to.
        """
        entrypoint = entrypoint.copy()
        self._set_entrypoint_item(entrypoint, directives, 'executable', sys.executable, job)

        # If a path is not provided, default to the path to the file where the
        # FlowProject (subclass) is defined.
        default_path = inspect.getfile(job._project.__class__)
        self._set_entrypoint_item(entrypoint, directives, 'path', default_path, job)
        return "{} {}".format(entrypoint['executable'], entrypoint['path']).lstrip()

    def _resolve_directives(self, name, defaults, job):
        if name in self.operation_directives:
            directives = deepcopy(self.operation_directives[name])
        else:
            directives = deepcopy(defaults.get(name, dict()))
        nranks = directives.get('nranks', 1)
        nthreads = directives.get('omp_num_threads', 1)
        if callable(nranks) or callable(nthreads):
            def np_callable(job):
                nr = nranks(job) if callable(nranks) else nranks
                nt = nthreads(job) if callable(nthreads) else nthreads
                return nr*nt

            directives.setdefault('np', np_callable)
        else:
            directives.setdefault('np', nranks*nthreads)

        directives.setdefault('ngpu', 0)
        directives.setdefault('nranks', 0)
        directives.setdefault('omp_num_threads', 0)
        directives.setdefault('processor_fraction', 1)

        # Evaluate strings and callables for job:
        def evaluate(value):
            if value and callable(value):
                return value(job)
            elif isinstance(value, str):
                return value.format(job=job)
            else:
                return value
        return {key: evaluate(value) for key, value in directives.items()}

    def _submit_cmd(self, entrypoint, ignore_conditions, job=None):
        entrypoint = self._determine_entrypoint(entrypoint, dict(), job)
        cmd = "{} run -o {}".format(entrypoint, self.name)
        cmd = cmd if job is None else cmd + ' -j {}'.format(job)
        cmd = cmd if self.options is None else cmd + ' ' + self.options
        if ignore_conditions != IgnoreConditions.NONE:
            return cmd.strip() + ' --ignore-conditions=' + str(ignore_conditions)
        else:
            return cmd.strip()

    def _run_cmd(self, entrypoint, operation_name, operation, directives, job):
        if isinstance(operation, FlowCmdOperation):
            return operation(job).lstrip()
        else:
            entrypoint = self._determine_entrypoint(entrypoint, directives, job)
            return '{} exec {} {}'.format(entrypoint, operation_name, job).lstrip()

    def __iter__(self):
        yield from self.operations.values()

    def __repr__(self):
        return "{type}(name='{name}', operations='{operations}', " \
               "operation_directives={directives}, options='{options}')".format(
                   type=type(self).__name__,
                   name=self.name,
                   operations=' '.join(list(self.operations)),
                   directives=self.operation_directives,
                   options=self.options)

    def eligible(self, job, ignore_conditions=IgnoreConditions.NONE):
        """Eligible, when at least one BaseFlowOperation is eligible.

        :param job:
            A :class:`signac.Job` from the signac workspace.
        :type job:
            :class:`signac.Job`
        :param ignore_conditions:
            Specify if pre and/or post conditions check is to be ignored for eligibility check.
            The default is :py:class:`IgnoreConditions.NONE`.
        :type ignore_conditions:
            :py:class:`~.IgnoreConditions`
        :return:
            Whether the group is eligible.
        :rtype:
            bool
        """
        return any(op.eligible(job, ignore_conditions) for op in self)

    def complete(self, job):
        """True when all BaseFlowOperation post-conditions are met.

        :param job:
            A :class:`signac.Job` from the signac workspace.
        :type job:
            :class:`signac.Job`
        :return:
            Whether the group is complete (all contained operations are
            complete).
        :rtype:
            bool
        """
        return all(op.complete(job) for op in self)

    def add_operation(self, name, operation, directives=None):
        """Add an operation to the FlowGroup.

        :param name:
            The name of the operation.
        :type name:
            str
        :param operation:
            The workflow operation to add to the FlowGroup.
        :type operation:
            :py:class:`BaseFlowOperation`
        :param directives:
            The operation specific directives.
        :type directives:
            dict
        """
        self.operations[name] = operation
        if directives is not None:
            self.operation_directives[name] = directives

    def isdisjoint(self, group):
        """Returns whether two groups are disjoint (do not share any common operations).

        :param group:
            The other FlowGroup to compare to.
        :type group:
            :py:class:`flow.FlowGroup`
        :return:
            Returns ``True`` if ``group`` and ``self`` share no operations,
            otherwise returns ``False``.
        :rtype:
            bool
        """
        return set(self).isdisjoint(set(group))

    def _generate_id(self, job, operation_name=None, index=0):
        "Return an id, which identifies this group with respect to this job."
        project = job._project

        # The full name is designed to be truly unique for each job-group.
        if operation_name is None:
            op_string = ''.join(sorted(list(self.operations)))
        else:
            op_string = operation_name

        full_name = '{}%{}%{}%{}'.format(project.root_directory(),
                                         job.get_id(),
                                         op_string,
                                         index)
        # The job_op_id is a hash computed from the unique full name.
        job_op_id = calc_id(full_name)

        # The actual job id is then constructed from a readable part and the job_op_id,
        # ensuring that the job-op is still somewhat identifiable, but guaranteed to
        # be unique. The readable name is based on the project id, job id, operation name,
        # and the index number. All names and the id itself are restricted in length
        # to guarantee that the id does not get too long.
        max_len = self.MAX_LEN_ID - len(job_op_id)
        if max_len < len(job_op_id):
            raise ValueError("Value for MAX_LEN_ID is too small ({}).".format(self.MAX_LEN_ID))

        separator = getattr(project._environment, 'JOB_ID_SEPARATOR', '/')
        readable_name = '{project}{sep}{job}{sep}{op_string}{sep}{index:04d}{sep}'.format(
                    sep=separator,
                    project=str(project)[:12],
                    job=str(job)[:8],
                    op_string=op_string[:12],
                    index=index)[:max_len]

        # By appending the unique job_op_id, we ensure that each id is truly unique.
        return readable_name + job_op_id

    def _get_status(self, job):
        """For a given job check the groups submission status."""
        try:
            return JobStatus(job._project.document['_status'][self._generate_id(job)])
        except KeyError:
            return JobStatus.unknown

    def _create_submission_job_operation(self, entrypoint, default_directives, job,
                                         ignore_conditions_on_execution=IgnoreConditions.NONE,
                                         index=0):
        """Create a JobOperation object from the FlowGroup.

        Creates a JobOperation for use in submitting and scripting.

        :param entrypoint:
            The path and executable, if applicable, to point to for execution.
        :type entrypoint:
            dict
        :param default_directives:
            The default directives to use for the operations. This is to allow for user specified
            groups to 'inherit' directives from ``default_directives``. If no defaults are desired,
            the argument can be set to an empty dictionary. This must be done explicitly, however.
        :type default_directives:
            dict
        :param job:
            The job that the :class:`~.JobOperation` is based on.
        :type job:
            :class:`signac.Job`
        :param ignore_conditions:
            Specify if pre and/or post conditions check is to be ignored for
            checking submission eligibility. The default is `IgnoreConditions.NONE`.
        :type ignore_conditions:
            :py:class:`~.IgnoreConditions`
        :param ignore_conditions_on_execution:
            Specify if pre and/or post conditions check is to be ignored for eligibility check after
            submitting.  The default is `IgnoreConditions.NONE`.
        :type ignore_conditions_on_execution:
            :py:class:`~.IgnoreConditions`
        :param index:
            Index for the :class:`~.JobOperation`.
        :type index:
            int
        :return:
            Returns a :py:class:`~.SubmissionJobOperation` for submitting the group. The
            :py:class:`~.JobOperation` will have directives that have been collected
            appropriately from its contained operations.
        :rtype:
            :py:class:`SubmissionJobOperation`
        """
        uneval_cmd = functools.partial(self._submit_cmd, entrypoint=entrypoint, job=job,
                                       ignore_conditions=ignore_conditions_on_execution)

        def _get_run_ops(ignore_ops, additional_ignores_flag):
            """Get operations that match the combination of the conditions required by
            _create_submission_job_operation and the ignored flags, and remove operations
            in the ignore_ops list."""
            return list(
                set(self._create_run_job_operations(
                    entrypoint=entrypoint,
                    default_directives=default_directives,
                    job=job,
                    ignore_conditions=ignore_conditions_on_execution | additional_ignores_flag)
                    ) - set(ignore_ops)
            )

        submission_directives = self._get_submission_directives(default_directives, job)
        eligible_operations = _get_run_ops(
            [], IgnoreConditions.NONE)
        operations_with_unmet_preconditions = _get_run_ops(
            eligible_operations, IgnoreConditions.PRE)
        operations_with_met_postconditions = _get_run_ops(
            eligible_operations, IgnoreConditions.POST)

        submission_job_operation = SubmissionJobOperation(
            self._generate_id(job, index=index),
            self.name,
            job,
            cmd=uneval_cmd,
            directives=submission_directives,
            eligible_operations=eligible_operations,
            operations_with_unmet_preconditions=operations_with_unmet_preconditions,
            operations_with_met_postconditions=operations_with_met_postconditions,
        )
        return submission_job_operation

    def _create_run_job_operations(self, entrypoint, default_directives, job,
                                   ignore_conditions=IgnoreConditions.NONE, index=0):
        """Create JobOperation object(s) from the FlowGroup.

        Yields a JobOperation for each contained operation given proper conditions are met.

        :param entrypoint:
            The path and executable, if applicable, to point to for execution.
        :type entrypoint:
            dict
        :param default_directives:
            The default directives to use for the operations. This is to allow for user specified
            groups to 'inherent' directives from ``default_directives``. If no defaults are desired,
            the argument must be explicitly set to an empty dictionary.
        :type default_directives:
            dict
        :param job:
            The job that the :class:`~.JobOperation` is based on.
        :type job:
            :class:`signac.Job`
        :param index:
            Index for the :class:`~.JobOperation`.
        :type index:
            int
        :return:
            Returns an iterator over eligible :py:class:`~.JobOperation`s.
        :rtype:
            Iterator[JobOperation]
        """
        for name, op in self.operations.items():
            if op.eligible(job, ignore_conditions):
                directives = self._resolve_directives(name, default_directives, job)
                cmd = self._run_cmd(entrypoint=entrypoint, operation_name=name,
                                    operation=op, directives=directives, job=job)
                # Uses a different id than the groups direct id. Do not use this for submitting
                # jobs as current implementation prevents checking for resubmission in this case.
                # The different ids allow for checking whether JobOperations created to run directly
                # are different.
                job_op = JobOperation(self._generate_id(job, name, index=index), name, job,
                                      cmd=cmd, directives=deepcopy(directives))
                # Get the prefix, and if it's not NULL, set the fork directive
                # to True since we must launch a separate process. Override
                # the command directly.
                prefix = job._project._environment.get_prefix(job_op)
                if prefix != '':
                    job_op.directives['fork'] = True
                    job_op._cmd = '{} {}'.format(prefix, job_op.cmd)
                yield job_op

    def _get_submission_directives(self, default_directives, job):
        """Get the combined resources for submission.

        No checks are done to mitigate inappropriate aggregation of operations.
        This can lead to poor utilization of computing resources.
        """
        directives = dict(ngpu=0, nranks=0, omp_num_threads=0, np=0)

        for name in self.operations:
            # get directives for operation
            op_dir = self._resolve_directives(name, default_directives, job)
            # Find the correct number of processors for operation
            directives['ngpu'] = max(directives['ngpu'], op_dir['ngpu'])
            directives['nranks'] = max(directives['nranks'], op_dir['nranks'])
            directives['omp_num_threads'] = max(directives['omp_num_threads'],
                                                op_dir['omp_num_threads'])
            directives['np'] = max(directives['np'], op_dir['np'])
        return directives


class _FlowProjectClass(type):
    """Metaclass for the FlowProject class."""
    def __new__(metacls, name, bases, namespace, **kwargs):
        cls = type.__new__(metacls, name, bases, dict(namespace))

        # All operation functions are registered with the operation() classmethod, which is
        # intended to be used as a decorator function. _OPERATION_FUNCTIONS is a list of tuples
        # of the operation name and the operation function. In addition, pre and post conditions
        # are registered with the class.

        cls._OPERATION_FUNCTIONS = list()
        cls._OPERATION_PRE_CONDITIONS = defaultdict(list)
        cls._OPERATION_POST_CONDITIONS = defaultdict(list)

        # All label functions are registered with the label() classmethod, which is intended
        # to be used as decorator function. The _LABEL_FUNCTIONS dict contains the function as
        # key and the label name as value, or None to use the default label name.
        cls._LABEL_FUNCTIONS = OrderedDict()

        # Give the class a pre and post class that are aware of the class they
        # are in.
        cls.pre = cls._setup_pre_conditions_class(parent_class=cls)
        cls.post = cls._setup_post_conditions_class(parent_class=cls)

        # All groups are registered with the function returned by the make_group
        # classmethod. In contrast to operations and labels, the
        # make_group classmethod does not serve as the decorator, the functor
        # it returns does. The _GROUPS list records the groups created and their
        # passed parameters for later initialization. The _GROUP_NAMES set stores
        # whether a group name has already been used.
        cls._GROUPS = list()
        cls._GROUP_NAMES = set()

        return cls

    @staticmethod
    def _setup_pre_conditions_class(parent_class):

        class pre(_condition):
            """Specify a function of job that must be true for this operation to
            be eligible for execution. For example:

            .. code-block:: python

                @Project.operation
                @Project.pre(lambda job: not job.doc.get('hello'))
                def hello(job):
                    print('hello', job)
                    job.doc.hello = True

            The *hello*-operation would only execute if the 'hello' key in the job
            document does not evaluate to True.

            An optional tag may be associated with the condition. These tags
            are used by :meth:`~.detect_operation_graph` when comparing
            conditions for equality. The tag defaults to the bytecode of the
            function.
            """

            _parent_class = parent_class

            def __init__(self, condition, tag=None):
                super(pre, self).__init__(condition, tag)

            def __call__(self, func):
                operation_functions = [operation[1] for operation
                                       in self._parent_class._collect_operations()]
                if self.condition in operation_functions:
                    raise ValueError("Operation functions cannot be used as preconditions.")
                self._parent_class._OPERATION_PRE_CONDITIONS[func].insert(0, self.condition)
                return func

            @classmethod
            def copy_from(cls, *other_funcs):
                "True if and only if all pre conditions of other operation-function(s) are met."
                return cls(_create_all_metacondition(cls._parent_class._collect_pre_conditions(),
                                                     *other_funcs))

            @classmethod
            def after(cls, *other_funcs):
                "True if and only if all post conditions of other operation-function(s) are met."
                operation_functions = [operation[1] for operation
                                       in cls._parent_class._collect_operations()]
                if not all(condition in operation_functions for condition in other_funcs):
                    raise ValueError("The arguments to pre.after must be operation functions.")
                return cls(_create_all_metacondition(cls._parent_class._collect_post_conditions(),
                                                     *other_funcs))

        return pre

    @staticmethod
    def _setup_post_conditions_class(parent_class):

        class post(_condition):
            """Specify a function of job that must evaluate to True for this operation
            to be considered complete. For example:

            .. code-block:: python

                @Project.operation
                @Project.post(lambda job: job.doc.get('bye'))
                def bye(job):
                    print('bye' job)
                    job.doc.bye = True

            The *bye*-operation would be considered complete and therefore no longer
            eligible for execution once the 'bye' key in the job document evaluates to True.

            An optional tag may be associated with the condition. These tags
            are used by :meth:`~.detect_operation_graph` when comparing
            conditions for equality. The tag defaults to the bytecode of the
            function.
            """
            _parent_class = parent_class

            def __init__(self, condition, tag=None):
                super(post, self).__init__(condition, tag)

            def __call__(self, func):
                operation_functions = [operation[1] for operation
                                       in self._parent_class._collect_operations()]
                if self.condition in operation_functions:
                    raise ValueError("Operation functions cannot be used as postconditions.")
                self._parent_class._OPERATION_POST_CONDITIONS[func].insert(0, self.condition)
                return func

            @classmethod
            def copy_from(cls, *other_funcs):
                "True if and only if all post conditions of other operation-function(s) are met."
                return cls(_create_all_metacondition(cls._parent_class._collect_post_conditions(),
                                                     *other_funcs))

        return post


class FlowProject(signac.contrib.Project, metaclass=_FlowProjectClass):
    """A signac project class specialized for workflow management.

    This class provides a command line interface for the definition, execution, and
    submission of workflows based on condition and operation functions.

    This is a typical example on how to use this class:

    .. code-block:: python

        @FlowProject.operation
        def hello(job):
            print('hello', job)

        FlowProject().main()

    :param config:
        A signac configuration, defaults to the configuration loaded
        from the environment.
    :type config:
        A signac config object.
    :param entrypoint:
        A dictionary with two possible keys: executable and path. Path
        represents the filepath location of the script file (the script file
        must call ``main``). Executable represents the location of the Python
        interpreter used for the executable of `FlowOperation` that are Python
        functions.
    :type entrypoint:
        dict
    """

    def __init__(self, config=None, environment=None, entrypoint=None):
        super(FlowProject, self).__init__(config=config)

        # Associate this class with a compute environment.
        self._environment = environment or get_environment()

        # Assign variables that give script location information
        self._entrypoint = dict() if entrypoint is None else entrypoint

        # The standard local template directory is a directory called 'templates' within
        # the project root directory. This directory may be specified with the 'template_dir'
        # configuration variable.
        self._template_dir = os.path.join(
            self.root_directory(), self._config.get('template_dir', 'templates'))
        self._template_environment_ = dict()

        # Register all label functions with this project instance.
        self._label_functions = OrderedDict()
        self._register_labels()

        # Register all operation functions with this project instance.
        self._operation_functions = dict()
        self._operations = OrderedDict()
        self._register_operations()

        # Register all groups with this project instance.
        self._groups = dict()
        self._register_groups()

    def _setup_template_environment(self):
        """Setup the jinja2 template environment.

        The templating system is used to generate templated scripts for the script()
        and submit_operations() / submit() function and the corresponding command line
        subcommands.
        """
        if self._config.get('flow') and self._config['flow'].get('environment_modules'):
            envs = self._config['flow'].as_list('environment_modules')
        else:
            envs = []

        # Templates are searched in the local template directory first, then in additionally
        # installed packages, then in the main package 'templates' directory.
        extra_packages = []
        for env in envs:
            try:
                extra_packages.append(jinja2.PackageLoader(env, 'templates'))
            except ImportError as error:
                logger.warning("Unable to load template from package '{}'.".format(error.name))

        load_envs = ([jinja2.FileSystemLoader(self._template_dir)] +
                     extra_packages +
                     [jinja2.PackageLoader('flow', 'templates')])

        template_environment = jinja2.Environment(
            loader=jinja2.ChoiceLoader(load_envs),
            trim_blocks=True,
            extensions=[TemplateError])

        # Setup standard filters that can be used to format context variables.
        template_environment.filters['format_timedelta'] = tf.format_timedelta
        template_environment.filters['identical'] = tf.identical
        template_environment.filters['with_np_offset'] = tf.with_np_offset
        template_environment.filters['calc_tasks'] = tf.calc_tasks
        template_environment.filters['calc_num_nodes'] = tf.calc_num_nodes
        template_environment.filters['check_utilization'] = tf.check_utilization
        template_environment.filters['homogeneous_openmp_mpi_config'] = \
            tf.homogeneous_openmp_mpi_config
        template_environment.filters['get_config_value'] = flow_config.get_config_value
        template_environment.filters['require_config_value'] = flow_config.require_config_value
        template_environment.filters['get_account_name'] = tf.get_account_name
        template_environment.filters['print_warning'] = tf.print_warning
        if 'max' not in template_environment.filters:    # for jinja2 < 2.10
            template_environment.filters['max'] = max
        if 'min' not in template_environment.filters:    # for jinja2 < 2.10
            template_environment.filters['min'] = min

        return template_environment

    def _template_environment(self, environment=None):
        if environment is None:
            environment = self._environment
        if environment not in self._template_environment_:
            template_environment = self._setup_template_environment()

            # Add environment-specific custom filters:
            for name, member in inspect.getmembers(environment):
                if getattr(member, '_flow_template_filter', False):
                    template_environment.filters[name] = member

            self._template_environment_[environment] = template_environment
        return self._template_environment_[environment]

    def _get_standard_template_context(self):
        "Return the standard templating context for run and submission scripts."
        context = dict()
        context['project'] = self
        return context

    def _show_template_help_and_exit(self, template_environment, context):
        "Print all context variables and filters to screen and exit."
        from textwrap import TextWrapper
        wrapper = TextWrapper(width=90, break_long_words=False)
        print(TEMPLATE_HELP.format(
            template_dir=self._template_dir,
            template_vars='\n'.join(wrapper.wrap(', '.join(sorted(context)))),
            filters='\n'.join(wrapper.wrap(', '.join(sorted(template_environment.filters))))))
        sys.exit(2)

    @classmethod
    def label(cls, label_name_or_func=None):
        """Designate a function to be a label function of this class.

        For example, we can define a label function like this:

        .. code-block:: python

            @FlowProject.label
            def foo_label(job):
                if job.document.get('foo', False):
                    return 'foo-label-text'

        The ``foo-label-text`` label will now show up in the status view for each job,
        where the ``foo`` key evaluates true.

        If the label functions returns any type other than ``str``, the label
        name will be the name of the function if and only if the return value
        evaluates to ``True``, for example:

        .. code-block:: python

            @FlowProject.label
            def foo_label(job):
                return job.document.get('foo', False)

        Finally, you can specify a different default label name by providing it as the first
        argument to the ``label()`` decorator.

        :param label_name_or_func:
            A label name or callable.
        :type label_name_or_func:
            str or callable
        """
        if callable(label_name_or_func):
            cls._LABEL_FUNCTIONS[label_name_or_func] = None
            return label_name_or_func

        def label_func(func):
            cls._LABEL_FUNCTIONS[func] = label_name_or_func
            return func

        return label_func

    def detect_operation_graph(self):
        """Determine the directed acyclic graph defined by operation pre- and
        post-conditions.

        In general, executing a given operation registered with a FlowProject
        just involves checking the operation's pre- and post-conditions to
        determine eligibility. More generally, however, the pre- and
        post-conditions define a directed acyclic graph that governs the
        execution of all operations. Visualizing this graph can be useful for
        finding logic errors in the specified conditions, and having this graph
        computed also enables additional execution modes. For example, using
        this graph it is possible to determine exactly what operations need to
        be executed in order to make the operation eligible so that the task of
        executing all necessary operations can be automated.

        The graph is determined by iterating over all pairs of operations and
        checking for equality of pre- and post-conditions. The algorithm builds
        an adjacency matrix based on whether the pre-conditions for one
        operation match the post-conditions for another. The comparison of
        operations is conservative; by default, conditions must be composed of
        identical code to be identified as equal (technically, they must be
        bytecode equivalent, i.e. ``cond1.__code__.co_code ==
        cond2.__code__.co_code``). Users can specify that conditions should be
        treated as equal by providing tags to the operations.

        Given a FlowProject subclass defined in a module ``project.py``, the
        output graph could be visualized using Matplotlib and NetworkX with the
        following code:

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

        Raises a ``RuntimeError`` if a condition does not have a tag. This can
        occur when using ``functools.partial``, and a manually specified
        condition tag has not been set.

        :raises: RuntimeError

        """

        def to_callbacks(conditions):
            """Get the actual callables associated with FlowConditions."""
            return [condition._callback for condition in conditions]

        def unpack_conditions(condition_functions):
            """Identify any metaconditions in the list and reduce them to the
            functions that they are composed of. The callbacks argument is used
            in recursive calls to the function and appended to directly, but
            only returned at the end."""
            callbacks = set()
            for cf in condition_functions:
                # condition may not have __name__ attribute in cases where functools is used
                # for condition creation
                if hasattr(cf, '__name__') and cf.__name__ == "_flow_metacondition":
                    callbacks = callbacks.union(unpack_conditions(cf._composed_of))
                else:
                    if cf._flow_tag is None:
                        raise RuntimeError("Condition {} was not tagged. To create a graph, ensure "
                                           "each base condition has a ``__code__`` attribute or "
                                           "manually specified tag.".format(cf))
                    callbacks.add(cf._flow_tag)

            return callbacks

        ops = list(self.operations.items())
        mat = [[0 for _ in range(len(ops))] for _ in range(len(ops))]

        for i, (name1, op1) in enumerate(ops):
            for j, (name2, op2) in enumerate(ops[i:]):
                postconds1 = unpack_conditions(to_callbacks(op1._postconds))
                postconds2 = unpack_conditions(to_callbacks(op2._postconds))
                prereqs1 = unpack_conditions(to_callbacks(op1._prereqs))
                prereqs2 = unpack_conditions(to_callbacks(op2._prereqs))
                if postconds1.intersection(prereqs2):
                    mat[i][j+i] = 1
                elif prereqs1.intersection(postconds2):
                    mat[j+i][i] = 1
        return mat

    def _register_class_labels(self):
        """This function registers all label functions, which are part of the class definition.

        To register a class method or function as label function, use the generalized label()
        function.
        """
        def predicate(m):
            return inspect.ismethod(m) or inspect.isfunction(m)

        class_label_functions = dict()
        for name, function in inspect.getmembers(type(self), predicate=predicate):
            if _is_label_func(function):
                class_label_functions[name] = function

        for name in sorted(class_label_functions):
            self._label_functions[class_label_functions[name]] = None

    def _register_labels(self):
        "Register all label functions registered with this class and its parent classes."
        self._register_class_labels()

        for cls in type(self).__mro__:
            self._label_functions.update(getattr(cls, '_LABEL_FUNCTIONS', dict()))

    ALIASES = {str(status).replace('JobStatus.', ''): symbol
               for status, symbol in _FMT_SCHEDULER_STATUS.items() if status != JobStatus.dummy}
    "These are default aliases used within the status output."

    @classmethod
    def _alias(cls, x):
        "Use alias if specified."
        try:
            return abbreviate(x, cls.ALIASES.get(x, x))
        except TypeError:
            return x

    def _fn_bundle(self, bundle_id):
        "Return the canonical name to store bundle information."
        return os.path.join(self.root_directory(), '.bundles', bundle_id)

    def _store_bundled(self, operations):
        """Store operation-ids as part of a bundle and return bundle id.

        The operation identifiers are stored in a text file whose name is
        determined by the _fn_bundle() method. This may be used to identify
        the status of individual operations from the bundle id. A single
        operation will not be stored, but instead the operation's id is
        directly returned.

        :param operations:
            The operations to bundle.
        :type operations:
            A sequence of instances of :py:class:`.JobOperation`
        :return:
            The bundle id.
        :rtype:
            str
        """
        if len(operations) == 1:
            return operations[0].id
        else:
            h = '.'.join(op.id for op in operations)
            bid = '{}/bundle/{}'.format(self, sha1(h.encode('utf-8')).hexdigest())
            fn_bundle = self._fn_bundle(bid)
            os.makedirs(os.path.dirname(fn_bundle), exist_ok=True)
            with open(fn_bundle, 'w') as file:
                for operation in operations:
                    file.write(operation.id + '\n')
            return bid

    def _expand_bundled_jobs(self, scheduler_jobs):
        "Expand jobs which were submitted as part of a bundle."
        for job in scheduler_jobs:
            if job.name().startswith('{}/bundle/'.format(self)):
                with open(self._fn_bundle(job.name())) as file:
                    for line in file:
                        yield ClusterJob(line.strip(), job.status())
            else:
                yield job

    def scheduler_jobs(self, scheduler):
        """Fetch jobs from the scheduler.

        This function will fetch all scheduler jobs from the scheduler
        and also expand bundled jobs automatically.

        However, this function will not automatically filter scheduler
        jobs which are not associated with this project.

        :param scheduler:
            The scheduler instance.
        :type scheduler:
            :class:`~.flow.manage.Scheduler`
        :yields:
            All scheduler jobs fetched from the scheduler instance.
        """
        for sjob in self._expand_bundled_jobs(scheduler.jobs()):
            yield sjob

    def _get_operations_status(self, job, cached_status):
        "Return a dict with information about job-operations for this job."
        starting_dict = functools.partial(dict, scheduler_status=JobStatus.unknown)
        status_dict = defaultdict(starting_dict)
        for group in self._groups.values():
            completed = group.complete(job)
            eligible = False if completed else group.eligible(job)
            scheduler_status = cached_status.get(group._generate_id(job),
                                                 JobStatus.unknown)
            for operation in group.operations:
                if scheduler_status >= status_dict[operation]['scheduler_status']:
                    status_dict[operation] = {
                            'scheduler_status': scheduler_status,
                            'eligible': eligible,
                            'completed': completed
                            }

        for key in sorted(status_dict):
            yield key, status_dict[key]

    def get_job_status(self, job, ignore_errors=False, cached_status=None):
        "Return a dict with detailed information about the status of a job."
        result = dict()
        result['job_id'] = str(job)
        try:
            if cached_status is None:
                try:
                    cached_status = self.document['_status']._as_dict()
                except KeyError:
                    cached_status = dict()
            result['operations'] = OrderedDict(self._get_operations_status(job, cached_status))
            result['_operations_error'] = None
        except Exception as error:
            msg = "Error while getting operations status for job '{}': '{}'.".format(job, error)
            logger.debug(msg)
            if ignore_errors:
                result['operations'] = dict()
                result['_operations_error'] = str(error)
            else:
                raise
        try:
            result['labels'] = sorted(set(self.labels(job)))
            result['_labels_error'] = None
        except Exception as error:
            logger.debug("Error while determining labels for job '{}': '{}'.".format(job, error))
            if ignore_errors:
                result['labels'] = list()
                result['_labels_error'] = str(error)
            else:
                raise
        return result

    def _fetch_scheduler_status(self, jobs=None, file=None, ignore_errors=False):
        "Update the status docs."
        if file is None:
            file = sys.stderr
        if jobs is None:
            jobs = list(self)
        try:
            scheduler = self._environment.get_scheduler()

            self.document.setdefault('_status', dict())
            scheduler_info = {sjob.name(): sjob.status() for sjob in self.scheduler_jobs(scheduler)}
            status = dict()
            print("Query scheduler...", file=file)
            for job in tqdm(jobs,
                            desc="Fetching operation status",
                            total=len(jobs), file=file):
                for group in self._groups.values():
                    _id = group._generate_id(job)
                    status[_id] = int(scheduler_info.get(_id, JobStatus.unknown))
            self.document._status.update(status)
        except NoSchedulerError:
            logger.debug("No scheduler available.")
        except RuntimeError as error:
            logger.warning("Error occurred while querying scheduler: '{}'.".format(error))
            if not ignore_errors:
                raise
        else:
            logger.info("Updated job status cache.")

    def _fetch_status(self, jobs, err, ignore_errors, status_parallelization='thread'):
        # The argument status_parallelization is used so that _fetch_status method
        # gets to know whether the deprecated argument no_parallelization passed
        # while calling print_status is True or False. This can also be done by
        # setting self.config['flow']['status_parallelization']='none' if the argument
        # is True. But the later functionality will last the rest of the session but in order
        # to do proper deprecation, it is not required for now.

        # Update the project's status cache
        self._fetch_scheduler_status(jobs, err, ignore_errors)
        # Get status dict for all selected jobs

        def _print_progress(x):
            print("Updating status: ", end='', file=err)
            err.flush()
            n = max(1, int(len(jobs) / 10))
            for i, _ in enumerate(x):
                if (i % n) == 0:
                    print('.', end='', file=err)
                    err.flush()
                yield _

        try:
            cached_status = self.document['_status']._as_dict()
        except KeyError:
            cached_status = dict()

        _get_job_status = functools.partial(self.get_job_status,
                                            ignore_errors=ignore_errors,
                                            cached_status=cached_status)

        with self._potentially_buffered():
            try:
                if status_parallelization == 'thread':
                    with contextlib.closing(ThreadPool()) as pool:
                        # First attempt at parallelized status determination.
                        # This may fail on systems that don't allow threads.
                        return list(tqdm(
                            iterable=pool.imap(_get_job_status, jobs),
                            desc="Collecting job status info", total=len(jobs), file=err))
                elif status_parallelization == 'process':
                    with contextlib.closing(Pool()) as pool:
                        try:
                            import pickle
                            results = self._fetch_status_in_parallel(
                                pool, pickle, jobs, ignore_errors, cached_status)
                        except Exception as error:
                            if not isinstance(error, (pickle.PickleError, self._PickleError)) and\
                                    'pickle' not in str(error).lower():
                                raise    # most likely not a pickle related error...

                            try:
                                import cloudpickle
                            except ImportError:  # The cloudpickle package is not available.
                                logger.error("Unable to parallelize execution due to a "
                                             "pickling error. "
                                             "\n\n - Try to install the 'cloudpickle' package, "
                                             "e.g., with 'pip install cloudpickle'!\n")
                                raise error
                            else:
                                try:
                                    results = self._fetch_status_in_parallel(
                                        pool, cloudpickle, jobs, ignore_errors, cached_status)
                                except self._PickleError as error:
                                    raise RuntimeError(
                                        "Unable to parallelize execution due to a pickling "
                                        "error: {}.".format(error))
                        return list(tqdm(
                            iterable=results,
                            desc="Collecting job status info", total=len(jobs), file=err))
                elif status_parallelization == 'none':
                    return list(tqdm(
                        iterable=map(_get_job_status, jobs),
                        desc="Collecting job status info", total=len(jobs), file=err))
                else:
                    raise RuntimeError("Configuration value status_parallelization is invalid. "
                                       "You can set it to 'thread', 'parallel', or 'none'"
                                       )
            except RuntimeError as error:
                if "can't start new thread" not in error.args:
                    raise   # unrelated error

                t = time.time()
                num_jobs = len(jobs)
                statuses = []
                for i, job in enumerate(jobs):
                    statuses.append(_get_job_status(job))
                    if time.time() - t > 0.2:  # status interval
                        print(
                            'Collecting job status info: {}/{}'.format(i+1, num_jobs),
                            end='\r', file=err)
                        t = time.time()
                # Always print the completed progressbar.
                print('Collecting job status info: {}/{}'.format(i+1, num_jobs), file=err)
                return statuses

    def _fetch_status_in_parallel(self, pool, pickle, jobs, ignore_errors, cached_status):
        try:
            s_project = pickle.dumps(self)
            s_tasks = [(pickle.loads, s_project, job.get_id(), ignore_errors, cached_status)
                       for job in jobs]
        except Exception as error:  # Masking all errors since they must be pickling related.
            raise self._PickleError(error)

        results = pool.imap(_serialized_get_job_status, s_tasks)

        return results

    PRINT_STATUS_ALL_VARYING_PARAMETERS = True
    """This constant can be used to signal that the print_status() method is supposed
    to automatically show all varying parameters."""

    def print_status(self, jobs=None, overview=True, overview_max_lines=None,
                     detailed=False, parameters=None,
                     param_max_width=None,
                     expand=False, all_ops=False, only_incomplete=False, dump_json=False,
                     unroll=True, compact=False, pretty=False,
                     file=None, err=None, ignore_errors=False,
                     no_parallelize=False, template=None, profile=False,
                     eligible_jobs_max_lines=None, output_format='terminal'):
        """Print the status of the project.

        :param jobs:
            Only execute operations for the given jobs, or all if the argument is omitted.
        :type jobs:
            Sequence of instances :class:`.Job`.
        :param overview:
            Aggregate an overview of the project' status.
        :type overview:
            bool
        :param overview_max_lines:
            Limit the number of overview lines.
        :type overview_max_lines:
            int
        :param detailed:
            Print a detailed status of each job.
        :type detailed:
            bool
        :param parameters:
            Print the value of the specified parameters.
        :type parameters:
            list of str
        :param param_max_width:
            Limit the number of characters of parameter columns.
        :type param_max_width:
            int
        :param expand:
            Present labels and operations in two separate tables.
        :type expand:
            bool
        :param all_ops:
            Include operations that are not eligible to run.
        :type all_ops:
            bool
        :param only_incomplete:
            Only show jobs that have eligible operations.
        :type only_incomplete:
            bool
        :param dump_json:
            Output the data as JSON instead of printing the formatted output.
        :type dump_json:
            bool
        :param unroll:
            Separate columns for jobs and the corresponding operations.
        :type unroll:
            bool
        :param compact:
            Print a compact version of the output.
        :type compact:
            bool
        :param pretty:
            Prettify the output.
        :type pretty:
            bool
        :param file:
            Redirect all output to this file, defaults to sys.stdout.
        :type file:
            str
        :param err:
            Redirect all error output to this file, defaults to sys.stderr.
        :type err:
            str
        :param ignore_errors:
            Print status even if querying the scheduler fails.
        :type ignore_errors:
            bool
        :param template:
            User provided Jinja2 template file.
        :type template:
            str
        :param profile:
            Show profile result.
        :type profile:
            bool
        :param eligible_jobs_max_lines:
            Limit the number of operations and its eligible job count printed in the overview.
        :type eligible_jobs_max_lines:
            int
        :param output_format:
            Status output format, supports:
            'terminal' (default), 'markdown' or 'html'.
        :type output_format:
            str
        :return:
            A Renderer class object that contains the rendered string.
        :rtype:
            :py:class:`~.Renderer`
        """
        if file is None:
            file = sys.stdout
        if err is None:
            err = sys.stderr
        if jobs is None:
            jobs = self     # all jobs

        if eligible_jobs_max_lines is None:
            eligible_jobs_max_lines = flow_config.get_config_value('eligible_jobs_max_lines')

        if no_parallelize:
            print(
                "WARNING: "
                "The no_parallelize argument is deprecated as of 0.10 "
                "and will be removed in 0.12. "
                "Instead, set the status_parallelization configuration value to 'none'. "
                "In order to do this from the CLI, you can execute "
                "`signac config set flow.status_parallelization 'none'`\n",
                file=sys.stderr
            )
            status_parallelization = 'none'
        else:
            status_parallelization = self.config['flow']['status_parallelization']

        # initialize jinja2 template environment and necessary filters
        template_environment = self._template_environment()

        context = self._get_standard_template_context()

        # get job status information
        if profile:
            try:
                import pprofile
            except ImportError:
                raise RuntimeWarning(
                    "Profiling requires the pprofile package. "
                    "Install with `pip install pprofile`.")
            prof = pprofile.StatisticalProfile()

            fn_filter = [
                inspect.getfile(threading),
                inspect.getfile(multiprocessing),
                inspect.getfile(Pool),
                inspect.getfile(ThreadPool),
                inspect.getfile(tqdm),
            ]

            with prof(single=False):
                tmp = self._fetch_status(jobs, err, ignore_errors, status_parallelization)

            prof._mergeFileTiming()

            # Unrestricted
            total_impact = 0
            hits = [hit for fn, ft in prof.merged_file_dict.items()
                    if fn not in fn_filter for hit in ft.iterHits()]
            sorted_hits = reversed(sorted(hits, key=lambda hit: hit[2]))
            total_num_hits = sum([hit[2] for hit in hits])

            profiling_results = ['# Profiling:\n']

            profiling_results.extend([
                'Rank Impact Code object',
                '---- ------ -----------'])
            for i, (line, code, hits, duration) in enumerate(sorted_hits):
                impact = hits / total_num_hits
                total_impact += impact
                profiling_results.append(
                    "{rank:>4} {impact:>6.0%} {code.co_filename}:"
                    "{code.co_firstlineno}:{code.co_name}".format(
                        rank=i+1, impact=impact, code=code))
                if i > 10 or total_impact > 0.8:
                    break

            for module_fn in prof.merged_file_dict:
                if re.match(profile, module_fn):
                    ft = prof.merged_file_dict[module_fn]
                else:
                    continue

                total_hits = ft.getTotalHitCount()
                total_impact = 0

                profiling_results.append(
                    "\nHits by line for '{}':".format(module_fn))
                profiling_results.append('-' * len(profiling_results[-1]))

                hits = list(sorted(ft.iterHits(), key=lambda h: 1/h[2]))
                for line, code, hits, duration in hits:
                    impact = hits / total_hits
                    total_impact += impact
                    profiling_results.append(
                        "{}:{} ({:2.0%}):".format(module_fn, line, impact))
                    try:
                        lines, start = inspect.getsourcelines(code)
                    except OSError:
                        continue
                    hits_ = [ft.getHitStatsFor(line)[0] for line in range(start, start+len(lines))]
                    profiling_results.extend(
                        ["{:>5} {:>4}: {}".format(h, lineno, l.rstrip())
                         for lineno, (l, h) in enumerate(zip(lines, hits_), start)])
                    profiling_results.append('')
                    if total_impact > 0.8:
                        break

            profiling_results.append("Total runtime: {}s".format(int(prof.total_time)))
            if prof.total_time < 20:
                profiling_results.append(
                    "Warning: Profiler ran only for a short time, "
                    "results may be highly inaccurate.")

        else:
            tmp = self._fetch_status(jobs, err, ignore_errors, status_parallelization)
            profiling_results = None

        operations_errors = {s['_operations_error'] for s in tmp}
        labels_errors = {s['_labels_error'] for s in tmp}
        errors = list(filter(None, operations_errors.union(labels_errors)))

        if errors:
            logger.warning(
                "Some job status updates did not succeed due to errors. "
                "Number of unique errors: {}. Use --debug to list all errors.".format(len(errors)))
            for i, error in enumerate(errors):
                logger.debug("Status update error #{}: '{}'".format(i + 1, error))

        if only_incomplete:
            # Remove all jobs from the status info, that have not a single
            # eligible operation.

            def _incomplete(s):
                return any(op['eligible'] for op in s['operations'].values())

            tmp = list(filter(_incomplete, tmp))

        statuses = OrderedDict([(s['job_id'], s) for s in tmp])

        # If the dump_json variable is set, just dump all status info
        # formatted in JSON to screen.
        if dump_json:
            print(json.dumps(statuses, indent=4), file=file)
            return

        if overview:
            # get overview info:
            progress = defaultdict(int)
            for status in statuses.values():
                for label in status['labels']:
                    progress[label] += 1
            progress_sorted = list(islice(
                sorted(progress.items(), key=lambda x: (x[1], x[0]), reverse=True),
                overview_max_lines))

        # Optionally expand parameters argument to all varying parameters.
        if parameters is self.PRINT_STATUS_ALL_VARYING_PARAMETERS:
            parameters = list(
                sorted({key for job in jobs for key in job.sp.keys() if
                        len(set([to_hashable(job.sp().get(key)) for job in jobs])) > 1}))

        if parameters:
            # get parameters info

            def _add_parameters(status):
                sp = self.open_job(id=status['job_id']).statepoint()

                def get(k, m):
                    if m is None:
                        return
                    t = k.split('.')
                    if len(t) > 1:
                        return get('.'.join(t[1:]), m.get(t[0]))
                    else:
                        return m.get(k)

                status['parameters'] = OrderedDict()
                for i, k in enumerate(parameters):
                    v = shorten(str(self._alias(get(k, sp))), param_max_width)
                    status['parameters'][k] = v

            for status in statuses.values():
                _add_parameters(status)

            for i, para in enumerate(parameters):
                parameters[i] = shorten(self._alias(str(para)), param_max_width)

        if detailed:
            # get detailed view info
            status_legend = ' '.join('[{}]:{}'.format(v, k) for k, v in self.ALIASES.items())

            if compact:
                num_operations = len(self._operations)

            if pretty:
                OPERATION_STATUS_SYMBOLS = OrderedDict([
                    ('ineligible', '\u25cb'),   # open circle
                    ('eligible', '\u25cf'),     # black circle
                    ('active', '\u25b9'),       # open triangle
                    ('running', '\u25b8'),      # black triangle
                    ('completed', '\u2714'),    # check mark
                ])
                "Pretty (unicode) symbols denoting the execution status of operations."
            else:
                OPERATION_STATUS_SYMBOLS = OrderedDict([
                    ('ineligible', '-'),
                    ('eligible', '+'),
                    ('active', '*'),
                    ('running', '>'),
                    ('completed', 'X')
                ])
                "Symbols denoting the execution status of operations."
            operation_status_legend = ' '.join('[{}]:{}'.format(v, k)
                                               for k, v in OPERATION_STATUS_SYMBOLS.items())

        context['jobs'] = list(statuses.values())
        context['overview'] = overview
        context['detailed'] = detailed
        context['all_ops'] = all_ops
        context['parameters'] = parameters
        context['compact'] = compact
        context['pretty'] = pretty
        context['unroll'] = unroll
        if overview:
            context['progress_sorted'] = progress_sorted
        if detailed:
            context['alias_bool'] = {True: 'Y', False: 'N'}
            context['scheduler_status_code'] = _FMT_SCHEDULER_STATUS
            context['status_legend'] = status_legend
            if compact:
                context['extra_num_operations'] = max(num_operations-1, 0)
            if not unroll:
                context['operation_status_legend'] = operation_status_legend
                context['operation_status_symbols'] = OPERATION_STATUS_SYMBOLS

        def _add_dummy_operation(job):
            job['operations'][''] = {
                'completed': False,
                'eligible': False,
                'scheduler_status': JobStatus.dummy}

        for job in context['jobs']:
            has_eligible_ops = any([v['eligible'] for v in job['operations'].values()])
            if not has_eligible_ops and not context['all_ops']:
                _add_dummy_operation(job)

        op_counter = Counter()
        for job in context['jobs']:
            for k, v in job['operations'].items():
                if k != '' and v['eligible']:
                    op_counter[k] += 1
        context['op_counter'] = op_counter.most_common(eligible_jobs_max_lines)
        n = len(op_counter) - len(context['op_counter'])
        if n > 0:
            context['op_counter'].append(('[{} more operations omitted]'.format(n), ''))

        status_renderer = StatusRenderer()
        # We have to make a deep copy of the template environment if we're
        # using a process Pool for parallelism. Somewhere in the process of
        # manually pickling and dispatching tasks to individual processes
        # Python's reference counter loses track of the environment and ends up
        # destructing the template environment. This causes subsequent calls to
        # print_status to fail (although _fetch_status calls will still
        # succeed).
        te = deepcopy(template_environment) if status_parallelization == "process" \
            else template_environment
        render_output = status_renderer.render(template, te, context, detailed,
                                               expand, unroll, compact, output_format)

        print(render_output, file=file)

        # Show profiling results (if enabled)
        if profiling_results:
            print('\n' + '\n'.join(profiling_results), file=file)

        return status_renderer

    def run_operations(self, operations=None, pretend=False, np=None, timeout=None, progress=False):
        """Execute the next operations as specified by the project's workflow.

        See also: :meth:`~.run`

        :param operations:
            The operations to execute (optional).
        :type operations:
            Sequence of instances of :class:`.JobOperation`
        :param pretend:
            Do not actually execute the operations, but show which command would have been used.
        :type pretend:
            bool
        :param np:
            The number of processors to use for each operation.
        :type np:
            int
        :param timeout:
            An optional timeout for each operation in seconds after which execution will
            be cancelled. Use -1 to indicate not timeout (the default).
        :type timeout:
            int
        :param progress:
            Show a progress bar during execution.
        :type progress:
            bool
        """
        if timeout is not None and timeout < 0:
            timeout = None
        if operations is None:
            operations = list(self._get_pending_operations(self))
        else:
            operations = list(operations)   # ensure list

        if np is None or np == 1 or pretend:
            if progress:
                operations = tqdm(operations)
            for operation in operations:
                self._execute_operation(operation, timeout, pretend)
        else:
            logger.debug("Parallelized execution of {} operation(s).".format(len(operations)))
            with contextlib.closing(Pool(processes=cpu_count() if np < 0 else np)) as pool:
                logger.debug("Parallelized execution of {} operation(s).".format(len(operations)))
                try:
                    import pickle
                    self._run_operations_in_parallel(pool, pickle, operations, progress, timeout)
                    logger.debug("Used cPickle module for serialization.")
                except Exception as error:
                    if not isinstance(error, (pickle.PickleError, self._PickleError)) and\
                            'pickle' not in str(error).lower():
                        raise    # most likely not a pickle related error...

                    try:
                        import cloudpickle
                    except ImportError:  # The cloudpickle package is not available.
                        logger.error("Unable to parallelize execution due to a pickling error. "
                                     "\n\n - Try to install the 'cloudpickle' package, e.g., with "
                                     "'pip install cloudpickle'!\n")
                        raise error
                    else:
                        try:
                            self._run_operations_in_parallel(
                                pool, cloudpickle, operations, progress, timeout)
                        except self._PickleError as error:
                            raise RuntimeError("Unable to parallelize execution due to a pickling "
                                               "error: {}.".format(error))

    class _PickleError(Exception):
        "Indicates a pickling error while trying to parallelize the execution of operations."
        pass

    @staticmethod
    def _dumps_op(op):
        return (op.id, op.name, op.job._id, op.cmd, op.directives)

    def _loads_op(self, blob):
        id, name, job_id, cmd, directives = blob
        return JobOperation(id, name, self.open_job(id=job_id), cmd, directives)

    def _run_operations_in_parallel(self, pool, pickle, operations, progress, timeout):
        """Execute operations in parallel.

        This function executes the given list of operations with the provided process pool.

        Since pickling of the project instance is likely to fail, we manually pickle the
        project instance and the operations before submitting them to the process pool to
        enable us to try different pool and pickle module combinations.
        """

        try:
            s_project = pickle.dumps(self)
            s_tasks = [(pickle.loads, s_project, self._dumps_op(op))
                       for op in tqdm(operations, desc='Serialize tasks', file=sys.stderr)]
        except Exception as error:  # Masking all errors since they must be pickling related.
            raise self._PickleError(error)

        results = [pool.apply_async(_execute_serialized_operation, task) for task in s_tasks]

        for result in tqdm(results) if progress else results:
            result.get(timeout=timeout)

    def _execute_operation(self, operation, timeout=None, pretend=False):
        if pretend:
            print(operation.cmd)
            return None

        logger.info("Execute operation '{}'...".format(operation))
        # Check if we need to fork for operation execution...
        if (
            # The 'fork' directive was provided and evaluates to True:
            operation.directives.get('fork', False)
            # Separate process needed to cancel with timeout:
            or timeout is not None
            # The operation function is not registered with the class:
            or operation.name not in self._operation_functions
            # The specified executable is not the same as the interpreter instance:
            or operation.directives.get('executable', sys.executable) != sys.executable
        ):
            # ... need to fork:
            logger.debug(
                "Forking to execute operation '{}' with "
                "cmd '{}'.".format(operation, operation.cmd))
            subprocess.run(operation.cmd, shell=True, timeout=timeout,
                           check=True)
        else:
            # ... executing operation in interpreter process as function:
            logger.debug(
                "Executing operation '{}' with current interpreter "
                "process ({}).".format(operation, os.getpid()))
            try:
                self._operation_functions[operation.name](operation.job)
            except Exception as e:
                raise UserOperationError(
                    'An exception was raised during operation {operation.name} '
                    'for job {operation.job}.'.format(operation=operation)) from e

    def _get_default_directives(self):
        return {name: self.groups[name].operation_directives.get(name, dict())
                for name in self.operations}

    def run(self, jobs=None, names=None, pretend=False, np=None, timeout=None, num=None,
            num_passes=1, progress=False, order=None, ignore_conditions=IgnoreConditions.NONE):
        """Execute all pending operations for the given selection.

        This function will run in an infinite loop until all pending operations
        are executed, unless it reaches the maximum number of passes per
        operation or the maximum number of executions.

        By default there is no limit on the total number of executions, but a specific
        operation will only be executed once per job. This is to avoid accidental
        infinite loops when no or faulty post conditions are provided.

        See also: :meth:`~.run_operations`

        :param jobs:
            Only execute operations for the given jobs, or all if the argument is omitted.
        :type jobs:
            Sequence of instances :class:`.Job`.
        :param names:
            Only execute operations that are in the provided set of names, or all, if the
            argument is omitted.
        :type names:
            Sequence of :class:`str`
        :param pretend:
            Do not actually execute the operations, but show which command would have been used.
        :type pretend:
            bool
        :param np:
            Parallelize to the specified number of processors. Use -1 to parallelize to all
            available processing units.
        :type np:
            int
        :param timeout:
            An optional timeout for each operation in seconds after which execution will
            be cancelled. Use -1 to indicate not timeout (the default).
        :type timeout:
            int
        :param num:
            The total number of operations that are executed will not exceed this argument
            if provided.
        :type num:
            int
        :param num_passes:
            The total number of one specific job-operation pair will not exceed this argument.
            The default is 1, there is no limit if this argument is `None`.
        :type num_passes:
            int
        :param progress:
            Show a progress bar during execution.
        :type progress:
            bool
        :param order:
            Specify the order of operations, possible values are:
                * 'none' or None (no specific order)
                * 'by-job' (operations are grouped by job)
                * 'cyclic' (order operations cyclic by job)
                * 'random' (shuffle the execution order randomly)
                * callable (a callable returning a comparison key for an
                            operation used to sort operations)

            The default value is `none`, which is equivalent to `by-job` in the current
            implementation.

            .. note::
                Users are advised to not rely on a specific execution order, as a
                substitute for defining the workflow in terms of pre- and post-conditions.
                However, a specific execution order may be more performant in cases where
                operations need to access and potentially lock shared resources.
        :type order:
            str, callable, or NoneType
        :param ignore_conditions:
            Specify if pre and/or post conditions check is to be ignored for eligibility check.
            The default is :py:class:`IgnoreConditions.NONE`.
        :type ignore_conditions:
            :py:class:`~.IgnoreConditions`
        """
        # If no jobs argument is provided, we run operations for all jobs.
        if jobs is None:
            jobs = self

        # Get all matching FlowGroups
        if isinstance(names, str):
            raise ValueError(
                "The names argument of FlowProject.run() must be a sequence of strings, "
                "not a string.")
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

        if type(ignore_conditions) != IgnoreConditions:
            raise ValueError(
                "The ignore_conditions argument of FlowProject.run() "
                "must be a member of class IgnoreConditions")

        messages = list()

        def log(msg, lvl=logging.INFO):
            messages.append((msg, lvl))

        reached_execution_limit = Event()

        def select(operation):
            if operation.job not in self:
                log("Job '{}' is no longer part of the project.".format(operation.job))
                return False
            if num is not None and select.total_execution_count >= num:
                reached_execution_limit.set()
                raise StopIteration  # Reached total number of executions

            # Check whether the operation was executed more than the total number of allowed
            # passes *per operation* (default=1).
            if num_passes is not None and select.num_executions.get(operation, 0) >= num_passes:
                log("Operation '{}' exceeds max. # of allowed "
                    "passes ({}).".format(operation, num_passes))

                # Warn if an operation has no post-conditions set.
                has_post_conditions = len(self.operations[operation.name]._postconds)
                if not has_post_conditions:
                    log("Operation '{}' has no post-conditions!".format(operation.name),
                        logging.WARNING)

                return False    # Reached maximum number of passes for this operation.

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
                logger.warning("Reached the maximum number of operations that can be executed, but "
                               "there are still operations pending.")
                break
            try:
                # Change groups to available run JobOperations
                with self._potentially_buffered():
                    operations = []
                    for flow_group in flow_groups:
                        for job in jobs:
                            operations.extend(
                                flow_group._create_run_job_operations(
                                    self._entrypoint, default_directives, job, ignore_conditions))

                    operations = list(filter(select, operations))
            finally:
                if messages:
                    for msg, level in set(messages):
                        logger.log(level, msg)
                    del messages[:]     # clear
            if not operations:
                break   # No more pending operations or execution limits reached.

            # Optionally re-order operations for execution if order argument is provided:
            if callable(order):
                operations = list(sorted(operations, key=order))
            elif order == 'cyclic':
                groups = [list(group)
                          for _, group in groupby(operations, key=lambda op: op.job)]
                operations = list(roundrobin(*groups))
            elif order == 'random':
                random.shuffle(operations)
            elif order is None or order in ('none', 'by-job'):
                pass  # by-job is the default order
            else:
                raise ValueError(
                    "Invalid value for the 'order' argument, valid arguments are "
                    "'none', 'by-job', 'cyclic', 'random', None, or a callable.")

            logger.info(
                "Executing {} operation(s) (Pass # {:02d})...".format(len(operations), i_pass))
            self.run_operations(operations, pretend=pretend,
                                np=np, timeout=timeout, progress=progress)

    def _generate_operations(self, cmd, jobs, requires=None):
        "Generate job-operations for a given 'direct' command."
        for job in jobs:
            if requires and set(requires).difference(self.labels(job)):
                continue
            cmd_ = cmd.format(job=job)
            yield JobOperation(name=cmd_.replace(' ', '-'), cmd=cmd_, job=job)

    def _gather_flow_groups(self, names=None):
        """Grabs FlowGroups that match any of a set of names."""
        operations = OrderedDict()
        # if no names are selected try all singleton groups
        if names is None:
            names = self._operations.keys()
        for name in names:
            if name in operations:
                continue
            groups = [group for gname, group in self.groups.items() if
                      re.fullmatch(name, gname)]
            if len(groups) > 0:
                for group in groups:
                    operations[group.name] = group
            else:
                continue
        operations = list(operations.values())
        if not self._verify_group_compatibility(operations):
            raise ValueError("Cannot specify groups or operations that "
                             "will be included twice when using the"
                             " -o/--operation option.")
        return operations

    def _get_submission_operations(self, jobs, default_directives, names=None,
                                   ignore_conditions=IgnoreConditions.NONE,
                                   ignore_conditions_on_execution=IgnoreConditions.NONE):
        """Grabs JobOperations that are eligible to run from FlowGroups."""
        for job in jobs:
            for group in self._gather_flow_groups(names):
                if group.eligible(job, ignore_conditions) and self._eligible_for_submission(group,
                                                                                            job):
                    yield group._create_submission_job_operation(
                        entrypoint=self._entrypoint,
                        default_directives=default_directives,
                        job=job, index=0,
                        ignore_conditions_on_execution=ignore_conditions_on_execution)

    def _get_pending_operations(self, jobs, operation_names=None,
                                ignore_conditions=IgnoreConditions.NONE):
        "Get all pending operations for the given selection."
        assert not isinstance(operation_names, str)
        for op in self.next_operations(* jobs, ignore_conditions=ignore_conditions):
            if operation_names is None or any(re.fullmatch(n, op.name) for n in operation_names):
                yield op

    def _verify_group_compatibility(self, groups):
        """Verifies that all selected groups can be submitted together."""
        return all(a.isdisjoint(b) for a in groups for b in groups if a != b)

    @contextlib.contextmanager
    def _potentially_buffered(self):
        """Enable the use of buffered mode for certain functions."""
        if self.config['flow'].as_bool('use_buffered_mode'):
            logger.debug("Entering buffered mode...")
            with signac.buffered():
                yield
            logger.debug("Exiting buffered mode.")
        else:
            yield

    def script(self, operations, parallel=False, template='script.sh', show_template_help=False):
        """Generate a run script to execute given operations.

        :param operations:
            The operations to execute.
        :type operations:
            Sequence of instances of :class:`.JobOperation`
        :param parallel:
            Execute all operations in parallel (default is False).
        :type parallel:
            bool
        :param template:
            The name of the template to use to generate the script.
        :type template:
            str
        :param show_template_help:
            Show help related to the templating system and then exit.
        :type show_template_help:
            bool
        """
        template_environment = self._template_environment()
        template = template_environment.get_template(template)
        context = self._get_standard_template_context()
        # For script generation we do not need the extra logic used for
        # generating cluster job scripts.
        context['base_script'] = 'base_script.sh'
        context['operations'] = list(operations)
        context['parallel'] = parallel
        if show_template_help:
            self._show_template_help_and_exit(template_environment, context)
        return template.render(** context)

    def _generate_submit_script(self, _id, operations, template, show_template_help, env, **kwargs):
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

        logger.info("Use environment '{}'.".format(env))
        logger.info("Set 'base_script={}'.".format(env.template))
        context['base_script'] = env.template
        context['environment'] = env
        context['id'] = _id
        context['operations'] = list(operations)
        context.update(kwargs)
        if show_template_help:
            self._show_template_help_and_exit(template_environment, context)
        return template.render(** context)

    def submit_operations(self, operations, _id=None, env=None, parallel=False, flags=None,
                          force=False, template='script.sh', pretend=False,
                          show_template_help=False, **kwargs):
        r"""Submit a sequence of operations to the scheduler.

        :param operations:
            The operations to submit.
        :type operations:
            A sequence of instances of :py:class:`.JobOperation`
        :param _id:
            The _id to be used for this submission.
        :type _id:
            str
        :param parallel:
            Execute all bundled operations in parallel.
        :type parallel:
            bool
        :param flags:
            Additional options to be forwarded to the scheduler.
        :type flags:
            list
        :param force:
            Ignore all warnings or checks during submission, just submit.
        :type force:
            bool
        :param template:
            The name of the template file to be used to generate the submission script.
        :type template:
            str
        :param pretend:
            Do not actually submit, but only print the submission script to screen. Useful
            for testing the submission workflow.
        :type pretend:
            bool
        :param show_template_help:
            Show information about available template variables and filters and exit.
        :type show_template_help:
            bool
        :param \*\*kwargs:
            Additional keyword arguments to be forwarded to the scheduler.
        :return:
            Returns the submission status after successful submission or None.
        """
        if _id is None:
            _id = self._store_bundled(operations)
        if env is None:
            env = self._environment
        else:
            warnings.warn("The env argument is deprecated as of 0.10 and will be removed in 0.12. "
                          "Instead, set the environment when constructing a FlowProject.",
                          DeprecationWarning)

        print("Submitting cluster job '{}':".format(_id), file=sys.stderr)

        def _msg(group):
            print(" - Group: {}".format(group), file=sys.stderr)
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
                **kwargs
            )
        except ConfigKeyError as error:
            raise SubmitError(
                "Unable to submit, because of a configuration error.\n"
                "The following key is missing: {key}.\n"
                "You can add the key to the configuration for example with:\n\n"
                "  $ signac config --global set {key} VALUE\n".format(key=str(error)))
        else:
            # Keys which were explicitly set by the user, but are not evaluated by the
            # template engine are cause for concern and might hint at a bug in the template
            # script or ill-defined directives. Here we check whether all directive keys that
            # have been explicitly set by the user were actually evaluated by the template
            # engine and warn about those that have not been.
            keys_unused = {
                key for op in operations for key in
                op.directives._keys_set_by_user.difference(op.directives.keys_used)
                if key not in ('fork', 'nranks', 'omp_num_threads')  # ignore list
            }
            if keys_unused:
                logger.warning(
                    "Some of the keys provided as part of the directives were not used by "
                    "the template script, including: {}".format(
                        ', '.join(sorted(keys_unused))))
            if pretend:
                print(script)

            else:
                return env.submit(_id=_id, script=script, flags=flags, **kwargs)

    def submit(self, bundle_size=1, jobs=None, names=None, num=None, parallel=False,
               force=False, walltime=None, env=None, ignore_conditions=IgnoreConditions.NONE,
               ignore_conditions_on_execution=IgnoreConditions.NONE, **kwargs):
        """Submit function for the project's main submit interface.

        :param bundle_size:
            Specify the number of operations to be bundled into one submission, defaults to 1.
        :type bundle_size:
            int
        :param jobs:
            Only submit operations associated with the provided jobs. Defaults to all jobs.
        :type jobs:
            Sequence of instances :class:`.Job`.
        :param names:
            Only submit operations with any of the given names, defaults to all names.
        :type names:
            Sequence of :class:`str`
        :param num:
            Limit the total number of submitted operations, defaults to no limit.
        :type num:
            int
        :param parallel:
            Execute all bundled operations in parallel.
        :type parallel:
            bool
        :param force:
            Ignore all warnings or checks during submission, just submit.
        :type force:
            bool
        :param walltime:
            Specify the walltime in hours or as instance of :py:class:`datetime.timedelta`.
        :param ignore_conditions:
            Specify if pre and/or post conditions check is to be ignored for eligibility check.
            The default is :py:class:`IgnoreConditions.NONE`.
        :type ignore_conditions:
            :py:class:`~.IgnoreConditions`
        :param ignore_conditions_on_execution:
            Specify if pre and/or post conditions check is to be ignored for eligibility check after
            submitting. The default is :py:class:`IgnoreConditions.NONE`.
        :type ignore_conditions:
            :py:class:`~.IgnoreConditions`
        """
        # Regular argument checks and expansion
        if jobs is None:
            jobs = self  # select all jobs
        if isinstance(names, str):
            raise ValueError(
                "The 'names' argument must be a sequence of strings, however you "
                "provided a single string: {}.".format(names))
        if env is None:
            env = self._environment
        else:
            warnings.warn("The env argument is deprecated as of 0.10 and will be removed in 0.12. "
                          "Instead, set the environment when constructing a FlowProject.",
                          DeprecationWarning)
        if walltime is not None:
            try:
                walltime = datetime.timedelta(hours=walltime)
            except TypeError as error:
                if str(error) != 'unsupported type for timedelta ' \
                                 'hours component: datetime.timedelta':
                    raise
        if type(ignore_conditions) != IgnoreConditions:
            raise ValueError(
                "The ignore_conditions argument of FlowProject.run() "
                "must be a member of class IgnoreConditions")

        # Gather all pending operations.
        with self._potentially_buffered():
            default_directives = self._get_default_directives()
            operations = self._get_submission_operations(jobs, default_directives, names,
                                                         ignore_conditions,
                                                         ignore_conditions_on_execution)
        if num is not None:
            operations = list(islice(operations, num))

        # Bundle them up and submit.
        for bundle in _make_bundles(operations, bundle_size):
            status = self.submit_operations(operations=bundle, env=env, parallel=parallel,
                                            force=force, walltime=walltime, **kwargs)
            if status is not None:  # operations were submitted, store status
                for operation in bundle:
                    operation.set_status(status)

    @classmethod
    def _add_submit_args(cls, parser):
        "Add arguments to submit sub command to parser."
        parser.add_argument(
            'flags',
            type=str,
            nargs='*',
            help="Flags to be forwarded to the scheduler.")
        parser.add_argument(
            '--pretend',
            action='store_true',
            help="Do not really submit, but print the submission script to screen.")
        parser.add_argument(
            '--force',
            action='store_true',
            help="Ignore all warnings and checks, just submit.")
        parser.add_argument(
            '--test',
            action='store_true',
            help="Do not interact with the scheduler, implies --pretend.")
        parser.add_argument(
            '--ignore-conditions',
            type=str,
            choices=['none', 'pre', 'post', 'all'],
            default=IgnoreConditions.NONE,
            action=_IgnoreConditionsConversion,
            help="Specify conditions to ignore for eligibility check.")
        parser.add_argument(
            '--ignore-conditions-on-execution',
            type=str,
            choices=['none', 'pre', 'post', 'all'],
            default=IgnoreConditions.NONE,
            action=_IgnoreConditionsConversion,
            help="Specify conditions to ignore after submitting. May be useful "
                 "for conditions that cannot be checked once scheduled.")

        cls._add_operation_selection_arg_group(parser)
        cls._add_operation_bundling_arg_group(parser)
        cls._add_template_arg_group(parser)

    @classmethod
    def _add_script_args(cls, parser):
        cls._add_operation_selection_arg_group(parser)
        execution_group = parser.add_argument_group('execution')
        execution_group.add_argument(
            '-p', '--parallel',
            action='store_true',
            help="Execute all operations in parallel.")
        cls._add_direct_cmd_arg_group(parser)
        cls._add_template_arg_group(parser)

    @classmethod
    def _add_template_arg_group(cls, parser, default='script.sh'):
        "Add argument group to parser for template handling."
        template_group = parser.add_argument_group(
            'templating',
            "The execution and submission scripts are always generated from a script "
            "which is by default called '{default}' and located within the default "
            "template directory. The system uses a default template if none is provided. "
            "The default template extends from a base template, which may be different "
            "depending on the local compute environment, e.g., 'slurm.sh' for an environment "
            "with SLURM scheduler. The name of the base template is provided with the "
            "'base_script' template variable.".format(default=default),
        )
        template_group.add_argument(
            '--template',
            type=str,
            default=default,
            help="The name of the template file within the template directory. "
                 "The standard template directory is '${{project_root}}/templates' and "
                 "can be configured with the 'template_dir' configuration variable. "
                 "Default: '{}'.".format(default))
        template_group.add_argument(
            '--template-help',
            dest='show_template_help',
            action='store_true',
            help="Show information about the template context, including available variables "
                 "and filter functions; then exit.")

    @classmethod
    def _add_job_selection_args(cls, parser):
        parser.add_argument(
            '-j', '--job-id',
            type=str,
            nargs='+',
            help="Only select jobs that match the given id(s).")
        parser.add_argument(
            '-f', '--filter',
            type=str,
            nargs='+',
            help="Only select jobs that match the given state point filter.")
        parser.add_argument(
            '--doc-filter',
            type=str,
            nargs='+',
            help="Only select jobs that match the given document filter.")

    @classmethod
    def _add_operation_selection_arg_group(cls, parser, operations=None):
        "Add argument group to parser for job-operation selection."
        selection_group = parser.add_argument_group(
            'job-operation selection',
            "By default, all eligible operations for all jobs are selected. Use "
            "the options in this group to reduce this selection.")
        cls._add_job_selection_args(selection_group)
        selection_group.add_argument(
            '-o', '--operation',
            dest='operation_name',
            nargs='+',
            help="Only select operation or groups that match the given "
            "operation/group name(s).")
        selection_group.add_argument(
            '-n', '--num',
            type=int,
            help="Limit the total number of operations/groups to be selected. A group is "
                 "considered to be one operation even if it consists of multiple operations.")

    @classmethod
    def _add_operation_bundling_arg_group(cls, parser):
        """Add argument group to parser for operation bundling."""

        bundling_group = parser.add_argument_group(
            'bundling',
            "Bundle multiple operations for execution, e.g., to submit them "
            "all together to a cluster job, or execute them in parallel within "
            "an execution script.")
        bundling_group.add_argument(
            '-b', '--bundle',
            type=int,
            nargs='?',
            const=0,
            default=1,
            dest='bundle_size',
            help="Bundle multiple operations for execution in a single "
            "scheduler job. When this option is provided without argument, "
            " all pending operations are aggregated into one bundle.")
        bundling_group.add_argument(
            '-p', '--parallel',
            action='store_true',
            help="Execute all operations in a single bundle in parallel.")

    @classmethod
    def _add_direct_cmd_arg_group(cls, parser):
        direct_cmd_group = parser.add_argument_group("direct cmd")
        direct_cmd_group.add_argument(
            '--cmd',
            type=str,
            help="Directly specify the command for an operation. "
                 "For example: --cmd='echo {job._id}'. "
                 "--cmd option is deprecated as of 0.9 and will be removed in 0.11.")
        direct_cmd_group.add_argument(
            '--requires',
            type=str,
            nargs='+',
            help="Manually specify all labels that are required for the direct command "
                 "to be considered eligible for execution.")

    def export_job_statuses(self, collection, statuses):
        "Export the job statuses to a database collection."
        for status in statuses:
            job = self.open_job(id=status['job_id'])
            status['statepoint'] = job.statepoint()
            collection.update_one({'_id': status['job_id']},
                                  {'$set': status}, upsert=True)

    @classmethod
    def _add_print_status_args(cls, parser):
        "Add arguments to parser for the :meth:`~.print_status` method."
        cls._add_job_selection_args(parser)
        view_group = parser.add_argument_group(
            'view',
            "Specify how to format the status display.")
        view_group.add_argument(
            '--json',
            dest='dump_json',
            action='store_true',
            help="Do not format the status display, but dump all data formatted in JSON.")
        view_group.add_argument(
            '-d', '--detailed',
            action='store_true',
            help="Show a detailed view of all jobs and their labels and operations.")
        view_group.add_argument(
            '-a', '--all-operations',
            dest='all_ops',
            action='store_true',
            help="Show information about all operations, not just active or eligible ones.")
        view_group.add_argument(
            '--only-incomplete-operations',
            dest='only_incomplete',
            action='store_true',
            help="Only show information for jobs with incomplete operations.")
        view_group.add_argument(
            '--stack',
            action='store_false',
            dest='unroll',
            help="Show labels and operations in separate rows.")
        view_group.add_argument(
            '-1', '--one-line',
            dest='compact',
            action='store_true',
            help="Show only one line per job.")
        view_group.add_argument(
            '-e', '--expand',
            action='store_true',
            help="Display job labels and job operations in two separate tables.")
        view_group.add_argument(
            '--pretty',
            action='store_true')
        view_group.add_argument(
            '--full',
            action='store_true',
            help="Show all available information (implies --detailed --all-operations).")
        view_group.add_argument(
            '--no-overview',
            action='store_false',
            dest='overview',
            help="Do not print an overview.")
        view_group.add_argument(
            '-m', '--overview-max-lines',
            type=_positive_int,
            help="Limit the number of lines in the overview.")
        view_group.add_argument(
            '-p', '--parameters',
            type=str,
            nargs='*',
            help="Display select parameters of the job's "
                 "statepoint with the detailed view.")
        view_group.add_argument(
            '--param-max-width',
            type=int,
            help="Limit the width of each parameter row.")
        view_group.add_argument(
            '--eligible-jobs-max-lines',
            type=_positive_int,
            help="Limit the number of eligible jobs that are shown.")
        parser.add_argument(
            '--ignore-errors',
            action='store_true',
            help="Ignore errors that might occur when querying the scheduler.")
        parser.add_argument(
            '--no-parallelize',
            action='store_true',
            help="Do not parallelize the status determination. "
                 "The '--no-parallelize' argument is deprecated. "
                 "Please use the status_parallelization configuration "
                 "instead (see above)."
            )
        view_group.add_argument(
            '-o', '--output-format',
            type=str,
            default='terminal',
            help="Set status output format: terminal, markdown, or html.")

    def labels(self, job):
        """Yields all labels for the given ``job``.

        See also: :meth:`~.label`
        """
        for label_func, label_name in self._label_functions.items():
            if label_name is None:
                label_name = getattr(label_func, '_label_name',
                                     getattr(label_func, '__name__', type(label_func).__name__))
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
        """
        Add an operation to the workflow.

        This method will add an instance of :py:class:`~.FlowOperation` to the
        operations-dict of this project.

        .. seealso::

            A Python function may be defined as an operation function directly using
            the :meth:`~.operation` decorator.

        Any FlowOperation is associated with a specific command, which should be
        a function of :py:class:`~signac.contrib.job.Job`. The command (cmd) can
        be stated as function, either by using str-substitution based on a job's
        attributes, or by providing a unary callable, which expects an instance
        of job as its first and only positional argument.

        For example, if we wanted to define a command for a program called 'hello',
        which expects a job id as its first argument, we could construct the following
        two equivalent operations:

        .. code-block:: python

            op = FlowOperation('hello', cmd='hello {job._id}')
            op = FlowOperation('hello', cmd=lambda 'hello {}'.format(job._id))

        Here are some more useful examples for str-substitutions:

        .. code-block:: python

            # Substitute job state point parameters:
            op = FlowOperation('hello', cmd='cd {job.ws}; hello {job.sp.a}')

        Pre-requirements (pre) and post-conditions (post) can be used to
        trigger an operation only when certain conditions are met. Conditions are unary
        callables, which expect an instance of job as their first and only positional
        argument and return either True or False.

        An operation is considered "eligible" for execution when all pre-requirements
        are met and when at least one of the post-conditions is not met.
        Requirements are always met when the list of requirements is empty and
        post-conditions are never met when the list of post-conditions is empty.

        Please note, eligibility in this contexts refers only to the workflow pipeline
        and not to other contributing factors, such as whether the job-operation is currently
        running or queued.

        :param name:
            A unique identifier for this operation, which may be freely chosen.
        :type name:
            str
        :param cmd:
            The command to execute operation; should be a function of job.
        :type cmd:
            str or callable
        :param pre:
            Required conditions.
        :type pre:
            sequence of callables
        :param post:
            Post-conditions to determine completion.
        :type pre:
            sequence of callables
        """
        if name in self.operations:
            raise KeyError("An operation with this identifier is already added.")
        op = self.operations[name] = FlowCmdOperation(cmd=cmd, pre=pre, post=post)
        if name in self._groups:
            raise KeyError("A group with this identifier already exists.")
        self._groups[name] = FlowGroup(name,
                                       operations={name: op},
                                       operation_directives=dict(name=kwargs))

    def completed_operations(self, job):
        """Determine which operations have been completed for job.

        :param job:
            The signac job handle.
        :type job:
            :class:`~signac.contrib.job.Job`
        :return:
            The name of the operations that are complete.
        :rtype:
            str
        """
        for name, op in self._operations.items():
            if op.complete(job):
                yield name

    def _job_operations(self, job, ignore_conditions=IgnoreConditions.NONE):
        "Yield instances of JobOperation constructed for specific jobs."
        for name in self.operations:
            group = self._groups[name]
            yield from group._create_run_job_operations(entrypoint=self._entrypoint, job=job,
                                                        default_directives=dict(),
                                                        ignore_conditions=ignore_conditions,
                                                        index=0)

    def next_operations(self, *jobs, ignore_conditions=IgnoreConditions.NONE):
        """Determine the next eligible operations for jobs.

        :param jobs:
            The signac job handles.
        :type job:
            :class:`~signac.contrib.job.Job`
        :param ignore_conditions:
            Specify if pre and/or post conditions check is to be ignored for eligibility check.
            The default is :py:class:`IgnoreConditions.NONE`.
        :type ignore_conditions:
            :py:class:`~.IgnoreConditions`
        :yield:
            All instances of :class:`~.JobOperation` jobs are eligible for.
        """
        for job in jobs:
            for op in self._job_operations(job, ignore_conditions):
                yield op

    @classmethod
    def operation(cls, func, name=None):
        """Add the function `func` as operation function to the class workflow definition.

        This function is designed to be used as a decorator function, for example:

        .. code-block:: python

            @FlowProject.operation
            def hello(job):
                print('Hello', job)

        See also: :meth:`~.flow.FlowProject.add_operation`.
        """
        if isinstance(func, str):
            return lambda op: cls.operation(op, name=func)

        if name is None:
            name = func.__name__

        if (name, func) in cls._OPERATION_FUNCTIONS:
            raise ValueError(
                "An operation with name '{}' is already registered.".format(name))
        if name in cls._GROUP_NAMES:
            raise ValueError("A group with name '{}' is already registered.".format(name))

        signature = inspect.signature(func)
        for i, (k, v) in enumerate(signature.parameters.items()):
            if i and v.default is inspect.Parameter.empty:
                raise ValueError(
                    "Only the first argument in an operation argument may not have "
                    "a default value! ({})".format(name))

        # Append the name and function to the class registry
        cls._OPERATION_FUNCTIONS.append((name, func))
        cls._GROUPS.append(FlowGroupEntry(name=name, options=""))
        if hasattr(func, '_flow_groups'):
            func._flow_groups.append(name)
        else:
            func._flow_groups = [name]
        return func

    @classmethod
    def _collect_operations(cls):
        "Collect all operations that were add via decorator."
        operations = []
        for parent_class in cls.__mro__:
            operations.extend(getattr(parent_class, '_OPERATION_FUNCTIONS', []))
        return operations

    @classmethod
    def _collect_conditions(cls, attr):
        "Collect conditions from attr using the mro hierarchy."
        ret = defaultdict(list)
        for parent_class in cls.__mro__:
            for func, conds in getattr(parent_class, attr, dict()).items():
                ret[func].extend(conds)
        return ret

    @classmethod
    def _collect_pre_conditions(cls):
        "Collect all pre-conditions that were added via decorator."
        return cls._collect_conditions('_OPERATION_PRE_CONDITIONS')

    @classmethod
    def _collect_post_conditions(cls):
        "Collect all post-conditions that were added via decorator."
        return cls._collect_conditions('_OPERATION_POST_CONDITIONS')

    def _register_operations(self):
        "Register all operation functions registered with this class and its parent classes."
        operations = self._collect_operations()
        pre_conditions = self._collect_pre_conditions()
        post_conditions = self._collect_post_conditions()

        for name, func in operations:
            if name in self._operations:
                raise ValueError(
                    "Repeat definition of operation with name '{}'.".format(name))

            # Extract pre/post conditions and directives from function:
            params = {
                'pre': pre_conditions.get(func, None),
                'post': post_conditions.get(func, None)}

            # Construct FlowOperation:
            if getattr(func, '_flow_cmd', False):
                self._operations[name] = FlowCmdOperation(cmd=func, **params)
            else:
                self._operations[name] = FlowOperation(name=name, **params)
                self._operation_functions[name] = func

    @classmethod
    def make_group(cls, name, options=""):
        """Make a FlowGroup named ``name`` and return a decorator to make groups.

        .. code-block:: python

            example_group = FlowProject.make_group('example')

            @example_group
            @FlowProject.operation
            def foo(job):
                return "hello world"

        FlowGroups group operations together for running and submitting
        JobOperations.

        :param name:
            The name of the :class:`~.FlowGroup`.
        :type name:
            str
        :param options:
            A string to append to submissions can be any valid :meth:`FlowOperation.run` option.
        :type options:
            str
        """
        if name in cls._GROUP_NAMES:
            raise ValueError("Repeat definition of group with name '{}'.".format(name))
        else:
            cls._GROUP_NAMES.add(name)

        group_entry = FlowGroupEntry(name, options)
        cls._GROUPS.append(group_entry)
        return group_entry

    def _register_groups(self):
        "Register all groups and add the correct operations to each."
        group_entries = []
        # Gather all groups from class and parent classes.
        for cls in type(self).__mro__:
            group_entries.extend(getattr(cls, '_GROUPS', []))

        # Initialize all groups without operations
        for entry in group_entries:
            self._groups[entry.name] = FlowGroup(entry.name, options=entry.options)

        # Add operations and directives to group
        for (op_name, op) in self._operations.items():
            if isinstance(op, FlowCmdOperation):
                func = op._cmd
            else:
                func = self._operation_functions[op_name]

            if hasattr(func, '_flow_groups'):
                operation_directives = getattr(func, '_flow_group_operation_directives', dict())
                for group_name in func._flow_groups:
                    self._groups[group_name].add_operation(
                        op_name, op, operation_directives.get(group_name, None))

            # For singleton groups add directives
            self._groups[op_name].operation_directives[op_name] = getattr(func,
                                                                          '_flow_directives',
                                                                          dict())

    @property
    def operations(self):
        "The dictionary of operations that have been added to the workflow."
        return self._operations

    @property
    def groups(self):
        return self._groups

    def _eligible_for_submission(self, flow_group, job):
        """Determine if a flow_group is eligible for submission with a given job.

        By default, an operation is eligible for submission when it
        is not considered active, that means already queued or running.
        """
        if flow_group is None or job is None:
            return False
        if flow_group._get_status(job) >= JobStatus.submitted:
            return False
        group_ops = set(flow_group)
        for other_group in self._groups.values():
            if group_ops & set(other_group):
                if other_group._get_status(job) >= JobStatus.submitted:
                    return False
        return True

    def _main_status(self, args):
        "Print status overview."
        jobs = self._select_jobs_from_args(args)
        if args.compact and not args.unroll:
            logger.warn("The -1/--one-line argument is incompatible with "
                        "'--stack' and will be ignored.")
        show_traceback = args.debug or args.show_traceback
        args = {key: val for key, val in vars(args).items()
                if key not in ['func', 'verbose', 'debug', 'show_traceback',
                               'job_id', 'filter', 'doc_filter']}
        if args.pop('full'):
            args['detailed'] = args['all_ops'] = True

        start = time.time()
        try:
            self.print_status(jobs=jobs, **args)
        except NoSchedulerError:
            self.print_status(jobs=jobs, **args)
        except Exception as error:
            if show_traceback:
                logger.error(
                    "Error during status update: {}\nUse '--ignore-errors' to "
                    "complete the update anyways.".format(str(error)))
            else:
                logger.error(
                    "Error during status update: {}\nUse '--ignore-errors' to "
                    "complete the update anyways or '--show-traceback' to show "
                    "the full traceback.".format(str(error)))
                error = error.__cause__  # Always show the user traceback cause.
            traceback.print_exception(type(error), error, error.__traceback__)
        else:
            # Use small offset to account for overhead with few jobs
            delta_t = (time.time() - start - 0.5) / max(len(jobs), 1)
            config_key = 'status_performance_warn_threshold'
            warn_threshold = flow_config.get_config_value(config_key)
            if not args['profile'] and delta_t > warn_threshold >= 0:
                print(
                    "WARNING: "
                    "The status compilation took more than {}s per job. Consider "
                    "using `--profile` to determine bottlenecks within your project "
                    "workflow definition.\n"
                    "Execute `signac config set flow.{} VALUE` to specify the "
                    "warning threshold in seconds.\n"
                    "To speed up the compilation, you can try executing "
                    "`signac config set flow.status_parallelization 'process'` to set "
                    "the status_parallelization config value to process."
                    "Use -1 to completely suppress this warning.\n"
                    .format(warn_threshold, config_key), file=sys.stderr)

    def _main_next(self, args):
        "Determine the jobs that are eligible for a specific operation."
        for job in self:
            if args.name in {op.name for op in self.next_operations(job)}:
                print(job)

    def _main_run(self, args):
        "Run all (or select) job operations."
        # Select jobs:
        jobs = self._select_jobs_from_args(args)

        # Setup partial run function, because we need to call this either
        # inside some context managers or not based on whether we need
        # to switch to the project root directory or not.
        run = functools.partial(self.run,
                                jobs=jobs, names=args.operation_name, pretend=args.pretend,
                                np=args.parallel, timeout=args.timeout, num=args.num,
                                num_passes=args.num_passes, progress=args.progress,
                                order=args.order,
                                ignore_conditions=args.ignore_conditions)

        if args.switch_to_project_root:
            with add_cwd_to_environment_pythonpath():
                with switch_to_directory(self.root_directory()):
                    run()
        else:
            run()

    def _main_script(self, args):
        "Generate a script for the execution of operations."
        if args.requires and not args.cmd:
            raise ValueError(
                "The --requires option can only be used in combination with --cmd.")
        if args.cmd and args.operation_name:
            raise ValueError(
                "Cannot use the -o/--operation-name and the --cmd options in combination!")
        # Select jobs:
        jobs = self._select_jobs_from_args(args)

        # Gather all pending operations or generate them based on a direct command...
        with self._potentially_buffered():
            if args.cmd:
                warnings.warn("The --cmd option for script is deprecated as of "
                              "0.9 and will be removed in 0.11.",
                              DeprecationWarning)
                operations = self._generate_operations(args.cmd, jobs, args.requires)
            else:
                names = args.operation_name if args.operation_name else None
                default_directives = self._get_default_directives()
                operations = self._get_submission_operations(jobs, default_directives, names,
                                                             args.ignore_conditions,
                                                             args.ignore_conditions_on_execution)
            operations = list(islice(operations, args.num))

        # Generate the script and print to screen.
        print(self.script(
            operations=operations, parallel=args.parallel,
            template=args.template, show_template_help=args.show_template_help))

    def _main_submit(self, args):
        "Submit jobs to a scheduler"
        if args.test:
            args.pretend = True
        kwargs = vars(args)

        # Select jobs:
        jobs = self._select_jobs_from_args(args)

        # Fetch the scheduler status.
        if not args.test:
            self._fetch_scheduler_status(jobs)

        names = args.operation_name if args.operation_name else None
        self.submit(jobs=jobs, names=names, **kwargs)

    def _main_exec(self, args):
        if len(args.jobid):
            jobs = [self.open_job(id=jid) for jid in args.jobid]
        else:
            jobs = self
        try:
            try:
                operation_function = self._operation_functions[args.operation]
            except KeyError:
                operation = self._operations[args.operation]

                def operation_function(job):
                    cmd = operation(job).format(job=job)
                    subprocess.run(cmd, shell=True, check=True)

        except KeyError:
            raise KeyError("Unknown operation '{}'.".format(args.operation))

        if getattr(operation_function, '_flow_aggregate', False):
            operation_function(jobs)
        else:
            for job in jobs:
                operation_function(job)

    def _select_jobs_from_args(self, args):
        "Select jobs with the given command line arguments ('-j/-f/--doc-filter')."
        if args.job_id and (args.filter or args.doc_filter):
            raise ValueError(
                "Cannot provide both -j/--job-id and -f/--filter or --doc-filter in combination.")

        if args.job_id:
            try:
                return [self.open_job(id=job_id) for job_id in args.job_id]
            except KeyError as error:
                raise LookupError("Did not find job with id {}.".format(error))
        else:
            filter_ = parse_filter_arg(args.filter)
            doc_filter = parse_filter_arg(args.doc_filter)
            return JobsCursor(self, filter_, doc_filter)

    def main(self, parser=None):
        """Call this function to use the main command line interface.

        In most cases one would want to call this function as part of the
        class definition, e.g.:

        .. code-block:: python

             my_project.py
            from flow import FlowProject

            class MyProject(FlowProject):
                pass

            if __name__ == '__main__':
                MyProject().main()

        You can then execute this script on the command line:

        .. code-block:: bash

            $ python my_project.py --help
        """
        # Find file that main is called in. When running through the command
        # line interface, we know exactly what the entrypoint path should be:
        # it's the file where main is called, which we can pull off the stack.
        self._entrypoint.setdefault('path', os.path.realpath(inspect.stack()[-1].filename))

        if parser is None:
            parser = argparse.ArgumentParser()

        base_parser = argparse.ArgumentParser(add_help=False)

        # The argparse module does not automatically merge options shared between the main
        # parser and the subparsers. We therefore assign different destinations for each
        # option and then merge them manually below.
        for prefix, _parser in (('main_', parser), ('', base_parser)):
            _parser.add_argument(
                '-v', '--verbose',
                dest=prefix + 'verbose',
                action='count',
                default=0,
                help="Increase output verbosity.")
            _parser.add_argument(
                '--show-traceback',
                dest=prefix + 'show_traceback',
                action='store_true',
                help="Show the full traceback on error.")
            _parser.add_argument(
                '--debug',
                dest=prefix + 'debug',
                action='store_true',
                help="This option implies `-vv --show-traceback`.")

        subparsers = parser.add_subparsers()

        parser_status = subparsers.add_parser(
            'status',
            parents=[base_parser],
            help="You can specify the parallelization of the status command "
                 "by setting the flow.status_parallelization config "
                 "value to 'thread' (default), 'none', or 'process'. You can do this by "
                 "executing `signac config set flow.status_parallelization "
                 "VALUE`.")
        self._add_print_status_args(parser_status)
        parser_status.add_argument(
            '--profile',
            const=inspect.getsourcefile(inspect.getmodule(self)),
            nargs='?',
            help="Collect statistics to determine code paths that are responsible "
                 "for the majority of runtime required for status determination. "
                 "Optionally provide a filename pattern to select for what files "
                 "to show result for. Defaults to the main module. "
                 "(requires pprofile)")
        parser_status.set_defaults(func=self._main_status)

        parser_next = subparsers.add_parser(
            'next',
            parents=[base_parser],
            description="Determine jobs that are eligible for a specific operation.")
        parser_next.add_argument(
            'name',
            type=str,
            help="The name of the operation.")
        parser_next.set_defaults(func=self._main_next)

        parser_run = subparsers.add_parser(
            'run',
            parents=[base_parser],
        )
        self._add_operation_selection_arg_group(parser_run, list(sorted(self._operations)))

        execution_group = parser_run.add_argument_group('execution')
        execution_group.add_argument(
            '--pretend',
            action='store_true',
            help="Do not actually execute commands, just show them.")
        execution_group.add_argument(
            '--progress',
            action='store_true',
            help="Display a progress bar during execution.")
        execution_group.add_argument(
            '--num-passes',
            type=int,
            default=1,
            help="Specify how many times a particular job-operation may be executed within one "
                 "session (default=1). This is to prevent accidental infinite loops, "
                 "where operations are executed indefinitely, because post conditions "
                 "were not properly set. Use -1 to allow for an infinite number of passes.")
        execution_group.add_argument(
            '-t', '--timeout',
            type=int,
            help="A timeout in seconds after which the execution of one operation is canceled.")
        execution_group.add_argument(
            '--switch-to-project-root',
            action='store_true',
            help="Temporarily add the current working directory to the python search path and "
                 "switch to the root directory prior to execution.")
        execution_group.add_argument(
            '-p', '--parallel',
            type=int,
            nargs='?',
            const='-1',
            help="Specify the number of cores to parallelize to. Defaults to all available "
                 "processing units if argument is omitted.")
        execution_group.add_argument(
            '--order',
            type=str,
            choices=['none', 'by-job', 'cyclic', 'random'],
            default=None,
            help="Specify the execution order of operations for each execution pass.")
        execution_group.add_argument(
            '--ignore-conditions',
            type=str,
            choices=['none', 'pre', 'post', 'all'],
            default=IgnoreConditions.NONE,
            action=_IgnoreConditionsConversion,
            help="Specify conditions to ignore for eligibility check.")
        parser_run.set_defaults(func=self._main_run)

        parser_script = subparsers.add_parser(
            'script',
            parents=[base_parser],
        )
        parser_script.add_argument(
            '--ignore-conditions',
            type=str,
            choices=['none', 'pre', 'post', 'all'],
            default=IgnoreConditions.NONE,
            action=_IgnoreConditionsConversion,
            help="Specify conditions to ignore for eligibility check.")
        parser_script.add_argument(
            '--ignore-conditions-on-execution',
            type=str,
            choices=['none', 'pre', 'post', 'all'],
            default=IgnoreConditions.NONE,
            action=_IgnoreConditionsConversion,
            help="Specify conditions to ignore after submitting. May be useful "
                 "for conditions that cannot be checked once scheduled.")
        self._add_script_args(parser_script)
        parser_script.set_defaults(func=self._main_script)

        parser_submit = subparsers.add_parser(
            'submit',
            parents=[base_parser],
            conflict_handler='resolve',
        )
        self._add_submit_args(parser_submit)
        env_group = parser_submit.add_argument_group(
            '{} options'.format(self._environment.__name__))
        self._environment.add_args(env_group)
        parser_submit.set_defaults(func=self._main_submit)
        print('Using environment configuration:', self._environment.__name__, file=sys.stderr)

        parser_exec = subparsers.add_parser(
            'exec',
            parents=[base_parser],
        )
        parser_exec.add_argument(
            'operation',
            type=str,
            choices=list(sorted(self._operations)),
            help="The operation to execute.")
        parser_exec.add_argument(
            'jobid',
            type=str,
            nargs='*',
            help="The job ids, as registered in the signac project. "
                 "Omit to default to all statepoints.")
        parser_exec.set_defaults(func=self._main_exec)

        args = parser.parse_args()
        if not hasattr(args, 'func'):
            parser.print_usage()
            sys.exit(2)

        # Manually 'merge' the various global options defined for both the main parser
        # and the parent parser that are shared by all subparsers:
        for dest in ('verbose', 'show_traceback', 'debug'):
            setattr(args, dest, getattr(args, 'main_' + dest) or getattr(args, dest))
            delattr(args, 'main_' + dest)

        # Read the config file and set the internal flag.
        # Do not overwrite with False if not present in config file
        if flow_config.get_config_value('show_traceback'):
            args.show_traceback = True

        if args.debug:  # Implies '-vv' and '--show-traceback'
            args.verbose = max(2, args.verbose)
            args.show_traceback = True

        # Support print_status argument alias
        if args.func == self._main_status and args.full:
            args.detailed = args.all_ops = True

        # Empty parameters argument on the command line means: show all varying parameters.
        if hasattr(args, 'parameters'):
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
                print("Execute with '--show-traceback' or '--debug' to show the "
                      "full traceback.", file=sys.stderr)
            else:
                print("Execute with '--show-traceback' or '--debug' to get more "
                      "information.", file=sys.stderr)
            sys.exit(1)

        try:
            args.func(args)
        except NoSchedulerError as error:
            print("ERROR: {}".format(error),
                  "Consider to use the 'script' command to generate an execution script instead.",
                  file=sys.stderr)
            _show_traceback_and_exit(error)
        except SubmitError as error:
            print("Submission error:", error, file=sys.stderr)
            _show_traceback_and_exit(error)
        except (TimeoutError, subprocess.TimeoutExpired) as error:
            print("Error: Failed to complete execution due to "
                  "timeout ({}s).".format(args.timeout), file=sys.stderr)
            _show_traceback_and_exit(error)
        except Jinja2TemplateNotFound as error:
            print("Did not find template script '{}'.".format(error), file=sys.stderr)
            _show_traceback_and_exit(error)
        except AssertionError as error:
            if not args.show_traceback:
                print("ERROR: Encountered internal error during program execution.",
                      file=sys.stderr)
            _show_traceback_and_exit(error)
        except (UserOperationError, UserConditionError) as error:
            if str(error):
                print("ERROR: {}\n".format(error), file=sys.stderr)
            else:
                print("ERROR: Encountered error during program execution.\n",
                      file=sys.stderr)
            _show_traceback_and_exit(error)
        except Exception as error:
            if str(error):
                print("ERROR: Encountered error during program execution: "
                      "'{}'\n".format(error), file=sys.stderr)
            else:
                print("ERROR: Encountered error during program execution.\n",
                      file=sys.stderr)
            _show_traceback_and_exit(error)


def _execute_serialized_operation(loads, project, operation):
    """Invoke the _execute_operation() method on a serialized project instance."""
    project = loads(project)
    project._execute_operation(project._loads_op(operation))


def _serialized_get_job_status(s_task):
    """Invoke the _get_job_status() method on a serialized project instance."""
    loads = s_task[0]
    project = loads(s_task[1])
    job = project.open_job(id=s_task[2])
    ignore_errors = s_task[3]
    cached_status = s_task[4]
    return project.get_job_status(job, ignore_errors=ignore_errors, cached_status=cached_status)


# Status-related helper functions


def _update_status(args):
    "Wrapper-function, that is probably obsolete."
    return update_status(* args)


def _update_job_status(job, scheduler_jobs):
    "Update the status entry for job."
    update_status(job, scheduler_jobs)


__all__ = [
    'FlowProject',
    'FlowOperation',
    'label', 'staticlabel', 'classlabel',
]
