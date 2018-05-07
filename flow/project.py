# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Workflow definition with the FlowProject.

The FlowProject is a signac Project, that allows the user to define
a workflow based on job classification and job operations.

A job may be classified based on its metadata and data in the form
of str labels. These str-labels are yielded in the classify() method.


Based on the classification a "next operation" may be identified, that
should be executed next to further the workflow. While the user is free
to choose any method for the determination of the "next operation", one
option is to use a FlowGraph.
"""
from __future__ import print_function
import sys
import os
import logging
import warnings
import argparse
import datetime
import json
import inspect
import subprocess
from collections import defaultdict
from itertools import islice
from itertools import count
from hashlib import sha1

import signac
from signac.common import six
from jinja2 import Environment
from jinja2 import PackageLoader
from jinja2 import ChoiceLoader
from jinja2 import FileSystemLoader

from .environment import get_environment
from .environment import NodesEnvironment
from .scheduling.base import Scheduler
from .scheduling.base import ClusterJob
from .scheduling.base import JobStatus
from .scheduling.status import update_status
from .errors import SubmitError
from .errors import NoSchedulerError
from .util import tabulate
from .util.tqdm import tqdm
from .util.misc import _positive_int
from .util.misc import _mkdir_p
from .util.misc import draw_progressbar
from .util.misc import _format_timedelta
from .util.misc import write_human_readable_statepoint
from .util.misc import switch_to_directory
from .util.translate import abbreviate
from .util.translate import shorten
from .labels import label
from .labels import staticlabel
from .labels import classlabel
from .labels import _is_label_func
from . import legacy

if not six.PY2:
    from subprocess import TimeoutExpired

logger = logging.getLogger(__name__)


# Global variable that is used internally to keep track of which
# FlowProject methods belong to the legacy templating system. Such
# a method is docorated with the _part_of_legacy_template_system()
# decorator and then registered in this variable.
_LEGACY_TEMPLATING_METHODS = set()


def _part_of_legacy_template_system(method):
    "Label a method to be part of the legacy templating system."
    _LEGACY_TEMPLATING_METHODS.add(method.__name__)
    method._legacy_intact = True
    return method


def _support_legacy_api(method):
    """Label a method to be wrapped with a legacy API compatibility layer.

    This is a decorator function, that will wrap 'method' with a wrapper function
    that attempts to detect and resolve legacy API use of said method.
    All wrapper functions are implemented in the 'legacy' module.
    """
    return getattr(legacy, 'support_{}_legacy_api'.format(method.__name__))(method)


def _execute(cmd, timeout=None):
    "Helper function for py2/3 compatible execution of forked processes."
    if six.PY2:
        subprocess.call(cmd, shell=True)
    elif sys.version_info >= (3, 5):
        subprocess.run(cmd, timeout=timeout, shell=True)
    else:    # Older high-level API
        subprocess.call(cmd, timeout=timeout, shell=True)


def cmd(func):
    setattr(func, '_flow_cmd', True)
    return func


class _condition(object):

    def __init__(self, condition):
        self.condition = condition

    @classmethod
    def isfile(cls, filename):
        return cls(lambda job: job.isfile(filename))

    @classmethod
    def true(cls, key):
        return cls(lambda job: job.document.get(key, False))

    @classmethod
    def always(cls, func):
        return cls(lambda _: True)(func)

    @classmethod
    def never(cls, func):
        return cls(lambda _: False)(func)

    @classmethod
    def after(cls, other_func):
        def check_preconds(job):
            post_conds = getattr(other_func, '_flow_post', list())
            return all(c(job) for c in post_conds)
        return cls(check_preconds)


class _pre(_condition):

    def __call__(self, func):
        pre_conditions = getattr(func, '_flow_pre', list())
        pre_conditions.append(self.condition)
        func._flow_pre = pre_conditions
        return func


class _post(_condition):

    def __init__(self, condition):
        self.condition = condition

    def __call__(self, func):
        post_conditions = getattr(func, '_flow_post', list())
        post_conditions.append(self.condition)
        func._flow_post = post_conditions
        return func


def make_bundles(operations, size=None):
    """Utility function for the generation of bundles.

    This function splits an iterable of operations into  equally
    sized bundles and a possibly smaller final bundle.
    """
    n = None if size == 0 else size
    while True:
        b = list(islice(operations, n))
        if b:
            yield b
        else:
            break


class JobOperation(object):
    """This class represents the information needed to execute one operation for one job.

    An operation function in this context is a shell command, which should be a function
    of one and only one signac job.

    .. note::

        Users should usually not instantiate this class themselves, but use the
        :meth:`.FlowProject.add_operation` method.

    :param name:
        The name of this JobOperation instance. The name is arbitrary,
        but helps to concisely identify the operation in various contexts.
    :type name:
        str
    :param job:
        The job instance associated with this operation.
    :type job:
        :py:class:`signac.Job`.
    :type cmd:
        str
    """
    def __init__(self, name, job, cmd, np=None):
        if np is None:
            np = 1
        self.name = name
        self.job = job
        self.cmd = cmd
        self.np = np

    def __str__(self):
        return "{}({})".format(self.name, self.job)

    def __repr__(self):
        return "{type}(name='{name}', job='{job}', cmd={cmd}, np={np})".format(
            type=type(self).__name__,
            name=self.name,
            job=str(self.job),
            cmd=repr(self.cmd),
            np=self.np)

    def get_id(self):
        "Return a name, which identifies this job-operation."
        return '{}-{}'.format(self.job, self.name)

    @classmethod
    def expand_id(self, _id):
        return {'job_id': _id[:32], 'operation-name': _id[33:]}

    def __hash__(self):
        return int(sha1(self.get_id().encode('utf-8')).hexdigest(), 16)

    def __eq__(self, other):
        return self.get_id() == other.get_id()

    def set_status(self, value):
        "Store the operation's status."
        status_doc = self.job.document.get('status', dict())
        status_doc[self.get_id()] = int(value)
        self.job.document['status'] = status_doc

    def get_status(self):
        "Retrieve the operation's last known status."
        try:
            return JobStatus(self.job.document['status'][self.get_id()])
        except KeyError:
            return JobStatus.unknown


class FlowCondition(object):
    """A FlowCondition represents a condition as a function of a signac job.

    The __call__() function of a FlowCondition object may return either True
    or False, representing whether the condition is met or not.
    This can be used to build a graph of conditions and operations.

    :param callback:
        A function with one positional argument (the job)
    :type callback:
        :py:class:`~signac.contrib.job.Job`
    """
    def __init__(self, callback):
        self._callback = callback

    def __call__(self, job):
        if self._callback is None:
            return True
        return self._callback(job)

    def __hash__(self):
        return hash(self._callback)

    def __eq__(self, other):
        return self._callback == other._callback


class FlowOperation(object):
    """A FlowOperation represents a data space operation, operating on any job.

    Any FlowOperation is associated with a specific command, which should be
    a function of :py:class:`~signac.contrib.job.Job`. The command (cmd) can
    be stated as function, either by using str-substitution based on a job's
    attributes, or by providing a unary callable, which expects an instance
    of job as its first and only positional argument.

    For example, if we wanted to define a command for a program called 'hello',
    which expects a job id as its first argument, we could contruct the following
    two equivalent operations:

    .. code-block:: python

        op = FlowOperation('hello', cmd='hello {job._id}')
        op = FlowOperation('hello', cmd=lambda 'hello {}'.format(job._id))

    Here is another example for possible str-substitutions:

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
    :type pre:
        sequence of callables
    :param np:
        Specify the number of processors this operation requires, defaults to 1.
    :type np:
        int
    """
    def __init__(self, cmd, pre=None, post=None, np=None):
        if pre is None:
            pre = []
        if post is None:
            post = []
        if np is None:
            np = 1
        self._cmd = cmd
        self._np = np

        self._prereqs = [FlowCondition(cond) for cond in pre]
        self._postconds = [FlowCondition(cond) for cond in post]

    def __str__(self):
        return "{type}(cmd='{cmd}')".format(type=type(self).__name__, cmd=self._cmd)

    def eligible(self, job):
        "Eligible, when all pre-conditions are true and at least one post-condition is false."
        pre = all([cond(job) for cond in self._prereqs])
        if len(self._postconds):
            post = any([not cond(job) for cond in self._postconds])
        else:
            post = True
        return pre and post

    def complete(self, job):
        "True when all post-conditions are met."
        if len(self._postconds):
            return all([cond(job) for cond in self._postconds])
        else:
            return False

    def __call__(self, job=None):
        if callable(self._cmd):
            return self._cmd(job).format(job=job)
        else:
            return self._cmd.format(job=job)

    def np(self, job):
        "Return the number of processors this operation requires."
        if callable(self._np):
            return self._np(job)
        else:
            return self._np


class _FlowProjectClass(type):
    """Metaclass for the FlowProject class."""

    def __new__(metacls, name, bases, namespace, **kwargs):
        cls = type.__new__(metacls, name, bases, dict(namespace))

        # All operation functions are registered with the operation() classmethod, which is
        # intended to be used as decorator function. The _OPERATION_FUNCTIONS dict maps the
        # the operation name to the operation function.
        cls._OPERATION_FUNCTIONS = list()

        # All label functions are registered with the label() classmethod, which is intendeded
        # to be used as decorator function. The _LABEL_FUNCTIONS dict contains the function as
        # key and the label name as value, or None to use the default label name.
        cls._LABEL_FUNCTIONS = dict()

        return cls


class FlowProject(six.with_metaclass(_FlowProjectClass, signac.contrib.Project)):
    """A signac project class specialized for workflow management.

    TODO: ADD BASIC DESCRIPTION ON HOW TO USE THIS CLASS HERE.

    :param config:
        A signac configuaration, defaults to the configuration loaded
        from the environment.
    :type config:
        A signac config object.
    """
    def __init__(self, config=None, environment=None):
        if environment is None:
            environment = get_environment()
        signac.contrib.Project.__init__(self, config)
        self._label_functions = dict()
        self._operation_functions = dict()
        self._operations = dict()
        self._environment = environment
        self._template_environment = Environment(
            loader=ChoiceLoader([
                FileSystemLoader(os.path.join(self.root_directory(), 'templates')),
                PackageLoader('flow', 'templates'),
                ]),
            trim_blocks=True)
        self._template_environment.filters['time_delta'] = _format_timedelta
        self._register_labels()
        self._register_operations()
        self._setup_legacy_templating()

    @classmethod
    def label(cls, label_name_or_func=None):
        if callable(label_name_or_func):
            cls._LABEL_FUNCTIONS[label_name_or_func] = None
            return label_name_or_func

        def label_func(func):
            cls._LABEL_FUNCTIONS[func] = label_name_or_func
            return func

        return label_func

    def _register_class_labels(self):
        """This function registers all label functions, which are part of the class definition.

        To register a class method or function as label function, use the generalized label()
        function.
        """
        def predicate(m):
            return inspect.ismethod(m) or inspect.isfunction(m)

        for name, method in inspect.getmembers(type(self), predicate=predicate):
            if _is_label_func(method):
                self._label_functions[method] = None

    def _register_labels(self):
        "Register all label functions registered with this class and its parent classes."
        self._register_class_labels()

        for cls in type(self).__mro__:
            self._label_functions.update(getattr(cls, '_LABEL_FUNCTIONS', dict()))

    pre = _pre
    post = _post

    # Simple translation table for output strings.
    NAMES = {
        'next_operation': 'next_op',
    }

    @classmethod
    def _tr(cls, x):
        "Use name translation table for x."
        return cls.NAMES.get(x, x)

    # These are default aliases used within the status output. You can add aliases
    # with the update_aliases() classmethod.
    ALIASES = dict(
        status='S',
        unknown='U',
        registered='R',
        queued='Q',
        active='A',
        inactive='I',
        requires_attention='!'
    )

    @classmethod
    def _alias(cls, x):
        "Use alias if specified."
        try:
            return abbreviate(x, cls.ALIASES.get(x, x))
        except TypeError:
            return x

    @classmethod
    def update_aliases(cls, aliases):
        "Update the ALIASES table for this class."
        cls.ALIASES.update(aliases)

    def _fn_bundle(self, bundle_id):
        "Return the canonical name to store bundle information."
        return os.path.join(self.root_directory(), '.bundles', bundle_id)

    def _store_bundled(self, operations):
        """Store operation-ids as part of a bundle and return bundle id.

        The operation identifiers are stored in a  text within a file
        determined by the _fn_bundle() method.

        This may be used to idenfity the status of individual operations
        root directory. This is necessary to be able to identify each

        A single operation will not be stored, but instead the operation's
        id is directly returned.

        :param operations:
            The operations to bundle.
        :type operations:
            A sequence of instances of :py:class:`.JobOperation`
        :return:
            The  bundle id
        :rtype:
            str
        """
        if len(operations) == 1:
            return operations[0].get_id()
        else:
            h = '.'.join(op.get_id() for op in operations)
            bid = '{}-bundle-{}'.format(self, sha1(h.encode('utf-8')).hexdigest())
            fn_bundle = self._fn_bundle(bid)
            _mkdir_p(os.path.dirname(fn_bundle))
            with open(fn_bundle, 'w') as file:
                for operation in operations:
                    file.write(operation.get_id() + '\n')
            return bid

    def _expand_bundled_jobs(self, scheduler_jobs):
        "Expand jobs which were submitted as part of a bundle."
        for job in scheduler_jobs:
            if job.name().startswith('{}-bundle-'.format(self)):
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

    @staticmethod
    def _map_scheduler_jobs(scheduler_jobs):
        "Map all scheduler jobs by job id and operation name."
        for sjob in scheduler_jobs:
            name = sjob.name()
            if name[32] == '-':
                expanded = JobOperation.expand_id(name)
                yield expanded['job_id'], expanded['operation-name'], sjob

    def map_scheduler_jobs(self, scheduler_jobs):
        """Map all scheduler jobs by job id and operation name.

        This function fetches all scheduled jobs from the scheduler
        and generates a nested dictionary, where the first key is
        the job id, the second key the operation name and the last
        value are the cooresponding scheduler jobs.

        For example, to print the status of all scheduler jobs, associated
        with a specific job operation, execute:

        .. code::

                sjobs = project.scheduler_jobs(scheduler)
                sjobs_map = project.map_scheduler_jobs(sjobs)
                for sjob in sjobs_map[job.get_id()][operation]:
                    print(sjob._id(), sjob.status())

        :param scheduler_jobs:
            An iterable of scheduler job instances.
        :return:
            A nested dictionary (job_id, op_name, scheduler jobs)
        """
        sjobs_map = defaultdict(dict)
        for job_id, op, sjob in self._map_scheduler_jobs(scheduler_jobs):
            sjobs = sjobs_map[job_id].setdefault(op, list())
            sjobs.append(sjob)
        return sjobs_map

    def get_job_status(self, job):
        "Return a dict with detailed information about the status of a job."
        result = dict()
        result['job_id'] = str(job)
        status = job.document.get('status', dict())
        result['active'] = is_active(status)
        result['labels'] = sorted(set(self.classify(job)))
        result['operation'] = self.next_operation(job)
        highest_status = max(status.values()) if len(status) else 1
        result['submission_status'] = [JobStatus(highest_status).name]
        return result

    def run_operations(self, operations=None, pretend=False, np=None, timeout=None, progress=False,
                       switch_to_project_root=True):
        """Execute the next operations as specified by the project's workflow.

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
        :type progess:
            bool
        """
        if six.PY2 and timeout is not None:
            logger.warning(
                "The timeout argument for run() is not supported for "
                "Python 2.7 and will be ignored!")
        if timeout is not None and timeout < 0:
            timeout = None
        if operations is None:
            operations = [op for job in self for op in self.next_operations(job) if op is not None]
        if progress:
            operations = tqdm(list(operations))

        for operation in operations:
            cmd = operation.cmd
            if pretend:
                print(cmd)
            else:
                with switch_to_directory(self.root_directory() if switch_to_project_root else None):
                    logger.info("Execute operation '{}'...".format(operation))
                    if not progress:
                        print("Execute operation '{}'...".format(operation), file=sys.stderr)
                    if six.PY2:
                        subprocess.call(cmd, shell=True)
                    else:
                        subprocess.call(cmd, shell=True, timeout=timeout)

    @_support_legacy_api
    def run(self, jobs=None, names=None, pretend=False, timeout=None, num=None,
            num_passes=1, progress=False, switch_to_project_root=True):
            if jobs is None:
                jobs = self

            num_executions = defaultdict(int)

            def select(op):
                if op is None or (names and op.name not in names):
                    return False
                if num_passes > 0 and num_executions.get(op, 0) >= num_passes:
                    print("Operation '{}' exceeds max. # of "
                          "allowed passes ({}).".format(op, num_passes), file=sys.stderr)
                    return False
                return True

            for i in count(1):
                ops = [op for job in jobs for op in self.next_operations(job) if select(op)][:num]
                if not ops:
                    break   # No more pending operations.

                print(
                    "Executing {} operations (Pass # {:02d})...".format(len(ops), i),
                    file=sys.stderr)
                self.run_operations(ops, pretend=pretend, timeout=timeout, progress=progress,
                                    switch_to_project_root=switch_to_project_root)
                if num is not None:
                    num = max(0, num - len(ops))
                for op in ops:
                    num_executions[op] += 1
            else:                       # Else block triggered if there are remaining pending jobs,
                logger.warning(         # but maximum number of passes is exhausted.
                    "Reached maximum number of passes, but there are still operations pending.")

    def _setup_legacy_templating(self):
        """This function identifies whether a subclass has implemented deprecated template
        functions.

        The legacy templating system is used to generate run and cluster submission scripts
        if that is the case. A warning is emitted to inform the user that they will not be
        able to use the standard templating system.

        The legacy templating functions are decorated with the _part_of_legacy_template_system()
        decorator.
        """
        self._legacy_templating = False
        legacy_methods = set()
        for method in _LEGACY_TEMPLATING_METHODS:
            if hasattr(self, method) and not hasattr(getattr(self, method), '_legacy_intact'):
                warnings.warn(
                    "The use of FlowProject method '{}' is deprecated!".format(method),
                    DeprecationWarning)
                legacy_methods.add(method)
        if legacy_methods:
            self._legacy_templating = True
            warnings.warn(
                "You are using the following deprecated templating methods: {}. Please remove "
                "those methods from your project class implementation to use the jinja2 templating "
                "system (version >= 0.6).".format(', '.join(legacy_methods)))

    @_part_of_legacy_template_system
    def write_script_header(self, script, **kwargs):
        """"Write the script header for the execution script.

        This function is deprecated and will be removed in version 0.7! Users are
        encouraged to migrate to the new templating system as of version 0.6.
        """
        # Add some whitespace
        script.writeline()
        # Don't use uninitialized environment variables.
        script.writeline('set -u')
        # Exit on errors.
        script.writeline('set -e')
        # Switch into the project root directory
        script.writeline('cd {}'.format(self.root_directory()))
        script.writeline()

    @_part_of_legacy_template_system
    def write_script_operations(self, script, operations, background=False, **kwargs):
        """"Write the commands for the execution of operations as part of a script.

        This function is deprecated and will be removed in version 0.7! Users are
        encouraged to migrate to the new templating system as of version 0.6.
        """
        for op in operations:
            write_human_readable_statepoint(script, op.job)
            script.write_cmd(op.cmd.format(job=op.job), bg=background)
            script.writeline()

    @classmethod
    def write_human_readable_statepoint(cls, script, job):
        """Write statepoint of job in human-readable format to script.

        This function is deprecated and will be removed in version 0.7! Users are
        encouraged to migrate to the new templating system as of version 0.6.
        """
        warnings.warn(
            "The write_human_readable_statepoint() function is deprecated.",
            DeprecationWarning)
        return write_human_readable_statepoint(script, job)

    @_part_of_legacy_template_system
    def write_script_footer(self, script, **kwargs):
        """"Write the script footer for the execution script.

        This function is deprecated and will be removed in version 0.7! Users are
        encouraged to migrate to the new templating system as of version 0.6.
        """
        # Wait until all processes have finished
        script.writeline('wait')

    @_part_of_legacy_template_system
    def write_script(self, script, operations, background=False, **kwargs):
        """Write a script for the execution of operations.

        This function is deprecated and will be removed in version 0.7! Users are
        encouraged to migrate to the new templating system as of version 0.6.

        By default, this function will generate a script with the following components:

        .. code-block:: python

            write_script_header(script)
            write_script_operations(script, operations, background=background)
            write_script_footer(script)

        Consider overloading any of the methods above, before overloading this method.

        :param script:
            The script to write the commands to.
        :param operations:
            The operations to be written to the script.
        :type operations:
            A sequence of JobOperation
        :param background:
            Whether operations should be executed in the background;
            useful to parallelize execution.
        :type background:
            bool
        """
        self.write_script_header(script, **kwargs)
        self.write_script_operations(script, operations, background=background, **kwargs)
        self.write_script_footer(script, **kwargs)

    def _gather_operations(self, job_id=None, operation_name=None, num=None,
                           cmd=None, requires=None, pool=None, force=False, **kwargs):
        "Gather operations to be executed or submitted."
        if job_id:
            jobs = (self.open_job(id=_id) for _id in job_id)
        else:
            jobs = iter(self)

        if operation_name is None:
            names = None
        elif isinstance(operation_name, six.string_types):
            names = {operation_name}
        else:
            names = operation_name

        def get_ops(job):
            if cmd is None:
                ops = set(self.next_operations(job))
                ops.add(self.next_operation(job))
                for op in ops:
                    yield op
            else:
                yield JobOperation(name='user-cmd', cmd=cmd.format(job=job), job=job)

        def eligible(op):
            if op is None:
                return False
            if force:
                return True
            if cmd is None:
                if names and op.name not in names:
                    return False
            if requires is not None:
                labels = set(self.classify(op.job))
                if not all([req in labels for req in requires]):
                    return False
            return self.eligible_for_submission(op)

        # Get the first num eligible operations
        map_ = map if pool is None else pool.imap  # parallelization
        ops = (op for ops in map_(get_ops, jobs) for op in ops)
        return islice((op for op in ops if eligible(op)), num)

    def _get_template_context(self):
        "Return the standard templating context for run and submission scripts."
        context = dict()
        context['project'] = self
        return context

    def script(self, operations, parallel=False, template='run.sh'):
        """Generate a run script to execute given operations (optional in parallel).

        :param operations:
            The operations to execute.
        :type operatons:
            Sequence of instances of :class:`.JobOperation`
        :param parallel:
            Execute all operations in parallel (default is False).
        :param parallel:
            bool
        :param template:
            The name of the template to use to generate the script.
        :type template:
            str
        """
        if self._legacy_templating:
            from .environment import TestEnvironment
            # We first check whether it appears that the user has provided a templating script
            # in which case we raise an exception to avoid highly unexpected behavior.
            fn_template = os.path.join(self.root_directory(), 'templates', template)
            if os.path.isfile(fn_template):
                raise RuntimeError(
                    "In legacy templating mode, unable to use template '{}'.".format(fn_template))
            script = TestEnvironment.script()
            self.write_script(script, operations, background=parallel)
            script.seek(0)
            return script.read()
        else:
            # By default we use the jinja2 templating system to generate the script.
            template = self._template_environment.get_template(template)
            context = self._get_template_context()
            context['base_run'] = 'base_run.sh'
            context['operations'] = list(operations)
            context['parallel'] = parallel
            return template.render(** context)

    def _generate_submit_script(self, _id, operations, parallel=False, template=None,
                                env=None, **kwargs):
        """Generate submission script to submit the execution of operations to a scheduler.
        :param _id:
            The name of the cluster job.
        :type _id:
            str
        :param operations:
            The operations to execute.
        :type operatons:
            Sequence of instances of :class:`.JobOperation`
        :param parallel:
            Execute all operations in parallel (default is False).
        :param parallel:
            bool
        :param template:
            The name of the template to use to generate the script.
        :type template:
            str
        """
        if template is None:
            template = env.template
        assert _id is not None

        if self._legacy_templating:
            fn_template = os.path.join(self.root_directory(), 'templates', template)
            if os.path.isfile(fn_template):
                raise RuntimeError(
                    "In legacy templating mode, unable to use template '{}'.".format(fn_template))
            script = env.script(_id=_id, **kwargs)
            self.write_script(script=script, operations=operations, background=parallel, **kwargs)
            script.seek(0)
            return script.read()
        else:
            template = self._template_environment.get_template(template)
            context = self._get_template_context()
            context['base_submit'] = env.template
            context['environment'] = env.__name__
            context['id'] = _id
            context['operations'] = operations
            context.update(kwargs)
            return template.render(** context)

    @_support_legacy_api
    def submit_operations(self, operations, _id=None, env=None, nn=None, ppn=None, serial=False,
                          flags=None, force=False, template=None, pretend=False, **kwargs):
        "Submit a sequence of operations to the scheduler."
        if _id is None:
            _id = self._store_bundled(operations)
        if env is None:
            env = self._environment
        if template is None:
            template = env.template

        if issubclass(env, NodesEnvironment):
            if nn is None:
                if serial:
                    np_total = max(op.np for op in operations)
                else:
                    np_total = sum(op.np for op in operations)
                try:
                    nn = env.calc_num_nodes(np_total, ppn, force, **kwargs)
                except SubmitError as e:
                    if not (flags or force):
                        raise e

        def _msg(op):
            print("Submitting operation '{}' for job '{}'.".format(op.name, op.job))
            return op

        operations = map(_msg, operations)
        script = self._generate_submit_script(
            # standard arguments:
            _id=_id, operations=operations, parallel=not serial, env=env,
            # legacy arguments:
            nn=nn, ppn=ppn, force=force or flags
            )
        return env.submit(_id=_id, script=script, nn=nn, ppn=ppn, flags=flags, **kwargs)

    @_support_legacy_api
    def submit(self, bundle_size=1, serial=False, force=False,
               nn=None, ppn=None, walltime=None, env=None, **kwargs):
        """Submit function for the project's main submit interface.

        This method gather and optionally bundle all operations which are eligible for execution,
        prepare a submission script using the write_script() method, and finally attempting
        to submit these to the scheduler.

        The primary advantage of using this method over a manual submission process, is that
        submit() will keep track of operation submit status (queued/running/completed/etc.)
        and will automatically prevent the submission of the same operation multiple times if
        it is considered active (e.g. queued or running).
        """
        # Regular argument checks and expansion
        if env is None:
            env = self._environment
        if walltime is not None:
            walltime = datetime.timedelta(hours=walltime)

        operations = self._gather_operations(**kwargs)

        for bundle in make_bundles(operations, bundle_size):
            submit = self.submit_operations
            status = submit(
                operations=bundle, env=env, ppn=ppn, serial=serial,
                force=force, walltime=walltime, **kwargs)

            if status is not None:  # operations were submitted, store status
                for op in bundle:
                    op.set_status(status)

    @classmethod
    def _add_submit_args(cls, parser):
        "Add arguments to parser for the :meth:`~.submit` method."
        parser.add_argument(
            'flags',
            type=str,
            nargs='*',
            help="Flags to be forwarded to the scheduler.")
        parser.add_argument(
            '--pretend',
            action='store_true',
            help="Do not really submit, but print the submittal script to screen.")
        parser.add_argument(
            '--force',
            action='store_true',
            help="Ignore all warnings and checks, just submit.")
        cls._add_script_args(parser)

    @classmethod
    def _add_script_args(cls, parser):
        "Add arguments to parser for the :meth:`~.script` method."
        parser.add_argument(
            '-j', '--job-id',
            type=str,
            nargs='+',
            help="The job id of the jobs to submit. "
            "Omit to automatically select all eligible jobs.")
        parser.add_argument(
            '--script-template',
            type=str,
            default='run.sh',
            help="Specify the template to use the generate the script. "
                 "Default: 'run.sh'")
        selection_group = parser.add_argument_group('job operation selection')
        selection_group.add_argument(
            '-o', '--operation',
            dest='operation_name',
            type=str,
            help="Only submit jobs eligible for the specified operation.")
        selection_group.add_argument(
            '-n', '--num',
            type=int,
            help="Limit the number of operations to be executed.")

        bundling_group = parser.add_argument_group('bundling')
        bundling_group.add_argument(
            '--bundle',
            type=int,
            nargs='?',
            const=0,
            default=1,
            dest='bundle_size',
            help="Specify how many operations to bundle into one submission. "
                 "When no specific size is give, all eligible operations are "
                 "bundled into one submission.")
        bundling_group.add_argument(
            '-p', '--parallel',
            action='store_true',
            help="Schedule the bundled operations to be executed in parallel.")
        bundling_group.add_argument(
            '-s', '--serial',
            action='store_const',
            const=True,
            #  help=argparse.SUPPRESS)   Suppress argument starting with version 0.7!
            help="(deprecated) Schedule the operations to be executed in serial. "
                 "This argument is deprecated as of version 0.6, because operations "
                 "are executed in serial by default. Please use the --parallel "
                 "argument to switch the execution mode.")

        manual_cmd_group = parser.add_argument_group("manual cmd")
        manual_cmd_group.add_argument(
            '--cmd',
            type=str,
            help="Directly specify the command that executes the desired operation.")
        manual_cmd_group.add_argument(
            '--requires',
            type=str,
            nargs='*',
            help="Manually specify all labels, that are required for a job to be "
                 "considered eligible for submission. This is especially useful "
                 "in combination with '--cmd'.")

    def fetch_status(self, jobs=None, file=sys.stderr,
                     ignore_errors=False, scheduler=None, pool=None):
        """Update the status cache for each job.

        This function queries the scheduler to obtain the current status of each
        submitted job-operation.

        :param jobs:
            The jobs to query, defaults to all jobs.
        :type jobs:
            A sequence of instances of :class:`signac.contrib.job.Job`
        :param file:
            A file to write logging output to, defaults to sys.stderr.
        :type file:
            A file-like object.
        :param ignore_errors:
            Ignore errors while querying the scheduler.
        :type ignore_errors:
            bool
        :param scheduler:
            The scheduler to use for querying (deprecated argument); defaults to
            the scheduler provided by the project's associated environment.
        :param pool:
            A multiprocessing pool. If provided, will parallelize the status update.
        :return:
            A dictionary of jobs mapped to their status dicts.
         """
        if jobs is None:
            jobs = list(self.find_jobs())
        try:
            scheduler = self._environment.get_scheduler()
        except NoSchedulerError:
            logger.debug("No scheduler available to update job status.")
        else:
            print(self._tr("Query scheduler..."), file=file)
            sjobs_map = defaultdict(list)
            try:
                for sjob in self.scheduler_jobs(scheduler):
                    sjobs_map[sjob.name()].append(sjob)
            except RuntimeError as e:
                if ignore_errors:
                    logger.warning("WARNING: Error while querying scheduler: '{}'.".format(e))
                else:
                    raise RuntimeError("Error while querying scheduler: '{}'.".format(e))
            if pool is None:
                for job in tqdm(jobs, file=file):
                    _update_job_status(job, sjobs_map)
            else:
                jobs_ = list((job, sjobs_map) for job in jobs)
                pool.map(_update_status, tqdm(jobs_, total=len(jobs), file=file))
        return {job: self.get_job_status(job) for job in jobs}

    def update_stati(self, scheduler, jobs=None, file=sys.stderr, pool=None, ignore_errors=False):
        "This function has been replaced with :meth:`.fetch_status`."
        warnings.warn(
            "The update_stati() method has been replaced by fetch_status() as of version 0.6.",
            DeprecationWarning)
        self.fetch_status(scheduler=scheduler, jobs=jobs, file=file, ignore_errors=ignore_errors)

    def _print_overview(self, stati, max_lines=None, file=sys.stdout):
        "Print the project's status overview."
        progress = defaultdict(int)
        for status in stati:
            for _label in status['labels']:
                progress[_label] += 1
        print("{} {}\n".format(self._tr("Total # of jobs:"), len(stati)), file=file)
        progress_sorted = list(islice(sorted(
            progress.items(), key=lambda x: (x[1], x[0]), reverse=True), max_lines))
        table_header = ['label', 'progress']
        if progress_sorted:
            rows = ([label, '{} {:0.2f}%'.format(
                draw_progressbar(num, len(stati)), 100 * num / len(stati))]
                for label, num in progress_sorted)
            print(tabulate.tabulate(rows, headers=table_header), file=file)
            if max_lines is not None:
                lines_skipped = len(progress) - max_lines
                if lines_skipped > 0:
                    print("{} {}".format(self._tr("Lines omitted:"), lines_skipped), file=file)
        else:
            print(tabulate.tabulate([], headers=table_header), file=file)
            print("[no labels]", file=file)

    def _format_row(self, status, statepoint=None, max_width=None):
        "Format each row in the detailed status output."
        row = [
            status['job_id'],
            ', '.join((self._alias(s) for s in status['submission_status'])),
            status['operation'],
            ', '.join(status.get('labels', [])),
        ]
        if statepoint:
            sps = self.open_job(id=status['job_id']).statepoint()

            def get(k, m):
                if m is None:
                    return
                t = k.split('.')
                if len(t) > 1:
                    return get('.'.join(t[1:]), m.get(t[0]))
                else:
                    return m.get(k)

            for i, k in enumerate(statepoint):
                v = self._alias(get(k, sps))
                row.insert(i + 3, None if v is None else shorten(str(v), max_width))
        if status['operation'] and not status['active']:
            row[1] += ' ' + self._alias('requires_attention')
        return row

    def _print_detailed(self, stati, parameters=None,
                        skip_active=False, param_max_width=None,
                        file=sys.stdout):
        "Print the project's detailed status."
        table_header = [self._tr(self._alias(s))
                        for s in ('job_id', 'status', 'next_operation', 'labels')]
        if parameters:
            for i, value in enumerate(parameters):
                table_header.insert(i + 3, shorten(self._alias(str(value)), param_max_width))
        rows = (self._format_row(status, parameters, param_max_width)
                for status in stati if not (skip_active and status['active']))
        print(tabulate.tabulate(rows, headers=table_header), file=file)
        if abbreviate.table:
            print(file=file)
            print(self._tr("Abbreviations used:"), file=file)
            for a in sorted(abbreviate.table):
                print('{}: {}'.format(a, abbreviate.table[a]), file=file)

    def print_status(self, jobs=None, overview=True, overview_max_lines=None,
                     detailed=False, parameters=None, skip_active=False, param_max_width=None,
                     file=sys.stdout, err=sys.stderr, ignore_errors=False,
                     scheduler=None, pool=None, job_filter=None):
        """Print the status of the project.

        :param job_filter:
            A JSON encoded filter, that all jobs to be submitted need to match.
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
        :param skip_active:
            Only print jobs that are currently inactive.
        :type skip_active:
            bool
        :param param_max_width:
            Limit the number of characters of parameter columns,
            see also: :py:meth:`~.update_aliases`.
        :param file:
            Redirect all output to this file, defaults to sys.stdout.
        :param err:
            Redirect all error output to this file, defaults to sys.stderr.
        :param pool:
            A multiprocessing or threading pool. Providing a pool parallelizes this method.
        :param scheduler:
            The scheduler instance used to fetch the job stati.
        :type scheduler:
            :class:`~.manage.Scheduler`
        """
        if jobs is None:
            if job_filter is not None and isinstance(job_filter, str):
                warnings.warn(
                    "The 'job_filter' argument is deprecated, use the 'jobs' instead.",
                    DeprecationWarning)
                job_filter = json.loads(job_filter)
            jobs = list(self.find_jobs(job_filter))
        elif isinstance(jobs, Scheduler):
            warnings.warn(
                "The signature of the print_status() method has changed!", DeprecationWarning)
            scheduler, jobs = jobs, None
        elif job_filter is not None:
            raise ValueError("Can't provide both the 'jobs' and 'job_filter' argument.")

        if scheduler is not None:
            warnings.warn(
                "print_status(): the scheduler argument is deprecated!", DeprecationWarning)

        stati = self.fetch_status(
            jobs=jobs, file=err, ignore_errors=ignore_errors,
            scheduler=scheduler, pool=pool).values()

        print(self._tr("Generate output..."), file=err)

        title = "{} '{}':".format(self._tr("Status project"), self)
        print('\n' + title, file=file)

        if overview:
            self._print_overview(stati, max_lines=overview_max_lines, file=file)

        if detailed:
            print(file=file)
            print(self._tr("Detailed view:"), file=file)
            self._print_detailed(stati, parameters, skip_active,
                                 param_max_width, file)

    def export_job_stati(self, collection, stati):
        "Export the job stati to a database collection."
        for status in stati:
            job = self.open_job(id=status['job_id'])
            status['statepoint'] = job.statepoint()
            collection.update_one({'_id': status['job_id']},
                                  {'$set': status}, upsert=True)

    @classmethod
    def _add_print_status_args(cls, parser):
        "Add arguments to parser for the :meth:`~.print_status` method."
        parser.add_argument(
            '-f', '--filter',
            dest='job_filter',
            type=str,
            help="Filter jobs.")
        parser.add_argument(
            '--no-overview',
            action='store_false',
            dest='overview',
            help="Do not print an overview.")
        parser.add_argument(
            '-m', '--overview-max-lines',
            type=_positive_int,
            help="Limit the number of lines in the overview.")
        parser.add_argument(
            '-d', '--detailed',
            action='store_true',
            help="Display a detailed view of the job stati.")
        parser.add_argument(
            '-p', '--parameters',
            type=str,
            nargs='*',
            help="Display select parameters of the job's "
                 "statepoint with the detailed view.")
        parser.add_argument(
            '--param-max-width',
            type=int,
            help="Limit the width of each parameter row.")
        parser.add_argument(
            '--skip-active',
            action='store_true',
            help="Display only jobs, which are currently not active.")
        parser.add_argument(
            '--ignore-errors',
            action='store_true',
            help="Ignore errors that might occur when querying the scheduler.")

    def labels(self, job):
        """Auto-generate labels from label-functions.

        This generator function will automatically yield labels,
        from project methods decorated with the ``@label`` decorator.

        For example, we can define a function like this:

        .. code-block:: python

            class MyProject(FlowProject):

                @label()
                def foo_label(self, job):
                    if job.document.get('foo', False):
                        return 'foo-label-text'

        The ``labels()`` generator method will now yield a label with message
        ``foo-label-text`` whenever the job document has a field ``foo`` which
        evaluates to True.

        If the label function returns ``True``, the label message is the
        argument of the ``@label('label_text')`` decorator, or the function
        name if no decorator argument is provided. A label function that
        returns ``False`` or ``None`` will not show a label.

        .. tip::

            In this particular case it may make sense to define the
            ``foo_label()`` method as a *staticmethod*, since it does not
            actually depend on the project instance. We can do this by
            using the ``@staticlabel()`` decorator, equivalently the
            ``@classlabel()`` for *class methods*.

        """
        for label_func, label_name in self._label_functions.items():
            if label_name is None:
                label_name = getattr(label, '_label_name',
                                     getattr(label, '__name__', type(label).__name__))
            try:
                label_value = label_func(job)
            except TypeError:
                try:
                    label_value = label_func(self, job)
                except Exception:
                    label_func = getattr(self, label.__func__.__name__)
                    label_value = label_func(job)

            label_name = getattr(label_func, '_label_name', label_func.__name__)
            assert label_name is not None
            if isinstance(label_value, six.string_types):
                yield label_value
            elif bool(label_value) is True:
                yield label_name

    def add_operation(self, name, cmd, pre=None, post=None, np=None, **kwargs):
        """
        Add an operation to the workflow.

        This method will add an instance of :py:class:`~.FlowOperation` to the
        operations-dict of this project.

        Any FlowOperation is associated with a specific command, which should be
        a function of :py:class:`~signac.contrib.job.Job`. The command (cmd) can
        be stated as function, either by using str-substitution based on a job's
        attributes, or by providing a unary callable, which expects an instance
        of job as its first and only positional argument.

        For example, if we wanted to define a command for a program called 'hello',
        which expects a job id as its first argument, we could contruct the following
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

        Please note, eligibility in this contexts refers only to the workflow pipline
        and not to other contributing factors, such as whether the job-operation is currently
        running or queued.

        :param name:
            A unique identifier for this operation, may be freely choosen.
        :type name:
            str
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
        :type pre:
            sequence of callables
        :param np:
            Specify the number of processors this operation requires,
            defaults to 1.
        :type np:
            int
        """
        if name in self.operations:
            raise KeyError("An operation with this identifier is already added.")
        self.operations[name] = FlowOperation(cmd=cmd, pre=pre, post=post, np=np, **kwargs)

    def classify(self, job):
        """Generator function which yields labels for job.

        By default, this method yields from the project's labels() method.

        :param job:
            The signac job handle.
        :type job:
            :class:`~signac.contrib.job.Job`
        :yields:
            The labels to classify job.
        :yield type:
            str
        """
        for _label in self.labels(job):
            yield _label

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

    def next_operations(self, job):
        """Determine the next operations for job.

        You can, but don't have to use this function to simplify
        the submission process. The default method returns yields all
        operation that a job is eligible for, as defined by the
        :py:meth:`~.add_operation` method.

        :param job:
            The signac job handle.
        :type job:
            :class:`~signac.contrib.job.Job`
        :yield:
            All instances of JobOperation a job is eligible for.
        """
        for name in sorted(self.operations):
            op = self.operations[name]
            if op.eligible(job):
                yield JobOperation(name=name, job=job, cmd=op(job), np=op.np(job))

    def next_operation(self, job):
        """Determine the next operation for this job.

        :param job:
            The signac job handle.
        :type job:
            :class:`~signac.contrib.job.Job`
        :return:
            An instance of JobOperation to execute next or `None`, if no operation is eligible.
        :rtype:
            `:py:class:`~.JobOperation` or `NoneType`
        """
        for op in self.next_operations(job):
            return op

    @classmethod
    def operation(cls, func, name=None):
        "Add the function 'func' as operator function to the class definition."
        if isinstance(func, six.string_types):
            return lambda op: cls.operation(op, name=func)
        if name is None:
            name = func.__name__

        if name in cls._OPERATION_FUNCTIONS:
            raise ValueError(
                "An operation with name '{}' is already registered.".format(name))

        if six.PY2:
            signature = inspect.getargspec(func)
            if len(signature.args) > 1:
                if signature.defaults is None or len(signature.defaults) + 1 < len(signature.args):
                    raise ValueError(
                        "Only the first argument in an operation argument may not have "
                        "a default value! ({})".format(name))
        else:
            signature = inspect.signature(func)
            for i, (k, v) in enumerate(signature.parameters.items()):
                if i and v.default is inspect.Parameter.empty:
                    raise ValueError(
                        "Only the first argument in an operation argument may not have "
                        "a default value! ({})".format(name))

        # Append the name and function to the class registry
        cls._OPERATION_FUNCTIONS.append((name, func))
        return func

    def _register_operations(self):
        "Register all operation functions registered with this class and its parent classes."
        operations = []
        for cls in type(self).__mro__:
            operations.extend(getattr(cls, '_OPERATION_FUNCTIONS', []))

        def _guess_cmd(func, name):
            path = inspect.getsourcefile(func)
            return 'python {} exec {} {{job._id}}'.format(path, name)

        for name, func in operations:
            if name in self._operations:
                raise ValueError(
                    "Repeat definition of operation with name '{}'.".format(name))
            returns_cmd = getattr(func, '_flow_cmd', False)
            if returns_cmd:
                self._operations[name] = FlowOperation(
                    cmd=func,
                    pre=getattr(func, '_flow_pre', None),
                    post=getattr(func, '_flow_post', None))
            else:
                self._operations[name] = FlowOperation(
                    cmd=_guess_cmd(func, name),
                    pre=getattr(func, '_flow_pre', None),
                    post=getattr(func, '_flow_post', None))
                self._operation_functions[name] = func

    @property
    def operations(self):
        "The dictionary of operations that have been added to the workflow."
        return self._operations

    def eligible(self, job_operation, **kwargs):
        """Determine if job is eligible for operation.

        .. warning::

            This function is deprecated, please use
            :py:meth:`~.eligible_for_submission` instead.
        """
        raise RuntimeError("The eligible() method is deprecated.")

    def eligible_for_submission(self, job_operation):
        """Determine if a job-operation is eligible for submission.

        By default, an operation is eligible for submission when it
        is not considered active, that means already queued or running.
        """
        if job_operation is None:
            return False
        if job_operation.get_status() >= JobStatus.submitted:
            return False
        return True

    def main(self, parser=None, pool=None):
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

        def _status(env, args):
            "Print status overview."
            args = vars(args)
            del args['func']
            del args['debug']
            try:
                self.print_status(pool=pool, **args)
            except NoSchedulerError:
                self.print_status(pool=pool, **args)
            return 0

        def _next(env, args):
            "Determine the jobs that are eligible for a specific operation."
            for job in self:
                next_op = self.next_operation(job)
                if next_op is not None and next_op.name == args.name:
                    print(job)

        def _run(env, args):
            "Run all (or select) job operations."
            if args.np is not None:  # Remove beginning of version 0.7.
                raise RuntimeError(
                    "The run --np argument is deprecated as of version 0.6!")
            if args.job_id:
                jobs = [self.open_job(id=jid) for jid in args.job_id]
            else:
                jobs = None
            try:
                self.run(
                    jobs=jobs,
                    names=set(args.name),
                    pretend=args.pretend,
                    timeout=args.timeout,
                    progress=args.progress,
                    num=args.num,
                    num_passes=args.num_passes,
                )
            except TimeoutExpired:
                print("Error: Failed to complete execution due to "
                      "timeout ({}s).".format(args.timeout), file=sys.stderr)
                if args.debug:
                    raise
                else:
                    return 1

        def _script(env, args):
            "Generate a script for the execution of operations."
            if args.serial:             # Handle legacy API: The --serial argument is deprecated
                if args.parallel:       # as of version 0.6. The default execution mode is 'serial'
                    raise ValueError(   # and can be switched with the '--parallel' argument.
                        "Cannot provide both --serial and --parallel arguments a the same time! "
                        "The --serial argument is deprecated as of version 0.6!")
                else:
                    logger.warning(
                        "The script --serial argument is deprecated as of version 0.6, because "
                        "serial execution is now the default behavior. Please use the '--parallel' "
                        "argument to execute bundled operations in parallel.")

            ops = self._gather_operations(
                job_id=args.job_id,
                operation_name=args.operation_name,
                num=args.num,
                cmd=args.cmd,
                requires=args.requires)

            for bundle in make_bundles(ops, args.bundle_size):
                script = self.script(
                    operations=bundle,
                    parallel=args.parallel,
                    template=args.script_template)
                print(script)

        def _submit(env, args):
            "Generate a script for the execution of operations, to be submitted to a scheduler."
            kwargs = vars(args)
            del kwargs['func']
            debug = kwargs.pop('debug')
            test = kwargs.pop('test')
            if test:
                env = get_environment(test=True)    # bad hack... ignoring the env argument here
            try:
                self.submit(env=env, **kwargs)
            except NoSchedulerError as e:
                print(
                    "Error:", e, "Consider using '--test', for testing purposes.", file=sys.stderr)
                if debug:
                    raise
                return 1
            except SubmitError as e:
                print("Submission error:", e, file=sys.stderr)
                if debug:
                    raise
                return 1
            else:
                return 0

        def _exec(env, args):
            if len(args.jobid):
                jobs = [self.open_job(id=jid) for jid in args.jobid]
            else:
                jobs = self
            try:
                operation = self._operation_functions[args.operation]
            except KeyError:
                raise KeyError("Unknown operation '{}'.".format(args.operation))

            if getattr(operation, '_flow_aggregate', False):
                operation(jobs)
            else:
                for job in jobs:
                    operation(job)

        if parser is None:
            parser = argparse.ArgumentParser()

        parser.add_argument(
            '-d', '--debug',
            action='store_true',
            help="Increase output verbosity for debugging.")

        subparsers = parser.add_subparsers()

        parser_status = subparsers.add_parser('status')
        self._add_print_status_args(parser_status)
        parser_status.set_defaults(func=_status)

        parser_next = subparsers.add_parser(
            'next',
            description="Determine jobs that are eligible for a specific operation.")
        parser_next.add_argument(
            'name',
            type=str,
            help="The name of the operation.")
        parser_next.set_defaults(func=_next)

        parser_run = subparsers.add_parser('run')
        parser_run.add_argument(
            'name',
            type=str,
            nargs='*',
            help="If provided, only run operations where the identifier "
                 "matches the provided set of names.")
        parser_run.add_argument(
            '-j', '--job-id',
            type=str,
            nargs='+',
            help="The job id of the jobs to run. "
            "Omit to automatically select all jobs.")
        parser_run.add_argument(
            '-p', '--pretend',
            action='store_true',
            help="Do not actually execute commands, just show them.")
        parser_run.add_argument(
            '-n', '--num',
            type=int,
            help="Limit the number of operations to be executed.")
        parser_run.add_argument(    # Remove beginning of version 0.7.
            '--np',
            type=int,
            help="(deprecated) Specify the number of cores to parallelize to. "
                 "This argument is deprecated as of version 0.6, please use the "
                 "script command for parallel execution.")
        parser_run.add_argument(
            '-t', '--timeout',
            type=int,
            help="A timeout in seconds after which the parallel execution "
                 "of operations is canceled.")
        parser_run.add_argument(
            '--progress',
            action='store_true',
            help="Display a progress bar during execution.")
        parser_run.add_argument(
            '--num-passes',
            type=int,
            default=1,
            help="Specify how many times a particular operation may be executed within one "
                 "session (default=1). This is to prevent accidental infinite loops, "
                 "where operations are executed indefinitely, because post conditions "
                 "were not properly set. Use -1 to allow for an infinite number of passes.")
        parser_run.set_defaults(func=_run)

        parser_script = subparsers.add_parser('script')
        self._add_script_args(parser_script)
        parser_script.set_defaults(func=_script)

        parser_submit = subparsers.add_parser('submit')
        self._add_submit_args(parser_submit)
        parser_submit.add_argument(
            '-d', '--debug',
            action="store_true",
            help="Print debugging information.")
        parser_submit.add_argument(
            '-t', '--test',
            action='store_true')
        env_group = parser_submit.add_argument_group(
            '{} options'.format(self._environment.__name__))
        self._environment.add_args(env_group)
        parser_submit.set_defaults(func=_submit)

        parser_exec = subparsers.add_parser('exec')
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
        parser_exec.set_defaults(func=_exec)

        args = parser.parse_args()
        if not hasattr(args, 'func'):
            parser.print_usage()
            sys.exit(2)
        if args.debug:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.WARNING)

        sys.exit(args.func(self._environment, args))

    @classmethod
    def add_submit_args(cls, parser):
        warnings.warn(
            "The add_submit_args() method is private as of version 0.6.", DeprecationWarning)
        return cls._add_submit_args(parser=parser)

    @classmethod
    def add_script_args(cls, parser):
        warnings.warn(
            "The add_script_args() method is private as of version 0.6.", DeprecationWarning)
        return cls._add_script_args(parser=parser)

    @classmethod
    def add_print_status_args(cls, parser):
        warnings.warn(
            "The add_print_status_args() method is private as of version 0.6.", DeprecationWarning)
        return cls._add_print_status_args(parser=parser)

    def format_row(self, *args, **kwargs):
        warnings.warn("The format_row() method is private as of version 0.6.", DeprecationWarning)
        return self._format_row(*args, **kwargs)


###
# Status-related helper functions

def _update_status(args):
    "Wrapper-function, that is probably obsolete."
    return update_status(* args)


def _update_job_status(job, scheduler_jobs):
    "Update the status entry for job."
    update_status(job, scheduler_jobs)


def is_active(status):
    """True if a specific status is considered 'active'.

    A active status usually means that no further operation should
    be executed at the same time to prevent race conditions and other
    related issues.
    """
    for gid, s in status.items():
        if s > JobStatus.inactive:
            return True
    return False


__all__ = [
    'FlowProject',
    'FlowOperation',
    'label', 'staticlabel', 'classlabel',
]
