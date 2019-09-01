# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Workflow definition with the FlowProject.

The FlowProject is a signac Project that allows the user to define a workflow.
"""
from __future__ import print_function
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
from deprecation import deprecated
from collections import defaultdict
from collections import OrderedDict
from collections import Counter
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

import signac
from signac.common import six
from signac.contrib.hashing import calc_id
from signac.contrib.filterparse import parse_filter_arg

import jinja2
from jinja2 import TemplateNotFound as Jinja2TemplateNotFound

from .environment import get_environment
from .scheduling.base import ClusterJob
from .scheduling.base import JobStatus
from .scheduling.status import update_status
from .errors import SubmitError
from .errors import ConfigKeyError
from .errors import NoSchedulerError
from .errors import TemplateError
from .util.tqdm import tqdm
from .util.misc import _positive_int
from .util.misc import _mkdir_p
from .util.misc import roundrobin
from .util.misc import to_hashable
from .util import template_filters as tf
from .util.misc import add_cwd_to_environment_pythonpath
from .util.misc import switch_to_directory
from .util.misc import TrackGetItemDict
from .util.misc import fullmatch
from .util.translate import abbreviate
from .util.translate import shorten
from .util.execution import fork
from .util.execution import TimeoutExpired
from .labels import label
from .labels import staticlabel
from .labels import classlabel
from .labels import _is_label_func
from . import legacy
from .util import config as flow_config


logger = logging.getLogger(__name__)
if six.PY2:
    logger.addHandler(logging.NullHandler())


# The TEMPLATE_HELP can be shown with the --template-help option available to all
# command line sub commands that use the templating system.
TEMPLATE_HELP = """Execution and submission scripts are generated with the jinja2 template files.
Standard files are shipped with the package, but maybe replaced or extended with
custom templates provided within a project.
The default template directory can be configured with the 'template_dir' configuration
variable, for example in the project configuration file. The current template directory is:
{template_dir}

All template variables can be placed within a template using the standard jinja2
syntax, e.g., the project root directory can be written like this: {{ project._rd }}.
The available template variables are:
{template_vars}

Filter functions can be used to format template variables in a specific way.
For example: {{ project.get_id() | captialize }}.

The available filters are:
{filters}"""


class _condition(object):

    def __init__(self, condition):
        self.condition = condition

    @classmethod
    def isfile(cls, filename):
        "True if the specified file exists for this job."
        return cls(lambda job: job.isfile(filename))

    @classmethod
    def true(cls, key):
        """True if the specified key is present in the job document and
        evaluates to True."""
        return cls(lambda job: job.document.get(key, False))

    @classmethod
    def false(cls, key):
        """True if the specified key is present in the job document and
        evaluates to False."""
        return cls(lambda job: not job.document.get(key, False))

    @classmethod
    def always(cls, func):
        "Returns True."
        return cls(lambda _: True)(func)

    @classmethod
    def never(cls, func):
        "Returns False."
        return cls(lambda _: False)(func)

    @classmethod
    def not_(cls, condition):
        "Returns ``not condition(job)`` for the provided condition function."
        return cls(lambda job: not condition(job))


def make_bundles(operations, size=None):
    """Utility function for the generation of bundles.

    This function splits an iterable of operations into  equally
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


class JobOperation(object):
    """This class represents the information needed to execute one operation for one job.

    An operation function in this context is a shell command, which should be a function
    of one and only one signac job.

    .. note::

        This class is used by the :class:`~.FlowProject` class for the execution and
        submission process and should not be instantiated by users themselves.

    .. versionchanged:: 0.6

    :param name:
        The name of this JobOperation instance. The name is arbitrary,
        but helps to concisely identify the operation in various contexts.
    :type name:
        str
    :param job:
        The job instance associated with this operation.
    :type job:
        :py:class:`signac.Job`.
    :param cmd:
        The command that executes this operation.
    :type cmd:
        str
    :param directives:
        A dictionary of additional parameters that provide instructions on how
        to execute this operation, e.g., specifically required resources.
    :type directives:
        :class:`dict`
    """
    MAX_LEN_ID = 100

    def __init__(self, name, job, cmd, directives=None, np=None):
        self.name = name
        self.job = job
        self.cmd = cmd
        if directives is None:
            directives = dict()  # default argument
        else:
            directives = dict(directives)  # explicit copy

        # Keys which were explicitly set by the user, but are not evaluated by the
        # template engine are cause for concern and might hint at a bug in the template
        # script or ill-defined directives. We are therefore keeping track of all
        # keys set by the user and check whether they have been evaluated by the template
        # script engine later.
        keys_set_by_user = set(directives.keys())

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
            elif isinstance(value, six.string_types):
                return value.format(job=job)
            else:
                return value

        # We use a special dictionary that allows us to track all keys that have been
        # evaluated by the template engine and compare them to those explicitly set
        # by the user. See also comment above.
        self.directives = TrackGetItemDict(
            {key: evaluate(value) for key, value in directives.items()})
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

    def get_id(self, index=0):
        "Return a name, which identifies this job-operation."
        project = self.job._project

        # The full name is designed to be truly unique for each job-operation.
        full_name = '{}%{}%{}%{}'.format(
            project.root_directory(), self.job.get_id(), self.name, index)

        # The job_op_id is a hash computed from the unique full name.
        job_op_id = calc_id(full_name)

        # The actual job id is then constructed from a readable part and the job_op_id,
        # ensuring that the job-op is still somewhat identifiable, but guarantueed to
        # be unique. The readable name is based on the project id, job id, operation name,
        # and the index number. All names and the id itself are restricted in length
        # to guarantuee that the id does not get too long.
        max_len = self.MAX_LEN_ID - len(job_op_id)
        if max_len < len(job_op_id):
            raise ValueError("Value for MAX_LEN_ID is too small ({}).".format(self.MAX_LEN_ID))

        readable_name = '{}/{}/{}/{:04d}/'.format(
            str(project)[:12], str(self.job)[:8], self.name[:12], index)[:max_len]

        # By appending the unique job_op_id, we ensure that each id is truly unique.
        return readable_name + job_op_id

    def __hash__(self):
        return int(sha1(self.get_id().encode('utf-8')).hexdigest(), 16)

    def __eq__(self, other):
        return self.get_id() == other.get_id()

    def set_status(self, value):
        "Store the operation's status."
        self.job._project.document.setdefault('_status', dict())
        self.job._project.document._status[self.get_id()] = int(value)

    def get_status(self):
        "Retrieve the operation's last known status."
        try:
            return JobStatus(self.job._project.document['_status'][self.get_id()])
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
    :param directives:
        A dictionary of additional parameters that provide instructions on how
        to execute this operation, e.g., specifically required resources.
    :type directives:
        :class:`dict`
    """

    def __init__(self, cmd, pre=None, post=None, directives=None):
        if pre is None:
            pre = []
        if post is None:
            post = []
        self._cmd = cmd
        self.directives = directives

        self._prereqs = [FlowCondition(cond) for cond in pre]
        self._postconds = [FlowCondition(cond) for cond in post]

    def __str__(self):
        return "{type}(cmd='{cmd}')".format(type=type(self).__name__, cmd=self._cmd)

    def eligible(self, job):
        "Eligible, when all pre-conditions are true and at least one post-condition is false."
        pre = all(cond(job) for cond in self._prereqs)
        if pre and len(self._postconds):
            post = any(not cond(job) for cond in self._postconds)
        else:
            post = True
        return pre and post

    def complete(self, job):
        "True when all post-conditions are met."
        if len(self._postconds):
            return all(cond(job) for cond in self._postconds)
        else:
            return False

    def __call__(self, job=None):
        if callable(self._cmd):
            return self._cmd(job).format(job=job)
        else:
            return self._cmd.format(job=job)


class _FlowProjectClass(type):
    """Metaclass for the FlowProject class."""
    def __new__(metacls, name, bases, namespace, **kwargs):
        cls = type.__new__(metacls, name, bases, dict(namespace))

        # All operation functions are registered with the operation() classmethod, which is
        # intended to be used as decorator function. The _OPERATION_FUNCTIONS dict maps the
        # the operation name to the operation function. In addition, pre and
        # post conditions are registered with the class.

        cls._OPERATION_FUNCTIONS = list()
        cls._OPERATION_PRE_CONDITIONS = defaultdict(list)
        cls._OPERATION_POST_CONDITIONS = defaultdict(list)

        cls._OPERATION_FUNCTIONS = list()
        cls._OPERATION_PRECONDITIONS = dict()
        cls._OPERATION_POSTCONDITIONS = dict()
        # All label functions are registered with the label() classmethod, which is intendeded
        # to be used as decorator function. The _LABEL_FUNCTIONS dict contains the function as
        # key and the label name as value, or None to use the default label name.
        cls._LABEL_FUNCTIONS = OrderedDict()

        # Give the class a pre and post class that are aware of the class they
        # are in.
        cls.pre = cls._setup_pre_conditions_class(parent_class=cls)
        cls.post = cls._setup_post_conditions_class(parent_class=cls)
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
            """

            _parent_class = parent_class

            def __init__(self, condition):
                self.condition = condition

            def __call__(self, func):
                self._parent_class._OPERATION_PRE_CONDITIONS[func].insert(0, self.condition)
                return func

            @classmethod
            def copy_from(cls, *other_funcs):
                "True if and only if all pre conditions of other operation-function(s) are met."
                def metacondition(job):
                    return all(c(job)
                               for other_func in other_funcs
                               for c in cls._parent_class._collect_pre_conditions()[other_func])
                return cls(metacondition)

            @classmethod
            def after(cls, *other_funcs):
                "True if and only if all post conditions of other operation-function(s) are met."
                def metacondition(job):
                    return all(c(job)
                               for other_func in other_funcs
                               for c in cls._parent_class._collect_post_conditions()[other_func])
                return cls(metacondition)
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
            """
            _parent_class = parent_class

            def __init__(self, condition):
                self.condition = condition

            def __call__(self, func):
                self._parent_class._OPERATION_POST_CONDITIONS[func].insert(0, self.condition)
                return func

            @classmethod
            def copy_from(cls, *other_funcs):
                "True if and only if all post conditions of other operation-function(s) are met."
                def metacondition(job):
                    return all(c(job)
                               for other_func in other_funcs
                               for c in cls._parent_class._collect_post_conditions()[other_func])
                return cls(metacondition)
        return post


class FlowProject(six.with_metaclass(_FlowProjectClass,
                                     signac.contrib.Project)):
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
        A signac configuaration, defaults to the configuration loaded
        from the environment.
    :type config:
        A signac config object.
    """

    def __init__(self, config=None, environment=None):
        super(FlowProject, self).__init__(config=config)

        # Associate this class with a compute environment.
        self._environment = environment or get_environment()

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

        # Enable the use of buffered mode for certain functions
        try:
            self._use_buffered_mode = self.config['flow'].as_bool('use_buffered_mode')
        except KeyError:
            self._use_buffered_mode = False

    def _setup_template_environment(self):
        """Setup the jinja2 template environemnt.

        The templating system is used to generate templated scripts for the script()
        and submit_operations() / submit() function and the corresponding command line
        sub commands.
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
                name = str(error) if six.PY2 else error.name
                logger.warning("Unable to load template from package '{}'.".format(name))

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
        template_environment.filters['require_config_value'] = \
            flow_config.require_config_value
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
            for filter_name, filter_function in getattr(environment, 'filters', {}).items():
                template_environment.filters[filter_name] = filter_function
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

        If instead of a ``str``, the label functions returns any other type, the label
        name will be the name of the function if and only if the return value evaluates
        to ``True``, for example:

        .. code-block:: python

            @FlowProject.label
            def foo_label(job):
                return job.document.get('foo', False)

        Finally, you can specify a different default label name by providing it as the first
        argument to the ``label()`` decorator.

        .. versionadded:: 0.6
        """
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

    """Decorator to add a pre-condition function for an operation function.

    Use a label function (or any function of :code:`job`) as a condition:

    .. code-block:: python

        @FlowProject.label
        def some_label(job):
            return job.doc.ready == True

        @FlowProject.operation
        @FlowProject.pre(some_label)
        def some_operation(job):
            pass

    Use a :code:`lambda` function of :code:`job` to create custom conditions:

    .. code-block:: python

        @FlowProject.operation
        @FlowProject.pre(lambda job: job.doc.ready == True)
        def some_operation(job):
            pass

    Use the post-conditions of an operation as a pre-condition for another operation:

    .. code-block:: python

        @FlowProject.operation
        @FlowProject.post(lambda job: job.isfile('output.txt'))
        def previous_operation(job):
            pass

        @FlowProject.operation
        @FlowProject.pre.after(previous_operation)
        def some_operation(job):
            pass
    """

    """Decorator to add a post-condition function for an operation function.

    Use a label function (or any function of :code:`job`) as a condition:

    .. code-block:: python

        @FlowProject.label
        def some_label(job):
            return job.doc.finished == True

        @FlowProject.operation
        @FlowProject.post(some_label)
        def some_operation(job):
            pass

    Use a :code:`lambda` function of :code:`job` to create custom conditions:

    .. code-block:: python

        @FlowProject.operation
        @FlowProject.post(lambda job: job.doc.finished == True)
        def some_operation(job):
            pass
    """

    ALIASES = dict(
        unknown='U',
        registered='R',
        queued='Q',
        active='A',
        inactive='I',
        requires_attention='!'
    )
    "These are default aliases used within the status output."

    @classmethod
    def _alias(cls, x):
        "Use alias if specified."
        try:
            return abbreviate(x, cls.ALIASES.get(x, x))
        except TypeError:
            return x

    @classmethod
    @deprecated(deprecated_in="0.8", removed_in="1.0")
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
            bid = '{}/bundle/{}'.format(self, sha1(h.encode('utf-8')).hexdigest())
            fn_bundle = self._fn_bundle(bid)
            _mkdir_p(os.path.dirname(fn_bundle))
            with open(fn_bundle, 'w') as file:
                for operation in operations:
                    file.write(operation.get_id() + '\n')
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

    @staticmethod
    def _map_scheduler_jobs(scheduler_jobs):
        "Map all scheduler jobs by job id and operation name."
        for sjob in scheduler_jobs:
            name = sjob.name()
            if name[32] == '-':
                expanded = JobOperation.expand_id(name)
                yield expanded['job_id'], expanded['operation-name'], sjob

    def _get_operations_status(self, job, cached_status):
        "Return a dict with information about job-operations for this job."
        for job_op in self._job_operations(job, False):
            flow_op = self.operations[job_op.name]
            completed = flow_op.complete(job)
            eligible = False if completed else flow_op.eligible(job)
            scheduler_status = cached_status.get(job_op.get_id(), JobStatus.unknown)
            yield job_op.name, {
                'scheduler_status': scheduler_status,
                'eligible': eligible,
                'completed': completed,
            }

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
                for op in self._job_operations(job, only_eligible=False):
                    status[op.get_id()] = int(scheduler_info.get(op.get_id(), JobStatus.unknown))
            self.document._status.update(status)
        except NoSchedulerError:
            logger.debug("No scheduler available.")
        except RuntimeError as error:
            logger.warning("Error occurred while querying scheduler: '{}'.".format(error))
            if not ignore_errors:
                raise
        else:
            logger.info("Updated job status cache.")

    def _fetch_status(self, jobs, err, ignore_errors, no_parallelize):
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
                with contextlib.closing(ThreadPool()) as pool:
                    _map = map if no_parallelize else pool.imap
                    # First attempt at parallelized status determination.
                    # This may fail on systems that don't allow threads.
                    return list(tqdm(
                        iterable=_map(_get_job_status, jobs),
                        desc="Collecting job status info", total=len(jobs), file=err))
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
                     eligible_jobs_max_lines=None):
        """Print the status of the project.

        .. versionchanged:: 0.6

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
        :param eligible_jobs_max_lines:
            Limit the number of eligible jobs that are printed in the overview.
        :type eligible_jobs_max_lines:
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
            Limit the number of characters of parameter columns,
            see also: :py:meth:`~.update_aliases`.
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
        :param no_parallelize:
            Do not parallelize the status update.
        :type no_parallelize:
            bool
        :param template:
            user provided Jinja2 template file.
        :type template:
            str
        """
        if file is None:
            file = sys.stdout
        if err is None:
            err = sys.stderr
        if jobs is None:
            jobs = self     # all jobs

        # use Jinja2 template for status output
        if template is None:
            if detailed and expand:
                template = 'status_expand.jinja'
            elif detailed and not unroll:
                template = 'status_stack.jinja'
            elif detailed and compact:
                template = 'status_compact.jinja'
            else:
                template = 'status.jinja'

        if eligible_jobs_max_lines is None:
            eligible_jobs_max_lines = flow_config.get_config_value('eligible_jobs_max_lines')

        # initialize jinja2 template evnronment and necessary filters
        template_environment = self._template_environment()

        def draw_progressbar(value, total, width=40):
            """Visualize progess with a progress bar.

            :param value:
                The current progress as a fraction of total.
            :type value:
                int
            :param total:
                The maximum value that 'value' may obtain.
            :type total:
                int
            :param width:
                The character width of the drawn progress bar.
            :type width:
                int
            """

            assert value >= 0 and total > 0
            ratio = ' %0.2f%%' % (100 * value / total)
            n = int(value / total * width)
            return '|' + ''.join(['#'] * n) + ''.join(['-'] * (width - n)) + '|' + ratio

        def job_filter(job_op, scheduler_status_code, all_ops):
            """filter eligible jobs for status print.

            :param job_ops:
                Operations information for a job.
            :type job_ops:
                OrderedDict
            :param scheduler_status_code:
                Dictionary information for status code
            :type scheduler_status_code:
                Dictionary
            :param all_ops:
                Boolean value indicate if all operations should be displayed
            :type all_ops:
                Boolean
            """

            if scheduler_status_code[job_op['scheduler_status']] != 'U' or \
               job_op['eligible'] or all_ops:
                return True
            else:
                return False

        def get_operation_status(operation_info, symbols):
            """Determine the status of an operation.

            :param operation_info:
                Dicionary containing operation information
            :type operation_info:
                Dictionary
            :param symbols:
                Dicionary containing code for different job status
            :type symbols:
                Dictionary
            """

            if operation_info['scheduler_status'] >= JobStatus.active:
                op_status = u'running'
            elif operation_info['scheduler_status'] > JobStatus.inactive:
                op_status = u'active'
            elif operation_info['completed']:
                op_status = u'completed'
            elif operation_info['eligible']:
                op_status = u'eligible'
            else:
                op_status = u'ineligible'

            return symbols[op_status]

        if pretty:
            def highlight(s, eligible):
                """Change font to bold within jinja2 template

                :param s:
                    The string to be printed
                :type s:
                    str
                :param eligible:
                    Boolean value for job eligibility
                :type eligible:
                    Boolean
                """
                if eligible:
                    return '\033[1m' + s + '\033[0m'
                else:
                    return s
        else:
            def highlight(s, eligible):
                """Change font to bold within jinja2 template

                :param s:
                    The string to be printed
                :type s:
                    str
                :param eligible:
                    Boolean value for job eligibility
                :type eligible:
                    boolean
                """
                return s

        template_environment.filters['highlight'] = highlight
        template_environment.filters['draw_progressbar'] = draw_progressbar
        template_environment.filters['get_operation_status'] = get_operation_status
        template_environment.filters['job_filter'] = job_filter

        template = template_environment.get_template(template)
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
                tmp = self._fetch_status(jobs, err, ignore_errors, no_parallelize)

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
                    hits_ = [ft.getHitStatsFor(l)[0] for l in range(start, start+len(lines))]
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
            tmp = self._fetch_status(jobs, err, ignore_errors, no_parallelize)
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
            column_width_bar = 50
            column_width_label = 5
            for key, value in self._label_functions.items():
                column_width_label = max(column_width_label, len(key.__name__))
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
            column_width_parameters = list([0]*len(parameters))
            for i, para in enumerate(parameters):
                column_width_parameters[i] = len(para)

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
                    v = shorten(str(self._alias(get(k, sp))))
                    column_width_parameters[i] = max(column_width_parameters[i], len(v))
                    status['parameters'][k] = v

            for status in statuses.values():
                _add_parameters(status)

        column_width_operation = 5
        for op_name in self._operations:
            column_width_operation = max(column_width_operation, len(op_name))

        if detailed:
            # get detailed view info
            column_width_id = 32
            column_width_total_label = 6
            status_legend = ' '.join('[{}]:{}'.format(v, k) for k, v in self.ALIASES.items())

            for job in tmp:
                column_width_total_label = max(
                    column_width_total_label, len(', '.join(job['labels'])))
            if compact:
                num_operations = len(self._operations)
                column_width_operations_count = len(str(max(num_operations-1, 0))) + 3

            if pretty:
                OPERATION_STATUS_SYMBOLS = OrderedDict([
                    ('ineligible', u'\u25cb'),   # open circle
                    ('eligible', u'\u25cf'),     # black circle
                    ('active', u'\u25b9'),       # open triangle
                    ('running', u'\u25b8'),      # black triangle
                    ('completed', u'\u2714'),    # check mark
                ])
                "Pretty (unicode) symbols denoting the execution status of operations."
            else:
                OPERATION_STATUS_SYMBOLS = OrderedDict([
                    ('ineligible', u'-'),
                    ('eligible', u'+'),
                    ('active', u'*'),
                    ('running', u'>'),
                    ('completed', u'X')
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
        context['unroll'] = unroll
        if overview:
            context['progress_sorted'] = progress_sorted
            context['column_width_bar'] = column_width_bar
            context['column_width_label'] = column_width_label
            context['column_width_operation'] = column_width_operation
        if detailed:
            context['column_width_id'] = column_width_id
            context['column_width_operation'] = column_width_operation
            context['column_width_total_label'] = column_width_total_label
            context['alias_bool'] = {True: 'T', False: 'U'}
            context['scheduler_status_code'] = _FMT_SCHEDULER_STATUS
            context['status_legend'] = status_legend
            if parameters:
                context['column_width_parameters'] = column_width_parameters
            if compact:
                context['extra_num_operations'] = max(num_operations-1, 0)
                context['column_width_operations_count'] = column_width_operations_count
            if not unroll:
                context['operation_status_legend'] = operation_status_legend
                context['operation_status_symbols'] = OPERATION_STATUS_SYMBOLS

        def _add_dummy_operation(job):
            job['operations'][''] = {
                'completed': False,
                'eligible': True,
                'scheduler_status': JobStatus.dummy}

        for job in context['jobs']:
            has_eligible_ops = any([v['eligible'] for v in job['operations'].values()])
            if not has_eligible_ops and not context['all_ops']:
                _add_dummy_operation(job)

        op_counter = Counter()
        for job in context['jobs']:
            for k, v in job['operations'].items():
                if v['eligible']:
                    op_counter[k] += 1
        context['op_counter'] = op_counter.most_common(eligible_jobs_max_lines)
        n = len(op_counter) - len(context['op_counter'])
        if n > 0:
            context['op_counter'].append(('[{} more operations omitted]'.format(n), ''))

        print(template.render(**context), file=file)

        # Show profiling results (if enabled)
        if profiling_results:
            print('\n' + '\n'.join(profiling_results), file=file)

    def run_operations(self, operations=None, pretend=False, np=None, timeout=None, progress=False):
        """Execute the next operations as specified by the project's workflow.

        See also: :meth:`~.run`

        .. versionadded:: 0.6

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
            operations = list(self._get_pending_operations(self))
        else:
            operations = list(operations)   # ensure list

        if np is None or np == 1 or pretend:
            if progress:
                operations = tqdm(operations)
            for operation in operations:
                if pretend:
                    print(operation.cmd)
                else:
                    self._fork(operation, timeout)
        else:
            logger.debug("Parallelized execution of {} operation(s).".format(len(operations)))
            with contextlib.closing(Pool(processes=cpu_count() if np < 0 else np)) as pool:
                logger.debug("Parallelized execution of {} operation(s).".format(len(operations)))
                try:
                    from six.moves import cPickle as pickle
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
        return (op.name, op.job._id, op.cmd, op.directives)

    def _loads_op(self, blob):
        name, job_id, cmd, directives = blob
        return JobOperation(name, self.open_job(id=job_id), cmd, directives)

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

        results = [pool.apply_async(_fork_with_serialization, task) for task in s_tasks]

        for result in tqdm(results) if progress else results:
            result.get(timeout=timeout)

    def _fork(self, operation, timeout=None):
        logger.info("Execute operation '{}'...".format(operation))

        # Execute without forking if possible...
        if timeout is None and operation.name in self._operation_functions and \
                operation.directives.get('executable', sys.executable) == sys.executable:
            logger.debug("Able to optimize execution of operation '{}'.".format(operation))
            self._operation_functions[operation.name](operation.job)
        else:   # need to fork
            fork(cmd=operation.cmd, timeout=timeout)

    def run(self, jobs=None, names=None, pretend=False, np=None, timeout=None, num=None,
            num_passes=1, progress=False, order=None):
        """Execute all pending operations for the given selection.

        This function will run in an infinite loop until all pending operations
        have been executed or the total number of passes per operation or the total
        number of exeutions have been reached.

        By default there is no limit on the total number of executions, but a specific
        operation will only be executed once per job. This is to avoid accidental
        infinite loops when no or faulty post conditions are provided.

        See also: :meth:`~.run_operations`

        .. versionchanged:: 0.6

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
        :type progess:
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
        """
        # If no jobs argument is provided, we run operations for all jobs.
        if jobs is None:
            jobs = self

        # Negative values for the execution limits, means 'no limit'.
        if num_passes and num_passes < 0:
            num_passes = None
        if num and num < 0:
            num = None

        # The 'names' argument must be a sequence, not a string.
        if isinstance(names, six.string_types):
            raise ValueError(
                "The names argument of FlowProject.run() must be a sequence of strings, "
                "not a string.")

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
                with self._potentially_buffered():
                    operations = list(filter(select, self._get_pending_operations(jobs, names)))
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
            if requires and requires.difference(self.labels(job)):
                continue
            cmd_ = cmd.format(job=job)
            yield JobOperation(name=cmd_.replace(' ', '-'), cmd=cmd_, job=job)

    def _get_pending_operations(self, jobs, operation_names=None):
        "Get all pending operations for the given selection."
        assert not isinstance(operation_names, six.string_types)
        for op in self.next_operations(* jobs):
            if operation_names is None or any(fullmatch(n, op.name) for n in operation_names):
                yield op

    @contextlib.contextmanager
    def _potentially_buffered(self):
        if self._use_buffered_mode:
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
        :param parallel:
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
        context['environment'] = env.__name__
        context['id'] = _id
        context['operations'] = list(operations)
        context.update(kwargs)
        if show_template_help:
            self._show_template_help_and_exit(template_environment, context)
        return template.render(** context)

    def submit_operations(self, operations, _id=None, env=None, parallel=False, flags=None,
                          force=False, template='script.sh', pretend=False,
                          show_template_help=False, **kwargs):
        """Submit a sequence of operations to the scheduler.

        .. versionchanged:: 0.6

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
        :param kwargs:
            Additional keyword arguments to be forwarded to the scheduler.
        :return:
            Return the submission status after successful submission or None.
        """
        if _id is None:
            _id = self._store_bundled(operations)
        if env is None:
            env = self._environment

        print("Submitting cluster job '{}':".format(_id), file=sys.stderr)

        def _msg(op):
            print(" - Operation: {}".format(op), file=sys.stderr)
            return op

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
                op.directives._keys_set_by_user.difference(op.directives.keys_used)}
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
               force=False, walltime=None, env=None, **kwargs):
        """Submit function for the project's main submit interface.

        .. versionchanged:: 0.6

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
            Execute all bundled operations in parallel. Does nothing with the
            default behavior or `bundle_size=1`.
        :type parallel:
            bool
        :param force:
            Ignore all warnings or checks during submission, just submit.
        :type force:
            bool
        :param walltime:
            Specify the walltime in hours or as instance of datetime.timedelta.
        """
        # Regular argument checks and expansion
        if jobs is None:
            jobs = self  # select all jobs
        if isinstance(names, six.string_types):
            raise ValueError(
                "The 'names' argument must be a sequence of strings, however you "
                "provided a single string: {}.".format(names))
        if env is None:
            env = self._environment
        if walltime is not None:
            try:
                walltime = datetime.timedelta(hours=walltime)
            except TypeError as error:
                if str(error) != 'unsupported type for timedelta ' \
                                 'hours component: datetime.timedelta':
                    raise

        # Gather all pending operations.
        with self._potentially_buffered():
            operations = (op for op in self._get_pending_operations(jobs, names)
                          if self._eligible_for_submission(op))
            if num is not None:
                operations = list(islice(operations, num))

        # Bundle them up and submit.
        for bundle in make_bundles(operations, bundle_size):
            status = self.submit_operations(
                operations=bundle, env=env, parallel=parallel,
                force=force, walltime=walltime, **kwargs)

            if status is not None:  # operations were submitted, store status
                for op in bundle:
                    op.set_status(status)

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
                 "and filter funtions; then exit.")

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
            help="Only select operations that match the given operation name(s).")
        selection_group.add_argument(
            '-n', '--num',
            type=int,
            help="Limit the total number of operations to be selected.")

    @classmethod
    def _add_operation_bundling_arg_group(cls, parser):
        """Add argument group to parser for operation bundling."""

        bundling_group = parser.add_argument_group(
            'bundling',
            "Bundle mutiple operations for execution, e.g., to submit them "
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
            help="Execute all operations within a single bundle in parallel.")

    @classmethod
    def _add_direct_cmd_arg_group(cls, parser):
        direct_cmd_group = parser.add_argument_group("direct cmd")
        direct_cmd_group.add_argument(
            '--cmd',
            type=str,
            help="Directly specify the command for an operation. "
                 "For example: --cmd='echo {job._id}'.")
        direct_cmd_group.add_argument(
            '--requires',
            type=str,
            nargs='+',
            help="Manually specify all labels that are required for the direct command "
                 "to be considered eligible for execution.")

    @deprecated(deprecated_in="0.8", removed_in="1.0", details="Use export_job_statuses() instead.")
    def export_job_stati(self, collection, stati):
        "Export the job stati to a database collection."
        self.export_job_statuses(self, collection, stati)

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
            help="Do not parallelize the status determination.")

    def labels(self, job):
        """Yields all labels for the given ``job``.

        See also: :meth:`~.label`
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
        """
        if name in self.operations:
            raise KeyError("An operation with this identifier is already added.")
        self.operations[name] = FlowOperation(cmd=cmd, pre=pre, post=post, directives=kwargs)

    @deprecated(deprecated_in="0.8", removed_in="1.0", details="Use labels() instead.")
    def classify(self, job):
        """Generator function which yields labels for job.

        By default, this method yields from the project's labels() method.

        :param job:
            The signac job handle.
        :type job:
            :class:`~signac.contrib.job.Job`
        :yields:
            The labels for the provided job.
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

    def _job_operations(self, job, only_eligible):
        "Yield instances of JobOperation constructed for specific jobs."
        for name, op in self.operations.items():
            if only_eligible and not op.eligible(job):
                continue
            yield JobOperation(name=name, job=job, cmd=op(job), directives=op.directives)

    def next_operations(self, *jobs):
        """Determine the next eligible operations for jobs.

        :param jobs:
            The signac job handles.
        :type job:
            :class:`~signac.contrib.job.Job`
        :yield:
            All instances of :class:`~.JobOperation` jobs are eligible for.
        """
        for job in jobs:
            for op in self._job_operations(job, True):
                yield op

    @deprecated(deprecated_in="0.8", removed_in="1.0", details="Use next_operations() instead.")
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
        """Add the function `func` as operation function to the class workflow definition.

        This function is designed to be used as a decorator function, for example:

        .. code-block:: python

            @FlowProject.operation
            def hello(job):
                print('Hello', job)

        See also: :meth:`~.flow.FlowProject.add_operation`.

        .. versionadded:: 0.6
        """
        if isinstance(func, six.string_types):
            return lambda op: cls.operation(op, name=func)

        if name is None:
            name = func.__name__

        if (name, func) in cls._OPERATION_FUNCTIONS:
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
        "Collect all pre-conditions that were add via decorator."
        return cls._collect_conditions('_OPERATION_PRE_CONDITIONS')

    @classmethod
    def _collect_post_conditions(cls):
        "Collect all post-conditions that were add via decorator."
        return cls._collect_conditions('_OPERATION_POST_CONDITIONS')

    def _register_operations(self):
        "Register all operation functions registered with this class and its parent classes."
        operations = self._collect_operations()
        pre_conditions = self._collect_pre_conditions()
        post_conditions = self._collect_post_conditions()

        def _guess_cmd(func, name, **kwargs):
            try:
                executable = kwargs['directives']['executable']
            except (KeyError, TypeError):
                executable = sys.executable

            path = getattr(func, '_flow_path', inspect.getsourcefile(inspect.getmodule(func)))
            cmd_str = "{} {} exec {} {{job._id}}"

            if callable(executable):
                return lambda job: cmd_str.format(executable(job), path, name)
            else:
                return cmd_str.format(executable, path, name)

        for name, func in operations:
            if name in self._operations:
                raise ValueError(
                    "Repeat definition of operation with name '{}'.".format(name))

            # Extract pre/post conditions and directives from function:
            params = {
                'pre': pre_conditions.get(func, None),
                'post': post_conditions.get(func, None),
                'directives': getattr(func, '_flow_directives', None)}

            # Construct FlowOperation:
            if getattr(func, '_flow_cmd', False):
                self._operations[name] = FlowOperation(cmd=func, **params)
            else:
                self._operations[name] = FlowOperation(
                    cmd=_guess_cmd(func, name, **params), **params)
                self._operation_functions[name] = func

    @property
    def operations(self):
        "The dictionary of operations that have been added to the workflow."
        return self._operations

    def _eligible_for_submission(self, job_operation):
        """Determine if a job-operation is eligible for submission.

        By default, an operation is eligible for submission when it
        is not considered active, that means already queued or running.
        """
        if job_operation is None:
            return False
        if job_operation.get_status() >= JobStatus.submitted:
            return False
        return True

    @deprecated(deprecated_in="0.8", removed_in="1.0")
    def eligible_for_submission(self, job_operation):
        return self._eligible_for_submission(self, job_operation)

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
        except Exception:
            logger.error(
                "Error occured during status update. Use '--show-traceback' to "
                "show the full traceback or '--ignore-errors' to complete the "
                "update anyways.")
            if show_traceback:
                raise
        else:
            # Use small offset to account for overhead with few jobs
            delta_t = (time.time() - start - 0.5) / max(len(jobs), 1)
            config_key = 'status_performance_warn_threshold'
            warn_threshold = flow_config.get_config_value(config_key)
            if not args['profile'] and delta_t > warn_threshold >= 0:
                print(
                    "WARNING: "
                    "The status compilation took more than {}s per job. Consider to "
                    "use `--profile` to determine bottlenecks within your project "
                    "workflow definition.\n"
                    "Execute `signac config set flow.{} VALUE` to specify the "
                    "warning threshold in seconds. Use -1 to completely suppress this "
                    "warning."
                    .format(warn_threshold, config_key), file=sys.stderr)

    def _main_next(self, args):
        "Determine the jobs that are eligible for a specific operation."
        for job in self:
            if args.name in {op.name for op in self.next_operations(job)}:
                print(job)

    def _main_run(self, args):
        "Run all (or select) job operations."
        if args.hidden_operation_name:
            print(
                "WARNING: "
                "The run command expects operation names under the -o/--operation argument "
                "as of version 0.6.\n         Positional arguments will no longer be "
                "accepted beginning with version 0.7.",
                file=sys.stderr)
            if args.operation_name:
                args.operation_name.extend(args.hidden_operation_name)
            else:
                args.operation_name = args.hidden_operation_name

        # Select jobs:
        jobs = self._select_jobs_from_args(args)

        # Setup partial run function, because we need to call this either
        # inside some context managers or not based on whether we need
        # to switch to the project root directory or not.
        run = functools.partial(self.run,
                                jobs=jobs, names=args.operation_name, pretend=args.pretend,
                                np=args.parallel, timeout=args.timeout, num=args.num,
                                num_passes=args.num_passes, progress=args.progress,
                                order=args.order)

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
                operations = self._generate_operations(args.cmd, jobs, args.requires)
            else:
                operations = self._get_pending_operations(jobs, args.operation_name)
            operations = list(islice(operations, args.num))

        # Generate the script and print to screen.
        print(self.script(
            operations=operations, parallel=args.parallel,
            template=args.template, show_template_help=args.show_template_help))

    def _main_submit(self, args):
        if args.test:
            args.pretend = True
        kwargs = vars(args)

        # Select jobs:
        jobs = self._select_jobs_from_args(args)

        # Fetch the scheduler status.
        if not args.test:
            self._fetch_scheduler_status(jobs)

        # Gather all pending operations ...
        with self._potentially_buffered():
            ops = (op for op in self._get_pending_operations(jobs, args.operation_name)
                   if self._eligible_for_submission(op))
            ops = list(islice(ops, args.num))

        # Bundle operations up, generate the script, and submit to scheduler.
        for bundle in make_bundles(ops, args.bundle_size):
            status = self.submit_operations(operations=bundle, **kwargs)
            if status is not None:
                for op in bundle:
                    op.set_status(status)

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
                    fork(cmd=cmd)

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
            return legacy.JobsCursorWrapper(self, filter_, doc_filter)

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
            parents=[base_parser])
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
        parser_run.add_argument(          # Hidden positional arguments for backwards-compatibility.
            'hidden_operation_name',
            type=str,
            nargs='*',
            help=argparse.SUPPRESS)
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
        parser_run.set_defaults(func=self._main_run)

        parser_script = subparsers.add_parser(
            'script',
            parents=[base_parser],
        )
        self._add_script_args(parser_script)
        parser_script.set_defaults(func=self._main_script)

        parser_submit = subparsers.add_parser(
            'submit',
            parents=[base_parser],
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

        def _exit_or_raise():
            if args.show_traceback:
                raise
            else:
                sys.exit(1)

        try:
            args.func(args)
        except NoSchedulerError as error:
            print("ERROR: {}".format(error),
                  "Consider to use the 'script' command to generate an execution script instead.",
                  file=sys.stderr)
            _exit_or_raise()
        except SubmitError as error:
            print("Submission error:", error, file=sys.stderr)
            _exit_or_raise()
        except (TimeoutError, TimeoutExpired):
            print("Error: Failed to complete execution due to "
                  "timeout ({}s).".format(args.timeout), file=sys.stderr)
            _exit_or_raise()
        except Jinja2TemplateNotFound as error:
            print("Did not find template script '{}'.".format(error), file=sys.stderr)
            _exit_or_raise()
        except AssertionError:
            if not args.show_traceback:
                print("ERROR: Encountered internal error during program execution. "
                      "Execute with '--show-traceback' or '--debug' to get more "
                      "information.", file=sys.stderr)
            _exit_or_raise()
        except Exception as error:
            if not args.debug:
                if str(error):
                    print("ERROR: Encountered error during program execution: '{}'\n"
                          "Execute with '--show-traceback' or '--debug' to get "
                          "more information.".format(error), file=sys.stderr)
                else:
                    print("ERROR: Encountered error during program execution.\n"
                          "Execute with '--show-traceback' or '--debug' to get "
                          "more information.", file=sys.stderr)
            _exit_or_raise()


def _fork_with_serialization(loads, project, operation):
    """Invoke the _fork() method on a serialized project instance."""
    project = loads(project)
    project._fork(project._loads_op(operation))


###
# Status-related helper functions


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
