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
import functools
import contextlib
from collections import defaultdict
from collections import OrderedDict
from itertools import islice
from itertools import count
from copy import copy
from hashlib import sha1
from multiprocessing import Pool
from multiprocessing import cpu_count
from multiprocessing import TimeoutError
from multiprocessing.pool import ThreadPool
from multiprocessing import Event

import signac
from signac.common import six
from signac.contrib.hashing import calc_id
from signac.contrib.filterparse import parse_filter_arg

# Try to import jinja2 for templating, used in script and submit functions.
try:
    import jinja2
    from jinja2 import TemplateNotFound as Jinja2TemplateNotFound
except ImportError:
    # Mock exception, which will never be raised.
    class Jinja2TemplateNotFound(Exception):
        pass

    JINJA2 = False
else:
    JINJA2 = True

from .environment import get_environment
from .scheduling.base import Scheduler
from .scheduling.base import ClusterJob
from .scheduling.base import JobStatus
from .scheduling.status import update_status
from .errors import SubmitError
from .errors import ConfigKeyError
from .errors import NoSchedulerError
from .errors import TemplateError
from .util import tabulate
from .util.tqdm import tqdm
from .util.misc import _positive_int
from .util.misc import _mkdir_p
from .util.misc import draw_progressbar
from .util.template_filters import _format_timedelta
from .util.template_filters import _identical
from .util.template_filters import _with_np_offset
from .util.misc import write_human_readable_statepoint
from .util.misc import add_cwd_to_environment_pythonpath
from .util.misc import switch_to_directory
from .util.progressbar import with_progressbar
from .util.translate import abbreviate
from .util.translate import shorten
from .util.execution import fork
from .util.execution import TimeoutExpired
from .util.dependencies import _requires_jinja2
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


class _pre(_condition):

    def __call__(self, func):
        pre_conditions = getattr(func, '_flow_pre', list())
        pre_conditions.append(self.condition)
        func._flow_pre = pre_conditions
        return func

    @classmethod
    def copy_from(cls, other_func):
        "True if and only if all pre conditions of other function are met."
        def metacondition(job):
            pre_conditions = getattr(other_func, '_flow_pre', list())
            return all(c(job) for c in pre_conditions)
        return cls(metacondition)

    @classmethod
    def after(cls, other_func):
        "True if and only if all post conditions of other function are met."
        def metacondition(job):
            post_conditions = getattr(other_func, '_flow_post', list())
            return all(c(job) for c in post_conditions)
        return cls(metacondition)


class _post(_condition):

    def __init__(self, condition):
        self.condition = condition

    def __call__(self, func):
        post_conditions = getattr(func, '_flow_post', list())
        post_conditions.append(self.condition)
        func._flow_post = post_conditions
        return func

    @classmethod
    def copy_from(cls, other_func):
        "True if and only if all post conditions of other function are met."
        def metacondition(job):
            post_conditions = getattr(other_func, '_flow_post', list())
            return all(c(job) for c in post_conditions)
        return cls(metacondition)


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
        if directives is None:
            directives = dict()
        self.name = name
        self.job = job
        self.cmd = cmd

        # Handle deprecated np argument:
        if np is not None:
            warnings.warn(
                "The np argument for the JobOperation constructor is deprecated.",
                DeprecationWarning)
            assert directives.setdefault('np', np) == np
        else:
            directives.setdefault(
                'np', directives.get('nranks', 1) * directives.get('omp_num_threads', 1))
        directives.setdefault('ngpu', 0)
        # Future: directives.setdefault('np', 1)

        # Evaluate strings and callables for job:
        def evaluate(value):
            if value and callable(value):
                return value(job)
            elif isinstance(value, six.string_types):
                return value.format(job=job)
            else:
                return value

        self.directives = {key: evaluate(value) for key, value in directives.items()}

    def __str__(self):
        return "{}({})".format(self.name, self.job)

    def __repr__(self):
        return "{type}(name='{name}', job='{job}', cmd={cmd}, directives={directives})".format(
            type=type(self).__name__,
            name=self.name,
            job=str(self.job),
            cmd=repr(self.cmd),
            directives=self.directives)

    def _get_legacy_id(self):
        "Return a name, which identifies this job-operation."
        return '{}-{}'.format(self.job, self.name)

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

    @classmethod
    def expand_id(self, _id):
        # TODO: Remove beginning version 0.7.
        raise RuntimeError("The expand_id() method has been removed as of version 0.6.")

    def __hash__(self):
        return int(sha1(self.get_id().encode('utf-8')).hexdigest(), 16)

    def __eq__(self, other):
        return self.get_id() == other.get_id()

    def set_status(self, value):
        "Store the operation's status."
        status_doc = self.job.document.get('_status', dict())
        status_doc[self.get_id()] = int(value)
        self.job.document['_status'] = status_doc

    def get_status(self):
        "Retrieve the operation's last known status."
        try:
            status_cache = self.job.document['_status']
            return JobStatus(status_cache[self.get_id()])
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

    def __init__(self, cmd, pre=None, post=None, directives=None, np=None):
        if pre is None:
            pre = []
        if post is None:
            post = []
        self._cmd = cmd
        self.directives = directives

        # Handle deprecated np argument.
        if np is not None:
            warnings.warn(
                "The np argument for the FlowOperation() constructor is deprecated.",
                DeprecationWarning)
            if self.directives is None:
                self.directives = dict(np=np)
            else:
                assert self.directives.setdefault('np', np) == np

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
        "(deprecated) Return the number of processors this operation requires."
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
        cls._LABEL_FUNCTIONS = OrderedDict()

        return cls


class FlowProject(six.with_metaclass(_FlowProjectClass, signac.contrib.Project)):
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

        # Setup the templating system for the generation of run and submission scripts.

        self._template_environment_ = None

        # The standard local template directory is a directory called 'templates' within
        # the project root directory. This directory may be specified with the 'template_dir'
        # configuration variable.
        self._template_dir = os.path.join(
            self.root_directory(), self._config.get('template_dir', 'templates'))

        self._setup_legacy_templating()  # TODO: Disable in 0.8.

        # Register all label functions with this project instance.
        self._label_functions = OrderedDict()
        self._register_labels()

        # Register all operation functions with this project instance.
        self._operation_functions = dict()
        self._operations = OrderedDict()
        self._register_operations()

    def _setup_template_environment(self):
        """Setup the jinja2 template environemnt.

        The templating system is used to generate templated scripts for the script()
        and submit_operations() / submit() function and the corresponding command line
        sub commands.
        """
        _requires_jinja2()

        if self._config.get('flow') and self._config['flow'].get('environment_modules'):
            envs = self._config['flow'].as_list('environment_modules')
        else:
            envs = []

        # Templates are searched in the local template directory first, then in additionally
        # installed packages, then in the main package 'templates' directory.
        # 'templates' directory.
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

        self._template_environment_ = jinja2.Environment(
            loader=jinja2.ChoiceLoader(load_envs),
            trim_blocks=True,
            extensions=[TemplateError])

        # Setup standard filters that can be used to format context variables.
        self._template_environment_.filters['format_timedelta'] = _format_timedelta
        self._template_environment_.filters['identical'] = _identical
        self._template_environment_.filters['get_config_value'] = flow_config.get_config_value
        self._template_environment_.filters['require_config_value'] = \
            flow_config.require_config_value
        if 'max' not in self._template_environment_.filters:    # for jinja2 < 2.10
            self._template_environment_.filters['max'] = max
        self._template_environment_.filters['with_np_offset'] = _with_np_offset

    @property
    def _template_environment(self):
        if self._template_environment_ is None:
            self._setup_template_environment()
        return self._template_environment_

    def _get_standard_template_context(self):
        "Return the standard templating context for run and submission scripts."
        context = dict()
        context['project'] = self
        return context

    def _show_template_help_and_exit(self, context):
        "Print all context variables and filters to screen and exit."
        from textwrap import TextWrapper
        wrapper = TextWrapper(width=90, break_long_words=False)
        print(TEMPLATE_HELP.format(
            template_dir=self._template_dir,
            template_vars='\n'.join(wrapper.wrap(', '.join(sorted(context)))),
            filters='\n'.join(wrapper.wrap(', '.join(sorted(self._template_environment.filters))))))
        sys.exit(2)

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

        .. deprecated:: 0.6
           Users should migrate to the new templating system.
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
        """Write the commands for the execution of operations as part of a script.

        .. deprecated:: 0.6
           Users should migrate to the new templating system.
        """
        for op in operations:
            write_human_readable_statepoint(script, op.job)
            script.write_cmd(op.cmd.format(job=op.job), bg=background)
            script.writeline()

    @classmethod
    def write_human_readable_statepoint(cls, script, job):
        """Write statepoint of job in human-readable format to script.

        .. deprecated:: 0.6
           Users should migrate to the new templating system.
        """
        warnings.warn(
            "The write_human_readable_statepoint() function is deprecated.",
            DeprecationWarning)
        return write_human_readable_statepoint(script, job)

    @_part_of_legacy_template_system
    def write_script_footer(self, script, **kwargs):
        """"Write the script footer for the execution script.

        .. deprecated:: 0.6
           Users should migrate to the new templating system.
        """
        # Wait until all processes have finished
        script.writeline('wait')

    @_part_of_legacy_template_system
    def write_script(self, script, operations, background=False, **kwargs):
        """Write a script for the execution of operations.

        .. deprecated:: 0.6
           Users should migrate to the new templating system.

        By default, this function will generate a script with the following components:

        .. code-block:: python

            write_script_header(script)
            write_script_operations(script, operations, background=background)
            write_script_footer(script)

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

    def _get_operations_status(self, job):
        "Return a dict with information about job-operations for this job."
        cached_status = job.document.get('_status', dict())
        for name, job_op in self._job_operations(job):
            flow_op = self.operations[name]
            completed = flow_op.complete(job)
            eligible = False if completed else flow_op.eligible(job)
            scheduler_status = cached_status.get(job_op.get_id(), JobStatus.unknown)
            yield name, {
                'scheduler_status': scheduler_status,
                'eligible': eligible,
                'completed': completed,
            }

    def get_job_status(self, job, ignore_errors=False):
        "Return a dict with detailed information about the status of a job."
        result = dict()
        result['job_id'] = str(job)
        try:
            result['operations'] = OrderedDict(self._get_operations_status(job))
            result['_operations_error'] = None
        except Exception as error:
            msg = "Error while getting operations status for job '{}': '{}'.".format(job, error)
            logger.debug(msg)
            if ignore_errors:
                result['operations'] = None
                result['_operations_error'] = str(error)
            else:
                raise
        try:
            result['labels'] = sorted(set(self.classify(job)))
            result['_labels_error'] = None
        except Exception as error:
            logger.debug("Error while classifying job '{}': '{}'.".format(job, error))
            if ignore_errors:
                result['labels'] = None
                result['_labels_error'] = str(error)
            else:
                raise
        return result

    def _format_row(self, status, statepoint=None, max_width=None):
        "Format each row in the detailed status output."
        row = [
            status['job_id'],
            status['operation'],
            ', '.join((self._alias(s) for s in status['submission_status'])),
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
            row[2] += ' ' + self._alias('requires_attention')
        return row

    def _fetch_scheduler_status(self, jobs=None, scheduler=None, file=None, ignore_errors=False):
        "Update the status docs."
        if file is None:
            file = sys.stderr
        try:
            if jobs is None:
                jobs = list(self)
            if scheduler is None:
                scheduler = self._environment.get_scheduler()

            # Map all scheduler jobs by their name.
            print(self._tr("Query scheduler..."), file=file)
            sjobs_map = defaultdict(list)
            for sjob in self.scheduler_jobs(scheduler):
                sjobs_map[sjob.name()].append(sjob)

            # Iterate through all jobs ...
            for job in with_progressbar(jobs, desc='Update status cache:', file=file):
                job_status = dict()
                # ... and all job-operations to attempt to map the job-operation id
                # to scheduler names.
                for name, op in self._job_operations(job):
                    scheduler_jobs = sjobs_map.get(
                        op.get_id(), sjobs_map.get(op._get_legacy_id(), []))
                    from operator import methodcaller
                    tmp = list(map(methodcaller('status'), scheduler_jobs))
                    job_status[op.get_id()] = int(max(tmp) if tmp else JobStatus.unknown)
                job.document['_status'] = job_status

        except NoSchedulerError:
            logger.debug("No scheduler available.")
        except RuntimeError as error:
            logger.warning("Error occurred while querying scheduler: '{}'.".format(error))
            if not ignore_errors:
                raise
        else:
            logger.info("Updated job status cache.")

    OPERATION_STATUS_SYMBOLS = OrderedDict([
        ('ineligible', u'-'),
        ('eligible', u'+'),
        ('active', u'*'),
        ('running', u'>'),
        ('completed', u'X')
    ])

    PRETTY_OPERATION_STATUS_SYMBOLS = OrderedDict([
        ('ineligible', u'\u25cb'),   # open circle
        ('eligible', u'\u25cf'),     # black circle
        ('active', u'\u25b9'),       # open triangel
        ('running', u'\u25b8'),      # black triangel
        ('completed', u'\u2714'),    # check mark
    ])

    def print_status(self, jobs=None, overview=True, overview_max_lines=None,
                     detailed=False, parameters=None, skip_active=False, param_max_width=None,
                     expand=False, all_ops=False, only_incomplete=False, dump_json=False,
                     unroll=True, compact=False, pretty=False,
                     file=None, err=None, ignore_errors=False,
                     scheduler=None, pool=None, job_filter=None, no_parallelize=False):
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
        :param scheduler:
            (deprecated) The scheduler instance used to fetch the job statuses.
        :type scheduler:
            :class:`~.manage.Scheduler`
        :param pool:
            (deprecated) A multiprocessing or threading pool. Providing a pool
            parallelizes this method.
        :param job_filter:
            (deprecated) A JSON encoded filter, that all jobs to be submitted need to match.
        """
        if file is None:
            file = sys.stdout
        if err is None:
            err = sys.stderr
        # TODO: Replace legacy code below with this code beginning version 0.7:
        # if jobs is None:
        #     jobs = list(self)     # all jobs
        # Handle legacy API:
        if jobs is None:
            if job_filter is not None and isinstance(job_filter, str):
                warnings.warn(
                    "The 'job_filter' argument is deprecated, use the 'jobs' argument instead.",
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

        # Update the status docs of each job:
        self._fetch_scheduler_status(jobs, scheduler, err, ignore_errors)

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

        _get_job_status = functools.partial(self.get_job_status, ignore_errors=ignore_errors)

        try:
            with contextlib.closing(ThreadPool()) as pool:
                _map = map if no_parallelize else pool.imap
                # First attempt at parallelized status determination.
                # This may fail on systems that don't allow threads.
                tmp = list(tqdm(
                    iterable=_map(_get_job_status, jobs),
                    desc="Collect job status info", total=len(jobs), file=err))
        except RuntimeError as error:
            if "can't start new thread" not in error.args:
                raise   # unrelated error
            # The parallelized status determination has failed and we fall
            # back to a serial approach.
            logger.warning(
                "A parallelized status update failed due to error ('{}'). "
                "Entering serial mode with fallback progress indicator. The "
                "status update may take longer than ususal.".format(error))
            tmp = list(with_progressbar(
                iterable=map(_get_job_status, jobs),
                total=len(jobs), desc='Collect job status info:', file=err))

        operations_errors = {s['_operations_error'] for s in tmp}
        labels_errors = {s['_labels_error'] for s in tmp}
        errors = list(filter(None, operations_errors.union(labels_errors)))

        if errors:
            logger.warning(
                "Some job status updates did not succeed due to errors. "
                "Number of unique errors: {}. Use --debug to list all errors.".format(len(errors)))
            for i, error in enumerate(errors):
                logger.debug("Status update error #{}: '{}'".format(i+1, error))

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

        # Generate status overview:
        if overview:
            print("# Overview:", file=file)
            print("{} {}\n".format(self._tr("Total # of jobs:"), len(statuses), file=file))

            # Draw progress bars
            progress = defaultdict(int)
            for status in statuses.values():
                for label in status['labels']:
                    progress[label] += 1
            progress_sorted = list(islice(
                sorted(progress.items(), key=lambda x: (x[1], x[0]), reverse=True),
                overview_max_lines))
            rows = [[
                label,
                '{} {:0.2f}%'.format(draw_progressbar(num, len(statuses)),
                                     100 * num / len(statuses))
            ]
                for label, num in progress_sorted]

            print(tabulate.tabulate(rows, headers=['label', 'ratio']), file=file)
            if not rows:
                print("[no labels to show]", file=file)
            if overview_max_lines is not None:
                lines_skipped = len(progress) - overview_max_lines
                if lines_skipped > 0:
                    print(self._tr("Lines omitted:"), lines_skipped, file=file)

        # Generate detailed view:
        def _select_op(doc):
            active = JobStatus(doc['scheduler_status']) > JobStatus.unknown
            if skip_active and active:
                return False
            if not all_ops and not (active or doc['eligible']):
                return False
            return True

        def _bold(x):
            "Bold markup."
            if pretty:
                return '\033[1m' + x + '\033[0m'
            else:
                return x

        if detailed:
            rows_status = []
            columns = ['job_id', 'labels']
            if unroll:
                columns.insert(1, 'operation')
            header_detailed = [self._tr(self._alias(c)) for c in columns]
            if parameters:
                offset = 2 if unroll else 1
                for i, value in enumerate(parameters):
                    header_detailed.insert(
                        i + offset, shorten(self._alias(str(value)), param_max_width))

            def _format_status(status):
                sp = self.open_job(id=status['job_id']).statepoint()

                def get(k, m):
                    if m is None:
                        return
                    t = k.split('.')
                    if len(t) > 1:
                        return get('.'.join(t[1:]), m.get(t[0]))
                    else:
                        return m.get(k)

                row = [status['job_id']]
                row.append(', '.join(status.get('labels', [])))
                if parameters:
                    for i, k in enumerate(parameters):
                        v = self._alias(get(k, sp))
                        row.insert(i + 1, None if v is None else shorten(str(v), param_max_width))
                if unroll:
                    selected_ops = [name for name, op in status['operations'].items()
                                    if _select_op(op)]
                    if compact:
                        if len(selected_ops):
                            next_name = selected_ops[0]
                            sched_stat = status['operations'][next_name]['scheduler_status']
                            cell = '{} [{}]'.format(next_name, _FMT_SCHEDULER_STATUS[sched_stat])
                            if len(selected_ops) > 1:
                                cell += ' (+{})'.format(len(selected_ops) - 1)
                        else:
                            cell = ''
                        row.insert(1, cell)
                        yield row
                    elif selected_ops:
                        row.insert(1, None)
                        max_len = max(len(header_detailed[1])-4, max(map(len, selected_ops)))
                        for i, name in enumerate(selected_ops):
                            if i:
                                row[0] = None
                            row[1] = name.ljust(max_len)

                            op = status['operations'][name]
                            if pretty and op['eligible']:
                                row[1] = _bold(row[1])
                            row[1] += " [{}]".format(_FMT_SCHEDULER_STATUS[op['scheduler_status']])
                            if six.PY2:
                                yield copy(row)
                            else:
                                yield row.copy()
                    else:
                        row.insert(1, None)
                        yield row.copy()
                else:
                    yield row

            for status in statuses.values():
                rows_status.extend(_format_status(status))

            rows_operations = []
            header_operations = [self._tr(self._alias(s))
                                 for s in ('job_id', 'operation', 'eligible', 'cluster_status')]

            def _fmt_status_operations(status):
                for name, doc in status['operations'].items():
                    if not _select_op(doc):
                        continue

                    yield [
                        status['job_id'],
                        _bold(name) if (pretty and doc['eligible']) else name,
                        'Y' if doc['eligible'] else 'N',
                        _FMT_SCHEDULER_STATUS[doc['scheduler_status']]]

            for status in statuses.values():
                rows_operations.append(list(_fmt_status_operations(status)))

        # Actually display information
        if detailed:
            print(('\n' if overview else '') + "# Detailed View:", file=file)
            status_table = tabulate.tabulate(rows_status, headers=header_detailed)
            if expand:  # Present labels and operations in two separate tables.
                print("\n## Labels:", file=file)
                print(status_table, file=file)
                print("\n## Operations:", file=file)
                rows_operations = [row for rows in rows_operations for row in rows]  # flatten list
                print(tabulate.tabulate(rows_operations, headers=header_operations), file=file)
            elif unroll:
                print(status_table, file=file)
            else:       # Present labels and operations in a combined 'compact' view.
                # We need to split the labels table into individual lines
                # to combine them with the operations lines.
                status_table_lines = iter(status_table.splitlines())

                # The first two lines are the table header.
                print(next(status_table_lines), file=file)
                print(next(status_table_lines), file=file)

                def _print_unicode(value):
                    "Python 2/3 compatibility layer."
                    if six.PY2:
                        print(value.encode('utf-8'), file=file)
                    else:
                        print(value, file=file)

                if pretty:
                    open_frame = u'\u2514'      # open frame
                    closing_frame = u'\u251c'   # closing frame
                    symbols = self.PRETTY_OPERATION_STATUS_SYMBOLS
                else:
                    open_frame, closing_frame = '', ''
                    symbols = self.OPERATION_STATUS_SYMBOLS

                for line, status in zip(status_table_lines, statuses.values()):
                    _print_unicode(line)
                    if status['operations']:
                        width = max(map(len, status['operations']))

                        def select(op):
                            name, doc = op
                            active = JobStatus(doc['scheduler_status']) > JobStatus.unknown
                            if skip_active and active:
                                return False
                            if not all_ops and not (active or doc['eligible']):
                                return False
                            return True

                        selected_ops = list(filter(select, status['operations'].items()))
                        for i, (name, doc) in enumerate(selected_ops):
                            name = name.ljust(width)
                            if pretty and doc['eligible']:
                                name = _bold(name)
                            if six.PY2:
                                name = name.decode('utf-8')
                            frame = closing_frame if (i+1) == len(selected_ops) else open_frame

                            if doc['scheduler_status'] >= JobStatus.active:
                                op_status = u'running'
                            elif doc['scheduler_status'] > JobStatus.inactive:
                                op_status = u'active'
                            elif doc['completed']:
                                op_status = u'completed'
                            elif doc['eligible']:
                                op_status = u'eligible'
                            else:
                                op_status = u'ineligible'

                            frame += symbols[op_status]

                            sched_stat = _FMT_SCHEDULER_STATUS[doc['scheduler_status']]
                            if six.PY2:
                                sched_stat = sched_stat.decode('utf-8')
                            msg = u"{} {} [{}]".format(frame, name, sched_stat)
                            _print_unicode(msg)
                legend = u'Legend: ' + u' '.join(u'{}:{}'.format(v, k) for k, v in symbols.items())
                _print_unicode(legend)

        # Show any abbreviations used
        if abbreviate.table:
            print('\n', self._tr("Abbreviations used:"), file=file)
            for a in sorted(abbreviate.table):
                print('{}: {}'.format(a, abbreviate.table[a]), file=file)

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
            operations = [op for job in self for op in self.next_operations(job) if op is not None]
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

    def _run_operations_in_parallel(self, pool, pickle, operations, progress, timeout):
        """Execute operations in parallel.

        This function executes the given list of operations with the provided process pool.

        Since pickling of the project instance is likely to fail, we manually pickle the
        project instance and the operations before submitting them to the process pool to
        enable us to try different pool and pickle module combinations.
        """
        try:
            s_project = pickle.dumps(self)
            s_tasks = [(pickle.loads, s_project, pickle.dumps(op))
                       for op in with_progressbar(operations, desc='Serialize tasks')]
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

    @_support_legacy_api
    def run(self, jobs=None, names=None, pretend=False, np=None, timeout=None, num=None,
            num_passes=1, progress=False):
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
            The default is 1, there is no limit if this argumet is `None`.
        :type num_passes:
            int
        :param progress:
            Show a progress bar during execution.
        :type progess:
            bool
        """
        # If no jobs argument is provided, we run operations for all jobs.
        if jobs is None:
            jobs = self
        jobs = list(jobs)   # Ensure that the list of jobs does not change during execution.

        # Negative values for the execution limits, means 'no limit'.
        if num_passes and num_passes < 0:
            num_passes = None
        if num and num < 0:
            num = None

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
                operations = list(filter(select, self._get_pending_operations(jobs, names)))
            finally:
                if messages:
                    for msg, level in set(messages):
                        logger.log(level, msg)
                    del messages[:]     # clear
            if not operations:
                break   # No more pending operations or execution limits reached.
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
        operation_names = None if operation_names is None else set(operation_names)

        if len(jobs) > 1:
            jobs = with_progressbar(jobs, desc='Gather pending operations:')
        for job in jobs:
            for op in self.next_operations(job):
                if operation_names and op.name not in operation_names:
                    continue
                if not self.eligible_for_submission(op):
                    continue
                yield op

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
            context = self._get_standard_template_context()
            # For script generation we do not need the extra logic used for
            # generating cluster job scripts.
            context['base_script'] = 'base_script.sh'
            context['operations'] = list(operations)
            context['parallel'] = parallel
            if show_template_help:
                self._show_template_help_and_exit(context)
            return template.render(** context)

    def _generate_submit_script(self, _id, operations, template, show_template_help, env, **kwargs):
        """Generate submission script to submit the execution of operations to a scheduler."""
        if template is None:
            template = env.template
        assert _id is not None

        if self._legacy_templating:
            fn_template = os.path.join(self._template_dir, template)
            if os.path.isfile(fn_template):
                raise RuntimeError(
                    "In legacy templating mode, unable to use template '{}'.".format(fn_template))

            # For maintaining backwards compatibility for Versions<0.7
            if kwargs['parallel']:
                np_total = sum(op.directives['np'] for op in operations)
            else:
                np_total = max(op.directives['np'] for op in operations)

            try:
                script = env.script(_id=_id, np_total=np_total, **kwargs)
            except ConfigKeyError as e:
                raise SubmitError("The following error was encountered during script generation: ",
                                  e.message)
            background = kwargs.pop('parallel', not kwargs.pop('serial', False))
            self.write_script(script=script, operations=operations, background=background, **kwargs)
            script.seek(0)
            return script.read()
        else:
            template = self._template_environment.get_template(template)
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
                self._show_template_help_and_exit(context)
            try:
                return template.render(** context)
            except ConfigKeyError as e:
                raise SubmitError(
                    "The following error was encountered during script generation: {}".format(e))

    @_support_legacy_api
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
        :param serial:
            Execute all bundled operations in serial.
        :type serial:
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

        operations = map(_msg, operations)
        script = self._generate_submit_script(
            _id=_id,
            operations=list(operations),
            template=template,
            show_template_help=show_template_help,
            env=env,
            parallel=parallel,
            force=force,
            **kwargs
        )
        if pretend:
            print(script)
        else:
            return env.submit(_id=_id, script=script, flags=flags, **kwargs)

    @_support_legacy_api
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
            Execute all bundled operations in parallel. Has no effect without bundling.
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
        operations = self._get_pending_operations(jobs, names)
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
        execution_group.add_argument(   # TODO: Remove with version 0.7.
            '-s', '--serial',
            action='store_const',
            const=True,
            help=argparse.SUPPRESS)
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
            choices=operations,
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
            help="Bundle multiple operations for execution. When this "
                 "option is provided without argument, all pending operations "
                 "are aggregated into one bundle.")
        bundling_group.add_argument(
            '-p', '--parallel',
            action='store_true',
            help="Execute all (bundled) operations in parallel.")
        bundling_group.add_argument(
            '-s', '--serial',
            action='store_const',
            const=True,
            help="(deprecated) Execute all (bundled) operations in serial. This is the "
                 "default mode as of version 0.6.")

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

    def update_stati(self, scheduler, jobs=None, file=sys.stderr, pool=None, ignore_errors=False):
        "This function has been removed as of version 0.6."
        raise RuntimeError(
            "The update_stati() method has been removed as of version 0.6.")

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
            '--skip-active',
            action='store_true',
            help=argparse.SUPPRESS)
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

    def _job_operations(self, job, only_eligible=False):
        "Yield instances of JobOperation constructed for specific job."
        for name, op in self.operations.items():
            if only_eligible and not op.eligible(job):
                continue
            yield name, JobOperation(name=name, job=job, cmd=op(job), directives=op.directives)

    def next_operations(self, job):
        """Determine the next eligible operations for job.

        :param job:
            The signac job handle.
        :type job:
            :class:`~signac.contrib.job.Job`
        :yield:
            All instances of :class:`~.JobOperation` job is eligible for.
        """
        next_ops = OrderedDict(self._job_operations(job, only_eligible=True))
        for op in next_ops.values():
            yield op

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

    def _register_operations(self):
        "Register all operation functions registered with this class and its parent classes."
        operations = []
        for cls in type(self).__mro__:
            operations.extend(getattr(cls, '_OPERATION_FUNCTIONS', []))

        def _guess_cmd(func, name, **kwargs):
            try:
                executable = kwargs['directives']['executable']
            except (KeyError, TypeError):
                executable = sys.executable
            path = getattr(func, '_flow_path', inspect.getsourcefile(func))
            return '{} {} exec {} {{job._id}}'.format(executable, path, name)

        for name, func in operations:
            if name in self._operations:
                raise ValueError(
                    "Repeat definition of operation with name '{}'.".format(name))

            # Extract pre/post conditions and directives from function:
            params = {key: getattr(func, '_flow_{}'.format(key), None)
                      for key in ('pre', 'post', 'directives')}

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

    def eligible(self, job_operation, **kwargs):
        """Determine if job is eligible for operation.

        .. deprecated:: 0.5
           Please use :py:meth:`~.eligible_for_submission` instead.
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

    def _main_status(self, args):
        "Print status overview."
        jobs = self._select_jobs_from_args(args)
        if args.compact and not args.unroll:
            logger.warn("The -1/--one-line argument is incompatible with "
                        "'--stack' and will be ignored.")
        debug = args.debug
        args = {key: val for key, val in vars(args).items()
                if key not in ['func', 'verbose', 'debug', 'show_traceback',
                               'job_id', 'filter', 'doc_filter']}
        if args.pop('full'):
            args['detailed'] = args['all_ops'] = True

        try:
            self.print_status(jobs=jobs, **args)
        except NoSchedulerError:
            self.print_status(jobs=jobs, **args)
        except Exception:
            logger.error(
                "Error occured during status update. Use '--ignore-errors' "
                "to complete the update anyways or '--debug' to show the full "
                "traceback.")
            if debug:
                raise

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

        if args.np is not None:  # Remove completely beginning of version 0.7.
            logger.warning(
                "The run --np option is deprecated as of version 0.6, use '-p/--parallel' instead.")
            if args.parallel is None:
                args.parallel = args.np
            else:
                raise ValueError("Conflicting arguments for '--np' and '-p/--parallel'!")

        # Select jobs:
        jobs = self._select_jobs_from_args(args)

        # Setup partial run function, because we need to call this either
        # inside some context managers or not based on whether we need
        # to switch to the project root directory or not.
        run = functools.partial(self.run,
                                jobs=jobs, names=args.operation_name, pretend=args.pretend,
                                np=args.parallel, timeout=args.timeout, num=args.num,
                                num_passes=args.num_passes, progress=args.progress)

        if args.switch_to_project_root:
            with add_cwd_to_environment_pythonpath():
                with switch_to_directory(self.root_directory()):
                    run()
        else:
            run()

    def _main_script(self, args):
        "Generate a script for the execution of operations."
        _requires_jinja2('The signac-flow script function')
        if args.serial:             # Handle legacy API: The --serial option is deprecated
            if args.parallel:       # as of version 0.6. The default execution mode is 'serial'
                raise ValueError(   # and can be switched with the '--parallel' argument.
                    "Cannot provide both --serial and --parallel arguments a the same time! "
                    "The --serial option is deprecated as of version 0.6!")
            else:
                logger.warning(
                    "The script --serial option is deprecated as of version 0.6, because "
                    "serial execution is now the default behavior. Please use the '--parallel' "
                    "argument to execute bundled operations in parallel.")

        if args.requires and not args.cmd:
            raise ValueError(
                "The --requires option can only be used in combination with --cmd.")
        if args.cmd and args.operation_name:
            raise ValueError(
                "Cannot use the -o/--operation-name and the --cmd options in combination!")

        # Select jobs:
        jobs = self._select_jobs_from_args(args)

        # Gather all pending operations or generate them based on a direct command...
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
        _requires_jinja2('The signac-flow submit function')
        if args.test:
            args.pretend = True
        kwargs = vars(args)

        # Select jobs:
        jobs = self._select_jobs_from_args(args)

        # Fetch the scheduler status.
        if not args.test:
            self._fetch_scheduler_status(jobs)

        # Gather all pending operations ...
        ops = self._get_pending_operations(jobs, args.operation_name)
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
            return list(self.find_jobs(filter=filter_, doc_filter=doc_filter))

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
        if pool is not None:
            logger.warning(
                "The 'pool' argument for the FlowProject.main() function is deprecated!")

        if parser is None:
            parser = argparse.ArgumentParser()

        base_parser = argparse.ArgumentParser(add_help=False)

        for _parser in (parser, base_parser):
            _parser.add_argument(
                '-v', '--verbose',
                action='count',
                default=0,
                help="Increase output verbosity.")
            _parser.add_argument(
                '--show-traceback',
                action='store_true',
                help="Show the full traceback on error.")
            _parser.add_argument(
                '--debug',
                action='store_true',
                help="This option implies `-vv --show-traceback`.")

        subparsers = parser.add_subparsers()

        parser_status = subparsers.add_parser(
            'status',
            parents=[base_parser])
        self._add_print_status_args(parser_status)
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
                 "processing units if argument is ommitted.")
        execution_group.add_argument(    # Remove beginning of version 0.7.
            '--np',
            type=int,
            help="(deprecated) Specify the number of cores to parallelize to. "
                 "This option is deprecated as of version 0.6, use '-p/--parallel' instead.")
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

        if args.debug:  # Implies '-vv' and '--show-traceback'
            args.verbose = max(2, args.verbose)
            args.show_traceback = True

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
                      "Run with '--show-traceback' or '--debug' to get more "
                      "information.", file=sys.stderr)
            _exit_or_raise()
        except Exception as error:
            if not args.debug:
                if str(error):
                    print("ERROR: Encountered error during program execution: '{}'\n"
                          "Execute with '--debug' to get more information.".format(error),
                          file=sys.stderr)
                else:
                    print("ERROR: Encountered error during program execution.\n"
                          "Run with '--debug' to get more information.", file=sys.stderr)
            _exit_or_raise()

    # All class methods below are wrappers for legacy API and should be removed as of version 0.7.

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


def _fork_with_serialization(loads, project, operation):
    """Invoke the _fork() method on a serialized project instance."""
    loads(project)._fork(loads(operation))


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
