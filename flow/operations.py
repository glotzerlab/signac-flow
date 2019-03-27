# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""This module implements the run() function, which when called equips
a regular Python module with a command line interface, which can be used
to execute functions defined within the same module, that operate on a
signac data space.
"""
from __future__ import print_function
import sys
import argparse
import logging
import inspect
from multiprocessing import Pool
from hashlib import sha1
from functools import wraps
from itertools import groupby

from signac import get_project
from signac.common import six
from signac.contrib.hashing import calc_id

from .scheduling.base import JobStatus
from .util.tqdm import tqdm
from .util.execution import fork
from .util.misc import TrackGetItemDict
if six.PY2:
    from collections import Iterable
else:
    from collections.abc import Iterable


logger = logging.getLogger(__name__)


def cmd(func):
    """Specifies that ``func`` returns a shell command.

    If this function is an operation function defined by :class:`~.FlowProject`, it will
    be interpreted to return a shell command, instead of executing the function itself.

    For example:

    .. code-block:: python

        @FlowProject.operation
        @flow.cmd
        def hello(job):
            return "echo {job._id}"
    """
    if getattr(func, "_flow_with_job", False):
        raise RuntimeError("@cmd should appear below the @with_job decorator in your script")
    setattr(func, '_flow_cmd', True)
    return func


def with_job(func):
    """Specifies that ``func(arg)`` will use ``arg`` as a context manager.

    If this function is an operation function defined by :class:`~.FlowProject`, it will
    be the same as using ``with job:``.

    For example:

    .. code-block:: python

        @FlowProject.operation
        @flow.with_job
        def hello(job):
            print("hello {}".format(job))

    Is equivalent to:

    .. code-block:: python

        @FlowProject.operation
        def hello(job):
            with job:
                print("hello {}".format(job))

    This also works with the `@cmd` decorator:

    .. code-block:: python

        @FlowProject.operation
        @with_job
        @cmd
        def hello(job):
            return "echo 'hello {}'".format(job)

    Is equivalent to:

    .. code-block:: python

        @FlowProject.operation
        @cmd
        def hello_cmd(job):
            return 'trap "cd `pwd`" EXIT && cd {} && echo "hello {job}"'.format(job.ws)
    """
    @wraps(func)
    def decorated(job):
        with job:
            if getattr(func, "_flow_cmd", False):
                return 'trap "cd $(pwd)" EXIT && cd {} && {}'.format(job.ws, func(job))
            else:
                return func(job)

    setattr(decorated, '_flow_with_job', True)
    return decorated


class directives(object):
    """Decorator for operation functions to provide additional execution directives.

    Directives can for example be used to provide information about required resources
    such as the number of processes required for execution of parallelized operations.
    """

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    @classmethod
    def copy_from(cls, func):
        return cls(** getattr(func, '_flow_directives', dict()))

    def __call__(self, func):
        directives = getattr(func, '_flow_directives', dict())
        directives.update(self.kwargs)
        setattr(func, '_flow_directives', directives)
        return func


class aggregate(object):
    """Decorator for operation functions that are aggregate operations."""

    def __init__(self, grouper=None):
        if grouper is None:
            def grouper(jobs):
                yield jobs

        self.grouper = grouper

    @classmethod
    def groupsof(cls, num, fillvalue=None):
        # copied from: https://docs.python.org/3/library/itertools.html#itertools.zip_longest
        def grouper(jobs):
            args = [iter(jobs)] * num
            if six.PY2:
                from itertools import izip_longest
                return list(izip_longest(*args, fillvalue=fillvalue))
            else:
                from itertools import zip_longest
                return zip_longest(*args, fillvalue=fillvalue)

        return cls(grouper)

    @classmethod
    def groupby(cls, key=None, default=None):
        if isinstance(key, six.string_types):
            if default is None:
                def keyfunction(job):
                    return job.sp[key]
            else:
                def keyfunction(job):
                    return job.sp.get(key, default)

        elif isinstance(key, Iterable):
            if default is None:
                def keyfunction(job):
                    return tuple(job.sp[k] for k in key)
            else:
                def keyfunction(job):
                    return tuple(job.sp.get(k, default) for k in key)

        elif key is None:
            # Must return a type that can be ordered with <, >
            def keyfunction(job):
                return str(job)

        else:
            keyfunction = key

        def grouper(jobs):
            for key, group in groupby(sorted(jobs, key=keyfunction), key=keyfunction):
                yield group

        return cls(grouper)

    def __call__(self, func):
        setattr(func, '_flow_aggregate', self.grouper)
        return func


def _get_operations(include_private=False):
    """"Yields the name of all functions that qualify as an operation function.

    The module is inspected and all functions that have only one argument
    is yielded. Unless the 'include_private' argument is True, all private
    functions, that means the name starts with one or more '_' characters
    are ignored.
    """
    module = inspect.getmodule(inspect.currentframe().f_back.f_back)
    for name, obj in inspect.getmembers(module):
        if not include_private and name.startswith('_'):
            continue
        if inspect.isfunction(obj):
            if six.PY2:
                signature = inspect.getargspec(obj)
            else:
                signature = inspect.getfullargspec(obj)
            if len(signature.args) == 1:
                yield name


def run(parser=None):
    """Access to the "run" interface of an operations module.

    Executing this function within a module will start a command line interface,
    that can be used to execute operations defined within the same module.
    All **top-level unary functions** will be intepreted as executable operation functions.

    For example, if we have a module as such:

    .. code-block:: python

        # operations.py

        def hello(job):
            print('hello', job)

        if __name__ == '__main__':
            import flow
            flow.run()

    Then we can execute the ``hello`` operation for all jobs from the command like like this:

    .. code-block:: bash

        $ python operations.py hello

    .. note::

        You can control the degree of parallelization with the ``--np`` argument.


    For more information, see:

    .. code-block:: bash

        $ python operations.py --help
    """
    if parser is None:
        parser = argparse.ArgumentParser()

    parser.add_argument(
        'operation',
        type=str,
        choices=list(_get_operations()),
        help="The operation to execute.")
    parser.add_argument(
        'jobid',
        type=str,
        nargs='*',
        help="The job ids, as registered in the signac project. "
             "Omit to default to all statepoints.")
    parser.add_argument(
        '--np',
        type=int,
        default=1,
        help="Specify the number of cores to parallelize to (default=1) or 0 "
             "to parallelize on as many cores as there are available.")
    parser.add_argument(
        '-t', '--timeout',
        type=int,
        help="A timeout in seconds after which the parallel execution "
             "of operations is canceled.")
    parser.add_argument(
        '--progress',
        action='store_true',
        help="Display a progress bar during execution.")
    args = parser.parse_args()

    project = get_project()

    def _open_job_by_id(_id):
        try:
            return project.open_job(id=_id)
        except KeyError:
            msg = "Did not find job corresponding to id '{}'.".format(_id)
            raise KeyError(msg)
        except LookupError:
            raise LookupError("Multiple matches for id '{}'.".format(_id))

    if len(args.jobid):
        try:
            jobs = [_open_job_by_id(jid) for jid in args.jobid]
        except (KeyError, LookupError) as e:
            print(e, file=sys.stderr)
            sys.exit(1)
    else:
        jobs = project

    module = inspect.getmodule(inspect.currentframe().f_back)
    try:
        operation_func = getattr(module, args.operation)
    except AttributeError:
        raise KeyError("Unknown operation '{}'.".format(args.operation))

    if getattr(operation_func, '_flow_cmd', False):

        def operation(job):
            cmd = operation_func(job).format(job=job)
            fork(cmd=cmd, timeout=args.timeout)
    else:
        operation = operation_func

    # Serial execution
    if args.np == 1 or len(jobs) < 2:
        if args.timeout is not None:
            logger.warning("A timeout has no effect in serial execution!")
        for job in tqdm(jobs) if args.progress else jobs:
            operation(job)

    # Parallel execution
    elif six.PY2:
        # Due to Python 2.7 issue #8296 (http://bugs.python.org/issue8296) we
        # always need to provide a timeout to avoid issues with "hanging"
        # processing pools.
        timeout = sys.maxint if args.timeout is None else args.timeout
        pool = Pool(args.np)
        result = pool.imap_unordered(operation, jobs)
        for _ in tqdm(jobs) if args.progress else jobs:
            result.next(timeout)
    else:
        with Pool(args.np) as pool:
            result = pool.imap_unordered(operation, jobs)
            for _ in tqdm(jobs) if args.progress else jobs:
                result.next(args.timeout)


class JobsOperation(object):
    """This class represents the information needed to execute one operation for jobs.

    An operation function in this context is a shell command, which should be a function
    of one or multiple signac jobs.

    .. note::

        This class is used by the :class:`~.FlowProject` class for the execution and
        submission process and should not be instantiated by users themselves.

    :param name:
        The name of this JobsOperation instance. The name is arbitrary,
        but helps to concisely identify the operation in various contexts.
    :type name:
        str
    :param cmd:
        The command that executes this operation.
    :type cmd:
        str
    :param jobs:
        The sequence of jobs associated with this operation.
    :type job:
        :py:class:`signac.Job`.
    :param directives:
        A dictionary of additional parameters that provide instructions on how
        to execute this operation, e.g., specifically required resources.
    :type directives:
        :class:`dict`
    """
    MAX_LEN_ID = 100

    def __init__(self, name, cmd, jobs, directives=None):
        if not len(jobs):
            raise ValueError("The jobs argument cannot be empty!")
        self.name = name
        self.jobs = jobs
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

        # Evaluate strings and callables for job:
        def evaluate(value):
            if value and callable(value):
                return value(* self.jobs)
            elif isinstance(value, six.string_types):
                if len(jobs) == 1:
                    return value.format(job=self.jobs[0])
                else:
                    return value.format(jobs=self.jobs)
            else:
                return value

        directives.setdefault('np',
                              evaluate(directives.get('nranks', 1))
                              * evaluate(directives.get('omp_num_threads', 1)))
        directives.setdefault('ngpu', 0)
        directives.setdefault('nranks', 0)
        directives.setdefault('omp_num_threads', 0)

        # We use a special dictionary that allows us to track all keys that have been
        # evaluated by the template engine and compare them to those explicitly set
        # by the user. See also comment above.
        self.directives = TrackGetItemDict(
            {key: evaluate(value) for key, value in directives.items()})
        self.directives._keys_set_by_user = keys_set_by_user

    def __str__(self):
        return "{}({})".format(self.name, ', '.join([job.get_id() for job in self.jobs]))

    def __repr__(self):
        return "{type}(name='{name}', cmd={cmd}, jobs='{jobs}', directives={directives})".format(
            type=type(self).__name__,
            name=self.name,
            jobs=', '.join([job.get_id() for job in self.jobs]),
            cmd=repr(self.cmd),
            directives=self.directives)

    @property
    def job(self):
        assert len(self.jobs) >= 1
        return self.jobs[0]

    def get_id(self, index=0):
        "Return a name, which identifies this job-operation."
        project = self.jobs[0]._project

        # The full name is designed to be truly unique for each job-operation.
        full_name = '{}%{}%{}%{}'.format(
            project.root_directory(),
            ','.join([job.get_id() for job in self.jobs]),
            self.name, index)

        # The jobs_op_id is a hash computed from the unique full name.
        jobs_op_id = calc_id(full_name)

        # The actual job id is then constructed from a readable part and the jobs_op_id,
        # ensuring that the job-op is still somewhat identifiable, but guarantueed to
        # be unique. The readable name is based on the project id, job id, operation name,
        # and the index number. All names and the id itself are restricted in length
        # to guarantuee that the id does not get too long.
        max_len = self.MAX_LEN_ID - len(jobs_op_id)
        if max_len < len(jobs_op_id):
            raise ValueError("Value for MAX_LEN_ID is too small ({}).".format(self.MAX_LEN_ID))

        readable_name = '{}/{}/{}/{:04d}/'.format(
            str(project)[:12], ','.join([str(job)[:8] for job in self.jobs]),
            self.name[:12], index)[:max_len]

        # By appending the unique jobs_op_id, we ensure that each id is truly unique.
        return readable_name + jobs_op_id

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


__all__ = ['cmd', 'directives', 'run']
