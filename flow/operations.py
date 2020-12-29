# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Defines operation decorators and a simple command line interface ``run``.

This module implements the run() function, which when called equips a regular
Python module with a command line interface. This interface can be used to
execute functions defined within the same module that operate on a signac data
space.

See also: :class:`~.FlowProject`.
"""
import argparse
import inspect
import logging
import subprocess
import sys
from functools import wraps
from multiprocessing import Pool

from signac import get_project
from tqdm.auto import tqdm

logger = logging.getLogger(__name__)


def cmd(func):
    """Indicate that ``func`` returns a shell command with this decorator.

    If this function is an operation function defined by :class:`~.FlowProject`, it will
    be interpreted to return a shell command, instead of executing the function itself.

    For example:

    .. code-block:: python

        @FlowProject.operation
        @flow.cmd
        def hello(job):
            return "echo {job.id}"

    .. note::
        The final shell command generated for :meth:`~.FlowProject.run` or
        :meth:`~.FlowProject.submit` still respects directives and will prepend e.g. MPI or OpenMP
        prefixes to the shell command provided here.
    """
    if getattr(func, "_flow_with_job", False):
        raise RuntimeError(
            "@cmd should appear below the @with_job decorator in your script"
        )
    setattr(func, "_flow_cmd", True)
    return func


def with_job(func):
    """Use ``arg`` as a context manager for ``func(arg)`` with this decorator.

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

    setattr(decorated, "_flow_with_job", True)
    return decorated


class directives:
    """Decorator for operation functions to provide additional execution directives.

    Directives can for example be used to provide information about required resources
    such as the number of processes required for execution of parallelized operations.
    For more information, read about :ref:`signac-docs:directives`.

    In addition, you can use the ``@directives(fork=True)`` directive to enforce that a
    particular operation is always executed within a subprocess and not within the
    Python interpreter's process even if there are no other reasons that would prevent that.

    .. note::

        Setting ``fork=False`` will not prevent forking if there are other reasons for forking,
        such as a timeout.
    """

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    @classmethod
    def copy_from(cls, func):
        """Copy directives from another operation."""
        return cls(**getattr(func, "_flow_directives", {}))

    def __call__(self, func):
        """Add directives to the function.

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
        directives = getattr(func, "_flow_directives", {})
        directives.update(self.kwargs)
        setattr(func, "_flow_directives", directives)
        return func


def _get_operations(include_private=False):
    """Yield the name of all functions that qualify as an operation function.

    The module is inspected and all functions that have only one argument
    is yielded. Unless the 'include_private' argument is True, all private
    functions, that means the name starts with one or more '_' characters
    are ignored.
    """
    module = inspect.getmodule(inspect.currentframe().f_back.f_back)
    for name, obj in inspect.getmembers(module):
        if not include_private and name.startswith("_"):
            continue
        if inspect.isfunction(obj):
            signature = inspect.getfullargspec(obj)
            if len(signature.args) == 1:
                yield name


def run(parser=None):
    """Access to the "run" interface of an operations module.

    Executing this function within a module will start a command line interface,
    that can be used to execute operations defined within the same module.
    All **top-level unary functions** will be interpreted as executable operation functions.

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
        "operation",
        type=str,
        choices=list(_get_operations()),
        help="The operation to execute.",
    )
    parser.add_argument(
        "job_id",
        type=str,
        nargs="*",
        help="The job ids, as registered in the signac project. "
        "Omit to default to all statepoints.",
    )
    parser.add_argument(
        "--np",
        type=int,
        default=1,
        help="Specify the number of cores to parallelize to (default=1) or 0 "
        "to parallelize on as many cores as there are available.",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=int,
        help="A timeout in seconds after which the parallel execution "
        "of operations is canceled.",
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="Display a progress bar during execution.",
    )
    args = parser.parse_args()

    project = get_project()

    def _open_job_by_id(_id):
        try:
            return project.open_job(id=_id)
        except KeyError:
            msg = f"Did not find job corresponding to id '{_id}'."
            raise KeyError(msg)
        except LookupError:
            raise LookupError(f"Multiple matches for id '{_id}'.")

    if len(args.job_id):
        try:
            jobs = [_open_job_by_id(job_id) for job_id in args.job_id]
        except (KeyError, LookupError) as error:
            print(error, file=sys.stderr)
            sys.exit(1)
    else:
        jobs = project

    module = inspect.getmodule(inspect.currentframe().f_back)
    try:
        operation_func = getattr(module, args.operation)
    except AttributeError:
        raise KeyError(f"Unknown operation '{args.operation}'.")

    if getattr(operation_func, "_flow_cmd", False):

        def operation(job):
            cmd = operation_func(job).format(job=job)
            subprocess.run(cmd, shell=True, timeout=args.timeout, check=True)

    else:
        operation = operation_func

    # Serial execution
    if args.np == 1 or len(jobs) < 2:
        if args.timeout is not None:
            logger.warning("A timeout has no effect in serial execution!")
        for job in tqdm(jobs) if args.progress else jobs:
            operation(job)
    else:
        with Pool(args.np) as pool:
            result = pool.imap_unordered(operation, jobs)
            for _ in tqdm(jobs) if args.progress else jobs:
                result.next(args.timeout)


__all__ = ["cmd", "directives", "run"]
