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
import inspect
import logging
from functools import wraps
from textwrap import indent

from deprecation import deprecated

from .environment import ComputeEnvironment
from .version import __version__

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

    This decorator can only be used for operations that accept a single job as a parameter.

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
    if getattr(func, "_flow_aggregate", False):
        raise RuntimeError("The @with_job decorator cannot be used with aggregation.")

    @wraps(func)
    def decorated(*jobs):
        with jobs[0] as job:
            if getattr(func, "_flow_cmd", False):
                return f'trap "cd $(pwd)" EXIT && cd {job.ws} && {func(job)}'
            else:
                return func(job)

    setattr(decorated, "_flow_with_job", True)
    return decorated


@deprecated(
    deprecated_in="0.15",
    removed_in="1.0",
    current_version=__version__,
    details="Use FlowProject.operation.with_directives",
)
class directives:
    """Decorator for operation functions to provide additional execution directives.

    Directives can for example be used to provide information about required resources
    such as the number of processes required for execution of parallelized operations.
    For more information, read about :ref:`signac-docs:directives`.

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


def _document_directive(directive):
    name = directive._name
    name = name.replace("_", r"\_")
    doc = directive.__doc__
    return f"**{name}**\n\n{doc}"


_directives_to_document = (
    ComputeEnvironment._get_default_directives()._directive_definitions.values()
)
directives.__doc__ += indent(
    "\n**Supported Directives:**\n\n"
    + "\n\n".join(
        _document_directive(directive) for directive in _directives_to_document
    ),
    "    ",
)


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


__all__ = ["cmd", "directives", "with_job"]
