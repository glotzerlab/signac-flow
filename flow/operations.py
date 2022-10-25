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
import logging
from textwrap import indent

from .directives import _document_directive
from .environment import ComputeEnvironment

logger = logging.getLogger(__name__)


class directives:
    """Decorator for operation functions to provide additional execution directives.

    Directives can for example be used to provide information about required resources
    such as the number of processes required for execution of parallelized operations.
    For more information, read about :ref:`signac-docs:cluster_submission_directives`.

    .. deprecated:: 0.15
        This decorator is deprecated and will be removed in 1.0.
        Use :class:`FlowProject.operation.with_directives` instead.

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


# Remove when @flow.directives is removed
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


__all__ = ["directives"]
