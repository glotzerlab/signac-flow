# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Definitions of Exception classes used in this package."""

import jinja2
from jinja2.ext import Extension as Jinja2Extension


class ConfigKeyError(KeyError):
    """Indicates that a config key was not found."""

    pass


class DirectivesError(ValueError):
    """Indicates that a directive was incorrectly set."""

    pass


class SubmitError(RuntimeError):
    """Indicates an error during cluster job submission."""

    pass


class NoSchedulerError(AttributeError):
    """Indicates that there is no scheduler type defined for an environment class."""

    pass


class UserConditionError(RuntimeError):
    """Indicates an error during evaluation of a condition."""

    pass


class UserOperationError(RuntimeError):
    """Indicates an error during execution of a :class:`~.BaseFlowOperation`."""

    pass


class TemplateError(Jinja2Extension):
    """Indicates an error in a jinja2 template."""

    # Reference: https://jinja.palletsprojects.com/en/2.11.x/extensions/
    # The tags are a set of strings that trigger the parse method.
    tags = {"raise"}

    def parse(self, parser):
        """Call :meth:`~.err` when a template raises an Exception."""
        lineno = next(parser.stream).lineno
        args = [parser.parse_expression()]
        return jinja2.nodes.CallBlock(
            self.call_method("err", args), [], [], []
        ).set_lineno(lineno)

    def err(self, msg, caller):
        """Raise a template error."""
        raise jinja2.TemplateError(msg)


class FlowProjectDefinitionError(ValueError):
    """Indicates an invalid FlowProject definition."""

    pass
