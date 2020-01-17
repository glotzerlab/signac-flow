# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Definitions of Exception classes used in this package."""

import jinja2
from jinja2.ext import Extension as Jinja2Extension


class ConfigKeyError(KeyError):
    "Indicates that a config key was not found."
    pass


class SubmitError(RuntimeError):
    "Indicates an error during cluster job submission."
    pass


class NoSchedulerError(AttributeError):
    "Indicates that there is no scheduler type defined for an environment class."
    pass


class UserConditionError(RuntimeError):
    "Indicates an error during evaluation of a FlowCondition."
    pass


class UserOperationError(RuntimeError):
    "Indicates an error during execution of a FlowOperation."
    pass


class TemplateError(Jinja2Extension):
    """Indicates errors in jinja2 templates"""
    # ref:http://jinja.pocoo.org/docs/2.10/extensions/#jinja-extensions
    tags = set(['raise'])

    def parse(self, parser):
        lineno = next(parser.stream).lineno
        args = [parser.parse_expression()]
        return jinja2.nodes.CallBlock(
            self.call_method('err', args), [], [], []).set_lineno(lineno)

    def err(self, msg, caller):
        raise jinja2.TemplateError(msg)
