# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Definitions of Exception classes used in this package."""

from jinja2 import nodes
from jinja2.ext import Extension
from jinja2.exceptions import TemplateRuntimeError


class SubmitError(RuntimeError):
    "Indicates an error during cluster job submission."
    pass


class NoSchedulerError(AttributeError):
    "Indicates that there is no scheduler type defined for an environment class."
    pass


class TemplateError(Extension):
    """Indicates errors in jinja2 templates"""
    # ref:http://jinja.pocoo.org/docs/2.10/extensions/#jinja-extensions
    tags = set(['raise'])

    def parse(self, parser):
        lineno = next(parser.stream).lineno
        args = [parser.parse_expression()]
        return nodes.CallBlock(
            self.call_method('err', args), [], [], []).set_lineno(lineno)

    def err(self, msg, caller):
        raise TemplateRuntimeError(msg)
