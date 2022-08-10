# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Workflow management based on the signac framework.

The signac-flow package provides the basic infrastructure to easily
configure and implement a workflow to operate on a signac_ data space.

.. _signac: https://signac.io/
"""
from . import environment, environments, errors, hooks, scheduling, testing
from .aggregates import aggregator, get_aggregate_id
from .environment import get_environment
from .operations import cmd, directives, with_job
from .project import FlowProject, IgnoreConditions, classlabel, label, staticlabel
from .template import init

# Import packaged environments unless disabled in config:
from .util.misc import redirect_log
from .version import __version__

__all__ = [
    "environment",
    "environments",
    "errors",
    "hooks",
    "scheduling",
    "testing",
    "aggregator",
    "get_aggregate_id",
    "get_environment",
    "cmd",
    "directives",
    "with_job",
    "FlowProject",
    "IgnoreConditions",
    "classlabel",
    "label",
    "staticlabel",
    "init",
    "redirect_log",
    "__version__",
]
