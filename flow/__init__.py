# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Workflow management based on the signac framework.

The signac-flow package provides the basic infrastructure to easily
configure and implement a workflow to operate on a signac_ data space.

.. _signac: https://signac.io/
"""
from . import environment, errors, scheduling, testing
from .environment import get_environment
from .operations import cmd, directives, run, with_job
from .project import FlowProject, IgnoreConditions, classlabel, label, staticlabel
from .template import init

# Import packaged environments unless disabled in config:
from .util.config import get_config_value
from .util.misc import redirect_log
from .version import __version__

if get_config_value("import_packaged_environments", default=True):
    from . import environments  # noqa: F401


__all__ = [
    "environment",
    "errors",
    "scheduling",
    "testing",
    "IgnoreConditions",
    "FlowProject",
    "label",
    "classlabel",
    "staticlabel",
    "cmd",
    "directives",
    "run",
    "get_environment",
    "init",
    "redirect_log",
    "with_job",
    "__version__",
]
