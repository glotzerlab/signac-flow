# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Workflow management based on the signac framework.

The signac-flow package provides the basic infrastructure to easily
configure and implement a workflow to operate on a signac_ data space.

.. _signac: https://signac.io/
"""
from . import environment
from . import scheduling
from . import errors
from . import testing
from .project import IgnoreConditions
from .project import FlowProject
from .project import JobOperation
from .project import label
from .project import classlabel
from .project import staticlabel
from .operations import cmd
from .operations import directives
from .operations import run
from .environment import get_environment
from .template import init
from .util.misc import redirect_log
from .operations import with_job
from .version import __version__

# Import packaged environments unless disabled in config:
from .util.config import get_config_value
if get_config_value('import_packaged_environments', default=True):
    from . import environments  # noqa: F401


__all__ = [
    'environment',
    'scheduling',
    'errors',
    'testing',
    'IgnoreConditions',
    'FlowProject',
    'JobOperation',
    'label',
    'classlabel',
    'staticlabel',
    'cmd',
    'directives',
    'run',
    'get_environment',
    'init',
    'redirect_log',
    'with_job',
    '__version__',
    ]
