# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Workflow management based on the signac framework.

The signac-flow package provides the basic infrastructure to easily
configure and implement a workflow to operate on a signac_ data space.

.. _signac: https://glotzerlab.engin.umich.edu/signac
"""
from __future__ import absolute_import
from . import environment
from . import scheduling
from . import errors
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

__version__ = '0.7.1'

__all__ = [
    'environment',
    'scheduling',
    'errors',
    'FlowProject',
    'JobOperation',
    'label',
    'classlabel',
    'staticlabel',
    'cmd',
    'directives',
    'run',
    'init',
    'redirect_log',
    'get_environment',
    'with_job'
    ]
