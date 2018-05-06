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
from .project import cmd
from .environment import get_environment
from .operations import run
from .template import init
from .util.misc import redirect_log

__version__ = '0.5.6'

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
    'get_environment',
    'run',
    'init',
    'redirect_log',
    ]
