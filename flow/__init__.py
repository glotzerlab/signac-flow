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
from . import scheduler
from . import manage
from . import errors
from .project import FlowProject
from .project import JobOperation
from .project import label
from .project import classlabel
from .project import staticlabel
from .environment import get_environment
from .operations import run
from .operations import redirect_log
from .template import init

__version__ = '0.5.3'

__all__ = [
    'environment',
    'scheduler',
    'manage',
    'errors',
    'FlowProject',
    'JobOperation',
    'label',
    'classlabel',
    'staticlabel',
    'get_environment',
    'run',
    'redirect_log',
    'init',
    ]
