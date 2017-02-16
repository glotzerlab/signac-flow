# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Workflow management based on the signac framework.

The signac-flow package provides the basic infrastructure to easily
configure and implement a workflow to operate on a signac_ data space.

.. _signac: https://signac.readthedocs.io
"""
from . import environment
from . import scheduler
from . import manage
from .project import FlowProject
from .project import JobOperation
from .project import label
from .project import classlabel
from .project import staticlabel
from .graph import FlowGraph

__version__ = '0.3.3'

__all__ = [
    'environment',
    'scheduler',
    'manage',
    'FlowProject',
    'JobOperation',
    'label',
    'classlabel',
    'staticlabel',
    'FlowGraph',
]
