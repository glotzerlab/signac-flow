# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Workflow management based on the signac framework.

The signac-flow package provides the basic infrastructure
to easily configure workflow for data operations on a
signac workspace executed by a scheduler."""
from . import environment
from . import scheduler
from . import manage
from . project import FlowProject

__version__ = '0.2.1'

__all__ = [
    'environment',
    'scheduler',
    'manage',
    'FlowProject'
]
