"""Workflow management based on the signac framework.

The signac-flow package provides the basic infrastructure
to easily configure workflow for data operations on a
signac workspace executed by a scheduler."""
from . import environment
from . import scheduler
from . import manage
from . project import FlowProject

__version__ = '0.0.1'

__all__ = [
    'environment',
    'scheduler',
    'manage',
    'FlowProject'
]
