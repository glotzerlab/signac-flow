# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Defines the API for the scheduling system."""
from .fakescheduler import FakeScheduler
from .torque import TorqueScheduler
from .slurm import SlurmScheduler


class MoabScheduler(TorqueScheduler):

    def __init__(self, *args, **kwargs):
        raise RuntimeError("The MoabScheduler has been renamed to TorqueScheduler.")


__all__ = [
    'FakeScheduler',
    'TorqueScheduler',
    'SlurmScheduler',
    'MoabScheduler',
    ]
