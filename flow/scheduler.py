# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import warnings

from .fakescheduler import FakeScheduler
from .torque import TorqueScheduler
from .slurm import SlurmScheduler


class MoabScheduler(TorqueScheduler):

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "The MoabScheduler has been renamed to TorqueScheduler.",
            DeprecationWarning)
        super(MoabScheduler, self).__init__(*args, **kwargs)


__all__ = [
    'FakeScheduler',
    'TorqueScheduler',
    'SlurmScheduler',
    'MoabScheduler',
    ]
