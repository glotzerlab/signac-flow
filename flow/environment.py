# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Define environments to automate submission scripts.

Partially adapted from clusterutils package by
Matthew Spellings."""

from __future__ import print_function
import re
import socket
import logging
from signac.common.six import with_metaclass


logger = logging.getLogger(__name__)

MODE_CPU = 'cpu'
MODE_GPU = 'gpu'


class ComputeEnvironmentType(type):

    def __init__(cls, name, bases, dct):
        if not hasattr(cls, 'registry'):
            cls.registry = dict()
        else:
            cls.registry[name] = cls
        return super(ComputeEnvironmentType, cls).__init__(name, bases, dct)


class ComputeEnvironment(with_metaclass(ComputeEnvironmentType)):
    hostname_pattern = None

    def __init__(self, mode=MODE_CPU):
        if mode not in self.header_scripts.keys():
            raise ValueError(mode)
        self.mode = mode

    @classmethod
    def is_present(cls):
        if cls.hostname_pattern is None:
            return False
        else:
            return re.match(
                cls.hostname_pattern, socket.gethostname()) is not None

    @classmethod
    def submit(cls, jobsid, np, walltime, script,
               pretend=False, *args, **kwargs):
        raise NotImplementedError()


class UnknownEnvironment(ComputeEnvironment):
    pass


class TestEnvironment(ComputeEnvironment):
    pass


class MoabEnvironment(ComputeEnvironment):
    submit_cmd = ['qsub']
    cores_per_node = None


class SlurmEnvironment(ComputeEnvironment):
    cores_per_node = None


class CPUEnvironment(ComputeEnvironment):
    pass


class GPUEnvironment(ComputeEnvironment):
    pass


def get_environment():
    for env_type in ComputeEnvironment.registry.values():
        if env_type.is_present():
            return env_type
    else:
        return UnknownEnvironment
