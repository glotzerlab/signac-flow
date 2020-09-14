# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for the Minnesota Supercomputing Institute at University of Minnesota."""
from ..environment import DefaultTorqueEnvironment


class MSIEnvironment(DefaultTorqueEnvironment):
    """Environment profile for the Mangi supercomputer at UMN.
    https://www.msi.umn.edu/mangi , https://www.msi.umn.edu/queues
    """

    hostname_pattern = r'.*\.msi\.umn\.edu'
    template = 'torque.sh'
    cores_per_node = 1


__all__ = ['MSIEnvironment']
