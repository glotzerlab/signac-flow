# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for the Minnesota Supercomputing Institute at University of Minnesota."""
from ..environment import DefaultSlurmEnvironment


class MangiEnvironment(DefaultSlurmEnvironment):
    """Environment profile for the Mangi supercomputer.

    Mangi is managed by the Minnesota Supercomputing Institute (University of
    Minnesota).

    Cluster information: https://www.msi.umn.edu/mangi

    Queue information: https://www.msi.umn.edu/queues
    """

    hostname_pattern = r".*\.mangi\.msi\.umn\.edu"
    template = "slurm.sh"
    cores_per_node = 1


__all__ = ["MangiEnvironment"]
