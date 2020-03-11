# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for the University of Michigan HPC environment."""
from ..environment import DefaultSlurmEnvironment


class GreatLakesEnvironment(DefaultSlurmEnvironment):
    """Environment profile for the Great Lakes supercomputer.

    https://arc-ts.umich.edu/greatlakes/
    """
    hostname_pattern = r'gl(-login)?[0-9]+\.arc-ts\.umich\.edu'
    template = 'umich-greatlakes.sh'
    cores_per_node = 1

    @classmethod
    def add_args(cls, parser):
        super(GreatLakesEnvironment, cls).add_args(parser)
        parser.add_argument(
            '--partition',
            choices=('standard', 'gpu', 'largemem'),
            default='standard',
            help="Specify the partition to submit to. "
                 "(default=standard)")
        parser.add_argument(
            '--memory',
            default='4g',
            help=("Specify how much memory to reserve per node, e.g. \"4g\" for "
                  "4 gigabytes or \"512m\" for 512 megabytes. Only relevant "
                  "for shared queue jobs. (default=4g)"))


__all__ = ['GreatLakesEnvironment']
