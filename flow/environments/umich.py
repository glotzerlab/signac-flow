# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for the University of Michigan HPC environment."""
from ..environment import DefaultTorqueEnvironment


class FluxEnvironment(DefaultTorqueEnvironment):
    """Environment profile for the flux supercomputing environment.

    http://arc-ts.umich.edu/systems-and-services/flux/
    """
    hostname_pattern = '(nyx|flux).*.umich.edu'
    template = 'umich-flux.sh'
    cores_per_node = 1

    @classmethod
    def add_args(cls, parser):
        super(FluxEnvironment, cls).add_args(parser)
        parser.add_argument(
            '--mode',
            choices=('cpu', 'gpu'),
            default='cpu',
            help="Specify whether to submit to the CPU or the GPU queue. "
                 "(default=cpu)")
        parser.add_argument(
            '--memory',
            default='4g',
            help="Specify how much memory to reserve per node. (default=4g)")


__all__ = ['FluxEnvironment']
