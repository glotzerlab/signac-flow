# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for the University of Michigan HPC environment."""
from ..environment import DefaultTorqueEnvironment

import math


class FluxEnvironment(DefaultTorqueEnvironment):
    """Environment profile for the flux supercomputing environment.

    http://arc-ts.umich.edu/systems-and-services/flux/
    """
    hostname_pattern = '(nyx|flux).*.umich.edu'
    template = 'umich.sh'
    cores_per_node = 1

    @classmethod
    def mpi_cmd(cls, cmd, np):
        return "mpirun -np {np} {cmd}".format(cmd=cmd, np=np)

    @classmethod
    def gen_tasks(cls, js, np_total, mode):
        """Helper function to generate the number of tasks (for overriding)"""
        if mode == 'cpu':
            js = js.writeline('#PBS -l nodes={}'.format(
                math.ceil(np_total/cls.cores_per_node)))
        elif mode == 'gpu':
            js.writeline('#PBS -l nodes={np}:gpus=1'.format(
                np=math.ceil(np_total/cls.cores_per_node)))
        return js

    @classmethod
    def script(cls, _id, np_total, mode, memory, **kwargs):
        js = super(FluxEnvironment, cls).script(_id=_id, **kwargs)
        js.writeline('#PBS -A {}'.format(cls.get_config_value('account')))
        js.writeline('#PBS -l qos={}'.format(cls.get_config_value('qos', 'flux')))
        js.writeline('#PBS -l pmem={}'.format(memory))
        if mode == 'cpu':
            js.writeline('#PBS -q {}'.format(cls.get_config_value('cpu_queue', 'flux')))
            js = cls.gen_tasks(js, np_total, mode)
        elif mode == 'gpu':
            q = cls.get_config_value('gpu_queue', cls.get_config_value('cpu_queue', 'flux') + 'g')
            js.writeline('#PBS -q {}'.format(q))
            js = cls.gen_tasks(js, np_total, mode)
        else:
            raise ValueError("Unknown mode '{}'.".format(mode))
        return js

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
