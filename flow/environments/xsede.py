# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for XSEDE supercomputers."""
from ..environment import DefaultSlurmEnvironment


class CometEnvironment(DefaultSlurmEnvironment):
    """Environment profile for the Comet supercomputer.

    http://www.sdsc.edu/services/hpc/hpc_systems.html#comet
    """
    hostname_pattern = 'comet'
    cores_per_node = 24

    @classmethod
    def script(cls, _id, ppn, memory=None, job_output=None, **kwargs):
        if ppn != cls.cores_per_node:
            partition = 'shared'
        else:
            if memory is not None:
                raise RuntimeError("Specifying memory is only valid for shared queue jobs")
            partition = 'compute'

        js = super(CometEnvironment, cls).script(_id=_id, ppn=ppn, **kwargs)
        js.writeline('#SBATCH -A {}'.format(cls.get_config_value('account')))
        js.writeline('#SBATCH --partition={}'.format(partition))
        if memory is not None:
            js.writeline('#SBATCH --mem={}G'.format(memory))
        if job_output is not None:
            js.writeline('#SBATCH --output="{}"'.format(job_output))
            js.writeline('#SBATCH --error="{}"'.format(job_output))
        return js

    @classmethod
    def mpi_cmd(cls, cmd, np):
        return "ibrun -v -np {np} {cmd}".format(cmd=cmd, np=np)

    @classmethod
    def add_args(cls, parser):
        super(CometEnvironment, cls).add_args(parser)
        parser.add_argument(
            '--memory',
            help=("Specify how much memory to reserve per node. "
            "Only relevant for shared queue jobs."))

        parser.add_argument(
            '--job-output',
            help=('What to name the job output file. '
            'If omitted, uses the system default '
            '(slurm default is "slurm-%%j.out").'))


__all__ = ['CometEnvironment']
