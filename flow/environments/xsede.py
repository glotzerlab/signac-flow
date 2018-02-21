# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for XSEDE supercomputers."""
from ..environment import DefaultSlurmEnvironment
from ..errors import SubmitError
import warnings


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
            help=("Specify how much memory to reserve per node in GB. "
                  "Only relevant for shared queue jobs."))

        parser.add_argument(
            '--job-output',
            help=('What to name the job output file. '
                  'If omitted, uses the system default '
                  '(slurm default is "slurm-%%j.out").'))


class Stampede2Environment(DefaultSlurmEnvironment):
    """Environment profile for the Stampede2 supercomputer.

    https://www.tacc.utexas.edu/systems/stampede2
    """
    hostname_pattern = '.*stampede2'
    cores_per_node = None

    @classmethod
    def script(cls, _id, ppn, node_type='skx', job_output=None, **kwargs):
        if node_type == 'skx':
            partition = 'skx-normal'
            cores_per_node = 48
        elif node_type == 'knl':
            partition = 'normal'
            cores_per_node = 68

        js = super(Stampede2Environment, cls).script(_id=_id, ppn=ppn, **kwargs)
        # Stampede does not require account specification if you only
        # have one, so it is optional here.
        acct = cls.get_config_value('account', 'None')
        if acct != 'None':
            js.writeline('#SBATCH -A {}'.format(acct))
        else:
            warnings.warn(
                    "No account found, assuming you can submit without one",
                    UserWarning)
        js.writeline('#SBATCH --partition={}'.format(partition))
        if ppn is None:
            js.writeline('#SBATCH --ntasks-per-node={}'.format(cores_per_node))
        if job_output is not None:
            js.writeline('#SBATCH --output="{}"'.format(job_output))
            js.writeline('#SBATCH --error="{}"'.format(job_output))
        return js

    @classmethod
    def mpi_cmd(cls, cmd, np):
        return "ibrun {cmd}".format(cmd=cmd)

    @classmethod
    def add_args(cls, parser):
        super(Stampede2Environment, cls).add_args(parser)
        parser.add_argument(
            '--node-type',
            default='skx',
            choices=['skx', 'knl'],
            help=("On stampede, can submit to either Skylake (skx) or"
                  "Knights Landing (knl) nodes. Skylake is default"))
        parser.add_argument(
            '--job-output',
            help=('What to name the job output file. '
                  'If omitted, uses the system default '
                  '(slurm default is "slurm-%%j.out").'))

__all__ = ['CometEnvironment', 'Stampede2Environment']
