# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for XSEDE supercomputers."""
from __future__ import print_function
import logging

from ..environment import DefaultSlurmEnvironment


logger = logging.getLogger(__name__)


class CometEnvironment(DefaultSlurmEnvironment):
    """Environment profile for the Comet supercomputer.

    http://www.sdsc.edu/services/hpc/hpc_systems.html#comet
    """
    hostname_pattern = 'comet'
    template = 'comet.sh'
    cores_per_node = 24
    mpi_cmd = 'ibrun'

    @classmethod
    def add_args(cls, parser):
        super(CometEnvironment, cls).add_args(parser)

        parser.add_argument(
          '--partition',
          choices=['compute', 'gpu', 'gpu-shared', 'shared', 'large-shared', 'debug'],
          default='shared',
          help="Specify the partition to submit to.")

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
    template = 'stampede2.sh'
    cores_per_node = 48
    mpi_cmd = 'ibrun'

    @classmethod
    def add_args(cls, parser):
        super(Stampede2Environment, cls).add_args(parser)
        parser.add_argument(
            '--partition',
            choices=['development', 'normal', 'large', 'flat-quadrant',
                     'skx-dev', 'skx-normal', 'skx-large', 'long'],
            default='skx-normal',
            help="Specify the partition to submit to.")
        parser.add_argument(
            '--job-output',
            help=('What to name the job output file. '
                  'If omitted, uses the system default '
                  '(slurm default is "slurm-%%j.out").'))

    @staticmethod
    def get_prefix(operation, mpi_prefix=None, cmd_prefix=None, parallel=False,
                   mpi_cmd='mpiexec'):
        """Template filter for getting prefix based on environment and proper directives.
        Template filter for Stampede2 supercomputers.

        :param operation:
            The operation for which to add prefix.
        :param parallel:
            If True, operations are assumed to be executed in parallel, which means
            that the number of total tasks is the sum of all tasks instead of the
            maximum number of tasks. Default is set to False.
        :type parallel:
            bool
        :return prefix:
            The prefix should be added for the operation.
        :type prefix:
            str
        """

        prefix = ''
        if operation.directives.get('omp_num_threads'):
            prefix += 'export OMP_NUM_THREADS={}\n'.format(operation.directives['omp_num_threads'])
        if mpi_prefix:
            prefix += mpi_prefix
        elif operation.directives.get('nranks'):
            if parallel:
                prefix += '{} -n {} -o {} task_affinity '.format(
                       mpi_cmd, operation.directives['nranks'],
                       operation.directives['np_offset'])
            else:
                prefix += '{} -n {} '.format(mpi_cmd, operation.directives['nranks'])
        if cmd_prefix:
            prefix += cmd_prefix
        return prefix


class BridgesEnvironment(DefaultSlurmEnvironment):
    """Environment profile for the Bridges super computer.

    https://portal.xsede.org/psc-bridges
    """
    hostname_pattern = r'.*\.bridges\.psc\.edu$'
    template = 'bridges.sh'
    cores_per_node = 28
    mpi_cmd = 'mpirun'

    @classmethod
    def add_args(cls, parser):
        super(BridgesEnvironment, cls).add_args(parser)
        parser.add_argument(
          '--partition',
          choices=['RM', 'RM-shared', 'RM-small', 'GPU', 'GPU-shared', 'LM'],
          default='RM-shared',
          help="Specify the partition to submit to.")


__all__ = ['CometEnvironment', 'BridgesEnvironment', 'Stampede2Environment']
