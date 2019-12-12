# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for INCITE supercomputers.

http://www.doeleadershipcomputing.org/
"""
from ..environment import DefaultLSFEnvironment, DefaultTorqueEnvironment
from ..environment import template_filter

from fractions import gcd


class SummitEnvironment(DefaultLSFEnvironment):
    """Environment profile for the Summit supercomputer.

    Example::

        @Project.operation
        @directives(nranks=3) # 3 MPI ranks per operation
        @directives(ngpu=3) # 3 GPUs
        @directives(np=3) # 3 CPU cores
        @directives(rs_tasks=3) # 3 tasks per resource set
        @directives(extra_jsrun_args='--smpiargs="-gpu"') # extra jsrun arguments
        def my_operation(job):
            ...

    https://www.olcf.ornl.gov/summit/
    """
    hostname_pattern = r'.*\.summit\.olcf\.ornl\.gov'
    template = 'summit.sh'
    mpi_cmd_string = 'jsrun'
    cores_per_node = 42
    gpus_per_node = 6

    @template_filter
    def calc_num_nodes(cls, resource_sets):
        cores_used = gpus_used = nodes_used = 0
        for nsets, tasks, cpus_per_task, gpus in resource_sets:
            for _ in range(nsets):
                cores_used += tasks * cpus_per_task
                gpus_used += gpus
                if (cores_used > cls.cores_per_node or
                        gpus_used > cls.gpus_per_node):
                    nodes_used += 1
                    cores_used = max(0, cores_used - cls.cores_per_node)
                    gpus_used = max(0, gpus_used - cls.gpus_per_node)
        if cores_used > 0 or gpus_used > 0:
            nodes_used += 1
        return nodes_used

    @template_filter
    def guess_resource_sets(cls, operation):
        ntasks = max(operation.directives.get('nranks', 1), 1)
        np = operation.directives.get('np', ntasks)

        cpus_per_task = max(operation.directives.get('omp_num_threads', 1), 1)
        # separate OMP threads (per resource sets) from tasks
        np //= cpus_per_task

        np_per_task = max(1, np//ntasks)
        ngpu = operation.directives.get('ngpu', 0)
        g = gcd(ngpu, ntasks)
        if ngpu >= ntasks:
            nsets = ngpu // (ngpu // g)
        else:
            nsets = ntasks // (ntasks // g)

        tasks_per_set = max(ntasks // nsets, 1)
        tasks_per_set = max(tasks_per_set, operation.directives.get('rs_tasks', 1))

        gpus_per_set = ngpu // nsets
        cpus_per_set = tasks_per_set*cpus_per_task*np_per_task

        return nsets, tasks_per_set, cpus_per_set, gpus_per_set

    @template_filter
    def jsrun_options(cls, resource_set):
        nsets, tasks, cpus, gpus = resource_set
        cuda_aware_mpi = "--smpiargs='-gpu'" if (nsets > 0 or tasks > 0) and gpus > 0 else ""
        return '-n {} -a {} -c {} -g {} {}'.format(nsets, tasks, cpus, gpus, cuda_aware_mpi)

    @template_filter
    def get_prefix(cls, operation, mpi_prefix=None, cmd_prefix=None, parallel=False):
        """Template filter for getting the prefix based on proper directives.

        :param operation:
            The operation for which to add prefix.
        :param mpi_prefix:
            User defined mpi_prefix string. Default is set to None.
            This will be deprecated and removed in the future.
        :param cmd_prefix:
            User defined cmd_prefix string. Default is set to None.
            This will be deprecated and removed in the future.
        :param parallel:
            If True, operations are assumed to be executed in parallel, which means
            that the number of total tasks is the sum of all tasks instead of the
            maximum number of tasks. Default is set to False.
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
        else:
            extra_args = str(operation.directives.get('extra_jsrun_args', ''))
            resource_set = cls.guess_resource_sets(operation)
            prefix += cls.mpi_cmd_string + ' ' + cls.jsrun_options(resource_set)
            prefix += ' -d packed -b rs ' + extra_args + (' ' if extra_args else '')
        if cmd_prefix:
            prefix += cmd_prefix
        return prefix


class TitanEnvironment(DefaultTorqueEnvironment):
    """Environment profile for the titan super computer.

    https://www.olcf.ornl.gov/titan/
    """
    hostname_pattern = 'titan'
    template = 'titan.sh'
    cores_per_node = 16
    mpi_cmd_string = 'aprun'


class EosEnvironment(DefaultTorqueEnvironment):
    """Environment profile for the eos super computer.

    https://www.olcf.ornl.gov/computing-resources/eos/
    """
    hostname_pattern = 'eos'
    template = 'eos.sh'
    cores_per_node = 32
    mpi_cmd_string = 'aprun'


__all__ = ['TitanEnvironment', 'EosEnvironment']
