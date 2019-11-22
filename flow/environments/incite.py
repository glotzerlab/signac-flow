# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for INCITE supercomputers.

http://www.doeleadershipcomputing.org/
"""
from ..environment import DefaultLSFEnvironment, DefaultTorqueEnvironment

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
    cores_per_node = 42
    gpus_per_node = 6

    @staticmethod
    def calc_num_nodes(resource_sets):
        cores_used = gpus_used = nodes_used = 0
        for nsets, tasks, cpus_per_task, gpus in resource_sets:
            for _ in range(nsets):
                cores_used += tasks * cpus_per_task
                gpus_used += gpus
                if (cores_used > SummitEnvironment.cores_per_node or
                        gpus_used > SummitEnvironment.gpus_per_node):
                    nodes_used += 1
                    cores_used = max(0, cores_used - SummitEnvironment.cores_per_node)
                    gpus_used = max(0, gpus_used - SummitEnvironment.gpus_per_node)
        if cores_used > 0 or gpus_used > 0:
            nodes_used += 1
        return nodes_used

    @staticmethod
    def guess_resource_sets(operation):
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

    @staticmethod
    def jsrun_options(resource_set):
        nsets, tasks, cpus, gpus = resource_set
        cuda_aware_mpi = "--smpiargs='-gpu'" if (nsets > 0 or tasks > 0) and gpus > 0 else ""
        return '-n {} -a {} -c {} -g {} {}'.format(nsets, tasks, cpus, gpus, cuda_aware_mpi)

    @staticmethod
    def generate_mpi_prefix(operation):
        """Template filter for generating mpi_prefix based on environment and proper directives

        :param:
            operation
        """
        extra_args = str(operation.directives.get('extra_jsrun_args', ''))
        resource_set = SummitEnvironment.guess_resource_sets(
                       operation)
        mpi_prefix = 'jsrun ' + SummitEnvironment.jsrun_options(resource_set)
        mpi_prefix += ' -d packed -b rs ' + extra_args + (' ' if extra_args else '')
        return mpi_prefix

    filters = {'calc_num_nodes': calc_num_nodes.__func__,
               'guess_resource_sets': guess_resource_sets.__func__,
               'jsrun_options': jsrun_options.__func__,
               'generate_mpi_prefix': generate_mpi_prefix.__func__}


class TitanEnvironment(DefaultTorqueEnvironment):
    """Environment profile for the titan super computer.

    https://www.olcf.ornl.gov/titan/
    """
    hostname_pattern = 'titan'
    template = 'titan.sh'
    cores_per_node = 16

    @staticmethod
    def generate_mpi_prefix(operation):
        """Template filter for generating mpi_prefix based on environment and proper directives

        :param:
            operation
        """

        return '{} -n {} '.format('aprun', operation.directives.nranks)

    filters = {'generate_mpi_prefix': generate_mpi_prefix.__func__}


class EosEnvironment(DefaultTorqueEnvironment):
    """Environment profile for the eos super computer.

    https://www.olcf.ornl.gov/computing-resources/eos/
    """
    hostname_pattern = 'eos'
    template = 'eos.sh'
    cores_per_node = 32

    @staticmethod
    def generate_mpi_prefix(operation):
        """Template filter for generating mpi_prefix based on environment and proper directives

        :param:
            operation
        """

        return '{} -n {} '.format('aprun', operation.directives.nranks)

    filters = {'generate_mpi_prefix': generate_mpi_prefix.__func__}


__all__ = ['TitanEnvironment', 'EosEnvironment']
