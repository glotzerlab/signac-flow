# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for INCITE supercomputers.

http://www.doeleadershipcomputing.org/
"""
from ..environment import DefaultLSFEnvironment, DefaultTorqueEnvironment

import math
from fractions import gcd

from ..legacy_templating import deprecated_since_06


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

    @staticmethod
    def calc_num_nodes(resource_sets, cores_per_node, gpus_per_node):
        cores_used = gpus_used = nodes_used = 0
        for nsets, tasks, cpus_per_task, gpus in resource_sets:
            for _ in range(nsets):
                cores_used += tasks * cpus_per_task
                gpus_used += gpus
                if cores_used > cores_per_node or gpus_used > gpus_per_node:
                    nodes_used += 1
                    cores_used = max(0, cores_used - cores_per_node)
                    gpus_used = max(0, gpus_used - gpus_per_node)
        if cores_used > 0 or gpus_used > 0:
            nodes_used += 1
        return nodes_used

    @staticmethod
    def guess_resource_sets(operation, cores_per_node, gpus_per_node):
        ntasks = max(operation.directives.get('nranks', 1), 1)
        cpus_per_task = max(operation.directives.get('omp_num_threads', 1), 1)
        np = operation.directives.get('np', ntasks * cpus_per_task)
        ngpu = operation.directives.get('ngpu', 0)
        if np % cores_per_node != 0 and (ngpu == 0 or ngpu == np):
            # fill the nodes using small resource sets
            # threads are never split across resource sets
            nsets = np//cpus_per_task
        else:
            nsets = max(math.ceil(np / cores_per_node),
                        math.ceil(ngpu / gpus_per_node), 1)
        cpus_per_set = max(np // nsets, 1)
        gpus_per_set = ngpu // nsets
        tasks_per_set = max(ntasks // nsets, 1)  # Require at least one task per set
        factor = gcd(tasks_per_set, gcd(gpus_per_set, cpus_per_set))
        min_tasks_per_set = operation.directives.get('rs_tasks', 1)
        factor = max(1, factor//min_tasks_per_set)
        nsets *= factor
        tasks_per_set //= factor
        cpus_per_set //= factor
        gpus_per_set //= factor
        return nsets, tasks_per_set, cpus_per_set, gpus_per_set

    @staticmethod
    def jsrun_options(resource_set):
        nsets, tasks, cpus, gpus = resource_set
        return '-n {} -a {} -c {} -g {}'.format(nsets, tasks, cpus, gpus)

    @staticmethod
    def jsrun_extra_args(operation):
        return str(operation.directives.get('extra_jsrun_args', ''))

    filters = {'calc_num_nodes': calc_num_nodes.__func__,
               'guess_resource_sets': guess_resource_sets.__func__,
               'jsrun_options': jsrun_options.__func__,
               'jsrun_extra_args': jsrun_extra_args.__func__}


class TitanEnvironment(DefaultTorqueEnvironment):
    """Environment profile for the titan super computer.

    https://www.olcf.ornl.gov/titan/
    """
    hostname_pattern = 'titan'
    template = 'titan.sh'
    cores_per_node = 16

    @classmethod
    @deprecated_since_06
    def mpi_cmd(cls, cmd, np):
        return "aprun -n {np} -N 1 -b {cmd}".format(cmd=cmd, np=np)

    @classmethod
    @deprecated_since_06
    def gen_tasks(cls, js, np_total):
        """Helper function to generate the number of tasks (for overriding)"""
        js.writeline('#PBS -l nodes={}'.format(math.ceil(np_total/cls.cores_per_node)))
        return js

    @classmethod
    @deprecated_since_06
    def script(cls, _id, **kwargs):
        js = super(TitanEnvironment, cls).script(_id=_id, **kwargs)
        js.writeline('#PBS -A {}'.format(cls.get_config_value('account')))
        return js


class EosEnvironment(DefaultTorqueEnvironment):
    """Environment profile for the eos super computer.

    https://www.olcf.ornl.gov/computing-resources/eos/
    """
    hostname_pattern = 'eos'
    template = 'eos.sh'
    cores_per_node = 32

    @classmethod
    @deprecated_since_06
    def mpi_cmd(cls, cmd, np):
        return "aprun -n {np} -b {cmd}".format(cmd=cmd, np=np)

    @classmethod
    @deprecated_since_06
    def gen_tasks(cls, js, np_total):
        """Helper function to generate the number of tasks (for overriding)"""
        js.writeline('#PBS -l nodes={}'.format(math.ceil(np_total/cls.cores_per_node)))
        return js

    @classmethod
    @deprecated_since_06
    def script(cls, _id, **kwargs):
        js = super(EosEnvironment, cls).script(_id=_id, **kwargs)
        js.writeline('#PBS -A {}'.format(cls.get_config_value('account')))
        return js


__all__ = ['TitanEnvironment', 'EosEnvironment']
