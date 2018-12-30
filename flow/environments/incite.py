# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for INCITE supercomputers.

http://www.doeleadershipcomputing.org/
"""
from ..environment import DefaultLSFEnvironment, DefaultTorqueEnvironment

import math
from fractions import gcd


class SummitEnvironment(DefaultLSFEnvironment):
    """Environment profile for the Summit supercomputer.

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
        cpus_per_task = operation.directives.get('omp_num_threads', 0)
        np = operation.directives.get('np', ntasks * max(cpus_per_task, 1))
        cpus_per_task = np // ntasks
        ngpu = operation.directives.get('ngpu', 0)
        nsets = max(math.ceil(np / cores_per_node),
                    math.ceil(ngpu / gpus_per_node), 1)
        gpus_per_set = ngpu // nsets
        tasks_per_set = max(ntasks // nsets, 1)  # Require at least one task per set
        factor = gcd(tasks_per_set, gpus_per_set)
        nsets *= factor
        tasks_per_set //= factor
        gpus_per_set //= factor
        return nsets, tasks_per_set, cpus_per_task, gpus_per_set

    @staticmethod
    def jsrun_options(resource_set):
        nsets, tasks, cpus, gpus = resource_set
        return '-n {} -a {} -c {} -g {}'.format(nsets, tasks, cpus, gpus)

    filters = {'calc_num_nodes': calc_num_nodes.__func__,
               'guess_resource_sets': guess_resource_sets.__func__,
               'jsrun_options': jsrun_options.__func__}


class AscentEnvironment(SummitEnvironment):
    """Environment profile for the Ascent supercomputer (Summit testing)."""
    hostname_pattern = r'.*\.ascent\.olcf\.ornl\.gov'


class TitanEnvironment(DefaultTorqueEnvironment):
    """Environment profile for the titan super computer.

    https://www.olcf.ornl.gov/titan/
    """
    hostname_pattern = 'titan'
    template = 'titan.sh'
    cores_per_node = 16

    @classmethod
    def mpi_cmd(cls, cmd, np):
        return "aprun -n {np} -N 1 -b {cmd}".format(cmd=cmd, np=np)

    @classmethod
    def gen_tasks(cls, js, np_total):
        """Helper function to generate the number of tasks (for overriding)"""
        js.writeline('#PBS -l nodes={}'.format(math.ceil(np_total/cls.cores_per_node)))
        return js

    @classmethod
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
    def mpi_cmd(cls, cmd, np):
        return "aprun -n {np} -b {cmd}".format(cmd=cmd, np=np)

    @classmethod
    def gen_tasks(cls, js, np_total):
        """Helper function to generate the number of tasks (for overriding)"""
        js.writeline('#PBS -l nodes={}'.format(math.ceil(np_total/cls.cores_per_node)))
        return js

    @classmethod
    def script(cls, _id, **kwargs):
        js = super(EosEnvironment, cls).script(_id=_id, **kwargs)
        js.writeline('#PBS -A {}'.format(cls.get_config_value('account')))
        return js


__all__ = ['TitanEnvironment', 'EosEnvironment']
