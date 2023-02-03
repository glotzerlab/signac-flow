# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for INCITE supercomputers.

http://www.doeleadershipcomputing.org/
"""
from math import ceil, gcd

from ..environment import (
    DefaultLSFEnvironment,
    DefaultSlurmEnvironment,
    template_filter,
)
from ..util.template_filters import check_utilization


class SummitEnvironment(DefaultLSFEnvironment):
    """Environment profile for the Summit supercomputer.

    Example::

        @Project.operation(directives={
            "nranks": 3,  # 3 MPI ranks per operation
            "ngpu": 3,  # 3 GPUs
            "np": 3,  # 3 CPU cores
            "rs_tasks": 3,  # 3 tasks per resource set
            "extra_jsrun_args": '--smpiargs="-gpu"',  # extra jsrun arguments
        })
        def my_operation(job):
            ...

    https://www.olcf.ornl.gov/summit/
    """

    hostname_pattern = r".*\.summit\.olcf\.ornl\.gov"
    template = "summit.sh"
    mpi_cmd = "jsrun"
    cores_per_node = 42
    gpus_per_node = 6

    @template_filter
    def calc_num_nodes(cls, resource_sets, parallel=False):
        """Compute the number of nodes needed.

        Parameters
        ----------
        resource_sets : iterable of tuples
            Resource sets for each operation, as a sequence of tuples of
            *(Number of resource sets, tasks (MPI Ranks) per resource set,
            physical cores (CPUs) per resource set, GPUs per resource set)*.
        parallel : bool
            Whether operations should run in parallel or serial. (Default value
            = False)

        Returns
        -------
        int
            Number of nodes needed.

        """
        nodes_used_final = 0
        cores_used = gpus_used = nodes_used = 0
        for nsets, tasks, cpus_per_task, gpus in resource_sets:
            if not parallel:
                # In serial mode we reset for every operation.
                cores_used = gpus_used = nodes_used = 0
            for _ in range(nsets):
                cores_used += tasks * cpus_per_task
                gpus_used += gpus
                while cores_used > cls.cores_per_node or gpus_used > cls.gpus_per_node:
                    nodes_used += 1
                    cores_used = max(0, cores_used - cls.cores_per_node)
                    gpus_used = max(0, gpus_used - cls.gpus_per_node)
            if not parallel:
                #  Note that when running in serial the "leftovers" must be
                #  accounted for on a per-operation basis.
                if cores_used > 0 or gpus_used > 0:
                    nodes_used += 1
                nodes_used_final = max(nodes_used, nodes_used_final)
        if parallel:
            if cores_used > 0 or gpus_used > 0:
                nodes_used += 1
            nodes_used_final = nodes_used
        return nodes_used_final

    @template_filter
    def guess_resource_sets(cls, operation):
        """Determine the resources sets needed for an operation.

        Parameters
        ----------
        operation : :class:`flow.BaseFlowOperation`
            The operation whose directives will be used to compute the resource
            set.

        Returns
        -------
        int
            Number of resource sets.
        int
            Number of tasks (MPI ranks) per resource set.
        int
            Number of physical cores (CPUs) per resource set.
        int
            Number of GPUs per resource set.

        """
        ntasks = max(operation.directives.get("nranks", 1), 1)
        np = operation.directives.get("np", ntasks)

        cpus_per_task = max(operation.directives.get("omp_num_threads", 1), 1)
        # separate OMP threads (per resource sets) from tasks
        np //= cpus_per_task

        np_per_task = max(1, np // ntasks)
        ngpu = operation.directives.get("ngpu", 0)
        g = gcd(ngpu, ntasks)
        if ngpu >= ntasks:
            nsets = ngpu // (ngpu // g)
        else:
            nsets = ntasks // (ntasks // g)

        tasks_per_set = max(ntasks // nsets, 1)
        tasks_per_set = max(tasks_per_set, operation.directives.get("rs_tasks", 1))

        gpus_per_set = ngpu // nsets
        cpus_per_set = tasks_per_set * cpus_per_task * np_per_task

        return nsets, tasks_per_set, cpus_per_set, gpus_per_set

    @classmethod
    def jsrun_options(cls, resource_set):
        """Return jsrun options for the provided resource set.

        Parameters
        ----------
        resource_set : tuple
            Tuple of *(Number of resource sets, tasks (MPI Ranks) per resource
            set, physical cores (CPUs) per resource set, GPUs per resource
            set)*.

        Returns
        -------
        str
            Resource set options.

        """
        nsets, tasks, cpus, gpus = resource_set
        cuda_aware_mpi = (
            "--smpiargs='-gpu'" if (nsets > 0 or tasks > 0) and gpus > 0 else ""
        )
        return f"-n {nsets} -a {tasks} -c {cpus} -g {gpus} {cuda_aware_mpi}"

    @classmethod
    def _get_mpi_prefix(cls, operation, parallel):
        """Get the jsrun options based on directives.

        Parameters
        ----------
        operation : :class:`flow.project._JobOperation`
            The operation to be prefixed.
        parallel : bool
            If True, operations are assumed to be executed in parallel, which
            means that the number of total tasks is the sum of all tasks
            instead of the maximum number of tasks. Default is set to False.

        Returns
        -------
        str
            The prefix to be added to the operation's command.

        """
        extra_args = str(operation.directives.get("extra_jsrun_args", ""))
        resource_set = cls.guess_resource_sets(operation)
        mpi_prefix = cls.mpi_cmd + " " + cls.jsrun_options(resource_set)
        mpi_prefix += " -d packed -b rs " + extra_args + (" " if extra_args else "")
        return mpi_prefix


class AndesEnvironment(DefaultSlurmEnvironment):
    """Environment profile for the Andes supercomputer.

    https://www.olcf.ornl.gov/olcf-resources/compute-systems/andes/
    """

    hostname_pattern = r"andes-.*\.olcf\.ornl\.gov"
    template = "andes.sh"
    mpi_cmd = "srun"
    cores_per_node = 32

    @classmethod
    def add_args(cls, parser):
        """Add arguments to parser.

        Parameters
        ----------
        parser : :class:`argparse.ArgumentParser`
            The argument parser where arguments will be added.

        """
        super().add_args(parser)
        parser.add_argument(
            "--partition",
            choices=["batch", "gpu"],
            default="batch",
            help="Specify the partition to submit to.",
        )


class CrusherEnvironment(DefaultSlurmEnvironment):
    """Environment profile for the Cluster supercomputer.

    https://docs.olcf.ornl.gov/systems/crusher_quick_start_guide.html
    """

    hostname_pattern = r".*\.crusher\.olcf\.ornl\.gov"
    template = "crusher.sh"
    cores_per_node = 56
    gpus_per_node = 8
    mpi_cmd = "srun"

    @template_filter
    def calc_num_nodes(cls, ngpus, ncpus, threshold):
        """Compute the number of nodes needed to meet the resource request.

        Also raise an error when the requested resource do not come close to saturating the asked
        for nodes.
        """
        nodes_gpu = max(1, int(ceil(ngpus / cls.gpus_per_node)))
        nodes_cpu = max(1, int(ceil(ncpus / cls.cores_per_node)))
        if nodes_gpu >= nodes_cpu:
            check_utilization(nodes_gpu, ngpus, cls.gpus_per_node, threshold, "compute")
            return nodes_gpu
        check_utilization(nodes_cpu, ncpus, cls.cores_per_node, threshold, "compute")
        return nodes_cpu

    @classmethod
    def _get_mpi_prefix(cls, operation, parallel):
        """Get the correct srun command for the job.

        We don't currently support CPU/GPU mapping and expect the program to do this in code.
        """
        nranks = operation.directives.get("nranks", 0)
        if nranks == 0:
            return ""
        ngpus = operation.directives["ngpu"]
        np = operation.directives.get("np", 1)
        omp_num_threads = max(operation.directives.get("omp_num_threads", 1), 1)
        mpi_np_calc = nranks * omp_num_threads
        if np > 1 and nranks > 1 and np != mpi_np_calc:
            raise RuntimeWarning(
                f"Using provided value for np={np}, which seems incompatible with MPI directives "
                f"{mpi_np_calc}."
            )
        base_str = f"{cls.mpi_cmd} --ntasks={nranks}"
        threads = max(omp_num_threads, np) if nranks == 1 else max(1, omp_num_threads)
        base_str += f" --cpus-per-task={threads} --gpus={ngpus}"
        return base_str


__all__ = ["SummitEnvironment", "AndesEnvironment", "CrusherEnvironment"]
