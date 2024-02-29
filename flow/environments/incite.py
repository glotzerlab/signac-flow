# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for INCITE supercomputers.

http://www.doeleadershipcomputing.org/
"""
from ..environment import (
    DefaultLSFEnvironment,
    DefaultSlurmEnvironment,
    _NodeTypes,
    _PartitionConfig,
    template_filter,
)


class SummitEnvironment(DefaultLSFEnvironment):
    """Environment profile for the Summit supercomputer.

    Example::

        @Project.operation(directives={
            "launcher": "mpi",  # use MPI
            "n_processes": 3,  # 3 ranks
            "gpus_per_process": 1,  # 3 GPUs
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
    _partition_config = _PartitionConfig(
        cpus_per_node={"default": 42},
        gpus_per_node={"default": 6},
        node_types={"default": _NodeTypes.WHOLENODE},
    )

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

    def guess_resource_sets(cls, directives):
        """Determine the resources sets needed for an operation.

        Parameters
        ----------
        directives : dict
            The directives to use to compute the resource set.

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
        ntasks = directives["processes"]
        cpus_per_task = directives["threads_per_process"]
        # separate OMP threads (per resource sets) from tasks
        gpus_per_process = directives["gpus_per_process"]
        ngpus = gpus_per_process * ntasks
        if ngpus >= ntasks:
            nsets = gpus_per_process // (gpus_per_process // ngpus)
        else:
            nsets = ntasks // (ntasks // ngpus)

        tasks_per_set = max(ntasks // nsets, 1)
        tasks_per_set = max(tasks_per_set, directives.get("rs_tasks", 1))

        gpus_per_set = gpus_per_process // nsets
        cpus_per_set = tasks_per_set * cpus_per_task

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
    def _get_mpi_prefix(cls, operation):
        """Get the jsrun options based on directives.

        Parameters
        ----------
        operation : :class:`flow.project._JobOperation`
            The operation to be prefixed.

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
    _partition_config = _PartitionConfig(
        cpus_per_node={"default": 32, "gpu": 28},
        gpus_per_node={"gpu": 2},
        node_types={"default": _NodeTypes.WHOLENODE},
    )

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
    _partition_config = _PartitionConfig(
        cpus_per_node={"default": 56},
        gpus_per_node={"default": 8},
        node_types={"default": _NodeTypes.WHOLENODE},
    )

    mpi_cmd = "srun"


class FrontierEnvironment(DefaultSlurmEnvironment):
    """Environment profile for the Frontier supercomputer.

    https://docs.olcf.ornl.gov/systems/frontier_user_guide.html
    """

    hostname_pattern = r".*\.frontier\.olcf\.ornl\.gov"
    template = "frontier.sh"
    _partition_config = _PartitionConfig(
        cpus_per_node={"default": 56},
        gpus_per_node={"default": 8},
        node_types={"default": _NodeTypes.WHOLENODE},
    )
    mpi_cmd = "srun"


__all__ = [
    "SummitEnvironment",
    "AndesEnvironment",
    "CrusherEnvironment",
    "FrontierEnvironment",
]
