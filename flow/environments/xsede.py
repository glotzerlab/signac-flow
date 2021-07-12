# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for XSEDE supercomputers."""
import logging
import os

from ..environment import DefaultSlurmEnvironment, template_filter

logger = logging.getLogger(__name__)


class CometEnvironment(DefaultSlurmEnvironment):
    """Environment profile for the Comet supercomputer.

    https://www.sdsc.edu/services/hpc/hpc_systems.html#comet
    """

    hostname_pattern = "comet"
    template = "comet.sh"
    cores_per_node = 24
    mpi_cmd = "ibrun"

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
            choices=["compute", "gpu", "gpu-shared", "shared", "large-shared", "debug"],
            default="shared",
            help="Specify the partition to submit to.",
        )

        parser.add_argument(
            "--job-output",
            help=(
                "What to name the job output file. "
                "If omitted, uses the system default "
                '(slurm default is "slurm-%%j.out").'
            ),
        )


_STAMPEDE_OFFSET = os.environ.get("_FLOW_STAMPEDE_OFFSET_`", 0)


class Stampede2Environment(DefaultSlurmEnvironment):
    """Environment profile for the Stampede2 supercomputer.

    https://www.tacc.utexas.edu/systems/stampede2
    """

    hostname_pattern = ".*stampede2"
    template = "stampede2.sh"
    cores_per_node = 48
    mpi_cmd = "ibrun"
    offset_counter = 0
    base_offset = _STAMPEDE_OFFSET

    @template_filter
    def return_and_increment(cls, increment):
        """Increment the base offset, then return the prior value.

        Note that this filter is designed for use at submission time, and the
        environment variable will be used upon script generation. At run time,
        the base offset will be set only once (when run initializes the
        environment).

        Parameters
        ----------
        increment : int
            The increment to apply to the base offset.

        Returns
        -------
        int
            The value of the base offset before incrementing.

        """
        cls.base_offset += increment
        return cls.base_offset - increment

    @template_filter
    def decrement_offset(cls, decrement):
        """Decrement the offset value.

        This function is a hackish solution to get around the fact that Jinja
        has very limited support for direct modification of Python objects, and
        we need to be able to reset the offset between bundles.

        Parameters
        ----------
        decrement : int
            The decrement to apply to the base offset.

        Returns
        -------
        str
            Empty string (to render nothing in the jinja template).

        """
        cls.base_offset -= int(decrement)
        return ""

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
            choices=[
                "development",
                "normal",
                "large",
                "flat-quadrant",
                "skx-dev",
                "skx-normal",
                "skx-large",
                "long",
            ],
            default="skx-normal",
            help="Specify the partition to submit to.",
        )
        parser.add_argument(
            "--job-output",
            help=(
                "What to name the job output file. "
                "If omitted, uses the system default "
                '(slurm default is "slurm-%%j.out").'
            ),
        )

    @classmethod
    def _get_mpi_prefix(cls, operation, parallel):
        """Get the MPI prefix based on directives and an offset.

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
        if operation.directives.get("nranks"):
            prefix = "{} -n {} -o {} task_affinity ".format(
                cls.mpi_cmd,
                operation.directives["nranks"],
                cls.base_offset + cls.offset_counter,
            )
            if parallel:
                cls.offset_counter += operation.directives["nranks"]
        else:
            prefix = ""
        return prefix


class Bridges2Environment(DefaultSlurmEnvironment):
    """Environment profile for the Bridges-2 supercomputer.

    https://www.psc.edu/resources/bridges-2/user-guide
    """

    hostname_pattern = r".*\.bridges2\.psc\.edu$"
    template = "bridges2.sh"
    cores_per_node = 128
    mpi_cmd = "mpirun"

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
            choices=[
                "RM",
                "RM-shared",
                "RM-small",
                "EM",
                "GPU",
                "GPU-shared",
            ],
            default="RM-shared",
            help="Specify the partition to submit to.",
        )


class ExpanseEnvironment(DefaultSlurmEnvironment):
    """Environment profile for the Expanse supercomputer.

    https://www.sdsc.edu/support/user_guides/expanse.html
    """

    hostname_pattern = r".*\.expanse\.sdsc\.edu$"
    template = "expanse.sh"
    cores_per_node = 128
    gpus_per_node = 4

    @classmethod
    def _get_mpi_prefix(cls, operation, parallel):
        # The Expanse user guide recommends the usage of `srun`. However, this will not
        # forward the MPI configuration to the container, meaning it cannot be used with
        # one of flow's most common use cases, HPC with singularity containers. Thus we
        # are using the default mpi command `mpiexec` with the other recommendations
        # given by the Expanse user guide. Native MPI programs with Expanse also require
        # the `--mpi=pmi2` option (or the documentation suggests as such), but this
        # would cause errors for the container.
        #
        # This has been tested with conda environments to work.
        if operation.directives["nranks"] == 0:
            return ""
        rank_option = f" -n {operation.directives['nranks']} "
        # We have to handle the case with and without OpenMP differently due to the
        # --cpus-per-task option recommended by the Expanse user guide.
        if operation.directives["omp_num_threads"] == 0:
            return cls.mpi_cmd + rank_option
        cpus_per_task = f" --cpus-per-task={operation.directives['omp_num_threads']}"
        return cls.mpi_cmd + cpus_per_task + rank_option

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
            choices=[
                "compute",
                "shared",
                "large-shared",
                "gpu",
                "gpu-shared",
                "debug",
            ],
            default="compute",
            help="Specify the partition to submit to.",
        )


__all__ = [
    "CometEnvironment",
    "Stampede2Environment",
    "Bridges2Environment",
    "ExpanseEnvironment",
]
