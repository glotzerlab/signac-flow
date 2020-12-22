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


class BridgesEnvironment(DefaultSlurmEnvironment):
    """Environment profile for the Bridges super computer.

    https://portal.xsede.org/psc-bridges
    """

    hostname_pattern = r".*\.bridges\.psc\.edu$"
    template = "bridges.sh"
    cores_per_node = 28
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
                "LM",
                "GPU",
                "GPU-shared",
                "GPU-small",
                "GPU-AI",
            ],
            default="RM-shared",
            help="Specify the partition to submit to.",
        )


__all__ = [
    "CometEnvironment",
    "Stampede2Environment",
    "BridgesEnvironment",
]
