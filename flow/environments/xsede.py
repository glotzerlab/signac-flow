# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for XSEDE supercomputers."""
import logging
import os

from ..environment import DefaultSlurmEnvironment, template_filter
from ..util import template_filters

logger = logging.getLogger(__name__)


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
    _cpus_per_node = {
        "default": 48,
        "skx-dev": 68,
        "skx-normal": 68,
        "skx-large": 68,
        "icx-normal": 80,
    }
    _gpus_per_node = {"default": 0.0}

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

    @classmethod
    def _get_scheduler_values(cls, context):
        threshold = 0.0 if context.get("force", False) else 0.9
        partition = context.get("partition", "default")
        cpu_tasks_total = template_filters.calc_tasks(
            context["operations"],
            "np",
            len(context["operations"]) > 1 or context.get("parallel", False),
            context.get("force", False),
        )
        gpu_tasks_total = template_filters.calc_tasks(
            context["operations"],
            "ngpu",
            context.get("parallel", False),
            context.get("force", False),
        )
        num_nodes = template_filters.calc_num_nodes(
            cpu_tasks_total, cls._get_cpus_per_node(partition), threshold
        )
        return {
            "ncpu_tasks": cpu_tasks_total,
            "ngpu_tasks": gpu_tasks_total,
            "num_nodes": num_nodes,
        }


class Bridges2Environment(DefaultSlurmEnvironment):
    """Environment profile for the Bridges-2 supercomputer.

    https://www.psc.edu/resources/bridges-2/user-guide
    """

    hostname_pattern = r".*\.bridges2\.psc\.edu$"
    template = "bridges2.sh"
    mpi_cmd = "mpirun"
    _cpus_per_node = {"default": 128, "EM": 96, "GPU": 40, "GPU-shared": 40}
    _gpus_per_node = {"default": 8}
    _shared_partitions = {"RM-shared", "GPU-shared"}

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
    _cpus_per_node = {"default": 128, "GPU": 40}
    _gpus_per_node = {"default": 4}
    _shared_partitions = {"shared", "gpu-shared"}

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


class DeltaEnvironment(DefaultSlurmEnvironment):
    """Environment profile for the Delta supercomputer.

    https://wiki.ncsa.illinois.edu/display/DSC/Delta+User+Guide
    """

    # Example hostnames
    # login: dt-login02.delta.internal.ncsa.edu
    # cpu host: cn001.delta.ncsa.illinois.edu
    # gpu host: gpua049.delta.ncsa.illinois.edu
    # Avoid full specification of patterns as Delta has a habit of changing hostnames. This should
    # be safer given the parts listed are less likely to change.
    hostname_pattern = r"(gpua|dt|cn)(-login)?[0-9]+\.delta.*\.ncsa.*\.edu"
    template = "delta.sh"
    _cpus_per_node = {
        "default": 128,
        "gpuA40x4": 64,
        "gpuA100x4": 64,
        "gpuA100x8": 128,
        "gpuMI100x8": 128,
    }
    _gpus_per_node = {"default": 4, "gpuA100x8": 8, "gpuMI100x8": 8}
    _shared_partitions = {"cpu", "gpuA100x4", "gpuA40x4", "gpuA100x8", "gpuMI100x8"}

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
            choices=("cpu", "gpuA40x4", "gpuA100x4", "gpuA100x8", "gpuMI100x8"),
            default="cpu",
            help="Specify the partition to submit to. (default=cpu)",
        )


__all__ = [
    "Stampede2Environment",
    "Bridges2Environment",
    "ExpanseEnvironment",
    "DeltaEnvironment",
]
