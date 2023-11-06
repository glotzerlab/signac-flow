# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for XSEDE supercomputers."""
import logging

from ..environment import DefaultSlurmEnvironment

logger = logging.getLogger(__name__)


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
    "Bridges2Environment",
    "ExpanseEnvironment",
    "DeltaEnvironment",
]
