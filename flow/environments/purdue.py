# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for Purdue supercomputers."""
import logging

from ..environment import DefaultSlurmEnvironment

logger = logging.getLogger(__name__)


class AnvilEnvironment(DefaultSlurmEnvironment):
    """Environment profile for the Anvil supercomputer.

    https://www.rcac.purdue.edu/knowledge/anvil
    """

    hostname_pattern = r".*\.anvil\.rcac\.purdue\.edu$"
    template = "anvil.sh"
    mpi_cmd = "mpirun"
    _cpus_per_node = {"default": 128}
    _gpus_per_node = {"default": 4}
    _shared_partitions = {"debug", "gpu-debug", "shared", "highmem", "gpu"}

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
                "debug",
                "gpu-debug",
                "wholenode",
                "wide",
                "shared",
                "highmem",
                "gpu",
            ],
            default="shared",
            help="Specify the partition to submit to.",
        )


__all__ = [
    "AnvilEnvironment",
]
