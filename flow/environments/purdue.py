# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for Purdue supercomputers."""
import logging

from ..environment import DefaultSlurmEnvironment, _NodeTypes, _PartitionConfig

logger = logging.getLogger(__name__)


class AnvilEnvironment(DefaultSlurmEnvironment):
    """Environment profile for the Anvil supercomputer.

    https://www.rcac.purdue.edu/knowledge/anvil
    """

    hostname_pattern = r".*\.anvil\.rcac\.purdue\.edu$"
    template = "anvil.sh"
    mpi_cmd = "mpirun"
    _partition_config = _PartitionConfig(
        cpus_per_node={"default": 128},
        gpus_per_node={"gpu": 4, "gpu-debug": 4},
        node_types={
            "gpu-debug": _NodeTypes.SHARED,
            "shared": _NodeTypes.SHARED,
            "highmem": _NodeTypes.SHARED,
            "wholenode": _NodeTypes.WHOLENODE,
            "wide": _NodeTypes.WHOLENODE,
        },
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
