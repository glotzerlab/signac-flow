# Copyright (c) 2021 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Drexel University HPC Environments."""
from ..environment import DefaultSlurmEnvironment, _PartitionConfig


class PicotteEnvironment(DefaultSlurmEnvironment):
    """Environment profile for the Picotte supercomputer.

    https://proteusmaster.urcf.drexel.edu/urcfwiki/
    Note: This link may require access to the Drexel University network.
    """

    hostname_pattern = r".*\.cm\.cluster$"
    template = "drexel-picotte.sh"

    _partition_config = _PartitionConfig(
        cpus_per_node={"default": 48}, gpus_per_node={"gpu": 4}
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
            choices=("def", "gpu"),
            default="def",
            help="Specify the partition to submit to. (default=def)",
        )


__all__ = ["PicotteEnvironment"]
