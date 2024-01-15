# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for the University of Michigan HPC environment."""
from ..environment import DefaultSlurmEnvironment, _PartitionConfig


class GreatLakesEnvironment(DefaultSlurmEnvironment):
    """Environment profile for the Great Lakes supercomputer.

    https://arc-ts.umich.edu/greatlakes/
    """

    hostname_pattern = r"gl(-login)?[0-9]+\.arc-ts\.umich\.edu"
    template = "umich-greatlakes.sh"
    _partition_config = _PartitionConfig(
        cpus_per_node={"default": 36, "gpu": 40},
        gpus_per_node={"gpu": 2},
    )
    # For unknown reasons, srun fails to export environment variables such as
    # PATH on Great Lakes unless explicitly requested to with --export=ALL.
    # On Great Lakes, srun also fails to flush the buffer until the end of
    # the job without explicitly setting -u.
    mpi_cmd = "srun -u --export=ALL"

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
            choices=("standard", "gpu", "largemem"),
            default="standard",
            help="Specify the partition to submit to. (default=standard)",
        )


__all__ = ["GreatLakesEnvironment"]
