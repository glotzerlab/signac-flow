# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for the University of Michigan HPC environment."""
from ..environment import DefaultSlurmEnvironment


class GreatLakesEnvironment(DefaultSlurmEnvironment):
    """Environment profile for the Great Lakes supercomputer.

    https://arc-ts.umich.edu/greatlakes/
    """

    hostname_pattern = r"gl(-login)?[0-9]+\.arc-ts\.umich\.edu"
    template = "umich-greatlakes.sh"
    cores_per_node = 1

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
