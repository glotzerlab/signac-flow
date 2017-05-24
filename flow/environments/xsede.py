# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Environments for XSEDE supercomputers."""
from ..environment import DefaultSlurmEnvironment


class CometEnvironment(DefaultSlurmEnvironment):
    """Environment profile for the Comet supercomputer.

    http://www.sdsc.edu/services/hpc/hpc_systems.html#comet
    """
    hostname_pattern = 'comet'
    cores_per_node = 24

    @classmethod
    def script(cls, _id, ppn, **kwargs):
        if ppn != cls.cores_per_node:
            partition = 'shared'
        else:
            partition = 'compute'

        js = super(CometEnvironment, cls).script(_id=_id, ppn=ppn, **kwargs)
        js.writeline('#SBATCH -A {}'.format(cls.get_config_value('account')))
        js.writeline('#SBATCH --partition={}'.format(partition))
        return js

    @classmethod
    def mpi_cmd(cls, cmd, np):
        return "ibrun -v -np {np} {cmd}".format(cmd=cmd, np=np)


__all__ = ['CometEnvironment']
