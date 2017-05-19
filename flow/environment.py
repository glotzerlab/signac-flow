# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Detection of compute environments.

This module provides the ComputeEnvironment class, which can be
subclassed to automatically detect specific computational environments.

This enables the user to adjust their workflow based on the present
environment, e.g. for the adjustemt of scheduler submission scripts.
"""
from __future__ import print_function
import sys
import re
import socket
import logging
import warnings
import io
from math import ceil
from collections import OrderedDict


from signac.common.six import with_metaclass
from signac.common.config import load_config
from . import scheduler
from . import manage
from .errors import SubmitError


logger = logging.getLogger(__name__)


UTILIZATION_WARNING = """You either specified or the environment is configured to use {ppn}
processors per node (ppn), however you only use {usage:0.2%} of each node.
Consider to increase the number of processors per operation (--np)
or adjust the processors per node (--ppn).

Alternatively, you can also use --force to ignore this warning.
"""

MISSING_ENV_CONF_KEY_MSG = """Your environment is missing the following configuration key: '{key}'
Please provide the missing information, for example by adding it to your global configuration:

signac config set --global {key} <VALUE>
"""


def format_timedelta(delta):
    "Format a time delta for interpretation by schedulers."
    hours, r = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(r, 60)
    hours += delta.days * 24
    return "{:0>2}:{:0>2}:{:0>2}".format(hours, minutes, seconds)


class ComputeEnvironmentType(type):
    """Meta class for the definition of ComputeEnvironments.

    This meta class automatically registers ComputeEnvironment definitions,
    which enables the automatic determination of the present environment.
    """

    def __init__(cls, name, bases, dct):
        if not hasattr(cls, 'registry'):
            cls.registry = OrderedDict()
        else:
            cls.registry[name] = cls
        return super(ComputeEnvironmentType, cls).__init__(name, bases, dct)


class JobScript(io.StringIO):
    """"Simple StringIO wrapper for the creation of job submission scripts.

    Using this class to write a job submission script allows us to use
    environment specific expressions, for example for MPI commands.
    """
    eol = '\n'

    def __init__(self, env):
        self._env = env
        super().__init__()

    def writeline(self, line=''):
        "Write one line to the job script."
        self.write(line + self.eol)

    def write_cmd(self, cmd, np=1, bg=False):
        """Write a command to the jobscript.

        This command wrapper function is a convenience function, which
        adds mpi and other directives whenever necessary.

        :param cmd: The command to write to the jobscript.
        :type cmd: str
        :param np: The number of processors required for execution.
        :type np: int
        """
        if np > 1:
            cmd = self._env.mpi_cmd(cmd, np=np)
        if bg:
            cmd = self._env.bg(cmd)
        self.writeline(cmd)


class ComputeEnvironment(with_metaclass(ComputeEnvironmentType)):
    """Define computational environments.

    The ComputeEnvironment class allows us to automatically determine
    specific environments in order to programatically adjust workflows
    in different environments.

    The default method for the detection of a specific environemnt is to
    provide a regular expression matching the environment's hostname.
    For example, if the hostname is my_server.com, one could identify the
    environment by setting the hostname_pattern to 'my_server'.
    """
    scheduler_type = None
    hostname_pattern = None
    submit_flags = None

    @classmethod
    def script(cls, **kwargs):
        """Return a JobScript instance.

        Derived ComputeEnvironment classes may require additional
        arguments for the creation of a job submission script.
        """
        return JobScript(cls)

    @classmethod
    def is_present(cls):
        """Determine whether this specific compute environment is present.

        The default method for environment detection is trying to match a
        hostname pattern.
        """
        if cls.hostname_pattern is None:
            if cls.scheduler_type is None:
                return False
            else:
                return cls.scheduler_type.is_present()
        else:
            return re.match(
                cls.hostname_pattern, socket.gethostname()) is not None

    @classmethod
    def get_scheduler(cls):
        """Return a environment specific scheduler driver.

        The returned scheduler class provides a standardized interface to
        different scheduler implementations.
        """
        try:
            return getattr(cls, 'scheduler_type')()
        except (AttributeError, TypeError):
            raise SubmitError("You must define a scheduler type for every environment")

    @classmethod
    def submit(cls, script, flags=None, *args, **kwargs):
        """Submit a job submission script to the environment's scheduler.

        Scripts should be submitted to the environment, instead of directly
        to the scheduler to allow for environment specific post-processing.
        """
        if flags is None:
            flags = []
        env_flags = getattr(cls, 'submit_flags', [])
        if env_flags:
            flags.extend(env_flags)

        # Hand off the actual submission to the scheduler
        script.seek(0)
        if cls.get_scheduler().submit(script, flags=flags, *args, **kwargs):
            return manage.JobStatus.submitted

    @staticmethod
    def bg(cmd):
        "Wrap a command (cmd) to be executed in the background."
        return cmd + ' &'

    @classmethod
    def add_args(cls, parser):
        return

    @classmethod
    def get_config_value(cls, key, default=None):
        try:
            flow_config = load_config()['flow']
            env_config = flow_config[cls.__name__]
            return env_config[key]
        except KeyError:
            if default is None:
                k = '{}.{}'.format(cls.__name__, key)
                print(MISSING_ENV_CONF_KEY_MSG.format(key='flow.' + k))
                raise SubmitError("Missing environment configuration key: '{}'".format(k))
            else:
                return default


class UnknownEnvironment(ComputeEnvironment):
    "This is a default environment, which is always present."

    @classmethod
    def is_present(cls):
        return True

    @classmethod
    def get_scheduler(cls):
        raise AttributeError(
            "No scheduler defined for unknown environment.")


class TestEnvironment(ComputeEnvironment):
    """This is a test environment.

    The test environment will print a mocked submission script
    and submission commands to screen. This enables testing of
    the job submission script generation in environments without
    an real scheduler.
    """
    scheduler_type = scheduler.FakeScheduler
    cores_per_node = 1

    @classmethod
    def mpi_cmd(cls, cmd, np):
        return 'mpirun -np {np} {cmd}'.format(np=np, cmd=cmd)

    @classmethod
    def script(cls, **kwargs):
        js = super(TestEnvironment, cls).script(**kwargs)
        for key in sorted(kwargs):
            js.writeline('#TEST {}={}'.format(key, kwargs[key]))
        return js


class TorqueEnvironment(ComputeEnvironment):
    "An environment with TORQUE scheduler."
    scheduler_type = scheduler.TorqueScheduler


class MoabEnvironment(ComputeEnvironment):
    """"An environment with TORQUE scheduler.

    This class is deprecated and only kept for backwards
    compatibility.
    """
    scheduler_type = scheduler.TorqueScheduler

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "The MoabEnvironment has been replaced by the TorqueEnvironment.",
            DeprecationWarning)
        super(MoabEnvironment, self).__init__(*args, **kwargs)


class SlurmEnvironment(ComputeEnvironment):
    "An environment with slurm scheduler."
    scheduler_type = scheduler.SlurmScheduler


class NodesEnvironment(ComputeEnvironment):

    @classmethod
    def add_args(cls, parser):
        super(NodesEnvironment, cls).add_args(parser)
        parser.add_argument(
            '--nn',
            type=int,
            help="Specify the number of nodes.")
        parser.add_argument(
            '--ppn',
            type=int,
            help="Specify the number of processors allocated to each node.")

    @classmethod
    def calc_num_nodes(cls, operations, ppn, np=None, serial=True, force=False):
        if np is None:
            np = 1
        if ppn is None:
            ppn = getattr(cls, 'cores_per_node', None)
        if ppn is None:
            raise SubmitError(
                "Unable to determine number of required nodes (nn), please provide "
                "directly or processors per node (ppn).")
        else:
            # Calculate the total number of required processors
            np_total = np if serial else np * len(operations)
            # Calculate the total number of required nodes
            nn = ceil(np_total / ppn)
            if not force:  # Perform basic check concerning the node utilization.
                usage = np * len(operations) / nn / ppn
                if usage < 0.9:
                    print(UTILIZATION_WARNING.format(ppn=ppn, usage=usage), file=sys.stderr)
                    raise SubmitError("Bad node utilization!")
            return nn


class DefaultTorqueEnvironment(NodesEnvironment, TorqueEnvironment):
    "A default environment for environments with TORQUE scheduler."

    @classmethod
    def mpi_cmd(cls, cmd, np):
        return 'mpirun -np {np} {cmd}'.format(np=np, cmd=cmd)

    @classmethod
    def add_args(cls, parser):
        super(DefaultTorqueEnvironment, cls).add_args(parser)
        parser.add_argument(
            '-w', '--walltime',
            type=float,
            default=12,
            help="The wallclock time in hours.")
        parser.add_argument(
            '--hold',
            action='store_true',
            help="Submit jobs, but put them on hold.")
        parser.add_argument(
            '--after',
            type=str,
            help="Schedule this job to be executed after "
                 "completion of a cluster job with this id.")
        parser.add_argument('--no-copy-env', action='store_true')

    @classmethod
    def script(cls, _id, nn=None, ppn=None, walltime=None, no_copy_env=False, **kwargs):
        js = super(DefaultTorqueEnvironment, cls).script()
        js.writeline('#PBS -N {}'.format(_id))
        if nn is not None:
            if ppn is None:
                js.writeline('#PBS -l nodes={}'.format(nn))
            else:
                js.writeline('#PBS -l nodes={}:ppn={}'.format(nn, ppn))
        js.writeline('#PBS -l walltime={}'.format(format_timedelta(walltime)))
        if not no_copy_env:
            js.writeline('#PBS -V')
        return js


class DefaultSlurmEnvironment(NodesEnvironment, SlurmEnvironment):
    "A default environment for environments with slurm scheduler."

    @classmethod
    def mpi_cmd(cls, cmd, np):
        return 'mpirun -np {np} {cmd}'.format(np=np, cmd=cmd)

    @classmethod
    def script(cls, _id, nn=None, ppn=None, walltime=None, **kwargs):
        js = super(DefaultSlurmEnvironment, cls).script()
        js.writeline('#!/bin/bash')
        js.writeline('#SBATCH --job-name={}'.format(_id))
        if nn is not None:
            js.writeline('#SBATCH --nodes={}'.format(nn))
        if ppn is not None:
            js.writeline('#SBATCH --ntasks-per-node={}'.format(ppn))
        if walltime is not None:
            js.writeline('#SBATCH -t {}'.format(format_timedelta(walltime)))
        return js

    @classmethod
    def add_args(cls, parser):
        super(DefaultSlurmEnvironment, cls).add_args(parser)
        parser.add_argument(
            '-w', '--walltime',
            type=float,
            default=12,
            help="The wallclock time in hours.")
        parser.add_argument(
            '--hold',
            action='store_true',
            help="Submit jobs, but put them on hold.")
        parser.add_argument(
            '--after',
            type=str,
            help="Schedule this job to be executed after "
                 "completion of a cluster job with this id.")


class CPUEnvironment(ComputeEnvironment):
    pass


class GPUEnvironment(ComputeEnvironment):
    pass


def get_environment(test=False):
    """Attempt to detect the present environment.

    This function iterates through all defined ComputeEnvironment
    classes in reversed order of definition and and returns the
    first EnvironmentClass where the is_present() method returns
    True.

    :param test: Return the TestEnvironment
    :type tets: bool
    :returns: The detected environment class.
    """
    if test:
        return TestEnvironment
    else:
        env_types = list(ComputeEnvironment.registry.values())
        for env_type in env_types:
            if getattr(env_type, 'DEBUG', False):
                return env_type
        for env_type in reversed(env_types):
            if env_type.is_present():
                return env_type
        else:
            return UnknownEnvironment
