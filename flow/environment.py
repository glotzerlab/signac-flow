# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Detection of compute environments.

This module provides the ComputeEnvironment class, which can be
subclassed to automatically detect specific computational environments.

This enables the user to adjust their workflow based on the present
environment, e.g. for the adjustemt of scheduler submission scripts.
"""
from __future__ import print_function
from __future__ import division
import os
import re
import socket
import logging
import importlib
from collections import OrderedDict
import importlib.machinery

from signac.common import config

from .scheduling.base import JobStatus
from .scheduling.lsf import LSFScheduler
from .scheduling.slurm import SlurmScheduler
from .scheduling.torque import TorqueScheduler
from .scheduling.simple_scheduler import SimpleScheduler
from .scheduling.fakescheduler import FakeScheduler
from .util import config as flow_config
from .errors import NoSchedulerError


logger = logging.getLogger(__name__)


# Global variable can be used to override detected environment
ENVIRONMENT = None


def setup(py_modules, **attrs):
    """Setup function for environment modules.

    Use this function in place of setuptools.setup to not only install
    a environments module, but also register it with the global signac
    configuration. Once registered, is automatically imported when the
    get_environment() function is called.
    """
    import setuptools
    from setuptools.command.install import install

    class InstallAndConfig(install):

        def run(self):
            super(InstallAndConfig, self).run()
            cfg = config.read_config_file(config.FN_CONFIG)
            try:
                envs = cfg['flow'].as_list('environment_modules')
            except KeyError:
                envs = []
            new = set(py_modules).difference(envs)
            if new:
                for name in new:
                    self.announce(
                        msg="registering module '{}' in global signac configuration".format(name),
                        level=2)
                cfg.setdefault('flow', dict())
                cfg['flow']['environment_modules'] = envs + list(new)
                cfg.write()

    return setuptools.setup(
        py_modules=py_modules,
        cmdclass={'install': InstallAndConfig},
        **attrs)


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


class ComputeEnvironment(metaclass=ComputeEnvironmentType):
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
    template = 'base_script.sh'

    @classmethod
    def is_present(cls):
        """Determine whether this specific compute environment is present.

        The default method for environment detection is trying to match a
        hostname pattern or delegate the environment check to the associated
        scheduler type.
        """
        if cls.hostname_pattern is None:
            if cls.scheduler_type is None:
                return False
            else:
                return cls.scheduler_type.is_present()
        else:
            return re.match(
                cls.hostname_pattern, socket.getfqdn()) is not None

    @classmethod
    def get_scheduler(cls):
        """Return a environment specific scheduler driver.

        The returned scheduler class provides a standardized interface to
        different scheduler implementations.
        """
        try:
            return getattr(cls, 'scheduler_type')()
        except (AttributeError, TypeError):
            raise NoSchedulerError(
                "No scheduler defined for environment '{}'.".format(cls.__name__))

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
        if cls.get_scheduler().submit(script, flags=flags, *args, **kwargs):
            return JobStatus.submitted

    @classmethod
    def add_args(cls, parser):
        """Add arguments related to this compute environment to an argument parser.

        :param parser:
            The argument parser to add arguments to.
        :type parser:
            :class:`argparse.ArgumentParser`
        """
        return

    @classmethod
    def get_config_value(cls, key, default=flow_config._GET_CONFIG_VALUE_NONE):
        """Request a value from the user's configuration.

        This method should be used whenever values need to be provided
        that are specific to a users's environment. A good example are
        account names.

        When a key is not configured and no default value is provided,
        a :py:class:`~.errors.SubmitError` will be raised and the user will
        be prompted to add the missing key to their configuration.

        Please note, that the key will be automatically expanded to
        be specific to this environment definition. For example, a
        key should be 'account', not 'MyEnvironment.account`.

        :param key: The environment specific configuration key.
        :type key: str
        :param default: A default value in case the key cannot be found
            within the user's configuration.
        :type key: str
        :return: The value or default value.
        :raises SubmitError: If the key is not in the user's configuration
            and no default value is provided.
        """
        return flow_config.require_config_value(key, ns=cls.__name__, default=default)


class StandardEnvironment(ComputeEnvironment):
    "This is a default environment, which is always present."

    @classmethod
    def is_present(cls):
        return True

    @classmethod
    def mpi_cmd(cls, cmd, np):
        return 'mpirun -np {np} {cmd}'.format(np=np, cmd=cmd)


class UnknownEnvironment(StandardEnvironment):
    "Deprecated 'standard' environment, replaced by 'StandardEnvironment.'"

    def __init__(self, *args, **kwargs):
        raise RuntimeError(
            "The 'flow.environment.UnknownEnvironment' class has been replaced by the "
            "'flow.environment.StandardEnvironment' class.")


class TestEnvironment(ComputeEnvironment):
    """This is a test environment.

    The test environment will print a mocked submission script
    and submission commands to screen. This enables testing of
    the job submission script generation in environments without
    a real scheduler.
    """
    scheduler_type = FakeScheduler


class SimpleSchedulerEnvironment(ComputeEnvironment):
    "An environment for the simple-scheduler scheduler."
    scheduler_type = SimpleScheduler
    template = 'simple_scheduler.sh'


class TorqueEnvironment(ComputeEnvironment):
    "An environment with TORQUE scheduler."
    scheduler_type = TorqueScheduler
    template = 'torque.sh'


class SlurmEnvironment(ComputeEnvironment):
    "An environment with SLURM scheduler."
    scheduler_type = SlurmScheduler
    template = 'slurm.sh'


class LSFEnvironment(ComputeEnvironment):
    "An environment with LSF scheduler."
    scheduler_type = LSFScheduler
    template = 'lsf.sh'


class NodesEnvironment(ComputeEnvironment):
    """A compute environment consisting of multiple compute nodes.

    Each compute node is assumed to have a specific number of compute units, e.g., CPUs.
    """


class DefaultTorqueEnvironment(NodesEnvironment, TorqueEnvironment):
    "A default environment for environments with TORQUE scheduler."

    @classmethod
    def add_args(cls, parser):
        super(DefaultTorqueEnvironment, cls).add_args(parser)
        parser.add_argument(
            '-w', '--walltime',
            type=float,
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
        parser.add_argument(
            '--no-copy-env',
            action='store_true',
            help="Do not copy current environment variables into compute node environment.")


class DefaultSlurmEnvironment(NodesEnvironment, SlurmEnvironment):
    "A default environment for environments with SLURM scheduler."

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


class DefaultLSFEnvironment(NodesEnvironment, LSFEnvironment):
    "A default environment for environments with LSF scheduler."

    @classmethod
    def add_args(cls, parser):
        super(DefaultLSFEnvironment, cls).add_args(parser)
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
    "An environment with CPUs."
    pass


class GPUEnvironment(ComputeEnvironment):
    "An environment with GPUs."
    pass


def _import_module(fn):
    return importlib.machinery.SourceFileLoader(fn, fn).load_module()


def _import_modules(prefix):
    for fn in os.path.listdir(prefix):
        if os.path.isfile(fn) and os.path.splitext(fn)[1] == '.py':
            _import_module(os.path.join(prefix, fn))


def _import_configured_environments():
    cfg = config.load_config(config.FN_CONFIG)
    try:
        for name in cfg['flow'].as_list('environment_modules'):
            try:
                importlib.import_module(name)
            except ImportError as e:
                logger.warning(e)
    except KeyError:
        pass


def registered_environments(import_configured=True):
    if import_configured:
        _import_configured_environments()
    return list(ComputeEnvironment.registry.values())


def get_environment(test=False, import_configured=True):
    """Attempt to detect the present environment.

    This function iterates through all defined ComputeEnvironment
    classes in reversed order of definition and and returns the
    first EnvironmentClass where the is_present() method returns
    True.

    :param test:
        Return the TestEnvironment
    :type test:
        bool
    :returns:
        The detected environment class.
    """
    if test:
        return TestEnvironment
    else:
        # Return a globally specified environment
        if ENVIRONMENT is not None:
            return ENVIRONMENT

        # Obtain a list of all registered environments
        env_types = registered_environments(import_configured=import_configured)
        logger.debug(
            "List of registered environments:\n\t{}".format(
                '\n\t'.join((str(env.__name__) for env in env_types))))

        # Select environment based on environment variable if set.
        env_from_env_var = os.environ.get('SIGNAC_FLOW_ENVIRONMENT')
        if env_from_env_var:
            for env_type in env_types:
                if env_type.__name__ == env_from_env_var:
                    return env_type
            else:
                raise ValueError("Unknown environment '{}'.".format(env_from_env_var))

        # Select based on DEBUG flag:
        for env_type in env_types:
            if getattr(env_type, 'DEBUG', False):
                logger.debug("Select environment '{}'; DEBUG=True.".format(env_type.__name__))
                return env_type

        # Default selection:
        for env_type in reversed(env_types):
            if env_type.is_present():
                logger.debug("Select environment '{}'; is present.".format(env_type.__name__))
                return env_type

        # Otherwise, just return a standard environment
        return StandardEnvironment
