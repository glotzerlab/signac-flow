# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Detection of compute environments.

This module provides the :class:`ComputeEnvironment` class, which can be
subclassed to automatically detect specific computational environments.

This enables the user to adjust their workflow based on the present
environment, e.g. for the adjustment of scheduler submission scripts.
"""
import logging
import os
import re
import socket
from functools import lru_cache

from .directives import (
    _FORK,
    _GET_EXECUTABLE,
    _MEMORY,
    _NGPU,
    _NP,
    _NRANKS,
    _OMP_NUM_THREADS,
    _PROCESSOR_FRACTION,
    _WALLTIME,
    _Directives,
)
from .errors import NoSchedulerError
from .scheduling.base import JobStatus
from .scheduling.fake_scheduler import FakeScheduler
from .scheduling.lsf import LSFScheduler
from .scheduling.pbs import PBSScheduler
from .scheduling.simple_scheduler import SimpleScheduler
from .scheduling.slurm import SlurmScheduler

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _cached_fqdn():
    """Return the fully qualified domain name.

    This value is cached because fetching the fully qualified domain name can
    be slow on macOS.
    """
    return socket.getfqdn()


class _ComputeEnvironmentType(type):
    """Metaclass used for :class:`~.ComputeEnvironment`.

    This metaclass automatically registers :class:`~.ComputeEnvironment`
    definitions, which enables the automatic determination of the present
    environment. The registry can be obtained from
    :func:`~.registered_environments`.
    """

    def __init__(cls, name, bases, dct):
        if not hasattr(cls, "registry"):
            cls.registry = {}
        else:
            cls.registry[name] = cls
        super().__init__(name, bases, dct)


def template_filter(func):
    """Decorate a function as a :class:`~.ComputeEnvironment` template filter.

    This decorator is applied to methods defined in a subclass of
    :class:`~.ComputeEnvironment` that are used in that environment's
    templates. The decorated function becomes a class method that is available
    as a :ref:`jinja2 filter <jinja2:filters>` in templates rendered by a
    :class:`~.FlowProject` with that :class:`~.ComputeEnvironment`.

    Parameters
    ----------
    func : callable
        Function to decorate.

    Returns
    -------
    callable
        Decorated function.

    """
    setattr(func, "_flow_template_filter", True)
    return classmethod(func)


class ComputeEnvironment(metaclass=_ComputeEnvironmentType):
    """Define computational environments.

    The ComputeEnvironment class allows us to automatically determine
    specific environments in order to programmatically adjust workflows
    in different environments.

    The default method for the detection of a specific environment is to
    provide a regular expression matching the environment's hostname.
    For example, if the hostname is ``my-server.com``, one could identify the
    environment by setting the ``hostname_pattern`` to ``'my-server'``.
    """

    scheduler_type = None
    hostname_pattern = None
    submit_flags = None
    template = "base_script.sh"
    mpi_cmd = "mpiexec"

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
            return cls.scheduler_type.is_present()
        return re.match(cls.hostname_pattern, _cached_fqdn()) is not None

    @classmethod
    def get_scheduler(cls):
        """Return an environment-specific scheduler driver.

        The returned scheduler class provides a standardized interface to
        different scheduler implementations.
        """
        try:
            return getattr(cls, "scheduler_type")()
        except (AttributeError, TypeError):
            raise NoSchedulerError(
                f"No scheduler defined for environment '{cls.__name__}'."
            )

    @classmethod
    def submit(cls, script, flags=None, *args, **kwargs):
        r"""Submit a job submission script to the environment's scheduler.

        Scripts should be submitted to the environment, instead of directly
        to the scheduler to allow for environment specific post-processing.

        Parameters
        ----------
        script : str
            The script to submit.
        flags : list
            A list of additional flags to provide to the scheduler.
            (Default value = None)
        \*args
            Positional arguments forwarded to the scheduler's submit method.
        \*\*kwargs
            Keyword arguments forwarded to the scheduler's submit method.

        Returns
        -------
        JobStatus.submitted or None
            Status of job, if submitted.

        """
        if flags is None:
            flags = []
        env_flags = getattr(cls, "submit_flags", [])
        if env_flags:
            flags.extend(env_flags)
        # parse the flag to check for --job-name
        for flag in flags:
            if "--job-name" in flag:
                raise ValueError('Assignment of "--job-name" is not supported.')
        # Hand off the actual submission to the scheduler
        if cls.get_scheduler().submit(script, flags=flags, *args, **kwargs):
            return JobStatus.submitted
        return None

    @classmethod
    def add_args(cls, parser):
        """Add arguments related to this compute environment to an argument parser.

        Parameters
        ----------
        parser : :class:`argparse.ArgumentParser`
            The argument parser where arguments will be added.

        """
        pass

    @classmethod
    def _get_omp_prefix(cls, operation):
        """Get the OpenMP prefix based on the ``omp_num_threads`` directive.

        Parameters
        ----------
        operation : :class:`flow.project._JobOperation`
            The operation to be prefixed.

        Returns
        -------
        str
            The prefix to be added to the operation's command.

        """
        return "export OMP_NUM_THREADS={}; ".format(
            operation.directives["omp_num_threads"]
        )

    @classmethod
    def _get_mpi_prefix(cls, operation, parallel):
        """Get the MPI prefix based on the ``nranks`` directives.

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
            return "{} -n {} ".format(cls.mpi_cmd, operation.directives["nranks"])
        return ""

    @template_filter
    def get_prefix(cls, operation, parallel=False, mpi_prefix=None, cmd_prefix=None):
        """Template filter generating a command prefix from directives.

        Parameters
        ----------
        operation : :class:`flow.project._JobOperation`
            The operation to be prefixed.
        parallel : bool
            If True, operations are assumed to be executed in parallel, which means
            that the number of total tasks is the sum of all tasks instead of the
            maximum number of tasks. Default is set to False.
        mpi_prefix : str
            User defined mpi_prefix string. Default is set to None.
            This will be deprecated and removed in the future.
        cmd_prefix : str
            User defined cmd_prefix string. Default is set to None.
            This will be deprecated and removed in the future.

        Returns
        -------
        str
            The prefix to be added to the operation's command.

        """
        prefix = ""
        if operation.directives.get("omp_num_threads"):
            prefix += cls._get_omp_prefix(operation)
        if mpi_prefix:
            prefix += mpi_prefix
        else:
            prefix += cls._get_mpi_prefix(operation, parallel)
        if cmd_prefix:
            prefix += cmd_prefix
        # if cmd_prefix and if mpi_prefix for backwards compatibility
        # Can change to get them from directives for future
        return prefix

    @classmethod
    def _get_default_directives(cls):
        return _Directives(
            (
                _GET_EXECUTABLE(),
                _FORK,
                _MEMORY,
                _NGPU,
                _NP,
                _NRANKS,
                _OMP_NUM_THREADS,
                _PROCESSOR_FRACTION,
                _WALLTIME,
            )
        )


class StandardEnvironment(ComputeEnvironment):
    """Default environment which is always present."""

    @classmethod
    def is_present(cls):
        """Determine whether this specific compute environment is present.

        The StandardEnvironment is always present, so this returns True.
        """
        return True


class TestEnvironment(ComputeEnvironment):
    """Environment used for testing.

    The test environment will print a mocked submission script
    and submission commands to screen. This enables testing of
    the job submission script generation in environments without
    a real scheduler.
    """

    scheduler_type = FakeScheduler


class SimpleSchedulerEnvironment(ComputeEnvironment):
    """An environment for the simple-scheduler scheduler."""

    scheduler_type = SimpleScheduler
    template = "simple_scheduler.sh"


class DefaultPBSEnvironment(ComputeEnvironment):
    """Default environment for clusters with a PBS scheduler."""

    scheduler_type = PBSScheduler
    template = "pbs.sh"

    @classmethod
    def add_args(cls, parser):
        """Add arguments to the parser.

        Parameters
        ----------
        parser : :class:`argparse.ArgumentParser`
            The argument parser where arguments will be added.

        """
        super().add_args(parser)
        parser.add_argument(
            "--hold", action="store_true", help="Submit jobs, but put them on hold."
        )
        parser.add_argument(
            "--after",
            type=str,
            help="Schedule this job to be executed after "
            "completion of a cluster job with this id.",
        )
        parser.add_argument(
            "--job-output",
            type=str,
            help=(
                "What to name the job output file. "
                "If omitted, uses the scheduler default name. "
                "Both stdout and stderr will be combined."
            ),
        )
        parser.add_argument(
            "--no-copy-env",
            action="store_true",
            help="Do not copy current environment variables into compute node environment.",
        )


class DefaultSlurmEnvironment(ComputeEnvironment):
    """Default environment for clusters with a SLURM scheduler."""

    scheduler_type = SlurmScheduler
    template = "slurm.sh"

    @classmethod
    def add_args(cls, parser):
        """Add arguments to the parser.

        Parameters
        ----------
        parser : :class:`argparse.ArgumentParser`
            The argument parser where arguments will be added.

        """
        super().add_args(parser)
        parser.add_argument(
            "--hold", action="store_true", help="Submit jobs, but put them on hold."
        )
        parser.add_argument(
            "--after",
            type=str,
            help="Schedule this job to be executed after "
            "completion of a cluster job with this id.",
        )
        parser.add_argument(
            "--job-output",
            type=str,
            help=(
                "What to name the job output file. "
                "If omitted, uses the scheduler default name. "
                "Both stdout and stderr will be combined."
            ),
        )


class DefaultLSFEnvironment(ComputeEnvironment):
    """Default environment for clusters with a LSF scheduler."""

    scheduler_type = LSFScheduler
    template = "lsf.sh"

    @classmethod
    def add_args(cls, parser):
        """Add arguments to the parser.

        Parameters
        ----------
        parser : :class:`argparse.ArgumentParser`
            The argument parser where arguments will be added.

        """
        super().add_args(parser)
        parser.add_argument(
            "--hold", action="store_true", help="Submit jobs, but put them on hold."
        )
        parser.add_argument(
            "--after",
            type=str,
            help="Schedule this job to be executed after "
            "completion of a cluster job with this id.",
        )
        parser.add_argument(
            "--job-output",
            type=str,
            help=(
                "What to name the job output file. "
                "If omitted, uses the scheduler default name. "
                "Both stdout and stderr will be combined."
            ),
        )


def registered_environments():
    """Return a list of registered environments.

    Returns
    -------
    list
        List of registered environments.

    """
    return list(ComputeEnvironment.registry.values())


def get_environment(test=False):
    """Attempt to detect the present environment.

    This function iterates through all defined :class:`~.ComputeEnvironment`
    classes in reversed order of definition and returns the first
    environment where the :meth:`~.ComputeEnvironment.is_present` method
    returns True.

    Parameters
    ----------
    test : bool
        Whether to return the TestEnvironment. (Default value = False)
    import_configured : bool
        Whether to import environments specified in the flow configuration.
        (Default value = True)

    Returns
    -------
    :class:`~.ComputeEnvironment`
        The detected environment class.

    """
    if test:
        return TestEnvironment

    # Obtain a list of all registered environments
    env_types = registered_environments()
    logger.debug(
        "List of registered environments:\n\t{}".format(
            "\n\t".join(str(env.__name__) for env in env_types)
        )
    )

    # Select environment based on environment variable if set.
    env_from_env_var = os.environ.get("SIGNAC_FLOW_ENVIRONMENT")
    if env_from_env_var:
        for env_type in env_types:
            if env_type.__name__ == env_from_env_var:
                return env_type
        raise ValueError(f"Unknown environment '{env_from_env_var}'.")

    # Select based on DEBUG flag:
    for env_type in env_types:
        if getattr(env_type, "DEBUG", False):
            logger.debug(f"Select environment '{env_type.__name__}'; DEBUG=True.")
            return env_type

    # Default selection:
    for env_type in reversed(env_types):
        if env_type.is_present():
            logger.debug(f"Select environment '{env_type.__name__}'; is present.")
            return env_type

    # Otherwise, just return a standard environment
    return StandardEnvironment
