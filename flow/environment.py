# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Detection of compute environments.

This module provides the :class:`ComputeEnvironment` class, which can be
subclassed to automatically detect specific computational environments.

This enables the user to adjust their workflow based on the present
environment, e.g. for the adjustment of scheduler submission scripts.
"""
import enum
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
from .errors import NoSchedulerError, SubmitError
from .scheduling.base import JobStatus
from .scheduling.fake_scheduler import FakeScheduler
from .scheduling.lsf import LSFScheduler
from .scheduling.pbs import PBSScheduler
from .scheduling.simple_scheduler import SimpleScheduler
from .scheduling.slurm import SlurmScheduler
from .util.misc import _deprecated_warning
from .util.template_filters import calc_num_nodes, calc_tasks

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


class _NodeTypes(enum.Enum):
    """Defines for a partition the acceptable node requests.

    - SHARED: Only support partial nodes requests to full node. Note that you
        can restrict the cores provided in `_PartitionConfig` to restrict below
        even full nodes.
    - MIXED: No restriction on partial to full node requests.
    - WHOLENODE: Only support submissions in units of whole nodes.
    """

    SHARED = 1
    MIXED = 2
    WHOLENODE = 3


class _PartitionConfig:
    """The configuration of partition for a given environment.

    Currently supports the ideas of
    - CPUs for a partition
    - GPUs for a partition
    - Node type of a partition

    When querying a value for a specific partition, the logic first searches the
    provided mapping, if any, for the partition. If it is not found, then the
    mapping is searched for "default" if it exists. If not, the class default is
    used. The list below shows the search order hierarchically.

    1. Partition specific
    2. Provided default
    3. _PartitionConfig default

    The class defaults are

    - CPUs: ``None`` which represents any number. This will be interpretted as
      an unlimited supply of CPUs practically.
    - GPUs: 0.
    - Node type: `_NodeTypes.MIXED`.

    Parameters
    ----------
    cpus_per_node: dict[str, int], optional
        Mapping from partition names to CPUs per node. Defaults to an empty
        `dict`.
    gpus_per_node: dict[str, int], optional
        Mapping from partition names to GPUs per node. Defaults to an empty
        `dict`.
    node_types: dict[str, _NodeTypes], optional
        Mapping from partitions to node types. Defaults to an empty `dict`.
    """

    _default_cpus_per_node = None
    _default_gpus_per_node = 0
    _default_node_type = _NodeTypes.MIXED

    def __init__(self, cpus_per_node=None, gpus_per_node=None, node_types=None):
        self.cpus_per_node = {} if cpus_per_node is None else cpus_per_node
        self.gpus_per_node = {} if gpus_per_node is None else gpus_per_node
        self.node_types = {} if node_types is None else node_types

    def __getitem__(self, partition):
        """Get the `_Partition` object for the provided partition."""
        return _Partition(
            partition,
            self._get(partition, self.cpus_per_node, self._default_cpus_per_node),
            self._get(partition, self.gpus_per_node, self._default_gpus_per_node),
            self._get(partition, self.node_types, self._default_node_type),
        )

    @staticmethod
    def _get(key, mapping, default):
        """Get the value of key following the class priority chain."""
        return mapping.get(key, mapping.get("default", default))


class _Partition:
    """Represents a partition and associated data.

    Parameters
    ----------
    name: str
        The name of the partition.
    cpus: int
        The CPUs per node.
    gpus: int
        The GPUs per node.
    node_type: _NodeTypes
        The node type for the partition.

    Attributes
    ----------
    name: str
        The name of the partition.
    cpus: int
        The CPUs per node.
    gpus: int
        The GPUs per node.
    node_type: _NodeTypes
        The node type for the partition.
    """

    def __init__(self, name, cpus, gpus, node_type):
        # Use empty string for error messages.
        self.name = name if name is not None else ""
        self.gpus = gpus
        self.cpus = cpus
        self.node_type = node_type

    def calculate_num_nodes(self, cpu_tasks, gpu_tasks, force):
        """Compute the number of nodes for the given workload.

        Parameters
        ----------
        cpu_tasks: int
            Total CPU tasks/cores.
        gpu_tasks: int
            Total GPUs requested.
        force: bool
            Whether to allow seemingly nonsensical/erronous resource requests.

        Raises
        ------
        SubmitError:
            Raises a SubmitError for
            1. non-zero GPUs on non-GPU partitions
            2. zero GPUs on GPU partitions.
            3. Requests larger than a node on `_NodeTypes.SHARED` partitions (through
               `~._nodes_for_task`).
            4. Requests less than 0.9 of the last node for `_NodeTypes.WHOLENODE`
               partitions (through `calc_num_nodes`)
            if ``not force``.
        """
        threshold = 0.9 if self.node_type == _NodeTypes.WHOLENODE and not force else 0.0
        if gpu_tasks > 0:
            if self.gpus == 0:
                # Required for current tests. Also skips a divide by zero error
                # if user actually wants to submit CPU only jobs to GPU partitions.
                if force:
                    num_nodes_gpu = 1
                else:
                    raise SubmitError(
                        f"Cannot request GPU's on nonGPU partition, {self.name}."
                    )
            else:
                num_nodes_gpu = self._nodes_for_task(gpu_tasks, self.gpus, threshold)
            num_nodes_cpu = self._nodes_for_task(cpu_tasks, self.cpus, 0)
        else:
            if self.gpus > 0 and not force:
                raise SubmitError(
                    f"Cannot submit to GPU partition, {self.name}, without GPUs."
                )
            num_nodes_gpu = 0
            num_nodes_cpu = self._nodes_for_task(cpu_tasks, self.cpus, threshold)
        return max(num_nodes_cpu, num_nodes_gpu, 1)

    def _nodes_for_task(self, tasks, processors, threshold):
        """Call calc_num_nodes but handles the None sentinal value."""
        if processors is None:
            return 1
        nodes = calc_num_nodes(tasks, processors, threshold)
        if self.node_type == _NodeTypes.SHARED and nodes > 1:
            raise SubmitError(
                f"Cannot submit {tasks} tasks to shared partition {self.name}"
            )
        return nodes


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

    _partition_config = _PartitionConfig()

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

    @classmethod
    def _get_scheduler_values(cls, context):
        """Return a dictionary of computed quantities regarding submission.

        Warning
        -------
            Must be called after the rest of the template context has been gathered.
        """
        partition = cls._partition_config[context.get("partition", None)]
        force = context.get("force", False)
        cpu_tasks_total = calc_tasks(
            context["operations"],
            "np",
            context.get("parallel", False),
            context.get("force", False),
        )
        gpu_tasks_total = calc_tasks(
            context["operations"],
            "ngpu",
            context.get("parallel", False),
            context.get("force", False),
        )

        num_nodes = partition.calculate_num_nodes(
            cpu_tasks_total, gpu_tasks_total, force
        )

        return {
            "ncpu_tasks": cpu_tasks_total,
            "ngpu_tasks": gpu_tasks_total,
            "num_nodes": num_nodes,
        }


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

    Note
    ----
    Environments can be set to raise FutureWarnings by setting a class attribute
    ``_deprecated`` to ``True``.

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
    env = _get_environment(test)
    if getattr(env, "_deprecated", False):
        _deprecated_warning(
            deprecation=str(env),
            alternative="",
            deprecated_in="v0.27.0",
            removed_in="v0.28.0",
        )
    return env


def _get_environment(test=False):
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
