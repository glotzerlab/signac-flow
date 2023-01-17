# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Provide jinja2 template environment filter functions."""
import datetime
import sys
from functools import partial
from math import ceil

from ..errors import ConfigKeyError, SubmitError
from .config import get_config_value


def identical(iterable):
    """Check that all elements of an iterable are identical."""
    return len(set(iterable)) <= 1


def format_timedelta(delta, style="HH:MM:SS"):
    """Format a time delta for interpretation by schedulers.

    Some schedulers require days to be converted to hours.
    """
    if not isinstance(delta, datetime.timedelta):
        delta = datetime.timedelta(hours=delta)

    total_hours, remainder = divmod(int(delta.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    if style == "HH:MM:SS":
        return f"{total_hours:0>2}:{minutes:0>2}:{seconds:0>2}"
    elif style == "HH:MM":
        return f"{total_hours:0>2}:{minutes:0>2}"
    else:
        raise ValueError("Unsupported style in format_timedelta.")


def homogeneous_openmp_mpi_config(operations):
    """Check whether operations have identical OpenMP and MPI specification."""
    return (
        len(
            {
                (op.directives.get("nranks"), op.directives.get("omp_num_threads"))
                for op in operations
            }
        )
        == 1
    )


def with_np_offset(operations):
    """Add the np_offset variable to the operations' directives."""
    offset = 0
    for operation in operations:
        operation.directives.setdefault("np_offset", offset)
        offset += operation.directives["np"]
    return operations


def calc_tasks(operations, name, parallel=False, allow_mixed=False):
    """Compute the number of tasks required for the given set of operations.

    Calculates the number of tasks for a specific processing unit requested in
    the operations' directive, e.g., 'np' or 'ngpu'.

    Parameters
    ----------
    operations : :class:`~._JobOperation`
        The operations used to calculate the total number of required tasks.
    name : str
        The name of the processing unit to calculate the tasks for, e.g., 'np'
        or 'ngpu'.
    parallel : bool
        If True, operations are assumed to be executed in parallel, which means
        that the number of total tasks is the sum of all tasks instead of the
        maximum number of tasks. (Default value = False)
    allow_mixed : bool
        By default, the number of requested processing units must be identical
        for all operations. Unless the argument to this parameter is False, a
        RuntimeError will be raised if there are mixed requirements.

    Returns
    -------
    int
        The number of total tasks required for the specified processing unit.

    Raises
    ------
    RuntimeError
        Raises a RuntimeError if the required processing units across
        operations is not identical, unless the ``allow_mixed`` parameter is
        set to True.

    """
    processing_units = [
        op.directives[name] * op.directives.get("processor_fraction", 1)
        for op in operations
    ]
    if identical(processing_units) or allow_mixed:
        if len(processing_units) > 0:
            sum_processing_units = round(sum(processing_units))
            max_processing_units = round(max(processing_units))
            return sum_processing_units if parallel else max_processing_units
        else:
            return 0  # empty set
    else:
        raise RuntimeError(
            "Mixed processing units requested warning:\n"
            "The number of required processing units ({}) differs between different operations.\n"
            "Use --force to ignore the warning, but users are encouraged to use --pretend to "
            "confirm that the submission script allocates processing units for different "
            "operations properly before force submission".format(name)
        )


def format_memory(memory):
    """Format memory request as a string with an appropriate suffix.

    Parameters
    ----------
    memory : float
        Amount of memory in gigabytes.

    Returns
    -------
    str
        String value with suffix "G" or "M".
    """
    memory = float(memory)
    if memory.is_integer():
        return f"{int(memory)}G"
    else:
        mem_in_mb = ceil(memory * 1024)
        return f"{mem_in_mb}M"


def calc_memory(operations, parallel=False):
    r"""Calculate the maximum memory to reserve for submission of operations.

    Parameters
    ----------
    operations : list
        A list of :class:`~._JobOperation`\ s used to calculate the maximum
        memory required.
    parallel : bool
        If True, operations are assumed to be executed in parallel, which
        means that the total memory requested will be the sum of all memory
        requested instead of the maximum memory requested (Default value =
        False).

    Returns
    -------
    float
        The reserved memory (numeric value) in gigabytes.
    """
    func = sum if parallel else max
    return func(operation.directives["memory"] or 0 for operation in operations)


def calc_walltime(operations, parallel=False):
    r"""Calculate the total walltime to reserve for submission of operations.

    Parameters
    ----------
    operations : list
        A list of :class:`~._JobOperation`\ s used to calculate the total
        walltime required.
    parallel : bool
        If True, operations are assumed to be executed in parallel, which
        means that the total walltime requested will be the maximum requested
        walltime instead of the sum of requested walltimes (Default value =
        False).

    Returns
    -------
    :class:`datetime.timedelta`
        The total walltime.
    """
    # Replace the sum function with partial(sum, start=datetime.timedelta())
    # when dropping Python 3.7 support.
    func = (
        max
        if parallel
        else partial(lambda start, iterable: sum(iterable, start), datetime.timedelta())
    )
    return func(
        operation.directives["walltime"] or datetime.timedelta()
        for operation in operations
    )


def check_utilization(nn, np, ppn, threshold=0.9, name=None):
    """Check whether the calculated node utilization is below threshold.

    This function raises a :class:`RuntimeError` if the calculated
    node utilization is below the given threshold or if the number
    of calculated required nodes is zero.

    Parameters
    ----------
    nn : int
        Number of requested nodes.
    np : int
        Number of required processing units (e.g. CPUs, GPUs).
    ppn : int
        Number of processing units available per node.
    threshold : float
        The minimum required node utilization. (Default value = 0.9)
    name : str
        A human-friendly name for the tested processing unit to be used in the
        error message, for example: CPU or GPU. (Default value = None)

    Returns
    -------
    int
        The number of calculated nodes.

    Raises
    ------
    RuntimeError
        Raised if the node utilization is below the given threshold.

    """
    if not (0 <= threshold <= 1.0):
        raise ValueError("The value for 'threshold' must be between 0 and 1.")

    # Zero nodes are just returned and possible utilization or validation checks
    # must be performed elsewhere.
    if nn == 0:
        return 0

    # The utilization is the number of processing units (np) required divided
    # by the product of the number of nodes (nn) and the number of processing
    # units per node (ppn).
    utilization = np / (nn * ppn)

    # Raise RuntimeError if the utilization is below the specified threshold.
    if utilization < threshold:
        raise RuntimeError(
            "Low{name} utilization warning: {util:.0%}\n"
            "Total resources requested would require {nn} node(s), "
            "but each node supports up to {ppn}{name} task(s).\n"
            "Requesting {np} total{name} task(s) would result in node underutilization. "
            "Use --force to ignore the warning, but users are encouraged to use --pretend to "
            "confirm that the submission script fully utilizes the compute resources before "
            "force submission".format(
                util=utilization, np=np, nn=nn, ppn=ppn, name=f" {name}" if name else ""
            )
        )

    # Everything fine, return number of nodes (nn).
    return nn


def calc_num_nodes(np, ppn=1, threshold=0, name=None):
    """Calculate the number of required nodes with optional utilization check.

    Parameters
    ----------
    np : int
        Number of required processing units (e.g. CPUs, GPUs).
    ppn : int
        Number of processing units available per node. (Default value = 1)
    threshold : float
        The required node utilization. The default is 0, which means no check.
    name : str
        A human-friendly name for the tested processing unit to be
        used in the error message in case of underutilization.  For example:
        CPU or GPU. (Default value = None)

    Returns
    -------
    int
        The number of required nodes.

    Raises
    ------
    RuntimeError
        If the calculated node utilization is below the given threshold.

    """
    nn = int(ceil(np / ppn))
    return check_utilization(nn, np, ppn, threshold, name)


def print_warning(msg):
    """Print warning message within jinja2 template.

    Parameters
    ----------
    msg : str
        Warning to print.

    Returns
    -------
    str
        Empty string (to render nothing in the jinja template).

    """
    import logging

    logger = logging.getLogger(__name__)
    logger.warning(msg)
    return ""


_GET_ACCOUNT_NAME_MESSAGES_SHOWN = set()


def get_account_name(project, required=False):
    """Get account name for environment with user-friendly messages on failure.

    Parameters
    ----------
    project : flow.FlowProject
        The project whose config is queried.
    required : bool
        Specify whether the account name is required instead of optional.
        (Default value = False)

    Returns
    -------
    str
        The account name for the given environment or None if missing and not required.

    Raises
    ------
    :class:`~flow.errors.SubmitError`
        Raised if ``required`` is True and the account name is missing.

    """
    env_name = project._environment.__name__
    try:
        return get_config_value(project, "account", ns=env_name)
    except ConfigKeyError as error:
        ACCOUNT_MESSAGE = (
            "Environment '{env}' {requires_or_allows} the specification of an "
            "account name that will be charged for jobs' compute time.\n"
            "Set the account name with the command:\n\n"
            "  $ signac config --global set {key} ACCOUNT_NAME\n"
        )
        if required:
            raise SubmitError(
                ACCOUNT_MESSAGE.format(
                    requires_or_allows="requires",
                    env=env_name,
                    key=str(error),
                )
            )
        elif env_name not in _GET_ACCOUNT_NAME_MESSAGES_SHOWN:
            print(
                ACCOUNT_MESSAGE.format(
                    requires_or_allows="allows",
                    env=env_name,
                    key=str(error),
                ),
                file=sys.stderr,
            )
            _GET_ACCOUNT_NAME_MESSAGES_SHOWN.add(env_name)
