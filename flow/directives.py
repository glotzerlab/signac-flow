# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Directives define how workflows are executed.

Directives affect both execution and submission, such as specifying the number
of processors required for an operation.
"""
import datetime
import functools
import sys
from collections.abc import MutableMapping

from flow.errors import DirectivesError


class _Directive:
    """The definition of a single directive.

    Logic for validation when setting and providing defaults. This is only meant to work
    with the internals of signac-flow.

    Since directive values can be dependent on jobs we allow all directives to
    be set to a callable which is lazily validated.

    Parameters
    ----------
    name : str
        The name of the directive.
    validator : callable, optional
        A callable that accepts inputs and attempts to convert the input to a
        valid value for the directive. If it fails, it should raise an
        appropriate error. If not provided or ``None``, the validator
        callable directly returns the passed value. Defaults to ``None``.
    default : any, optional
        Sets the default for the directive, defaults to ``None``.
    """

    def __init__(
        self,
        name,
        *,
        validator=None,
        default=None,
    ):
        self._name = name
        self._default = default

        def identity(value):
            return value

        self._validator = identity if validator is None else validator

    def __call__(self, value):
        """Return a validated value for the given directive.

        Parameters
        ----------
        value : any
            The value to be validated by the :class:`~._Directive`. Accepted
            values depend on the given instance. All `~._Directive`
            instances support lazily validating callables of single (or multiple) jobs
            that return an acceptable value for the given `~._Directive`
            instance.

        Returns
        -------
        any
            Returns a immediately validated value for the given directive, or if
            a callable was passed, a new callable is returned that wraps the
            given callable with a validator.

        """
        if callable(value):

            @functools.wraps(value)
            def validate_callable(*jobs):
                return self._validator(value(*jobs))

            return validate_callable
        return self._validator(value)


class _Directives(MutableMapping):
    """Class that maps environment and user specified objects for execution.

    Resource request and execution style directives are the primary purpose for
    this class. Examples include the number of processes expected to run or the
    time to request to run.

    Parameters
    ----------
    environment_directives : sequence of _Directive
        The sequence of all environment-specified directives. All other
        directives are user-specified and not validated. All environment
        directives must be specified at initialization.

    """

    def __init__(self, environment_directives):
        self._directive_definitions = {}
        self._defined_directives = {}
        self._user_directives = {}
        self._evaluated = False

        for directive in environment_directives:
            self._add_directive(directive)

    def _add_directive(self, directive):
        if not isinstance(directive, _Directive):
            raise TypeError(
                f"Expected a _Directive object. Received {type(directive)}."
            )
        if directive._name in self._directive_definitions:
            raise ValueError(f"Cannot redefine directive name {directive._name}.")
        self._directive_definitions[directive._name] = directive
        self._defined_directives[directive._name] = directive._default

    def _set_defined_directive(self, key, value):
        try:
            self._defined_directives[key] = self._directive_definitions[key](value)
        except (KeyError, ValueError, TypeError) as error:
            raise DirectivesError(f"Error setting directive {key}") from error

    def __getitem__(self, key):
        if key in self._defined_directives and key in self._directive_definitions:
            value = self._defined_directives[key]
            return value
        if key in self._user_directives:
            return self._user_directives[key]
        raise KeyError(f"{key} not in directives.")

    def __setitem__(self, key, value):
        if key in self._directive_definitions:
            self._set_defined_directive(key, value)
        else:
            self._user_directives[key] = value
        self._evaluated = False

    def __delitem__(self, key):
        if key in self._directive_definitions:
            self._defined_directives[key] = self._directive_definitions[key]._default
        else:
            del self._user_directives[key]

    def __iter__(self):
        yield from self._defined_directives
        yield from self._user_directives

    def __len__(self):
        return len(self._defined_directives) + len(self._user_directives)

    def __str__(self):
        return str(dict(self))

    def __repr__(self):
        return f"_Directives({str(self)})"

    def evaluate(self, jobs):
        """Evaluate directives for the provided jobs.

        This method updates the directives in place, replacing callable
        directives with their evaluated values.

        Parameters
        ----------
        jobs : :class:`signac.job.Job` or tuple of :class:`signac.job.Job`
            The jobs used to evaluate directives.

        """
        if not self._evaluated:
            for key, value in self.items():
                self[key] = _evaluate(value, jobs)
            self._evaluated = True
        return self

    @property
    def user_keys(self):  # noqa: D401
        """A generator of user specified keys."""
        return (key for key in self._user_directives)


def _evaluate(value, jobs):
    if callable(value):
        if jobs is None:
            raise RuntimeError(
                "jobs must be specified when evaluating a callable directive."
            )
        return value(*jobs)
    return value


class _OnlyTypes:
    def __init__(self, *types, preprocess=None, postprocess=None):
        def identity(value):
            return value

        self.types = types
        self.preprocess = identity if preprocess is None else preprocess
        self.postprocess = identity if postprocess is None else postprocess

    def __call__(self, value):
        return self.postprocess(self._validate(self.preprocess(value)))

    def _validate(self, value):
        if isinstance(value, self.types):
            return value
        for type_ in self.types:
            try:
                return type_(value)
            except Exception:
                pass
        raise TypeError(
            f"Expected an object convertible to one of the following types: {self.types}. "
            f"Received object {value} of type {type(value)}."
        )


def _raise_below(threshold, allow_none=False):
    def is_greater_or_equal(value):
        if allow_none and value is None:
            return value
        try:
            if value < threshold:
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError(
                f"Expected a number greater than or equal to {threshold}. Received {value}."
            )
        return value

    return is_greater_or_equal


def _is_fraction(value):
    if 0 <= value <= 1:
        return value
    raise ValueError("Value must be between 0 and 1.")


def _parse_walltime(walltime):
    """Parse walltime from a walltime directive passed by a user.

    A valid walltime argument is defined as:

    1. Numeric value indicating walltime requested in hours.
    2. :class:`datetime.timedelta` value.
    3. None to indicate no specific walltime request.

    Parameters
    ----------
    walltime : float or :class:`datetime.timedelta` or None
        Requested walltime.

    Returns
    -------
    :class:`datetime.timedelta` or None
        The parsed walltime value.
    """
    if walltime is None:
        return None
    if not isinstance(walltime, datetime.timedelta):
        walltime = datetime.timedelta(hours=walltime)
    return walltime


def _parse_memory(memory):
    """Parse memory value from a memory directive passed by a user.

    A valid memory argument is defined as:

    1. Numeric value with suffix "G" or "GB" indicating memory requested in gigabytes.
    2. Numeric value with suffix "M" or "MB" indicating memory requested in megabytes.
    3. Numeric value with no suffix indicating memory requested in gigabytes.
    4. None to indicate no specific memory request.

    Memory arguments passed as strings are case insensitive.

    Parameters
    ----------
    memory : str or float or None
        Requested memory.

    Returns
    -------
    float or None
        The parsed memory value in gigabytes.
    """
    try:
        if memory is None:
            return None
        # Convert to uppercase, remove "B" suffix if provided
        memory = str(memory).upper().rstrip("B")
        size_type = memory[-1]
        if size_type == "M":
            return float(memory[:-1]) / 1024
        elif size_type == "G":
            return float(memory[:-1])
        else:
            return float(memory)
    except ValueError:
        raise ValueError(
            'Invalid memory passed. For gigabytes use suffix "G" or "GB", '
            'for megabytes use suffix "M" or "MB". If a numeric value is passed then '
            "it will be interpreted as memory in gigabytes."
        )


# Definitions used for validating directives
_natural_number = _OnlyTypes(int, postprocess=_raise_below(1))
_nonnegative_int = _OnlyTypes(int, postprocess=_raise_below(0))
_positive_real_walltime = _OnlyTypes(
    float,
    datetime.timedelta,
    type(None),
    preprocess=_parse_walltime,
    postprocess=_raise_below(datetime.timedelta(seconds=1), True),
    # 1 second is an arbitrarily chosen minimum threshold
)
_positive_real_memory = _OnlyTypes(
    float,
    str,
    type(None),
    preprocess=_parse_memory,
    postprocess=_raise_below(1e-12, True),
    # 1e-12 is an arbitrarily chosen minimum threshold for what constitutes 0
)


# Common directives and their instantiation as _Directive
def _GET_EXECUTABLE():
    # Evaluate the executable directive at call-time instead of definition time.
    # This is because we mock `sys.executable` while generating template reference data.
    _EXECUTABLE = _Directive(
        "executable",
        validator=_OnlyTypes(str, type(None)),
        default=sys.executable,
    )
    _EXECUTABLE.__doc__ = """Return the path to the executable to be used for an operation.

The executable directive expects a string pointing to a valid executable
file in the current file system.

When called, by default this should point to a Python executable (interpreter);
however, if the :class:`FlowProject` path is an empty string, the executable
can be a path to an executable Python script. Defaults to ``sys.executable``.
"""
    return _EXECUTABLE


_LAUNCHER = _Directive("launcher", validator=_OnlyTypes(str, type(None)), default=None)
_LAUNCHER.__doc__ = """The launcher to use to execute this operation.

A launcher is defined as a separate program used to launch an application.
Primarily this is designed to specify whether or not MPI should be used to
launch the operation. Set to "mpi" for this case. Defaults to ``None``.

For example:

.. code-block:: python

    @Project.operation(directives={"launcher": "mpi"})
    def op(job):
        pass
"""


_MEMORY_PER_CPU = _Directive(
    "memory_per_cpu", validator=_positive_real_memory, default=None
)
_MEMORY_PER_CPU.__doc__ = """The memory to request per CPU for this operation.

The memory to validate should be either a float, int, or string.
A valid memory argument is defined as:

- Positive numeric value with suffix "g" or "G" indicating memory requested in gigabytes.

For example:

.. code-block:: python

    @Project.operation(directives={"memory": "4g"})
    def op(job):
        pass

- Positive numeric value with suffix "m" or "M" indicating memory requested in megabytes.

For example:

.. code-block:: python

    @Project.operation(directives={"memory": "512m"})
    def op(job):
        pass

- Positive numeric value with no suffix indicating memory requested in gigabytes.

For example:

.. code-block:: python

    @Project.operation(directives={"memory": "4"})
    def op1(job):
        pass

    @Project.operation(directives={"memory": 4})
    def op2(job):
        pass
"""

_GPUS_PER_PROCESS = _Directive(
    "gpus_per_process", validator=_nonnegative_int, default=0
)
_GPUS_PER_PROCESS.__doc__ = """The number of GPUs to use per process.

Expects a nonnegative integer. Defaults to 0.
"""

_PROCESSES = _Directive("processes", validator=_natural_number, default=1)
_PROCESSES.__doc__ = """The number of processes the operation plans on using.

Expects a natural number (i.e. an integer >= 1). Defualts to 1.
"""

_THREADS_PER_PROCESS = _Directive(
    "threads_per_process", validator=_nonnegative_int, default=0
)
_THREADS_PER_PROCESS.__doc__ = """The number of threads to use per process. Defaults to 0.

Using this directive sets the environmental variable ``OMP_NUM_THREADS`` in the operation's
execution environment.

Expects a nonnegative integer.
"""

_WALLTIME = _Directive("walltime", validator=_positive_real_walltime, default=None)
_WALLTIME.__doc__ = """The number of hours to request for executing this job.

This directive expects a float representing the walltime in hours. Fractional
values are supported. For example, a value of 0.5 will request 30 minutes of
walltime. If no walltimes are requested, the submission will not specify a
walltime in the output script. Some schedulers have a default value that will
be used.

For example:

.. code-block:: python

    @Project.operation(directives={"walltime": 24})
    def op(job):
        # This operation takes 1 day to run
        pass

"""


def _document_directive(directive):
    """Dynamically generates documentation for a directive."""
    name = directive._name
    name = name.replace("_", r"\_")
    doc = directive.__doc__
    return f"**{name}**\n\n{doc}"
