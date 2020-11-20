import functools
import operator
import sys
from collections.abc import MutableMapping

from flow.errors import DirectivesError


class _Directive:
    """The definition of a single directive.

    Logic for validation of values when setting, defaults, and the ability for
    directives to inspect other directives (such as using nranks and
    omp_num_threads for finding np). This is only meant to work with the
    internals of signac-flow.

    The validation of a directive occurs before the call to ``finalize``. It is
    the caller's responsibility to ensure that finalized values are still valid.

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
    serial : callable, optional
        A callable that takes two inputs for the directive and returns the
        appropriate value for these operations running in serial. If ``None`` or
        not provided, the ``max`` function is used. Defaults to ``None``.
    parallel : callable, optional
        A callable that takes two inputs for the directive and returns the
        appropriate value for these operations running in parallel. If ``None``
        or not provided, the ``operator.add`` function is used. Defaults to
        ``None``.  Defaults to ``None``.
    finalize : callable, optional
        A callable that takes the set value of the directive and the
        :class:`~._Directives` object it is a child of and outputs the finalized
        value for that directive. This is useful if some directives have
        multiple ways to be set or are dependent in some way on other
        directives. If ``None`` or not provided, the set value is returned.
        Defaults to ``None``.
    """

    def __init__(
        self,
        name,
        *,
        validator=None,
        default=None,
        serial=max,
        parallel=operator.add,
        finalize=None,
    ):
        self._name = name
        self._default = default
        self._serial = serial
        self._parallel = parallel

        def identity(v):
            return v

        def default_finalize(v, directives):
            return v

        self._validator = identity if validator is None else validator
        self._finalize = default_finalize if finalize is None else finalize

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
        else:
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
        self._directive_definitions = dict()
        self._defined_directives = dict()
        self._user_directives = dict()

        for directive in environment_directives:
            self._add_directive(directive)

    def _add_directive(self, directive):
        if not isinstance(directive, _Directive):
            raise TypeError(
                f"Expected a _Directive object. Received {type(directive)}."
            )
        elif directive._name in self._directive_definitions:
            raise ValueError(f"Cannot redefine directive name {directive._name}.")
        else:
            self._directive_definitions[directive._name] = directive
            self._defined_directives[directive._name] = directive._default

    def _set_defined_directive(self, key, value):
        try:
            self._defined_directives[key] = self._directive_definitions[key](value)
        except (KeyError, ValueError, TypeError) as err:
            raise DirectivesError(f"Error setting directive {key}") from err

    def __getitem__(self, key):
        if key in self._defined_directives and key in self._directive_definitions:
            value = self._defined_directives[key]
            return self._directive_definitions[key]._finalize(value, self)
        elif key in self._user_directives:
            return self._user_directives[key]
        else:
            raise KeyError(f"{key} not in directives.")

    def __setitem__(self, key, value):
        if key in self._directive_definitions:
            self._set_defined_directive(key, value)
        else:
            self._user_directives[key] = value

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

    def update(self, other, aggregate=False, jobs=None, parallel=False):
        if aggregate:
            self._aggregate(other, jobs=jobs, parallel=parallel)
        else:
            super().update(other)

    def evaluate(self, jobs):
        for key, value in self.items():
            self[key] = _evaluate(value, jobs)

    def _aggregate(self, other, jobs=None, parallel=False):
        self.evaluate(jobs)
        other.evaluate(jobs)
        agg_func_attr = "_parallel" if parallel else "_serial"
        for name in self._defined_directives:
            agg_func = getattr(self._directive_definitions[name], agg_func_attr)
            default_value = self._directive_definitions[name]._default
            other_directive = other.get(name, default_value)
            directive = self[name]
            if other_directive is not None:
                self._defined_directives[name] = agg_func(directive, other_directive)


def _evaluate(value, jobs):
    if callable(value):
        if jobs is None:
            raise RuntimeError(
                "jobs must be specified when evaluating a callable directive."
            )
        else:
            return value(*jobs)
    else:
        return value


class _OnlyType:
    def __init__(self, type_, preprocess=None, postprocess=None):
        def identity(v):
            return v

        self.type = type_
        self.preprocess = identity if preprocess is None else preprocess
        self.postprocess = identity if postprocess is None else postprocess

    def __call__(self, v):
        return self.postprocess(self._validate(self.preprocess(v)))

    def _validate(self, v):
        if isinstance(v, self.type):
            return v
        else:
            try:
                return self.type(v)
            except Exception:
                raise TypeError(
                    f"Expected an object of type {self.type}. "
                    f"Received {v} of type {type(v)}."
                )


def _raise_below(value):
    def is_greater_or_equal(v):
        try:
            if v < value:
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError(
                f"Expected a number greater than or equal to {value}. Received {v}."
            )
        return v

    return is_greater_or_equal


_NP_DEFAULT = 1


def _finalize_np(np, directives):
    """Return the actual number of processes/threads to use.

    We check the default np because when aggregation occurs we multiply the
    number of MPI ranks and OMP_NUM_THREADS. If we always took the greater of
    the given NP and ranks * threads then after aggregating we will inflate the
    number of processors needed as (r1 * t1) + (r2 * t2) <= (r1 + r2) * (t1 + t2)
    for numbers greater than one.
    """
    if callable(np) or np != _NP_DEFAULT:
        return np
    nranks = directives.get("nranks", 1)
    omp_num_threads = directives.get("omp_num_threads", 1)
    if callable(nranks) or callable(omp_num_threads):
        return np
    else:
        return max(np, max(1, nranks) * max(1, omp_num_threads))


# Helper validators for defining _Directive
def _no_aggregation(v, o):
    return v


def _is_fraction(value):
    if 0 <= value <= 1:
        return value
    else:
        raise ValueError("Value must be between 0 and 1.")


_natural_number = _OnlyType(int, postprocess=_raise_below(1))
_nonnegative_int = _OnlyType(int, postprocess=_raise_below(0))
_nonnegative_real = _OnlyType(float, postprocess=_raise_below(0))
_positive_real = _OnlyType(float, postprocess=_raise_below(1e-12))

# Common directives and their instantiation as _Directive
_NP = _Directive(
    "np", validator=_natural_number, default=_NP_DEFAULT, finalize=_finalize_np
)
"""The number of tasks to launch for a given operation i.e., the number of CPU
cores to be requested for a given operation.

Expects a natural number (i.e. an integer >= 1). This directive introspects into
the "nranks" or "omp_num_threads" directives and uses their product if it is
greater than the current set value. Defaults to 1.
"""

_NGPU = _Directive("ngpu", validator=_nonnegative_int, default=0)
"""The number of GPUs to use for this operation.

Expects a nonnegative integer. Defaults to 0.
"""

_NRANKS = _Directive("nranks", validator=_nonnegative_int, default=0)
"""The number of MPI ranks to use for this operation. Defaults to 0.

Expects a nonnegative integer.
"""

_OMP_NUM_THREADS = _Directive("omp_num_threads", validator=_nonnegative_int, default=0)
"""The number of OpenMP threads to use for this operation. Defaults to 0.

Expects a nonnegative integer.
"""

_EXECUTABLE = _Directive(
    "executable",
    validator=_OnlyType(str),
    default=sys.executable,
    serial=_no_aggregation,
    parallel=_no_aggregation,
)
"""The path to the executable to be used for this operation.

Expects a string pointing to a valid executable file in the
current file system.

By default this should point to a Python executable (interpreter); however, if
the :py:class:`FlowProject` path is an empty string, the executable can be a
path to an executable Python script. Defaults to ``sys.executable``.
"""

_WALLTIME = _Directive(
    "walltime",
    validator=_nonnegative_real,
    default=12.0,
    serial=operator.add,
    parallel=max,
)
"""The number of hours to request for executing this job.

This directive expects a float representing the walltime in hours. Fractional
values are supported. For example, a value of 0.5 will request 30 minutes of
walltime. Defaults to 12 hours.
"""

_MEMORY = _Directive("memory", validator=_positive_real, default=4)
"""The number of gigabytes of memory to request for this operation.

Expects a real number greater than zero.
"""

_PROCESSOR_FRACTION = _Directive(
    "processor_fraction",
    validator=_OnlyType(float, postprocess=_is_fraction),
    default=1.0,
    serial=_no_aggregation,
    parallel=_no_aggregation,
)
"""Fraction of a resource to use on a single operation.

If set to 0.5 for a bundled job with 20 operations (all with 'np' set to 1), 10
CPUs will be used. Defaults to 1.

.. note::

    This can be particularly useful on Stampede2's launcher.
"""
