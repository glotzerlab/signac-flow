from collections.abc import MutableMapping
import operator
import sys


class _DirectivesItem:
    """The representation of a single directive.

    Logic for validation of values when setting, defaults, and the ability for
    directives to inspect other directives (such as using nranks and
    omp_num_threads for finding np). This is only meant to work with the
    Directives class itself.

    While validation is used to ensure proper setting of directives, through
    ``finalize`` this could be abused. Also, since directive values can be
    dependent on jobs we allow all directives to be set to a callable.
    Directives set to callable cannot be validated.

    Parameters
    ----------
    name: str
        The name of the directive
    validation : callable, optional
        A callable that accepts inputs and attempts to convert the input to a
        valid value for the directive. If it fails, it should raise an
        appropriate error. When not provided, the validation function just
        returns the passed value.
    default : any, optional
        Sets the default for the directive, defaults to `None`.
    serial : callable, optional
        A function that takes two inputs for the directive and returns the
        appropriate value for these operations running in serial. Defaults to
        the maximum of the two.
    parallel : callable, optional
        A function that takes two inputs for the directive and returns the
        appropriate value for these operations running in parallel. Defaults to
        the sum of the two.
    finalize : callable, optional:
        A function that takes the current value of the directive and the
        directives object it is a child of and outputs the finalized value for
        that directive. This is useful if some directives have multiple ways to
        be set or are dependent in some way on other directives. The default is
        to just return the current set value.
    """
    def __init__(self, name, *, validation=None, default=None,
                 serial=max, parallel=operator.add, finalize=None):
        self.name = name
        self.default = default
        self.serial = serial
        self.parallel = parallel

        def identity(v):
            return v

        def default_finalize(v, directives):
            return v

        self.validation = identity if validation is None else validation
        self.finalize = default_finalize if finalize is None else finalize

    def __call__(self, value):
        if callable(value):
            return value
        else:
            return self.validation(value)


class Directives(MutableMapping):
    """Class that maps environment and user specified objects for execution.

    Resource request and execution style directives are the primary purpose for
    this class. Examples include the number of processes expected to run or the
    time to request to run.

    Parameters
    ----------
    available_directives_list : Sequence[_DirectivesItem]
        The sequence of all environment-specified directives. All other
        directives are user-specified and not validated. There is no way to
        include environment directives not specified at initialization without
        using the object's private API.
    """
    def __init__(self, available_directives_list):
        self._directive_definitions = dict()
        self._defined_directives = dict()
        self._user_directives = dict()

        for directive in available_directives_list:
            self._add_directive(directive)

    def _add_directive(self, directive):
        if not isinstance(directive, _DirectivesItem):
            raise TypeError("Expected a _DirectivesItem object. Received {}".format(
                type(directive)))
        elif directive.name in self._directive_definitions:
            raise ValueError("Cannot define {} twice.".format(directive.name))
        else:
            self._directive_definitions[directive.name] = directive
            self._defined_directives[directive.name] = directive.default

    def _set_defined_directive(self, key, value):
        try:
            self._defined_directives[key] = self._directive_definitions[key](value)
        except Exception as err:
            raise err.__class__('Error setting directive "{}": {}'.format(key, str(err)))

    def __getitem__(self, key):
        try:
            value = self._defined_directives[key]
            return self._directive_definitions[key].finalize(value, self)
        except KeyError:
            try:
                return self._user_directives[key]
            except KeyError:
                raise KeyError("{} not in directives.".format(key))

    def __setitem__(self, key, value):
        if key in self._directive_definitions:
            self._set_defined_directive(key, value)
        else:
            self._user_directives[key] = value

    def __delitem__(self, key):
        if key in self._directive_definitions:
            self._defined_directives[key] = self._directive_definitions[key].default
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
        return "Directives({})".format(str(self))

    def update(self, other, aggregate=False, job=None, parallel=False):
        if aggregate:
            self._aggregate(other, job=job, parallel=parallel)
        else:
            super().update(other)

    def evaluate(self, job):
        for key, value in self.items():
            self[key] = _evaluate(value, job)

    def _aggregate(self, other, job=None, parallel=False):
        self.evaluate(job)
        other.evaluate(job)
        agg_func_attr = "parallel" if parallel else "serial"
        for name in self._defined_directives:
            agg_func = getattr(self._directive_definitions[name], agg_func_attr)
            default_value = self._directive_definitions[name].default
            other_directive = other.get(name, default_value)
            directive = self[name]
            if other_directive is None:
                continue
            else:
                self._defined_directives[name] = agg_func(directive,
                                                          other_directive)


def _evaluate(value, job=None):
    if callable(value):
        if job is None:
            raise RuntimeError("job must be specified when evaluating a callable directive.")
        else:
            return value(job)
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
                raise TypeError("Expected an object of type {}. Received {} "
                                "of type {}".format(self.type, v, type(v)))


def _raise_below(value):
    def is_greater(v):
        try:
            if v < value:
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError("Expected a number greater than {}. Received {}"
                             "".format(value, v))
        return v
    return is_greater


_NP_DEFAULT = 1


def _finalize_np(np, directives):
    """Return the actual number of processes/threads to use.

    We check the default np because when aggregation occurs we multiple the
    number of MPI ranks and OMP_NUM_THREADS. If we always took the greater of
    the given NP and ranks * threads then after aggregating we will inflate the
    number of processors needed as (r1 * t1) + (r2 * t2) <= (r1 * r2 * t1 * t2)
    for numbers greater than one.
    """
    if callable(np) or np != _NP_DEFAULT:
        return np
    nranks = directives.get('nranks', 1)
    omp_num_threads = directives.get('omp_num_threads', 1)
    if callable(nranks) or callable(omp_num_threads):
        return np
    else:
        return max(np, max(1, nranks) * max(1, omp_num_threads))


_natural_number = _OnlyType(int, postprocess=_raise_below(1))
# Common directives and their instantiation as _DirectivesItem
NP = _DirectivesItem('np', validation=_natural_number,
                     default=_NP_DEFAULT, finalize=_finalize_np)
NP.__doc__ = """
The number of tasks expected to run for a given operation

Expects a natural number (i.e. an integer >= 1). When querying
this directive, introspection into the Directives is used to determine if the
product of "nranks" or "omp_num_threads" is greater than the current value. The
maximum of these two values is used.
"""

_nonnegative_int = _OnlyType(int, postprocess=_raise_below(0))
NGPU = _DirectivesItem('ngpu', validation=_nonnegative_int, default=0)
NGPU.__doc__ = """
The number of GPUs to use for this operation.

Expects a nonnegative integer.
"""
NRANKS = _DirectivesItem('nranks', validation=_nonnegative_int, default=0)
NRANKS.__doc__ = """
The number of MPI ranks to use for this operation.

Expects a nonnegative integer.
"""

OMP_NUM_THREADS = _DirectivesItem(
    'omp_num_threads', validation=_nonnegative_int, default=0)
OMP_NUM_THREADS.__doc__ = """
The number of OpenMP threads to use for this operation.

Expects a nonnegative integer.
"""


def _no_aggregation(v, o):
    return v


EXECUTABLE = _DirectivesItem('executable', validation=_OnlyType(str), default=sys.executable,
                             serial=_no_aggregation, parallel=_no_aggregation)
EXECUTABLE.__doc__ = """
The path to the executable to be used for this operation.

Expects a string pointing to a valid executable file in the
current file system.

By default this should point to a Python executable (interpreter); however, if
the :py:class:`FlowProject` path is an empty string, the executable can be a
path to an executable Python script.
"""


_nonnegative_real = _OnlyType(float, postprocess=_raise_below(0))
WALLTIME = _DirectivesItem('walltime', validation=_nonnegative_real, default=12.,
                           serial=operator.add, parallel=max)
WALLTIME.__doc__ = """
The number of hours to request for executing this job.

This directive expects a float representing the walltime in hours. Fractional
values are supported. For example, a value of 0.5 will request 30 minutes of
walltime.
"""

_positive_real = _OnlyType(float, postprocess=_raise_below(1e-12))
MEMORY = _DirectivesItem('memory', validation=_positive_real, default=4)
MEMORY.__doc__ = """
The number of gigabytes of memory to request for this operation.

Expects a real number greater than zero.
"""


def _is_fraction(value):
    if 0 <= value <= 1:
        return value
    else:
        raise ValueError("Value must be between 0 and 1.")


PROCESS_FRACTION = _DirectivesItem('processor_fraction',
                                   validation=_OnlyType(float, postprocess=_is_fraction),
                                   default=1., serial=_no_aggregation, parallel=_no_aggregation)
PROCESS_FRACTION.__doc__ = """
Needs to be filled out.
"""
