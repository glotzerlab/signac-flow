from collections.abc import MutableMapping
import sys


class _DirectivesItem:
    """The representation of a single directive.

    Logic for validation of values when setting, defaults, and the ability for
    directives to interspect before returning a directive are supported. This is
    only meant to work with the Directives class itself.

    While validation is used to ensure proper setting of directives, through
    ``finalize`` this could be abused. Also any callable can be set to any
    directive regardless of the function taking a job as an argument and
    returning a valid directive.

    Parameters
    ----------
    name: str
        The name of the directive
    validation : (Callable[[T], S], optional)
        A callable that accepts inputs and attempts to convert the input to a
        valid value for the directive. If it fails, it should raise an
        appropriate error. When not provided, the validation function just
        returns the passed value.
    default
        Sets the default for the directive.
    serial : (Callable[[S, S], S], optional)
        A function that takes two inputs for the directive and returns the
        appropriate value for these operations running in serial. Defaults to
        the maximum of the two.
    parallel : (Callable[[S, S], S], optional)
        A function that takes two inputs for the directive and returns the
        appropriate value for these operations running in parallel. Defaults to
        the sum of the two.
    finalize : (Callable[[S, Directives], S], optional):
        A function that takes the current value of the directive and the
        directives object it is a child of and outputs the finalized value for
        that directive. This is useful if some directives have multiple ways to
        be set or are dependent in some way on other directives. The default is
        to just return the current set value.
    """
    def __init__(self, name, validation=None, default=None,
                 serial=max, parallel=lambda x, y: x + y, finalize=None):
        self.name = name
        self.default = default
        self.serial = serial
        self.parallel = parallel

        def identity(v):
            return v

        def dft_finalize(v, directives):
            return v

        self.validation = identity if validation is None else validation
        self.finalize = dft_finalize if finalize is None else finalize

    def __call__(self, value):
        if callable(value):
            return value
        else:
            return self.validation(value)

    def _validate(self, value):
        if not isinstance(value, self.type):
            try:
                return self.type(value)
            except Exception:
                raise TypeError("Expected something of type {0} or "
                                "convertable to {0}. Received {1} of type {2}"
                                "".format(self.type, value, type(value)))
        else:
            return value


class Directives(MutableMapping):
    """Class that maps environment and user specified objects for execution.

    Resource request and execution style directives are the primary purpose for
    this class. Examples include the number of processes expected to run or the
    time to request to run.

    Parameters
    ----------
    available_directives_list : Sequence[_DirectivesItem]
        The sequence of all environment specified directives. All other
        directives are user specified and not validated. There is no way to
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
            dft = self._directive_definitions[name].default
            other_directive = other.get(name, dft)
            directive = self[name]
            if other_directive is None:
                continue
            else:
                self._defined_directives[name] = agg_func(directive,
                                                          other_directive)


def raise_if_too_small(value):
    def is_greater(v):
        try:
            if v < value:
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError("Expected a number greater than {}. Received {}"
                             "".format(value, v))
        return v


def _evaluate(value, job=None):
    if callable(value):
        if job is None:
            raise RuntimeError("job not specified when evaluating directive.")
        else:
            return value(job)
    else:
        return value


def finalize_np(value, directives):
    nranks = directives.get('nranks', 1)
    omp_num_threads = directives.get('omp_num_threads', 1)
    if callable(nranks) or callable(omp_num_threads):
        return value
    else:
        return max(value, max(1, nranks) * max(1, omp_num_threads))


class OnlyType:
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
                raise TypeError("Excepted an object of type {}. Received {} "
                                "of type {}".format(self.type, v, type(v)))


nonnegative_int = OnlyType(int, postprocess=raise_if_too_small(-1))
natural_number = OnlyType(int, postprocess=raise_if_too_small(0))
nonnegative_real = OnlyType(float, postprocess=raise_if_too_small(0))


def is_fraction(value):
    if 0 <= value <= 1:
        return value
    else:
        raise ValueError("Value must be beween 0 and 1.")


def no_aggregation(v, o):
    return v


# Common directives and their instantiation as _DirectivesItem
NP = _DirectivesItem('np', natural_number, 1, finalize=finalize_np)
NP.__doc__ = """
The number of tasks expected to run for a given operation

Expects a natural number (i.e. an integer >= 1). When querying
this directive, introspection into the Directives is used to determine if the
product of "nranks" or "omp_num_threads" is greater than the current value. The
maximum of these two values is used.
"""

NGPU = _DirectivesItem('ngpu', nonnegative_int, 0)
NGPU.__doc__ = """
The number of GPU's to request for this operation.

Expects a nonnegative integer.
"""
NRANKS = _DirectivesItem('nranks', nonnegative_int, 0)
NRANKS.__doc__ = """
The number of MPI ranks to use for this operation.

Expects a nonnegative integer.
"""

OMP_NUM_THREADS = _DirectivesItem('omp_num_threads', nonnegative_int, 0)
OMP_NUM_THREADS.__doc__ = """
The number of OpenMP threads to use for this operation.

Expects a nonnegative integer.
"""

EXECUTABLE = _DirectivesItem('executable', OnlyType(str), sys.executable,
                             no_aggregation, no_aggregation)
EXECUTABLE.__doc__ = """
The location in the file system to the executable to be used for this operation.

Expects a string pointing to a valid executable file in the
current file system.

By default this should point to a Python executable (interpreter); however, if
the :py:class:`FlowProject` path is set to be empty, the executable can be the
file path to an executable Python script.
"""

WALLTIME = _DirectivesItem('walltime', nonnegative_real, 12.,
                           lambda x, y: x + y, max)
WALLTIME.__doc__ = """
The number of hours to request for executing this job.

This directive expects a float representing the number hours including
fraction's of hours.
"""

MEMORY = _DirectivesItem('memory', natural_number, 4)
MEMORY.__doc__ = """
The number of GB to request for an operation.

Expects a natural number (i.e. an integer >= 1).
"""

PROCESS_FRACTION = _DirectivesItem('processor_fraction',
                                   OnlyType(float, postprocess=is_fraction),
                                   1., no_aggregation, no_aggregation)
PROCESS_FRACTION.__doc__ = """
Needs to be filled out.
"""
