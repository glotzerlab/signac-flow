# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Miscellaneous utility functions."""
import argparse
import logging
import os
from collections.abc import MutableMapping
from contextlib import contextmanager
from functools import lru_cache, partial
from itertools import cycle, islice

import cloudpickle
from tqdm.contrib import tmap
from tqdm.contrib.concurrent import process_map, thread_map

try:
    # If ipywidgets is installed, use "auto" tqdm to improve notebook support.
    # Otherwise, use only text-based progress bars. This workaround can be
    # removed after https://github.com/tqdm/tqdm/pull/1218.
    import ipywidgets  # noqa: F401
except ImportError:
    from tqdm import tqdm
else:
    from tqdm.auto import tqdm


def _positive_int(value):
    """Parse a command line argument as a positive integer.

    Designed to be used in conjunction with :class:`argparse.ArgumentParser`.

    Parameters
    ----------
    value : str
        The value to parse.

    Returns
    -------
    int
        The provided value, cast to an integer.

    Raises
    ------
    :class:`argparse.ArgumentTypeError`
        If value cannot be cast to an integer or is negative.

    """
    try:
        ivalue = int(value)
        if ivalue <= 0:
            raise argparse.ArgumentTypeError("Value must be positive.")
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError(f"{value} must be a positive integer.")
    return ivalue


@contextmanager
def redirect_log(job, filename="run.log", formatter=None, logger=None):
    """Redirect all messages logged via the logging interface to the given file.

    This method is a context manager. The logging handler is removed when
    exiting the context.

    Parameters
    ----------
    job : :class:`signac.contrib.job.Job`
        The signac job whose workspace will store the redirected logs.
    filename : str
        File name of the log. (Default value = "run.log")
    formatter : :class:`logging.Formatter`
        The logging formatter to use, uses a default formatter if None.
        (Default value = None)
    logger : :class:`logging.Logger`
        The instance of logger to which the new file log handler is added.
        Defaults to the default logger returned by :meth:`logging.getLogger` if
        this argument is not provided.

    """
    if formatter is None:
        formatter = logging.Formatter(
            "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
        )
    if logger is None:
        logger = logging.getLogger()

    filehandler = logging.FileHandler(filename=job.fn("run.log"))
    filehandler.setFormatter(formatter)
    logger.addHandler(filehandler)
    try:
        yield
    finally:
        logger.removeHandler(filehandler)


@contextmanager
def _add_path_to_environment_pythonpath(path):
    """Insert the provided path into the environment PYTHONPATH variable.

    This method is a context manager. It restores the previous PYTHONPATH when
    exiting the context.

    Parameters
    ----------
    path : str
        Path to add to PYTHONPATH.

    """
    path = os.path.realpath(path)
    pythonpath = os.environ.get("PYTHONPATH")
    if pythonpath:
        for path_ in pythonpath:
            if os.path.isabs(path_) and os.path.realpath(path_) == path:
                yield  # Path is already in PYTHONPATH, nothing to do here.
                return
        try:
            # Append the current working directory to the PYTHONPATH.
            tmp_path = [path] + pythonpath.split(":")
            os.environ["PYTHONPATH"] = ":".join(tmp_path)
            yield
        finally:
            os.environ["PYTHONPATH"] = pythonpath
    else:
        try:
            # The PYTHONPATH was previously not set, set to current working directory.
            os.environ["PYTHONPATH"] = path
            yield
        finally:
            del os.environ["PYTHONPATH"]


@contextmanager
def _add_cwd_to_environment_pythonpath():
    """Add current working directory to PYTHONPATH."""
    with _add_path_to_environment_pythonpath(os.getcwd()):
        yield


@contextmanager
def _switch_to_directory(root=None):
    """Temporarily switch into the given root directory (if not None).

    This method is a context manager. It switches to the previous working
    directory when exiting the context.

    Parameters
    ----------
    root : str
        Current working directory to use for within the context. (Default value
        = None)

    """
    if root is None:
        yield
    else:
        cwd = os.getcwd()
        try:
            os.chdir(root)
            yield
        finally:
            os.chdir(cwd)


class _TrackGetItemDict(dict):
    """A dict that tracks which keys have been accessed.

    Keys accessed with ``__getitem__`` are stored in the property
    :attr:`~.keys_used`.
    """

    def __init__(self, *args, **kwargs):
        self._keys_used = set()
        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
        """Get item by key."""
        self._keys_used.add(key)
        return super().__getitem__(key)

    def get(self, key, default=None):
        """Return the value for key if key is in the dictionary, else default.

        If default is not given, it defaults to ``None``, so that this method
        never raises a :class:`KeyError`.
        """
        self._keys_used.add(key)
        return super().get(key, default)

    @property
    def keys_used(self):
        """Return all keys that have been accessed."""
        return self._keys_used.copy()


def _roundrobin(*iterables):
    """Round robin iterator.

    Cycles through a sequence of iterables, taking one item from each iterable
    until all iterables are exhausted.
    """
    # From: https://docs.python.org/3/library/itertools.html#itertools-recipes
    # roundrobin('ABC', 'D', 'EF') --> A D E B F C
    # Recipe credited to George Sakkis
    num_active = len(iterables)
    nexts = cycle(iter(it).__next__ for it in iterables)
    while num_active:
        try:
            for next in nexts:
                yield next()
        except StopIteration:
            # Remove the iterator we just exhausted from the cycle.
            num_active -= 1
            nexts = cycle(islice(nexts, num_active))


class _hashable_dict(dict):
    def __hash__(self):
        return hash(tuple(sorted(self.items())))


def _to_hashable(obj):
    """Create a hash of passed type.

    Parameters
    ----------
    obj
        Object to make hashable. Lists are converted to tuples, and hashes are
        defined for dicts.

    Returns
    -------
    object
        Hashable object.

    """
    if type(obj) is list:
        return tuple(_to_hashable(_) for _ in obj)
    elif type(obj) is dict:
        return _hashable_dict(obj)
    else:
        return obj


def _cached_partial(func, *args, maxsize=None, **kwargs):
    r"""Cache the results of a partial.

    Useful for wrapping functions that must only be evaluated lazily, one time.

    Parameters
    ----------
    func : callable
        The function to call.
    \*args
        Positional arguments bound to the function.
    maxsize : int
        The maximum size of the LRU cache, or None for no limit. (Default value
        = None)
    \*\*kwargs
        Keyword arguments bound to the function.

    Returns
    -------
    callable
        Function with bound arguments and cached return values.

    """
    return lru_cache(maxsize=maxsize)(partial(func, *args, **kwargs))


class _bidict(MutableMapping):
    r"""A bidirectional dictionary.

    The attribute ``inverse`` contains the inverse mapping, where the inverse
    values are stored as a :class:`list` of keys with that value.

    Both keys and values must be hashable.
    A key is associated with exactly one value.
    A value is associated with one or more keys.
    The inverse mapping should not be modified directly.
    The list of inverse values (keys) must be insertion-ordered.

    """

    # Based on: https://stackoverflow.com/a/21894086

    def __init__(self, *args, **kwargs):
        self._data = dict(*args, **kwargs)
        self.inverse = {}
        for key, value in self._data.items():
            self.inverse.setdefault(value, []).append(key)

    def __getitem__(self, key):
        """Get a value from the provided key."""
        return self._data[key]

    def __setitem__(self, key, value):
        """Assign a value to the provided key."""
        if key in self._data:
            old_value = self._data[key]
            self.inverse[old_value].remove(key)
            if len(self.inverse[old_value]) == 0:
                del self.inverse[old_value]
        self._data[key] = value
        self.inverse.setdefault(value, []).append(key)

    def __delitem__(self, key):
        """Delete the provided key."""
        value = self._data[key]
        self.inverse[value].remove(key)
        if len(self.inverse[value]) == 0:
            del self.inverse[value]
        del self._data[key]

    def __iter__(self):
        yield from self._data

    def __len__(self):
        return len(self._data)


def _run_cloudpickled_func(func, *args):
    """Execute a cloudpickled function.

    The set of functions that can be pickled by the built-in pickle module is
    very limited, which prevents the usage of various useful cases such as
    locally-defined functions or functions that internally call class methods.
    This function circumvents that difficulty by allowing the user to pickle
    the function object a priori and bind it as the first argument to a partial
    application of this function. All subsequent arguments are transparently
    passed through.
    """
    unpickled_func = cloudpickle.loads(func)
    args = list(map(cloudpickle.loads, args))
    return unpickled_func(*args)


def _get_parallel_executor(parallelization="none"):
    """Get an executor for the desired parallelization strategy.

    This executor shows a progress bar while executing a function over an
    iterable in parallel. The returned callable has signature ``func,
    iterable, **kwargs``. The iterable must have a length (generators are not
    supported). The keyword argument ``chunksize`` is used for chunking the
    iterable in supported parallelization modes
    (see :meth:`concurrent.futures.Executor.map`). All other ``**kwargs`` are
    passed to the tqdm progress bar.

    Parameters
    ----------
    parallelization : str
        Parallelization mode. Allowed values are "thread", "process", or
        "none". (Default value = "none")

    Returns
    -------
    callable
        A callable with signature ``func, iterable, **kwargs``.

    """
    if parallelization == "thread":

        def parallel_executor(func, iterable, **kwargs):
            return thread_map(func, iterable, tqdm_class=tqdm, **kwargs)

    elif parallelization == "process":

        def parallel_executor(func, iterable, **kwargs):
            # The tqdm progress bar requires a total. We compute the total in
            # advance because a map iterable (which has no total) is passed to
            # process_map.
            if "total" not in kwargs:
                kwargs["total"] = len(iterable)

            return process_map(
                # The top-level function called on each process cannot be a
                # local function, it must be a module-level function. Creating
                # a partial here allows us to use the passed function "func"
                # regardless of whether it is a local function.
                partial(_run_cloudpickled_func, cloudpickle.dumps(func)),
                map(cloudpickle.dumps, iterable),
                tqdm_class=tqdm,
                **kwargs,
            )

    else:

        def parallel_executor(func, iterable, **kwargs):
            if "chunksize" in kwargs:
                # Chunk size only applies to thread/process parallel executors
                del kwargs["chunksize"]
            return list(tmap(func, iterable, tqdm_class=tqdm, **kwargs))

    return parallel_executor


__all__ = [
    "redirect_log",
]
