# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Miscellaneous utility functions."""
import argparse
import json
import logging
import os
from contextlib import contextmanager
from itertools import cycle, islice


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


def write_human_readable_statepoint(script, job):
    """Human-readable representation of a signac state point."""
    script.write("# Statepoint:\n#\n")
    sp_dump = (
        json.dumps(job.statepoint(), indent=2).replace("{", "{{").replace("}", "}}")
    )
    for line in sp_dump.splitlines():
        script.write("# " + line + "\n")


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
def add_path_to_environment_pythonpath(path):
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
            pass
    else:
        try:
            # The PYTHONPATH was previously not set, set to current working directory.
            os.environ["PYTHONPATH"] = path
            yield
        finally:
            del os.environ["PYTHONPATH"]


@contextmanager
def add_cwd_to_environment_pythonpath():
    """Add current working directory to PYTHONPATH."""
    with add_path_to_environment_pythonpath(os.getcwd()):
        yield


@contextmanager
def switch_to_directory(root=None):
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


class TrackGetItemDict(dict):
    """A dict that tracks which keys have been accessed.

    Keys accessed with ``__getitem__`` are stored in the property
    :attr:`~.keys_used`.
    """

    def __init__(self, *args, **kwargs):
        self._keys_used = set()
        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
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


def roundrobin(*iterables):
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
