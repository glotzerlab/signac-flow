# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import json
import argparse
import logging
from contextlib import contextmanager
from itertools import cycle, islice


def _positive_int(value):
    """Expect a command line argument to be a positive integer.

    Designed to be used in conjunction with an argparse.ArgumentParser.

    :param value:
        This function will raise an argparse.ArgumentTypeError if value
        is not a positive integer.
    :raises:
        :class:`argparse.ArgumentTypeError`
    """
    try:
        ivalue = int(value)
        if ivalue <= 0:
            raise argparse.ArgumentTypeError("Value must be positive.")
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError(
            "{} must be a positive integer.".format(value))
    return ivalue


def write_human_readable_statepoint(script, job):
    """Human-readable representation of a signac state point."""
    script.write('# Statepoint:\n#\n')
    sp_dump = json.dumps(job.statepoint(), indent=2).replace(
        '{', '{{').replace('}', '}}')
    for line in sp_dump.splitlines():
        script.write('# ' + line + '\n')


@contextmanager
def redirect_log(job, filename='run.log', formatter=None, logger=None):
    """Redirect all messages logged via the logging interface to the given file.

    :param job:
        An instance of a signac job.
    :type job:
        :class:`signac.Project.Job`
    :formatter:
        The logging formatter to use, uses a default formatter if this argument
        is not provided.
    :type formatter:
        :class:`logging.Formatter`
    :param logger:
        The instance of logger to which the new file log handler is added. Defaults
        to the default logger returned by `logging.getLogger()` if this argument is
        not provided.
    :type logger:
        :class:`logging.Logger`
    """
    if formatter is None:
        formatter = logging.Formatter(
            '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    if logger is None:
        logger = logging.getLogger()

    filehandler = logging.FileHandler(filename=job.fn('run.log'))
    filehandler.setFormatter(formatter)
    logger.addHandler(filehandler)
    try:
        yield
    finally:
        logger.removeHandler(filehandler)


@contextmanager
def add_path_to_environment_pythonpath(path):
    "Temporarily insert the current working directory into the environment PYTHONPATH variable."
    path = os.path.realpath(path)
    pythonpath = os.environ.get('PYTHONPATH')
    if pythonpath:
        for path_ in pythonpath:
            if os.path.isabs(path_) and os.path.realpath(path_) == path:
                yield   # Path is already in PYTHONPATH, nothing to do here.
                return
        try:
            # Append the current working directory to the PYTHONPATH.
            tmp_path = [path] + pythonpath.split(':')
            os.environ['PYTHONPATH'] = ':'.join(tmp_path)
            yield
        finally:
            os.environ['PYTHONPATH'] = pythonpath
            pass
    else:
        try:
            # The PYTHONPATH was previously not set, set to current working directory.
            os.environ['PYTHONPATH'] = path
            yield
        finally:
            del os.environ['PYTHONPATH']


@contextmanager
def add_cwd_to_environment_pythonpath():
    with add_path_to_environment_pythonpath(os.getcwd()):
        yield


@contextmanager
def switch_to_directory(root=None):
    "Temporarily switch into the given root directory (if not None)."
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
    "A dict that keeps track of which keys were accessed via __getitem__."

    def __init__(self, *args, **kwargs):
        self._keys_used = set()
        super(TrackGetItemDict, self).__init__(*args, **kwargs)

    def __getitem__(self, key):
        self._keys_used.add(key)
        return super(TrackGetItemDict, self).__getitem__(key)

    def get(self, key, default=None):
        self._keys_used.add(key)
        return super(TrackGetItemDict, self).get(key, default)

    @property
    def keys_used(self):
        "Return all keys that have been accessed."
        return self._keys_used.copy()


def roundrobin(*iterables):
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


def to_hashable(obj):
    # if isinstance(l, Sequence):
    if type(obj) == list:
        return tuple(to_hashable(_) for _ in obj)
    elif type(obj) == dict:
        return _hashable_dict(obj)
    else:
        return obj
