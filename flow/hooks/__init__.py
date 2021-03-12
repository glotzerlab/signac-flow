# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import logging

from .log_operations import LogOperations
from .track_operations import TrackOperations

try:
    from .git_workspace_tracking import Git
except ImportError:

    class Git:
        def __init__(self, *args, **kwargs):
            raise ImportError("'Git' requires GitPython.")


logger = logging.getLogger(__name__)


__all__ = [
    "Hooks",
    "LogOperations",
    "TrackOperations",
    "SnapshotProject",
    "TrackWorkspaceWithGit",
]


class _HooksList(list):
    def __call__(self, *args, **kwargs):
        "Call all hook functions as part of this list."
        for hook in self:
            logger.debug(f"Executing hook function '{hook}'.")
            try:
                hook(*args, **kwargs)
            except Exception as error:
                logger.debug(
                    "Error occurred during execution of "
                    "hook '{}': {}.".format(hook, error)
                )
                raise


class Hooks:
    "Define execution hooks that are executed on certain conditions."

    _keys = [
        "on_start",
        "on_finish",
        "on_success",
        "on_fail",
    ]

    def __init__(self, **kwargs):
        for key in self._keys:
            setattr(self, key, _HooksList(kwargs.get(key, [])))

    def __setattr__(self, name, value):
        if name in self._keys:
            super().__setattr__(name, _HooksList(value))
        else:
            super().__setattr__(name, value)

    @classmethod
    def from_dict(cls, mapping):
        "Instantiate the hook class from a dictionary."
        hook = cls()
        for key in mapping:
            getattr(hook, key)[:] = list(mapping[key])
        return hook

    def update(self, other):
        "Update this instance with hooks from another instance."
        for key in self._keys:
            getattr(self, key).extend(getattr(other, key))

    def __str__(self):
        return "{}({})".format(
            type(self).__name__,
            ", ".join(["{}={}".format(key, getattr(self, key)) for key in self._keys]),
        )

    def __bool__(self):
        return any(getattr(self, key, None) for key in self._keys)
