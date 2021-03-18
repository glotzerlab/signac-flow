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

    _hook_triggers = [
        "on_start",
        "on_finish",
        "on_success",
        "on_fail",
    ]

    def __init__(self, **kwargs):
        for hook_trigger in self._hook_triggers:
            setattr(self, hook_trigger, _HooksList(kwargs.get(hook_trigger, [])))

    def __setattr__(self, name, value):
        if name in self._hook_triggers:
            super().__setattr__(name, _HooksList(value))
        else:
            super().__setattr__(name, value)

    @classmethod
    def from_dict(cls, mapping):
        "Instantiate the hook class from a dictionary."
        hook = cls()
        for trigger_type in mapping:
            getattr(hook, trigger_type)[:] = list(mapping[trigger_type])
        return hook

    def update(self, other):
        "Update this instance with hooks from another instance."
        for hook_trigger in self._hook_triggers:
            getattr(self, hook_trigger).extend(getattr(other, hook_trigger))

    def __str__(self):
        return "{}({})".format(
            type(self).__name__,
            ", ".join(["{}={}".format(hook_trigger, getattr(self, hook_trigger)) for hook_trigger in self._hook_triggers]),
        )

    def __bool__(self):
        return any(getattr(self, hook_trigger, None) for hook_trigger in self._hook_triggers)
