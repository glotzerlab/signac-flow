# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import logging

logger = logging.getLogger(__name__)


class _HooksList(list):

    def __call__(self, *args, **kwargs):
        "Call all hook functions as part of this list."
        for hook in self:
            logger.debug("Executing hook function '{}'.".format(hook))
            try:
                hook(*args, **kwargs)
            except Exception as error:
                logger.error("Error occurred during execution of "
                             "hook '{}': {}.".format(hook, error))
                raise


class Hooks(object):
    "Define execution hooks that are executed on certain conditions."

    def __init__(self):
        self.on_start = _HooksList()
        self.on_finish = _HooksList()
        self.on_success = _HooksList()
        self.on_fail = _HooksList()

    @classmethod
    def from_dict(cls, mapping):
        "Instantiate the hook class from a dictionary."
        hook = cls()
        for key in mapping:
            getattr(hook, key)[:] = list(mapping[key])
        return hook

    def update(self, other):
        "Update this instance with hooks from another instance."
        self.on_start.extend(other.on_start)
        self.on_finish.extend(other.on_finish)
        self.on_success.extend(other.on_success)
        self.on_fail.extend(other.on_fail)
