# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Operation hooks."""

import logging

logger = logging.getLogger(__name__)


__all__ = [
    "Hooks",
]


class _HooksList(list):
    def __call__(self, *args, **kwargs):
        """Call all hook functions as part of this list."""
        for hook in self:
            logger.debug(f"Executing hook function '{hook}'.")
            try:
                hook(*args, **kwargs)
            except Exception as error:
                logger.error(
                    "Error occurred during execution of "
                    "hook '{}': {}.".format(hook, error)
                )
                raise


class Hooks:
    """:class:`~.Hooks` execute an action or set of actions at specific stages of operation execution.

    :class:`~.Hooks` can execute a user defined function when an operation begins, fails, succeeds,
    or finishes (regardless of if the operation executed successfully or failed).

    Hooks can be installed at the operation level as decorators,
    or at the flow-project level.

    Examples
    --------
    In the example below, a operation level decorator that prints the operation name
    and job id at the start of the operation execution.

    .. code-block:: python
        @Flowproject.operation
        @Flowproject.hook.on_start(lambda operation_name, job: print(
            f"Starting operation {operation.name} on job {job.id}")
        def op(job):
            pass

    Parameters
    ----------
    on_start : function
        Function(s) to execute when the operation begins execution.
    on_finish : function
        Function(s) to execute when the operation exits, with or without errors.
    on_success : function
        Function(s) to execute when the operation exits without error.
    on_fail : function
        Function(s) to execute when the operation exits with error.

    """

    _hook_triggers = [
        "on_start",
        "on_finish",
        "on_success",
        "on_fail",
    ]

    def __init__(self, *, on_start=None, on_finish=None, on_success=None, on_fail=None):
        def set_hooks(self, trigger_name, trigger_value):
            if trigger_value is None:
                trigger_value = []
            setattr(self, trigger_name, _HooksList(trigger_value))

        set_hooks(self, "on_start", on_start)
        set_hooks(self, "on_finish", on_finish)
        set_hooks(self, "on_success", on_success)
        set_hooks(self, "on_fail", on_fail)

    def __setattr__(self, name, value):
        """Convert to _HooksList when setting a hook trigger attribute."""
        if name in self._hook_triggers:
            super().__setattr__(name, _HooksList(value))
        else:
            super().__setattr__(name, value)

    def update(self, other):
        """Update this instance with hooks from another instance."""
        for hook_trigger in self._hook_triggers:
            getattr(self, hook_trigger).extend(getattr(other, hook_trigger))

    def __str__(self):
        """Return a string representation of all hooks."""
        return "{}({})".format(
            type(self).__name__,
            ", ".join(
                [
                    f"{hook_trigger}={getattr(self, hook_trigger)}"
                    for hook_trigger in self._hook_triggers
                ]
            ),
        )

    def __bool__(self):
        """Return true if hooks are defined."""
        return any(
            getattr(self, hook_trigger, None) for hook_trigger in self._hook_triggers
        )
