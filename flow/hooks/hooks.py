# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Operation hooks."""
import logging

logger = logging.getLogger(__name__)


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


class _Hooks:
    """:class:`~._Hooks` execute actions at specific stages of operation execution.

    :class:`~._Hooks` can execute user defined functions when an operation
    starts, succeeds, fails, or finishes (regardless of whether the operation
    executed successfully or failed).

    Hooks can be installed at the operation level as decorators, or on an
    instance of :class:`~.FlowProject` through
    :meth:`~.FlowProject.project_hooks`.

    Examples
    --------
    The example below shows an operation level decorator that prints the
    operation name and job id at the start of the operation execution.

    .. code-block:: python

        def start_hook(operation_name, job):
            print(f"Starting operation {operation_name} on job {job.id}.")

        @FlowProject.operation
        @FlowProject.operation_hooks.on_start(start_hook)
        def foo(job):
            pass

    Parameters
    ----------
    on_start : list of callables
        Function(s) to execute before the operation begins execution.
    on_exit : list of callables
        Function(s) to execute after the operation exits, with or without errors.
    on_success : list of callables
        Function(s) to execute after the operation exits without error.
    on_exception : list of callables
        Function(s) to execute after the operation exits with an error.

    """

    _hook_triggers = [
        "on_start",
        "on_exit",
        "on_success",
        "on_exception",
    ]

    def __init__(
        self, *, on_start=None, on_exit=None, on_success=None, on_exception=None
    ):
        def set_hooks(self, trigger_name, trigger_value):
            if trigger_value is None:
                trigger_value = []
            setattr(self, trigger_name, _HooksList(trigger_value))

        set_hooks(self, "on_start", on_start)
        set_hooks(self, "on_exit", on_exit)
        set_hooks(self, "on_success", on_success)
        set_hooks(self, "on_exception", on_exception)

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
                f"{hook_trigger}={getattr(self, hook_trigger)}"
                for hook_trigger in self._hook_triggers
            ),
        )

    def __bool__(self):
        """Return True if hooks are defined."""
        return any(
            getattr(self, hook_trigger, None) for hook_trigger in self._hook_triggers
        )
