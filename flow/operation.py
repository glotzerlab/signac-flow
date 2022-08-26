"""TODO: Add docs"""

import textwrap
from itertools import chain

from flow.aggregates import aggregator
from flow.environment import ComputeEnvironment
from flow.errors import FlowProjectDefinitionError
from flow.project import FlowGroupEntry

from .directives import _document_directive
from .util._decorate import decorate_with_job


class OperationRegister:
    """Add operation functions to the class workflow definition.

    This object is designed to be used as a decorator, for example:

    .. code-block:: python

        @FlowProject.operation
        def hello(job):
            print('Hello', job)

    Directives can also be specified by using :meth:`FlowProject.operation.with_directives`.

    .. code-block:: python

        @FlowProject.operation.with_directives({"nranks": 4})
        def mpi_hello(job):
            print("hello")

    Parameters
    ----------
    func : callable
        The function to add to the workflow.
    name : str
        The operation name. Uses the name of the function if None.
        (Default value = None)

    Returns
    -------
    callable
        The operation function.
    """

    _parent_class = None

    def __call__(self, func=None, name=None, cmd=False, with_job=False):
        """TODO: Add documentation"""
        if not func:
            return
        if isinstance(func, str):
            return lambda op: self(op, name=func)

        if func in chain(
            *self._parent_class._OPERATION_PRECONDITIONS.values(),
            *self._parent_class._OPERATION_POSTCONDITIONS.values(),
        ):
            raise FlowProjectDefinitionError(
                "A condition function cannot be used as an operation."
            )

        if name is None:
            name = func.__name__

        if cmd:
            self._setup_cmd(func)

        if with_job:
            func = self._decorate_with_job(func)

        for (
            registered_name,
            registered_func,
        ) in self._parent_class._OPERATION_FUNCTIONS:
            if name == registered_name:
                raise FlowProjectDefinitionError(
                    f"An operation with name '{name}' is already registered."
                )
            if func is registered_func:
                raise FlowProjectDefinitionError(
                    "An operation with this function is already registered."
                )
        if name in self._parent_class._GROUP_NAMES:
            raise FlowProjectDefinitionError(
                f"A group with name '{name}' is already registered."
            )

        if not getattr(func, "_flow_aggregate", False):
            func._flow_aggregate = aggregator.groupsof(1)

        # Append the name and function to the class registry
        self._parent_class._OPERATION_FUNCTIONS.append((name, func))
        # We register aggregators associated with operation functions in
        # `_register_groups` and we do not set the aggregator explicitly.  We
        # delay setting the aggregator because we do not restrict the decorator
        # placement in terms of `@FlowGroupEntry`, `@aggregator`, or
        # `@operation`.
        self._parent_class._GROUPS.append(
            FlowGroupEntry(name=name, project=self._parent_class)
        )
        if not hasattr(func, "_flow_groups"):
            func._flow_groups = {}
        func._flow_groups[self._parent_class] = {name}
        return func

    def _setup_cmd(self, func):
        if getattr(func, "_flow_with_job", False):
            # Check to support backwards compatibility with @flow.with_job decorator
            # This check should be removed in the 1.0.0.
            raise FlowProjectDefinitionError(
                "The @flow.with_job decorator must appear above the "
                "@FlowProject.operation decorator."
            )

        setattr(func, "_flow_cmd", True)

    def _decorate_with_job(self, func):
        if getattr(func, "_flow_with_job", False):
            # Check to support backwards compatibility with @flow.with_job decorator
            # This check should be removed in the 1.0.0.
            raise FlowProjectDefinitionError(
                "Cannot use with_job as both decorator and argument."
            )

        if getattr(func, "_flow_aggregate", False):
            # Check to support backwards compatibility with @flow.aggregator decorator
            # This check should be removed in the 1.0.0.
            raise FlowProjectDefinitionError(
                "The with_job argument cannot be used with aggregation."
            )

        return decorate_with_job(func)

    def with_directives(self, directives, name=None):
        """Decorate a function to make it an operation with additional execution directives.

        Directives can be used to provide information about required
        resources such as the number of processors required for
        execution of parallelized operations. For more information, see
        :ref:`signac-docs:cluster_submission_directives`. To apply
        directives to an operation that is part of a group, use
        :meth:`.FlowGroupEntry.with_directives`.

        Parameters
        ----------
        directives : dict
            Directives to use for resource requests and execution.
        name : str
            The operation name. Uses the name of the function if None
            (Default value = None).

        Returns
        -------
        function
            A decorator which registers the function with the provided
            name and directives as an operation of the
            :class:`~.FlowProject` subclass.
        """

        def add_operation_with_directives(function):
            function._flow_directives = directives
            return self(function, name)

        return add_operation_with_directives

    _directives_to_document = (
        ComputeEnvironment._get_default_directives()._directive_definitions.values()
    )
    with_directives.__doc__ += textwrap.indent(
        "\n\n**Supported Directives:**\n\n"
        + "\n\n".join(
            _document_directive(directive) for directive in _directives_to_document
        ),
        " " * 16,
    )
