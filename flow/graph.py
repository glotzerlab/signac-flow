# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import warnings
import logging
from itertools import chain

import networkx as nx

logger = logging.getLogger(__name__)

warnings.warn(
    "The graph module is provisional and may be deprecated in future version.",
    PendingDeprecationWarning)
logger.warning("The graph module is provisional and may be deprecated in future version.")


class _FlowNode:

    def __init__(self, callback):
        self._callback = callback

    def __eq__(self, other):
        return self._callback == other._callback

    def __hash__(self):
        return hash(self._callback)

    def __repr__(self):
        return "{}({})".format(type(self), self._callback)

    def __str__(self):
        if self._callback is None:
            return repr(self)
        elif callable(self._callback):
            return self._callback.__name__
        else:
            return str(self._callback)

    @classmethod
    def as_this_type(cls, node):
        if isinstance(node, cls):
            return node
        else:
            return cls(node)


class FlowOperation(_FlowNode):
    "A node within the FlowGraph corresponding to an operation."

    def __call__(self, job):
        return self._callback(job)


class FlowCondition(_FlowNode):
    "A node within the FlowGraph corresponding to a condition function."

    def __init__(self, callback):
        if callback is not None and not callable(callback):
            raise ValueError(callback)
        super().__init__(callback)

    def __call__(self, job):
        if self._callback is None:
            return True
        else:
            return self._callback(job)


class FlowGraph(object):
    """Define a workflow based on conditions and operations as a graph.

    The ``FlowGraph`` class is designed to simplify the definition of
    more complex workflows, by adding operations to a graph, linking them
    by pre- and post-conditions. The assumption is that an operation is
    *eligible* for operation when the pre-condition is met, and at least
    one of the post-conditions is not met.


    For example, assuming that we a have a *foo* operation, that requires
    a *ready* condition for execution and *should* result in a *started*
    and in a *done* condition, we can express this in graph form like this:

    .. code-block:: python

        g = FlowGraph()
        g.add_operation('foo', prereq=ready, postconds=[started, done])

    The condition functions (``ready``, ``started``, and ``done``) need to
    be implemented as functions with a single argument, for example a
    signac job.

    We can then determine *all* eligible operations within the graph, by
    calling the :py:meth:`~.eligible_operations` method.

    In the example above, the operation was defined as a :py:class:`str`,
    however in principle we can use any object as operation, as long as
    they are uniquely comparable, e.g., an instance of :py:class:`flow.JobOperation`,
    or a callable.

    If we use callables, we can execute all eligible operations for all
    jobs in a project like this:

    .. code-block:: python

        for i in range(max_num_iterations):  # make sure to limit this loop!
            for job in project:
                for op in g.eligible_operations(job):
                    op(job)

    """
    def __init__(self):
        self._graph = nx.DiGraph()

    def add_operation(self, operation, prereq=None, postconds=None):
        """Add an operation to the graph.

        This method adds the operation, the optional pre-requirement condition,
        and optional post-conditions to the graph.

        The method will be considered *eligible* for execution when the
        pre-condition is met and at least one of the post-conditions is
        **not** met. The assumption is that the executing the operation
        will eventually lead to all post-conditions to be met.

        The operation may be any object that can be uniquely compared
        via ``hash()``, e.g. a ``str`` or an instance of
        :py:class:`flow.JobOperation` and *callables*.

        All conditions must be callables, with exactly one argument.

        .. note::

            The operation and condition arguments can also be
            provided as instances of :py:class:`~.FlowOperation` and
            :py:class:`~.FlowCondition` since they will be internally
            converted to these classes anyways.

        :param operation: The operation to add to the graph.
        :param prereq: The pre-condition callback function.
        :param postconds: The post-condition callback functions.
        """
        self._graph.add_edge(
            FlowCondition.as_this_type(prereq),
            FlowOperation.as_this_type(operation))
        if postconds is not None:
            for c in postconds:
                self._graph.add_edge(
                    FlowOperation.as_this_type(operation),
                    FlowCondition.as_this_type(c))

    def link_conditions(self, a, b):
        """Link a condition a with a condition b.

        Linking conditions may be necessary to define a path
        within the graph, that is otherwise difficult to express.

        The arguments may passedd as condition callback functions or
        instances of :py:class:`~.FlowCondition`.

        :param a: The condition to link with ``b``.
        :param b: The condition to link with ``a``.
        """
        self._graph.add_edge(FlowCondition.as_this_type(a), FlowCondition.as_this_type(b))

    def conditions(self):
        "Yields all conditions which are part of the graph."
        for node in self._graph.nodes():
            if isinstance(node, FlowCondition):
                yield node

    def operations(self):
        "Yields all operations which are part of the graph."
        for node in self._graph.nodes():
            if isinstance(node, FlowOperation):
                yield node

    def eligible(self, operation, job):
        """Determine whether operation is eligible for execution.

        The operation is considered to be *eligible* for execution
        if the pre-condition is met and at least one of the post-conditions
        is not met.

        :param operation: The operation to check.
        :param job: The argument passed to the condition functions.
        """
        pre = self._graph.predecessors(operation)
        post = self._graph.successors(operation)
        assert all(isinstance(c, FlowCondition) for c in chain(pre, post))
        return all(c(job) for c in pre) and not all(c(job) for c in post)

    def eligible_operations(self, job):
        """Determine all eligible operations within the graph.

        This method yields all operations that are determined to
        be *eligible* for execution given their respective pre-
        and post-conditions.

        :param job: The argument passed to the condition functions.
        """
        for op in self.operations():
            if self.eligible(op, job):
                yield op

    def _operation_chain(self, job, src, dst):
        for path in nx.all_simple_paths(self._graph, src, dst):
            for node in path:
                if isinstance(node, FlowOperation):
                    yield node
            break

    def operation_chain(self, job, target, start=None):
        """Generate a chain of operations to link a start and target condition.

        This function will yield operations that need to be executed in order
        to reach a target condition given an optional start condition.

        :param job: The argument passed to the condition functions.
        :param target: The target condition that is to be reached.
        :param start: The start condition, by default None.
        """
        src = FlowCondition.as_this_type(start)
        dst = FlowCondition.as_this_type(target)
        if dst not in self._graph:
            raise ValueError("Target '{}' not in flow graph.".format(dst))
        if src not in self._graph:
            raise ValueError("Start '{}' not in flow graph.".format(src))
        if not nx.has_path(self._graph, src, dst):
            raise RuntimeError("No path between '{}' and '{}'.".format(src, dst))
        for node in self._operation_chain(job, src, dst):
            yield node
