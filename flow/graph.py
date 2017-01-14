# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from itertools import chain

import networkx as nx


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

    def __call__(self, job):
        return self._callback(job)


class FlowCondition(_FlowNode):

    def __init__(self, callback):
        if callback is not None and not callable(callback):
            raise ValueError(callback)
        super().__init__(callback)

    def __call__(self, job):
        if self._callback is None:
            return True
        else:
            return self._callback(job)


class FlowGraph:

    def __init__(self):
        self._graph = nx.DiGraph()

    def add_operation(self, callback, prereq=None, postconds=None):
        self._graph.add_edge(
            FlowCondition.as_this_type(prereq),
            FlowOperation.as_this_type(callback))
        if postconds is not None:
            for c in postconds:
                self._graph.add_edge(
                    FlowOperation.as_this_type(callback),
                    FlowCondition.as_this_type(c))

    def link_conditions(self, a, b):
        self._graph.add_edge(FlowCondition.as_this_type(a), FlowCondition.as_this_type(b))

    def conditions(self):
        for node in self._graph.nodes():
            if isinstance(node, FlowCondition):
                yield node

    def operations(self):
        for node in self._graph.nodes():
            if isinstance(node, FlowOperation):
                yield node

    def eligible(self, node, job):
        pre = self._graph.predecessors(node)
        post = self._graph.successors(node)
        assert all(isinstance(c, FlowCondition) for c in chain(pre, post))
        return all(c(job) for c in pre) and not all(c(job) for c in post)

    def eligible_operations(self, job):
        for op in self.operations():
            if self.eligible(op, job):
                yield op

    def _operation_chain(self, job, src, dst):
        for path in nx.all_simple_paths(self._graph, src, dst):
            for node in path:
                if isinstance(node, FlowOperation):
                    if self.eligible(node, job):
                        yield node

    def operation_chain(self, job, target, start=None):
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
