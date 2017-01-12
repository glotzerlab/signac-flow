# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from itertools import chain

import networkx as nx


class FlowNode:

    def __init__(self, callback):
        self._callback = callback

    def __eq__(self, other):
        return self._callback == other._callback

    def __hash__(self):
        return hash(self._callback)

    def __repr__(self):
        return "{}({})".format(type(self), self._callback)

    @classmethod
    def as_this_type(cls, node):
        if isinstance(node, cls):
            return node
        else:
            return cls(node)


class FlowOperation(FlowNode):

    def __init__(self, callback):
        if callback is None or not callable(callback):
            raise ValueError(callback)
        super().__init__(callback)

    def __call__(self, job):
        return self._callback(job)


class FlowCondition(FlowNode):

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

    def operation_chain(self, job, finish, start=None):
        src = FlowCondition.as_this_type(start)
        dst = FlowCondition.as_this_type(finish)
        for path in nx.all_simple_paths(self._graph, src, dst):
            for node in path:
                if isinstance(node, FlowOperation):
                    if self.eligible(node, job):
                        yield node
