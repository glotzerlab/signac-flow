# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from itertools import chain

import networkx as nx

class FlowNode:

    def __init__(self,callback):
        self._callback = callback

    def __eq__(self, other):
        return self._callback == other._callback

    def __hash__(self):
        return hash(self._callback)

    def __repr__(self):
        return "{}({})".format(type(self), self._callback)

    def __call__(self, job):
        return self._callback(job)

class FlowOperation(FlowNode):
    pass

class FlowCondition(FlowNode):

    def __call__(self, job):
        if self._callback is None:
            return True
        else:
            return self._callback(job)
        

class FlowGraph:

    def __init__(self):
        self._graph = nx.DiGraph();

    def add_operation(self, callback, prereq, postconds):
        self._graph.add_edge(FlowCondition(prereq), FlowOperation(callback))
        assert hash(FlowOperation(callback)) == hash(FlowOperation(callback))
        self._graph.add_edge(FlowCondition(prereq), FlowOperation(callback))
        for c in postconds:
            assert hash(FlowCondition(c)) == hash(FlowCondition(c))
            self._graph.add_edge(FlowOperation(callback), FlowCondition(c))

    def next_operations(self, job):
        for node in self._graph.nodes():
            if isinstance(node, FlowOperation):
                if self.eligible(node, job):
                    yield node

    def get_operation_chain(self, job, finish, start=None):
        src = FlowCondition(start)
        dst = FlowCondition(finish)
        for path in nx.all_simple_paths(self._graph, src, dst):
            for node in path:
                if isinstance(node, FlowOperation):
                    if self.eligible(node, job):
                        yield node

    def eligible(self, node, job):
        pre = self._graph.predecessors(node)
        post = self._graph.successors(node)
        assert all(isinstance(c, FlowCondition) for c in chain(pre, post))
        return all(c(job) for c in pre) and not all(c(job) for c in post)
