# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import networkx as nx
class And:
    def __init__(self, first, second):
        self.first = first;
        self.second = second;

    def __call__(self, job):
        return self.first(job) and self.second(job)

class Or:
    def __init__(self, first, second):
        self.first = first;
        self.second = second;

    def __call__(self, job):
        return self.first(job) or self.second(job)
# not sure if we want this?
class FlowOperation:
    def __init__(self, name, callback, pre, post):
        self._name = name
        self._callback = callback
        self._preconditions = pre
        self._post_condtions = post if post else list()

    def __repr__(self):
        return self._name

    def eligible(self, job):
        # if no conditions all will return True. is this correct?
        pre = self._preconditions(job) #all([ cond(job) for cond in self._preconditions])
        post = not all([ cond(job) for cond in self._post_condtions]) # if no post conditions this will return False => must supply at least one post condition
        return pre and post

class FlowGraph:
    def __init__(self):
        self._operations = []; #make this a dict?
        self._graph = nx.DiGraph();

    def add_operation(self, name, callback, prereqs, postcond):
        self._operations.append( FlowOperation(name, callback, pre=prereqs, post=postcond));

    def next_operations(self, job):
        for op in self._operations:
            if op.eligible(job):
                yield op

    def _build_graph(self):
        def _connection(op1, op2):
            for out_cond in op1._post_condtions:
                for in_cond in op2._preconditions:
                    if out_cond == in_cond:
                        return True;
            return False;

        self._graph = nx.DiGraph();
        nodes = [op._name for op in self._operations]
        self._graph.add_nodes(nodes);

        for i in range(0,len(self._operations)):
            for j in range(i+1, len(self._operations)):
                if _connection(self._operations[i], self._operations[j]):
                    self._graph.add_edge(self._operations[i]._name, self._operations[j].name); # i -> j

    def get_operation_chain(self, job, condition):
        # not sure about this is yet.
        pass;
