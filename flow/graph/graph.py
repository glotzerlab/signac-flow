# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import networkx as nx

# not sure if we want this?
class FlowOperation:
    def __init__(self, name, callback, pre, post):
        self._name = name
        self._callback = callback
        self._preconditions = pre if pre else list()
        self._post_condtions = post if post else list()

    def __repr__(self):
        return self._name

    def eligible(self, job):
        # if no conditions all will return True. is this correct?
        pre = all([ cond(job) for cond in self._preconditions])
        post = !all([ cond(job) for cond in self._preconditions])
        return pre and post



class FlowGraph:
    def __init__(self):
        self._operations = [];
        self._graph = nx.Graph();

    def add_operation(self, name, callback, prereqs, postcond):
        self._operations.append( FlowOperation(name, callback, pre=prereqs, post=postcond));

    def next_operations(self, job):
        for op in self._operations:
            if op.eligible(job):
                yield op;


    def _build_graph(self):
        pass;

    def get_operation_chain(self, job, condition):
        pass;


from flow.graph import FlowGraph
from conditions import *
import operations
g = GraphFlow()
def ts_gte(job, ts):
    with job.fn('dump.gsd') as file:
        return int(file.read()) >= ts
is_init = IsFile('init.gsd')
checked = ts_gte(10)
dumped = ts_gte(100)
g.add_operation(operations.init, None, [is_init])
g.add_operation(operations.check, [is_init], [checked])
g.add_operation(operations.equil, [checked], [dumped])
for op in g.next_operations():
    print(op)
