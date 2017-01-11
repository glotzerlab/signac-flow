# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.

import networkx as nx

class FlowGraph:
    def __init__(self):
        self._operations = None;
        self._graph = nx.Graph();

    def add_operation(self, callback, prereqs, postcond):
        pass;

    def next_operations(self, job):
        pass;

    def _build_graph(self):
        pass;

    def get_operation_chain(self, job, condition):
        pass;
