# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.

from flow import get_environment
from flow.environment import ComputeEnvironment, TestEnvironment


class TestProject:
    def test_get_TestEnvironment(self):
        env = get_environment()
        assert issubclass(env, ComputeEnvironment)
        assert not issubclass(env, TestEnvironment)
        env = get_environment(test=True)
        assert issubclass(env, TestEnvironment)
