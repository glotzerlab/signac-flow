# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest

from flow import get_environment
from flow.environment import ComputeEnvironment
from flow.environment import TestEnvironment
from flow.errors import ConfigKeyError
from test_project import StringIO, redirect_stdout


class TestProject():

    def test_get_TestEnvironment(self):
        env = get_environment()
        assert issubclass(env, ComputeEnvironment)
        assert not issubclass(env, TestEnvironment)
        env = get_environment(test=True)
        assert issubclass(env, TestEnvironment)

    def test_environment_get_config_value(self):
        env = get_environment(test=True)

        with redirect_stdout(StringIO()):
            with pytest.raises(ConfigKeyError):
                a = env.get_config_value('a')

        a = env.get_config_value('a', None)
        assert a is None

        a = env.get_config_value('a', 42)
        assert a == 42
