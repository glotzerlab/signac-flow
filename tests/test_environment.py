# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import unittest

from flow import get_environment
from flow.environment import ComputeEnvironment
from flow.environment import TestEnvironment
from flow.errors import ConfigKeyError
from test_project import StringIO, redirect_stdout


class ProjectTest(unittest.TestCase):

    def test_get_TestEnvironment(self):
        env = get_environment()
        self.assertTrue(issubclass(env, ComputeEnvironment))
        self.assertFalse(issubclass(env, TestEnvironment))
        env = get_environment(test=True)
        self.assertTrue(issubclass(env, TestEnvironment))

    def test_environment_get_config_value(self):
        env = get_environment(test=True)

        with redirect_stdout(StringIO()):
            with self.assertRaises(ConfigKeyError):
                a = env.get_config_value('a')

        a = env.get_config_value('a', None)
        self.assertIsNone(a)

        a = env.get_config_value('a', 42)
        self.assertEqual(a, 42)


if __name__ == '__main__':
    unittest.main()
