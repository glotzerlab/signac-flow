# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import unittest

from flow import get_environment
from flow.environment import JobScript
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

    def test_JobScript(self):
        env = get_environment(test=True)
        sscript = env.script()
        sscript_ = JobScript(env)
        self.assertEqual(type(sscript), type(sscript_))
        self.assertEqual(sscript._env, sscript_._env)
        self.assertEqual(len(sscript.read()), 0)
        sscript.writeline('test')
        sscript.seek(0)
        self.assertEqual(sscript.read(), 'test\n')
        sscript = env.script()
        sscript.write_cmd('test')
        sscript.seek(0)
        self.assertEqual(sscript.read(), 'test\n')
        sscript = env.script()
        sscript.write_cmd('test', bg=True)
        sscript.seek(0)
        self.assertTrue(sscript.read().endswith('&\n'))

    def test_write_test_submission_script(self):
        env = get_environment(test=True)
        sscript = env.script()
        self.assertTrue(isinstance(sscript, JobScript))
        sscript = env.script(a=0)
        sscript.seek(0)
        self.assertEqual(sscript.read(), '#TEST a=0\n')

    def test_submit_test_submission_script(self):
        env = get_environment(test=True)
        sscript = env.script(a=0)
        sscript.seek(0)
        tmp_out = StringIO()
        with redirect_stdout(tmp_out):
            env.submit(sscript, hold=True)
        tmp_out.seek(0)

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
