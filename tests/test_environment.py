# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import sys
import unittest
import io
from contextlib import contextmanager

from flow import get_environment
from flow.environment import JobScript
from flow.environment import ComputeEnvironment
from flow.environment import TestEnvironment


@contextmanager
def redirect_stdout(file):
    try:
        stdout = sys.stdout
        sys.stdout = file
        yield
    finally:
        sys.stdout = stdout


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
        self.assertEqual(sscript._parent, sscript_._parent)
        self.assertEqual(len(sscript.read()), 0)
        sscript.writeline('test')
        sscript.seek(0)
        self.assertEqual(sscript.read(), 'test\n')
        sscript = env.script()
        sscript.write_cmd('test')
        sscript.seek(0)
        self.assertEqual(sscript.read(), 'test\n')
        sscript = env.script()
        sscript.write_cmd('test', np=2)
        sscript.seek(0)
        self.assertTrue(sscript.read().startswith('mpirun'))
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
        tmp_out = io.StringIO()
        with redirect_stdout(tmp_out):
            env.submit(sscript, hold=True)
        tmp_out.seek(0)
        self.assertEqual(tmp_out.read(), "# Submit command: testsub --hold\n#TEST a=0\n\n")


if __name__ == '__main__':
    unittest.main()
