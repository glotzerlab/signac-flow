# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import unittest
import os
import warnings

from signac.common import six
import deprecation
from flow import FlowProject
from flow import get_environment
from flow.scheduling.base import JobStatus
from flow import label
from flow import classlabel
from flow import staticlabel
from flow import init
from test_project import redirect_stdout, redirect_stderr, suspend_logging, \
    MockEnvironment, MockScheduler, StringIO

if six.PY2:
    from tempdir import TemporaryDirectory
else:
    from tempfile import TemporaryDirectory


class MockProject(FlowProject):

    def __init__(self, *args, **kwargs):
        super(MockProject, self).__init__(*args, **kwargs)
        self.add_operation(
            name='a_op',
            cmd='echo "hello" > {job.ws}/world.txt',
            pre=[lambda job: 'has_a' in self.labels(job)])

    @label()
    def said_hello(self, job):
        return job.isfile('world.txt')

    @label()
    def a(self, job):
        return True

    @classlabel()
    def b(cls, job):
        return True

    @staticlabel()
    def c(job):
        return True

    # Test label decorator argument
    @staticlabel('has_a')
    def d(job):
        return 'a' in job.statepoint() and not job.document.get('a', False)

    @staticlabel()
    def b_is_even(job):
        return job.sp.b % 2 == 0

    # Test string label return
    @staticlabel()
    def a_gt_zero(job):
        if job.sp.a > 0:
            return 'a is {}'.format(job.sp.a)
        else:
            return None

    def classify(self, job):
        if 'a' in job.statepoint() and not job.document.get('a', False):
            yield 'has_a'
        if job.sp.b % 2 == 0:
            yield 'b_is_even'

    def submit_user(self, env, _id, operations, **kwargs):
        js = env.script(_id=_id)
        for op in operations:
            js.write(op.cmd)
        return env.submit(js, _id=_id)


class ProjectTest(unittest.TestCase):
    project_class = MockProject

    def setUp(self):
        MockScheduler.reset()
        self._tmp_dir = TemporaryDirectory(prefix='signac-flow_')
        self.addCleanup(self._tmp_dir.cleanup)
        self.project = FlowProject.init_project(
            name='FlowTestProject',
            root=self._tmp_dir.name)

    def mock_project(self):
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', module='flow', category=DeprecationWarning)
            warnings.filterwarnings('ignore', module='flow', category=UserWarning)
            project = self.project_class.get_project(root=self._tmp_dir.name)
        for a in range(3):
            for b in range(3):
                project.open_job(dict(a=a, b=b)).init()
        return project

    def test_print_status(self):
        project = self.mock_project()
        for job in project:
            list(project.classify(job))
            self.assertEqual(project.next_operation(job).name, 'a_op')
            self.assertEqual(project.next_operation(job).job, job)
        fd = StringIO()
        with redirect_stderr(StringIO()):
            with redirect_stdout(StringIO()):
                project.print_status(file=fd, err=fd)

    def test_submit_operations(self):
        env = get_environment()
        sched = env.scheduler_type()
        sched.reset()
        project = self.mock_project()
        operations = []
        for job in project:
            operations.extend(project.next_operations(job))
        self.assertEqual(len(list(sched.jobs())), 0)
        cluster_job_id = project._store_bundled(operations)
        with suspend_logging():
            with redirect_stderr(StringIO()):
                project.submit_operations(_id=cluster_job_id, env=env, operations=operations)
        self.assertEqual(len(list(sched.jobs())), 1)
        sched.reset()


if __name__ == '__main__':
    unittest.main()
