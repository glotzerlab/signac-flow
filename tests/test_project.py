# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import unittest
import io
import uuid
import os
import sys
from contextlib import contextmanager

import signac
from signac.common import six
from flow import FlowProject
from flow.scheduling.base import Scheduler
from flow.scheduling.base import ClusterJob
from flow.scheduling.base import JobStatus
from flow.environment import ComputeEnvironment
from flow import init

from define_test_project import TestProject

if six.PY2:
    from tempdir import TemporaryDirectory
else:
    from tempfile import TemporaryDirectory


# Need to implement context managers below while supporting
# Python versions 2.7 and 3.4. These managers are both part
# of the the standard library as of Python version 3.5.

@contextmanager
def redirect_stdout(new_target):
    "Temporarily redirect all output to stdout to new_target."
    old_target = sys.stdout
    try:
        sys.stdout = new_target
        yield
    finally:
        sys.stdout = old_target


@contextmanager
def redirect_stderr(new_target):
    "Temporarily redirect all output to stderr to new_target."
    old_target = sys.stderr
    try:
        sys.stderr = new_target
        yield
    finally:
        sys.stderr = old_target


class StringIO(io.StringIO):
    "PY27 compatibility layer."

    def write(self, s):
        if six.PY2:
            super(StringIO, self).write(unicode(s))  # noqa
        else:
            super(StringIO, self).write(s)


class MockScheduler(Scheduler):
    _jobs = {}  # needs to be singleton

    @classmethod
    def jobs(cls):
        for job in cls._jobs.values():
            yield job

    @classmethod
    def submit(cls, script, _id=None, *args, **kwargs):
        if _id is None:
            for line in script:
                _id = str(line).strip()
                break
        cid = uuid.uuid4()
        cls._jobs[cid] = ClusterJob(_id, status=JobStatus.submitted)
        return JobStatus.submitted

    @classmethod
    def step(cls):
        "Mock pushing of jobs through the queue."
        remove = set()
        for cid, job in cls._jobs.items():
            if job._status == JobStatus.inactive:
                remove.add(cid)
            else:
                job._status = JobStatus(job._status + 1)
                if job._status > JobStatus.active:
                    job._status = JobStatus.inactive
        for cid in remove:
            del cls._jobs[cid]

    @classmethod
    def reset(cls):
        cls._jobs.clear()


class MockEnvironment(ComputeEnvironment):
    scheduler_type = MockScheduler

    @classmethod
    def is_present(cls):
        return True


class BaseProjectTest(unittest.TestCase):
    project_class = signac.Project

    def setUp(self):
        self._tmp_dir = TemporaryDirectory(prefix='signac-flow_')
        self.addCleanup(self._tmp_dir.cleanup)
        self.project = self.project_class.init_project(
            name='FlowTestProject',
            root=self._tmp_dir.name)


class ProjectClassTest(BaseProjectTest):

    def test_operation_definition(self):

        class A(FlowProject):
            pass

        class B(A):
            pass

        class C(FlowProject):
            pass

        @A.operation
        def foo(job):
            pass

        @A.operation
        def bar(job):
            pass

        @B.operation
        @C.operation
        def baz(job):
            pass

        a = A.get_project(root=self._tmp_dir.name)
        b = B.get_project(root=self._tmp_dir.name)
        c = C.get_project(root=self._tmp_dir.name)

        self.assertEqual(len(a.operations), 2)
        self.assertEqual(len(b.operations), 3)
        self.assertEqual(len(c.operations), 1)

    def test_repeat_operation_definition(self):

        class A(FlowProject):
            pass

        @A.operation
        @A.operation
        def op1(job):
            pass

        with self.assertRaises(ValueError):
            A.get_project(root=self._tmp_dir.name)

    def test_repeat_operation_definition_with_inheritance(self):

        class A(FlowProject):
            pass

        class B(A):
            pass

        @A.operation
        @B.operation
        def op1(job):
            pass

        # Should raise no error
        A.get_project(root=self._tmp_dir.name)

        with self.assertRaises(ValueError):
            B.get_project(root=self._tmp_dir.name)

    def test_label_definition(self):

        class A(FlowProject):
            pass

        class B(A):
            pass

        class C(FlowProject):
            pass

        @A.label
        @C.label
        def label1(job):
            pass

        @B.label
        def label2(job):
            pass

        a = A.get_project(root=self._tmp_dir.name)
        b = B.get_project(root=self._tmp_dir.name)
        c = C.get_project(root=self._tmp_dir.name)

        self.assertEqual(len(a._label_functions), 1)
        self.assertEqual(len(b._label_functions), 2)
        self.assertEqual(len(c._label_functions), 1)


class ProjectTest(unittest.TestCase):
    project_class = TestProject

    def setUp(self):
        MockScheduler.reset()
        self._tmp_dir = TemporaryDirectory(prefix='signac-flow_')
        self.addCleanup(self._tmp_dir.cleanup)
        self.project = FlowProject.init_project(
            name='FlowTestProject',
            root=self._tmp_dir.name)

    def mock_project(self):
        project = self.project_class.get_project(root=self._tmp_dir.name)
        for a in range(3):
            for b in range(3):
                project.open_job(dict(a=a, b=b)).init()
        return project

    def test_instance(self):
        self.assertTrue(isinstance(self.project, FlowProject))

    def test_labels(self):
        project = self.mock_project()
        for job in project:
            labels = list(project.classify(job))
            self.assertEqual(len(labels), 2 - (job.sp.b % 2))
            self.assertTrue(all((isinstance(l, str)) for l in labels))
            self.assertIn('default_label', labels)
            self.assertNotIn('negative_default_label', labels)

    def test_script(self):
        project = self.mock_project()
        for job in project:
            script = project._generate_run_script(project.next_operations(job))
            if job.sp.b % 2 == 0:
                self.assertIn('echo "hello"', script)
                self.assertIn(str(job), script)
            else:
                self.assertNotIn('echo "hello"', script)
                self.assertNotIn(str(job), script)

    def test_script_with_custom_script(self):
        project = self.mock_project()
        template_dir = os.path.join(project.root_directory(), 'templates')
        os.mkdir(template_dir)
        with open(os.path.join(template_dir, 'run.sh'), 'w') as file:
            file.write("{% extends base_run %}\n")
            file.write("{% block header %}\n")
            file.write("THIS IS A CUSTOM SCRIPT!\n")
            file.write("{% endblock %}\n")
        for job in project:
            script = project._generate_run_script(project.next_operations(job))
            self.assertIn("THIS IS A CUSTOM SCRIPT", script)
            if job.sp.b % 2 == 0:
                self.assertIn('echo "hello"', script)
                self.assertIn(str(job), script)

    def test_run(self):
        project = self.mock_project()
        project.run()
        for job in project:
            if job.sp.b % 2 == 0:
                self.assertTrue(job.isfile('world.txt'))
            else:
                self.assertFalse(job.isfile('world.txt'))

    def test_submit_operations(self):
        MockScheduler.reset()
        project = self.mock_project()
        operations = []
        for job in project:
            operations.extend(project.next_operations(job))
        self.assertEqual(len(list(MockScheduler.jobs())), 0)
        cluster_job_id = project._store_bundled(operations)
        with redirect_stdout(StringIO()):
            project.submit_operations(_id=cluster_job_id, operations=operations)
        self.assertEqual(len(list(MockScheduler.jobs())), 1)

    def test_submit(self):
        MockScheduler.reset()
        project = self.mock_project()
        self.assertEqual(len(list(MockScheduler.jobs())), 0)
        with redirect_stdout(StringIO()):
            project.submit()
        even_jobs = [job for job in project if job.sp.b % 2 == 0]
        self.assertEqual(len(list(MockScheduler.jobs())), len(even_jobs))
        MockScheduler.reset()

    def test_submit_limited(self):
        MockScheduler.reset()
        project = self.mock_project()
        self.assertEqual(len(list(MockScheduler.jobs())), 0)
        with redirect_stdout(StringIO()):
            project.submit(num=1)
        self.assertEqual(len(list(MockScheduler.jobs())), 1)
        with redirect_stdout(StringIO()):
            project.submit(num=1)
        self.assertEqual(len(list(MockScheduler.jobs())), 2)

    def test_resubmit(self):
        MockScheduler.reset()
        project = self.mock_project()
        even_jobs = [job for job in project if job.sp.b % 2 == 0]
        self.assertEqual(len(list(MockScheduler.jobs())), 0)
        with redirect_stdout(StringIO()):
            project.submit()
            for i in range(5):  # push all jobs through the queue
                self.assertEqual(len(list(MockScheduler.jobs())), len(even_jobs))
                project.submit()
                MockScheduler.step()
        self.assertEqual(len(list(MockScheduler.jobs())), 0)

    def test_bundles(self):
        MockScheduler.reset()
        project = self.mock_project()
        self.assertEqual(len(list(MockScheduler.jobs())), 0)
        with redirect_stdout(StringIO()):
            project.submit(bundle_size=2, num=2)
            self.assertEqual(len(list(MockScheduler.jobs())), 1)
            project.submit(bundle_size=2, num=4)
            self.assertEqual(len(list(MockScheduler.jobs())), 3)
            MockScheduler.reset()
            project.fetch_status(file=StringIO())
            project.submit(bundle_size=0)
            self.assertEqual(len(list(MockScheduler.jobs())), 1)

    def test_submit_status(self):
        MockScheduler.reset()
        project = self.mock_project()
        even_jobs = [job for job in project if job.sp.b % 2 == 0]
        for job in project:
            if job not in even_jobs:
                continue
            list(project.classify(job))
            self.assertEqual(project.next_operation(job).name, 'a_op')
            self.assertEqual(project.next_operation(job).job, job)
        with redirect_stdout(StringIO()):
            project.submit()
        self.assertEqual(len(list(MockScheduler.jobs())), len(even_jobs))

        for job in project:
            next_op = project.next_operation(job)
            if job in even_jobs:
                self.assertIsNotNone(next_op)
                self.assertEqual(next_op.get_status(), JobStatus.submitted)
            else:
                self.assertIsNone(next_op)

        MockScheduler.step()
        MockScheduler.step()
        project.fetch_status(file=StringIO())

        for job in project:
            next_op = project.next_operation(job)
            if job in even_jobs:
                self.assertIsNotNone(next_op)
                self.assertEqual(next_op.get_status(), JobStatus.queued)
            else:
                self.assertIsNone(next_op)

    def test_init(self):
        with open(os.devnull, 'w') as out:
            for fn in init(root=self._tmp_dir.name, out=out):
                self.assertTrue(os.path.isfile(fn))

    @unittest.skipIf(__name__ != '__main__', 'can only be tested if __main__')
    def test_main(self):
        project = self.mock_project()
        with redirect_stderr(StringIO()):
            with redirect_stdout(StringIO()):
                with self.assertRaises(SystemExit):
                    project.main()


if __name__ == '__main__':
    unittest.main()
