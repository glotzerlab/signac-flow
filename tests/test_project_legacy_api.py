# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import unittest
import os
import warnings
from contextlib import contextmanager

from signac.common import six
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


class LegacyMockProject(MockProject):

    def write_script_header(self, script, **kwargs):
        super(LegacyMockProject, self).write_script_header(script, **kwargs)


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

    def test_instance(self):
        self.assertTrue(isinstance(self.project, FlowProject))

    def test_classify(self):
        project = self.mock_project()
        for job in project:
            labels = list(project.classify(job))
            self.assertEqual(len(labels), 2 - (job.sp.b % 2))
            self.assertTrue(all((isinstance(l, str)) for l in labels))

    def test_labels(self):
        project = self.mock_project()
        for job in project:
            labels = list(project.labels(job))
            if job.sp.a == 0:
                if job.sp.b % 2:
                    self.assertEqual(set(labels), {'a', 'b', 'c', 'has_a'})
                else:
                    self.assertEqual(len(labels), 5)
                    self.assertIn('b_is_even', labels)
            else:
                self.assertIn('a is {}'.format(job.sp.a), labels)

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

    def test_script(self):
        project = self.mock_project()
        for job in project:
            script = project.script(project.next_operations(job))
            self.assertIn('echo "hello"', script)
            self.assertIn(str(job), script)

    def test_script_with_custom_script(self):
        project = self.mock_project()
        if project._legacy_templating:
            return
        template_dir = project._template_dir
        os.mkdir(template_dir)
        with open(os.path.join(template_dir, 'script.sh'), 'w') as file:
            file.write("{% extends base_script %}\n")
            file.write("{% block header %}\n")
            file.write("THIS IS A CUSTOM SCRIPT!\n")
            file.write("{% endblock %}\n")
        for job in project:
            script = project.script(project.next_operations(job))
            self.assertIn("THIS IS A CUSTOM SCRIPT", script)
            self.assertIn('echo "hello"', script)
            self.assertIn(str(job), script)

    def test_run_operations_explicit_argument(self):
        project = self.mock_project()
        for job in project:
            ops = project.next_operations(job)
            with suspend_logging():
                with redirect_stderr(StringIO()):
                    project.run(operations=ops)
        for job in project:
            self.assertIn('said_hello', list(project.labels(job)))

    def test_run_operations_implicit_argument(self):
        project = self.mock_project()
        for job in project:
            ops = project.next_operations(job)
            with suspend_logging():
                with redirect_stderr(StringIO()):
                    project.run(ops)
        for job in project:
            self.assertIn('said_hello', list(project.labels(job)))

    def test_run(self):
        project = self.mock_project()
        with redirect_stderr(StringIO()):
            project.run()
        for job in project:
            self.assertIn('said_hello', list(project.labels(job)))

    def test_single_submit(self):
        env = get_environment()
        env.scheduler_type.reset()
        self.assertTrue(issubclass(env, MockEnvironment))
        sscript = env.script()
        with suspend_logging():
            with redirect_stdout(StringIO()):
                env.submit(sscript, _id='test')
        scheduler = env.get_scheduler()
        self.assertEqual(len(list(scheduler.jobs())), 1)
        for job in scheduler.jobs():
            self.assertEqual(job.status(), JobStatus.submitted)

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

    def test_submit(self):
        env = get_environment()
        sched = env.scheduler_type()
        sched.reset()
        project = self.mock_project()
        self.assertEqual(len(list(sched.jobs())), 0)
        with suspend_logging():
            with redirect_stdout(StringIO()):
                project.submit(env)
        self.assertEqual(len(list(sched.jobs())), len(project))
        sched.reset()

    def test_submit_limited(self):
        env = get_environment()
        sched = env.scheduler_type()
        sched.reset()
        project = self.mock_project()
        self.assertEqual(len(list(sched.jobs())), 0)
        with suspend_logging():
            with redirect_stderr(StringIO()):
                project.submit(env, num=1)
                self.assertEqual(len(list(sched.jobs())), 1)
                project.submit(env, num=1)
                self.assertEqual(len(list(sched.jobs())), 2)

    def test_resubmit(self):
        env = get_environment()
        sched = env.scheduler_type()
        sched.reset()
        project = self.mock_project()
        self.assertEqual(len(list(sched.jobs())), 0)
        with suspend_logging():
            with redirect_stdout(StringIO()):
                project.submit(env)
                for i in range(5):  # push all jobs through the queue
                    self.assertEqual(len(list(sched.jobs())), len(project))
                    project.submit(env)
                    sched.step()
        self.assertEqual(len(list(sched.jobs())), 0)

    def test_bundles(self):
        env = get_environment()
        sched = env.scheduler_type()
        sched.reset()
        project = self.mock_project()
        self.assertEqual(len(list(sched.jobs())), 0)
        with suspend_logging():
            with redirect_stderr(StringIO()):
                project.submit(bundle_size=2, num=2)
                self.assertEqual(len(list(sched.jobs())), 1)
                project.submit(bundle_size=2, num=4)
                self.assertEqual(len(list(sched.jobs())), 3)
                sched.reset()
                project._fetch_scheduler_status(file=StringIO())
                project.submit(bundle_size=0)
                self.assertEqual(len(list(sched.jobs())), 1)

    def test_submit_status(self):
        env = get_environment()
        sched = env.scheduler_type()
        sched.reset()
        project = self.mock_project()
        for job in project:
            list(project.classify(job))
            self.assertEqual(project.next_operation(job).name, 'a_op')
            self.assertEqual(project.next_operation(job).job, job)
        with suspend_logging():
            with redirect_stdout(StringIO()):
                project.submit(env)
        self.assertEqual(len(list(sched.jobs())), len(project))

        for job in project:
            self.assertEqual(project.next_operation(job).get_status(), JobStatus.submitted)

        sched.step()
        sched.step()
        project._fetch_scheduler_status(file=StringIO())

        for job in project:
            self.assertEqual(project.next_operation(job).get_status(), JobStatus.queued)

    def test_init(self):
        with open(os.devnull, 'w') as out:
            for fn in init(root=self._tmp_dir.name, out=out):
                fn_ = os.path.join(self._tmp_dir.name, fn)
                self.assertTrue(os.path.isfile(fn_))

    @unittest.skipIf(__name__ != '__main__', 'can only be tested if __main__')
    def test_main(self):
        project = self.mock_project()
        with redirect_stderr(StringIO()):
            with redirect_stdout(StringIO()):
                with self.assertRaises(SystemExit):
                    project.main()


class LegacyProjectTest(ProjectTest):
    project_class = LegacyMockProject


if __name__ == '__main__':
    unittest.main()
