# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import unittest
import io
import uuid
import os

from signac.common import six
from flow import FlowProject
from flow import JobOperation
from flow import get_environment
from flow.manage import Scheduler
from flow.manage import ClusterJob
from flow.manage import JobStatus
from flow.environment import ComputeEnvironment
from flow import label
from flow import classlabel
from flow import staticlabel
from flow import init

if six.PY2:
    from tempdir import TemporaryDirectory
else:
    from tempfile import TemporaryDirectory


class StringIO(io.StringIO):
    "PY27 compatibility layer."

    def write(self, s):
        if six.PY2:
            super(StringIO, self).write(unicode(s))
        else:
            super(StringIO, self).write(s)


class MockScheduler(Scheduler):
    _jobs = {}  # needs to be singleton

    def jobs(self):
        for job in self._jobs.values():
            yield job

    def submit(self, script, _id, *args, **kwargs):
        cid = uuid.uuid4()
        self._jobs[cid] = ClusterJob(_id, status=JobStatus.submitted)
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


class MockProject(FlowProject):

    @label()
    def a(self, job):
        return True

    @classlabel()
    def b(cls, job):
        return True

    @staticlabel()
    def c(job):
        return True

    @staticlabel()
    def b_is_even(job):
        return job.sp.b % 2 == 0

    def classify(self, job):
        if 'a' in job.statepoint() and not job.document.get('a', False):
            yield 'has_a'
        if job.sp.b % 2 == 0:
            yield 'b_is_even'

    def next_operation(self, job):
        labels = self.classify(job)
        if 'has_a' in labels:
            return JobOperation('a_op', job, 'run_a_op {}'.format(job))

    def submit_user(self, env, _id, operations, **kwargs):
        js = env.script(_id=_id)
        for op in operations:
            js.write(op.cmd)
        return env.submit(js, _id=_id)


class ProjectTest(unittest.TestCase):

    def setUp(self):
        self._tmp_dir = TemporaryDirectory(prefix='signac-flow_')
        self.addCleanup(self._tmp_dir.cleanup)
        self.project = FlowProject.init_project(
            name='FlowTestProject',
            root=self._tmp_dir.name)

    def mock_project(self):
        project = MockProject.get_project(root=self._tmp_dir.name)
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
            if job.sp.b % 2:
                self.assertEqual(set(labels), {'a', 'b', 'c'})
            else:
                self.assertEqual(len(labels), 4)
                self.assertIn('b_is_even', labels)

    def test_print_status(self):
        project = self.mock_project()
        for job in project:
            list(project.classify(job))
            self.assertEqual(project.next_operation(job).name, 'a_op')
            self.assertEqual(project.next_operation(job).job, job)
        fd = StringIO()
        project.print_status(file=fd, err=fd)

    def test_single_submit(self):
        env = get_environment()
        env.scheduler_type.reset()
        self.assertTrue(issubclass(env, MockEnvironment))
        sscript = env.script()
        env.submit(sscript, _id='test')
        scheduler = env.get_scheduler()
        self.assertEqual(len(list(scheduler.jobs())), 1)
        for job in scheduler.jobs():
            self.assertEqual(job.status(), JobStatus.submitted)

    def test_submit(self):
        env = get_environment()
        sched = env.scheduler_type()
        sched.reset()
        project = self.mock_project()
        self.assertEqual(len(list(sched.jobs())), 0)
        project.submit(env)
        self.assertEqual(len(list(sched.jobs())), len(project))
        sched.reset()

    def test_submit_limited(self):
        env = get_environment()
        sched = env.scheduler_type()
        sched.reset()
        project = self.mock_project()
        self.assertEqual(len(list(sched.jobs())), 0)
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
        project.submit(env, bundle_size=2, num=2)
        self.assertEqual(len(list(sched.jobs())), 1)
        project.submit(env, bundle_size=2, num=4)
        self.assertEqual(len(list(sched.jobs())), 3)
        sched.reset()
        project.update_stati(sched, file=StringIO())
        project.submit(env, bundle_size=0)
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
        fd = StringIO()
        project.submit(env)
        self.assertEqual(len(list(sched.jobs())), len(project))

        for job in project:
            self.assertEqual(project.next_operation(job).get_status(), JobStatus.submitted)

        sched.step()
        sched.step()
        project.update_stati(sched, file=fd)

        for job in project:
            self.assertEqual(project.next_operation(job).get_status(), JobStatus.queued)

    def test_init(self):
        with open(os.devnull, 'w') as out:
            for fn in init(root=self._tmp_dir.name, out=out):
                self.assertTrue(os.path.isfile(fn))



if __name__ == '__main__':
    unittest.main()
