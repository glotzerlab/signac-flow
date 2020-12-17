# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import collections.abc
import inspect
import logging
import os
import subprocess
import sys
import tempfile
import uuid
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from distutils.version import StrictVersion
from functools import partial
from io import StringIO
from itertools import groupby
from tempfile import TemporaryDirectory

import pytest
import signac
from define_dag_test_project import DagTestProject
from define_test_project import _DynamicTestProject, _TestProject
from deprecation import fail_if_not_removed

import flow
from flow import FlowProject, cmd, directives, init, with_job
from flow.environment import ComputeEnvironment
from flow.scheduling.base import ClusterJob, JobStatus, Scheduler
from flow.util.misc import (
    add_cwd_to_environment_pythonpath,
    add_path_to_environment_pythonpath,
    switch_to_directory,
)


@contextmanager
def suspend_logging():
    try:
        logging.disable(logging.WARNING)
        yield
    finally:
        logging.disable(logging.NOTSET)


class MockScheduler(Scheduler):
    _jobs = {}  # needs to be singleton
    _scripts = {}
    _invalid_chars = []

    @classmethod
    def jobs(cls):
        for job in cls._jobs.values():
            for char in cls._invalid_chars:
                if char in job._id():
                    raise RuntimeError(f"Invalid character in job id: {char}")
            yield job

    @classmethod
    def submit(cls, script, _id=None, *args, **kwargs):
        if _id is None:
            for line in script:
                _id = str(line).strip()
                break
        cid = uuid.uuid4()
        cls._jobs[cid] = ClusterJob(_id, status=JobStatus.submitted)
        signac_path = os.path.dirname(os.path.dirname(os.path.abspath(signac.__file__)))
        flow_path = os.path.dirname(os.path.dirname(os.path.abspath(flow.__file__)))
        pythonpath = ":".join(
            [os.environ.get("PYTHONPATH", "")] + [signac_path, flow_path]
        )
        cls._scripts[cid] = f"export PYTHONPATH={pythonpath}\n" + script
        return JobStatus.submitted

    @classmethod
    def step(cls):
        """Mock pushing of jobs through the queue."""
        remove = set()
        for cid, job in cls._jobs.items():
            if job._status == JobStatus.inactive:
                remove.add(cid)
            else:
                if job._status in (JobStatus.submitted, JobStatus.held):
                    job._status = JobStatus(job._status + 1)
                elif job._status == JobStatus.queued:
                    job._status = JobStatus.active
                    try:
                        with tempfile.NamedTemporaryFile() as tmpfile:
                            tmpfile.write(cls._scripts[cid].encode("utf-8"))
                            tmpfile.flush()
                            subprocess.check_call(
                                ["/bin/bash", tmpfile.name], stderr=subprocess.DEVNULL
                            )
                    except Exception:
                        job._status = JobStatus.error
                        raise
                    else:
                        job._status = JobStatus.inactive
                else:
                    raise RuntimeError(f"Unable to process status '{job._status}'.")
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


class TestProjectBase:
    project_class = signac.Project
    entrypoint = dict(path="")

    @pytest.fixture(autouse=True)
    def setUp(self, request):
        self._tmp_dir = TemporaryDirectory(prefix="signac-flow_")
        request.addfinalizer(self._tmp_dir.cleanup)
        self.project = self.project_class.init_project(
            name="FlowTestProject", root=self._tmp_dir.name
        )

    def mock_project(
        self, project_class=None, heterogeneous=False, config_overrides=None
    ):
        project_class = project_class if project_class else self.project_class
        project = project_class.get_project(root=self._tmp_dir.name)
        if config_overrides is not None:

            def recursive_update(d, u):
                for k, v in u.items():
                    if isinstance(v, collections.abc.Mapping):
                        d[k] = recursive_update(d.get(k, {}), v)
                    else:
                        d[k] = v
                return d

            config = project.config.copy()
            config = recursive_update(config, config_overrides)
            project = project_class(config=config)
        for a in range(3):
            if heterogeneous:
                # Add jobs with only the `a` key without `b`.
                project.open_job(dict(a=a)).init()
                project.open_job(dict(a=dict(a=a))).init()
            for b in range(3):
                project.open_job(dict(a=a, b=b)).init()
                project.open_job(dict(a=dict(a=a), b=b)).init()
        project._entrypoint = self.entrypoint
        return project


# Tests for single operation groups
class TestProjectStatusPerformance(TestProjectBase):
    class Project(FlowProject):
        pass

    @Project.operation
    @Project.post.isfile("DOES_NOT_EXIST")
    def foo(job):
        pass

    project_class = Project

    def mock_project(self):
        project = self.project_class.get_project(root=self._tmp_dir.name)
        for i in range(1000):
            project.open_job(dict(i=i)).init()
        return project

    @pytest.mark.skipif(
        signac.__version__ < "1.3.0",
        reason="Project.__contains__ was refactored to run in constant time "
        "in signac version 1.3.0. Generating status output relies on "
        "this check and is therefore very slow for signac versions "
        "below 1.3.0.",
    )
    def test_status_performance(self):
        """Ensure that status updates take less than 1 second for a data space of 1000 jobs."""
        import timeit

        project = self.mock_project()

        MockScheduler.reset()

        time = timeit.timeit(
            lambda: project._fetch_status(
                None, project, StringIO(), ignore_errors=False
            ),
            number=10,
        )
        assert time < 10
        MockScheduler.reset()


class TestProjectClass(TestProjectBase):
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

        with suspend_logging():
            a = A.get_project(root=self._tmp_dir.name)
            b = B.get_project(root=self._tmp_dir.name)
            c = C.get_project(root=self._tmp_dir.name)

        assert len(a.operations) == 2
        assert len(b.operations) == 3
        assert len(c.operations) == 1

    def test_repeat_operation_definition(self):
        class A(FlowProject):
            pass

        with pytest.raises(ValueError):

            @A.operation
            @A.operation
            def op1(job):
                pass

        return

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
        with suspend_logging():
            A.get_project(root=self._tmp_dir.name)

        with pytest.raises(ValueError):
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

        assert len(a._label_functions) == 1
        assert len(b._label_functions) == 2
        assert len(c._label_functions) == 1

    def test_conditions_with_inheritance(self):
        """Tests the inheritance of preconditions/postconditions.

        Class A should only have one precondition/postcondition, while class C
        that inherits from A should have three, and class B should just have
        two explicitly defined. Proper execution is tested in the
        TestExecutionProject.
        """

        class A(FlowProject):
            pass

        class B(FlowProject):
            pass

        class C(A):
            pass

        @A.pre.true("test_A")
        @C.pre.true("test_C")
        @C.pre.true("test_C")
        @B.pre.true("test_B")
        @B.pre.true("test_B")
        @A.operation
        @B.operation
        def op1(job):
            pass

        assert len(A._collect_pre_conditions()[op1]) == 1
        assert len(B._collect_pre_conditions()[op1]) == 2
        assert len(C._collect_pre_conditions()[op1]) == 3

        @A.post.true("test_A")
        @C.post.true("test_C")
        @C.post.true("test_C")
        @B.post.true("test_B")
        @B.post.true("test_B")
        @A.operation
        @B.operation
        def op2(job):
            pass

        assert len(A._collect_post_conditions()[op2]) == 1
        assert len(B._collect_post_conditions()[op2]) == 2
        assert len(C._collect_post_conditions()[op2]) == 3

    def test_with_job_decorator(self):
        class A(FlowProject):
            pass

        @A.operation
        @with_job
        def test_context(job):
            assert os.path.realpath(os.getcwd()) == os.path.realpath(job.ws)

        project = self.mock_project(A)
        with add_cwd_to_environment_pythonpath():
            with switch_to_directory(project.root_directory()):
                starting_dir = os.getcwd()
                with redirect_stderr(StringIO()):
                    project.run()
                assert os.getcwd() == starting_dir

    def test_cmd_with_job_wrong_order(self):
        class A(FlowProject):
            pass

        with pytest.raises(RuntimeError):

            @A.operation
            @cmd
            @with_job
            def test_cmd(job):
                pass

    def test_with_job_works_with_cmd(self):
        class A(FlowProject):
            pass

        @A.operation
        @with_job
        @cmd
        def test_context(job):
            return "echo 'hello' > world.txt"

        project = self.mock_project(A)
        with add_cwd_to_environment_pythonpath():
            with switch_to_directory(project.root_directory()):
                starting_dir = os.getcwd()
                with redirect_stderr(StringIO()):
                    project.run()
                assert os.getcwd() == starting_dir
                for job in project:
                    assert os.path.isfile(job.fn("world.txt"))

    def test_with_job_error_handling(self):
        class A(FlowProject):
            pass

        @A.operation
        @with_job
        def test_context(job):
            raise Exception

        project = self.mock_project(A)
        with add_cwd_to_environment_pythonpath():
            with switch_to_directory(project.root_directory()):
                starting_dir = os.getcwd()
                with pytest.raises(Exception):
                    with redirect_stderr(StringIO()):
                        project.run()
                assert os.getcwd() == starting_dir

    def test_cmd_with_job_error_handling(self):
        class A(FlowProject):
            pass

        @A.operation
        @with_job
        @cmd
        def test_context(job):
            return "exit 1"

        project = self.mock_project(A)
        with add_cwd_to_environment_pythonpath():
            with switch_to_directory(project.root_directory()):
                starting_dir = os.getcwd()
                with pytest.raises(subprocess.CalledProcessError):
                    with redirect_stderr(StringIO()):
                        project.run()
                assert os.getcwd() == starting_dir

    def test_function_in_directives(self):
        class A(FlowProject):
            pass

        @A.operation
        @directives(executable=lambda job: f"mpirun -np {job.doc.np} python")
        def test_context(job):
            return "exit 1"

        project = self.mock_project(A)
        for job in project:
            job.doc.np = 3
            for next_op in project._next_operations([(job,)]):
                assert "mpirun -np 3 python" in next_op.cmd
            break

    def test_callable_directives(self):
        class A(FlowProject):
            pass

        @A.operation
        @directives(nranks=lambda job: job.doc.get("nranks", 1))
        @directives(omp_num_threads=lambda job: job.doc.get("omp_num_threads", 1))
        def a(job):
            return "hello!"

        project = self.mock_project(A)

        # test setting neither nranks nor omp_num_threads
        for job in project:
            for next_op in project._next_operations([(job,)]):
                assert next_op.directives["np"] == 1

        # test only setting nranks
        for i, job in enumerate(project):
            job.doc.nranks = i + 1
            for next_op in project._next_operations([(job,)]):
                assert next_op.directives["np"] == next_op.directives["nranks"]
            del job.doc["nranks"]

        # test only setting omp_num_threads
        for i, job in enumerate(project):
            job.doc.omp_num_threads = i + 1
            for next_op in project._next_operations([(job,)]):
                assert next_op.directives["np"] == next_op.directives["omp_num_threads"]
            del job.doc["omp_num_threads"]
        # test setting both nranks and omp_num_threads
        for i, job in enumerate(project):
            job.doc.omp_num_threads = i + 1
            job.doc.nranks = i % 3 + 1
            expected_np = (i + 1) * (i % 3 + 1)
            for next_op in project._next_operations([(job,)]):
                assert next_op.directives["np"] == expected_np

    def test_copy_conditions(self):
        class A(FlowProject):
            pass

        @A.operation
        @A.post(lambda job: "a" in job.doc)
        def op1(job):
            job.doc.a = True

        @A.operation
        @A.post.true("b")
        def op2(job):
            job.doc.b = True

        @A.operation
        @A.pre.after(op1, op2)
        @A.post.true("c")
        def op3(job):
            job.doc.c = True

        @A.operation
        @A.pre.copy_from(op1, op3)
        @A.post.true("d")
        def op4(job):
            job.doc.d = True

        project = self.mock_project(project_class=A)
        op3_ = project.operations["op3"]
        op4_ = project.operations["op4"]
        for job in project:
            assert not op3_._eligible((job,))
            assert not op4_._eligible((job,))

        project.run(names=["op1"])
        for job in project:
            assert job.doc.a
            assert "b" not in job.doc
            assert "c" not in job.doc
            assert "d" not in job.doc
            assert not op3_._eligible((job,))
            assert not op4_._eligible((job,))

        project.run(names=["op2"])
        for job in project:
            assert op3_._eligible((job,))
            assert op4_._eligible((job,))

        project.run()
        for job in project:
            assert job.doc.a
            assert job.doc.b
            assert job.doc.c

    def test_precondition_postcondition(self):
        class A(FlowProject):
            pass

        def condition_fun():
            pass

        @A.operation
        def op1(job):
            pass

        with pytest.raises(ValueError):

            @A.operation
            @A.pre.after(condition_fun)
            def op2(job):
                pass

        with pytest.raises(ValueError):

            @A.operation
            @A.pre(op1)
            def op3(job):
                pass

        with pytest.raises(ValueError):

            @A.operation
            @A.post(op1)
            def op4(job):
                pass

    def test_condition_using_functools(self):
        """Tests that error isn't raised when a tag cannot be autogenerated for a condition."""

        def cond(job, extra_arg):
            return extra_arg

        class A(FlowProject):
            pass

        @A.operation
        @A.post(partial(cond, extra_arg=True))
        def op1(job):
            job.doc.a = True


class TestProject(TestProjectBase):
    project_class = _TestProject
    entrypoint = dict(
        path=os.path.realpath(
            os.path.join(os.path.dirname(__file__), "define_test_project.py")
        )
    )

    def test_instance(self):
        assert isinstance(self.project, FlowProject)

    def test_labels(self):
        project = self.mock_project()
        for job in project:
            labels = list(project.labels(job))
            assert len(labels) == 3 - (job.sp.b % 2)
            assert all(isinstance(label, str) for label in labels)
            assert "default_label" in labels
            assert "negative_default_label" not in labels
            assert "named_label" in labels
            assert "anonymous_label" not in labels

    def test_next_operations(self):
        project = self.mock_project()
        even_jobs = [job for job in project if job.sp.b % 2 == 0]
        for job in project:
            for i, op in enumerate(project._next_operations([(job,)])):
                assert op._jobs == (job,)
                if job in even_jobs:
                    assert op.name == ["op1", "op2", "op3"][i]
                else:
                    assert op.name == ["op2", "op3"][i]
            assert i == 2 if job in even_jobs else 1

    def test_get_job_status(self):
        project = self.mock_project()
        for job in project:
            status = project.get_job_status(job)
            assert status["job_id"] == job.get_id()
            assert len(status["operations"]) == len(project.operations)
            for op in project._next_operations([(job,)]):
                assert op.name in status["operations"]
                op_status = status["operations"][op.name]
                assert op_status["eligible"] == project.operations[op.name]._eligible(
                    (job,)
                )
                assert op_status["completed"] == project.operations[op.name]._complete(
                    (job,)
                )
                assert op_status["scheduler_status"] == JobStatus.unknown

    def test_project_status_homogeneous_schema(self):
        project = self.mock_project()
        for parameters in (None, True, ["a"], ["b"], ["a", "b"]):
            with redirect_stdout(StringIO()):
                with redirect_stderr(StringIO()):
                    project.print_status(parameters=parameters, detailed=True)

    def test_serial_project_status_homogeneous_schema(self):
        project = self.mock_project(
            config_overrides={"flow": {"status_parallelization": "none"}}
        )
        for parameters in (None, True, ["a"], ["b"], ["a", "b"]):
            with redirect_stdout(StringIO()):
                with redirect_stderr(StringIO()):
                    project.print_status(parameters=parameters, detailed=True)

    def test_process_parallelized_project_status_homogeneous_schema(self):
        project = self.mock_project(
            config_overrides={"flow": {"status_parallelization": "process"}}
        )
        for parameters in (None, True, ["a"], ["b"], ["a", "b"]):
            with redirect_stdout(StringIO()):
                with redirect_stderr(StringIO()):
                    project.print_status(parameters=parameters, detailed=True)

    def test_project_status_invalid_parallelization_config(self):
        project = self.mock_project(
            config_overrides={"flow": {"status_parallelization": "invalid"}}
        )
        for parameters in (None, True, ["a"], ["b"], ["a", "b"]):
            with redirect_stdout(StringIO()):
                with redirect_stderr(StringIO()):
                    with pytest.raises(RuntimeError):
                        project.print_status(parameters=parameters, detailed=True)

    def test_project_status_heterogeneous_schema(self):
        project = self.mock_project(heterogeneous=True)
        for parameters in (None, True, ["a"], ["b"], ["a", "b"]):
            with redirect_stdout(StringIO()):
                with redirect_stderr(StringIO()):
                    project.print_status(parameters=parameters, detailed=True)

    def test_script(self):
        project = self.mock_project()
        for job in project:
            script = project._script(project._next_operations([(job,)]))
            if job.sp.b % 2 == 0:
                assert str(job) in script
                assert 'echo "hello"' in script
                assert "exec op2" in script
            else:
                assert str(job) in script
                assert 'echo "hello"' not in script
                assert "exec op2" in script

    def test_script_with_custom_script(self):
        project = self.mock_project()
        template_dir = project._template_dir
        os.mkdir(template_dir)
        with open(os.path.join(template_dir, "script.sh"), "w") as file:
            file.write("{% extends base_script %}\n")
            file.write("{% block header %}\n")
            file.write("THIS IS A CUSTOM SCRIPT!\n")
            file.write("{% endblock %}\n")
        for job in project:
            script = project._script(project._next_operations([(job,)]))
            assert "THIS IS A CUSTOM SCRIPT" in script
            if job.sp.b % 2 == 0:
                assert str(job) in script
                assert 'echo "hello"' in script
                assert "exec op2" in script
            else:
                assert str(job) in script
                assert 'echo "hello"' not in script
                assert "exec op2" in script

    def test_init(self):
        with open(os.devnull, "w") as out:
            for fn in init(root=self._tmp_dir.name, out=out):
                fn_ = os.path.join(self._tmp_dir.name, fn)
                assert os.path.isfile(fn_)

    def test_graph_detection_error_raising(self):
        """Test failure when condition does not have tag and success when manual tag set."""

        def cond(job, extra_arg):
            return extra_arg

        # Test that graph detection fails properly when no tag is assigned
        class A(FlowProject):
            pass

        @A.operation
        @A.post(partial(cond, extra_arg=True))
        def op1(job):
            job.doc.a = True

        with pytest.raises(RuntimeError):
            self.mock_project(project_class=A).detect_operation_graph()

        # Test explicitly setting tag works
        class B(FlowProject):
            pass

        @B.operation
        @B.post(partial(cond, extra_arg=True), tag="tag")
        def op2(job):
            job.doc.a = True

        self.mock_project(project_class=B).detect_operation_graph()


execution_orders = (
    None,
    "none",
    "cyclic",
    "by-job",
    "random",
    lambda op: (op.name, op._jobs[0].get_id()),
)


class TestExecutionProject(TestProjectBase):
    project_class = _TestProject
    expected_number_of_steps = 4
    entrypoint = dict(
        path=os.path.realpath(
            os.path.join(os.path.dirname(__file__), "define_test_project.py")
        )
    )

    def test_pending_operations_order(self):
        # The execution order of local runs is internally assumed to be
        # 'by-op' by default.
        project = self.mock_project()
        ops = list(project._get_pending_operations())
        # The length of the list of job-operations grouped by operation is equal
        # to the length of its set if and only if the job-operations are grouped
        # by operations already:
        jobs_order_none = [name for name, _ in groupby(ops, key=lambda op: op.name)]
        assert len(jobs_order_none) == len(set(jobs_order_none))

    def test_run_invalid_order(self):
        project = self.mock_project()
        with pytest.raises(ValueError):
            project.run(order="invalid-order")

    @pytest.mark.parametrize("order", execution_orders)
    def test_run_order(self, order):
        project = self.mock_project()
        output = StringIO()
        with add_cwd_to_environment_pythonpath():
            with switch_to_directory(project.root_directory()):
                with redirect_stderr(output):
                    project.run(order=order)
        output.seek(0)
        output.read()
        even_jobs = [job for job in project if job.sp.b % 2 == 0]
        for job in project:
            if job in even_jobs:
                assert job.isfile("world.txt")
            else:
                assert not job.isfile("world.txt")

    def test_run_with_selection(self):
        project = self.mock_project()
        output = StringIO()
        with add_cwd_to_environment_pythonpath():
            with switch_to_directory(project.root_directory()):
                with redirect_stderr(output):
                    if StrictVersion(signac.__version__) < StrictVersion("0.9.4"):
                        project.run(list(project.find_jobs(dict(a=0))))
                    else:
                        project.run(project.find_jobs(dict(a=0)))
        output.seek(0)
        output.read()
        even_jobs = [job for job in project if job.sp.b % 2 == 0]
        for job in project:
            if job in even_jobs and job.sp.a == 0:
                assert job.isfile("world.txt")
            else:
                assert not job.isfile("world.txt")

    def test_run_with_operation_selection(self):
        project = self.mock_project()
        even_jobs = [job for job in project if job.sp.b % 2 == 0]
        with add_cwd_to_environment_pythonpath():
            with switch_to_directory(project.root_directory()):
                with pytest.raises(ValueError):
                    # The names argument must be a sequence of strings, not a string.
                    project.run(names="op1")
                project.run(names=["non-existent-op"])
                assert not any(job.isfile("world.txt") for job in even_jobs)
                assert not any(job.doc.get("test") for job in project)
                project.run(names=["op1", "non-existent-op"])
                assert all(job.isfile("world.txt") for job in even_jobs)
                assert not any(job.doc.get("test") for job in project)
                project.run(names=["op[^4]", "non-existent-op"])
                assert all(job.isfile("world.txt") for job in even_jobs)
                assert all(job.doc.get("test") for job in project)
                assert all("dynamic" not in job.doc for job in project)

    def test_run_parallel(self):
        project = self.mock_project()
        output = StringIO()
        with add_cwd_to_environment_pythonpath():
            with switch_to_directory(project.root_directory()):
                with redirect_stderr(output):
                    project.run(np=2)
        output.seek(0)
        output.read()
        even_jobs = [job for job in project if job.sp.b % 2 == 0]
        for job in project:
            if job in even_jobs:
                assert job.isfile("world.txt")
            else:
                assert not job.isfile("world.txt")

    def test_run_condition_inheritance(self):

        # This assignment is necessary to use the `mock_project` function on
        # classes A, B, and C. Otherwise, the self.project_class reassignment
        # would break future tests.

        class A(FlowProject):
            pass

        class B(FlowProject):
            pass

        class C(A):
            pass

        @A.pre.never
        @A.post.never
        @B.post.never
        @A.operation
        @B.operation
        def op1(job):
            job.doc.op1 = True

        @B.pre.never
        @A.post.never
        @B.post.never
        @A.operation
        @B.operation
        def op2(job):
            job.doc.op2 = True

        @C.pre.never
        @B.pre.never
        @A.post.never
        @B.post.never
        @A.operation
        @B.operation
        def op3(job):
            job.doc.op3 = True

        all_ops = {"op1", "op2", "op3"}
        for project_class, bad_ops in zip(
            [A, B, C], [["op1"], ["op2", "op3"], ["op1", "op3"]]
        ):

            for job in self.project.find_jobs():
                job.remove()
            project = self.mock_project(project_class=project_class)
            project.run()
            # All bad operations do not run
            assert all(
                [not job.doc.get(op, False) for op in bad_ops for job in project]
            )
            # All good operations do run
            good_ops = all_ops.difference(bad_ops)
            assert all([job.doc.get(op, False) for op in good_ops for job in project])

    def test_run_fork(self):
        project = self.mock_project()
        output = StringIO()
        for job in project:
            job.doc.fork = True
            break

        with add_cwd_to_environment_pythonpath():
            with switch_to_directory(project.root_directory()):
                with redirect_stderr(output):
                    project.run()

        for job in project:
            if job.doc.get("fork"):
                assert os.getpid() != job.doc.test
            else:
                assert os.getpid() == job.doc.test

    def test_submit_operations(self):
        MockScheduler.reset()
        project = self.mock_project()
        operations = []
        for job in project:
            operations.extend(project._next_operations([(job,)]))
        assert len(list(MockScheduler.jobs())) == 0
        cluster_job_id = project._store_bundled(operations)
        with redirect_stderr(StringIO()):
            project._submit_operations(_id=cluster_job_id, operations=operations)
        assert len(list(MockScheduler.jobs())) == 1

    def test_submit(self):
        MockScheduler.reset()
        project = self.mock_project()
        assert len(list(MockScheduler.jobs())) == 0
        with redirect_stderr(StringIO()):
            project.submit()
        even_jobs = [job for job in project if job.sp.b % 2 == 0]
        num_jobs_submitted = (2 * len(project)) + len(even_jobs)
        assert len(list(MockScheduler.jobs())) == num_jobs_submitted
        MockScheduler.reset()

    def test_submit_bad_names_argument(self):
        MockScheduler.reset()
        project = self.mock_project()
        assert len(list(MockScheduler.jobs())) == 0
        with pytest.raises(ValueError):
            project.submit(names="foo")
        project.submit(names=["foo"])

    def test_submit_limited(self):
        MockScheduler.reset()
        project = self.mock_project()
        assert len(list(MockScheduler.jobs())) == 0
        with redirect_stderr(StringIO()):
            project.submit(num=1)
        assert len(list(MockScheduler.jobs())) == 1
        with redirect_stderr(StringIO()):
            project.submit(num=1)
        assert len(list(MockScheduler.jobs())) == 2

    def test_resubmit(self):
        MockScheduler.reset()
        project = self.mock_project()
        even_jobs = [job for job in project if job.sp.b % 2 == 0]
        num_jobs_submitted = (2 * len(project)) + len(even_jobs)
        assert len(list(MockScheduler.jobs())) == 0
        with redirect_stderr(StringIO()):
            # Initial submission
            project.submit()
            assert len(list(MockScheduler.jobs())) == num_jobs_submitted

            # Resubmit a bunch of times:
            for i in range(1, self.expected_number_of_steps + 3):
                MockScheduler.step()
                project.submit()
                if len(list(MockScheduler.jobs())) == 0:
                    break  # break when there are no jobs left

        # Check that the actually required number of steps is equal to the expected number:
        assert i == self.expected_number_of_steps

    def test_bundles(self):
        MockScheduler.reset()
        project = self.mock_project()
        assert len(list(MockScheduler.jobs())) == 0
        with redirect_stderr(StringIO()):
            project.submit(bundle_size=2, num=2)
            assert len(list(MockScheduler.jobs())) == 1
            project.submit(bundle_size=2, num=4)
            assert len(list(MockScheduler.jobs())) == 3
            MockScheduler.reset()
            project._fetch_scheduler_status(file=StringIO())
            project.submit(bundle_size=0)
            assert len(list(MockScheduler.jobs())) == 1

    def test_submit_status(self):
        MockScheduler.reset()
        project = self.mock_project()
        even_jobs = [job for job in project if job.sp.b % 2 == 0]
        num_jobs_submitted = (2 * len(project)) + len(even_jobs)
        for job in project:
            if job not in even_jobs:
                continue
            list(project.labels(job))
            next_op = list(project._next_operations([(job,)]))[0]
            assert next_op.name == "op1"
            assert next_op._jobs == (job,)
        with redirect_stderr(StringIO()):
            project.submit()
        assert len(list(MockScheduler.jobs())) == num_jobs_submitted

        for job in project:
            next_op = list(project._next_operations([(job,)]))[0]
            assert next_op.get_status() == JobStatus.submitted

        MockScheduler.step()
        MockScheduler.step()
        project._fetch_scheduler_status(file=StringIO())

        for job in project:
            next_op = list(project._next_operations([(job,)]))[0]
            assert next_op.get_status() == JobStatus.queued

        MockScheduler.step()
        project._fetch_scheduler_status(file=StringIO())
        for job in project:
            job_status = project.get_job_status(job)
            for op in ("op1", "op2"):
                assert job_status["operations"][op]["scheduler_status"] in (
                    JobStatus.unknown,
                    JobStatus.inactive,
                )

    def test_submit_operations_bad_directive(self):
        MockScheduler.reset()
        project = self.mock_project()
        operations = []
        for job in project:
            operations.extend(project._next_operations([(job,)]))
        assert len(list(MockScheduler.jobs())) == 0
        cluster_job_id = project._store_bundled(operations)
        stderr = StringIO()
        with redirect_stderr(stderr):
            project._submit_operations(_id=cluster_job_id, operations=operations)
        assert len(list(MockScheduler.jobs())) == 1
        assert "Some of the keys provided as part of the directives were not used by the template "
        "script, including: bad_directive\n" in stderr.getvalue()

    @fail_if_not_removed
    def test_condition_evaluation(self):
        project = self.mock_project()

        evaluated = 0
        state = None

        def make_cond(cond):
            def cond_func(job):
                nonlocal evaluated
                evaluated |= cond
                return cond & state

            return cond_func

        class Project(FlowProject):
            pass

        @Project.operation
        @Project.pre(make_cond(0b1000))
        @Project.pre(make_cond(0b0100))
        @Project.post(make_cond(0b0010))
        @Project.post(make_cond(0b0001))
        def op1(job):
            pass

        project = Project(project.config)
        assert len(project)
        with redirect_stderr(StringIO()):
            for state, expected_evaluation in [
                (0b0000, 0b1000),  # First precondition is not met
                (0b0001, 0b1000),  # means only the first precondition
                (0b0010, 0b1000),  # should be evaluated.
                (0b0011, 0b1000),
                (0b0100, 0b1000),
                (0b0101, 0b1000),
                (0b0110, 0b1000),
                (0b0111, 0b1000),
                (0b1000, 0b1100),  # The first, but not the second
                (0b1001, 0b1100),  # precondition is met, need to evaluate
                (0b1010, 0b1100),  # both preconditions, but not postconditions.
                (0b1011, 0b1100),
                (0b1100, 0b1110),  # Both preconditions met, evaluate
                (0b1101, 0b1110),  # first postcondition.
                (0b1110, 0b1111),  # All preconditions and 1st postcondition
                # are met, need to evaluate all.
                (0b1111, 0b1111),  # All conditions met, need to evaluate all.
            ]:
                evaluated = 0
                project.run()
                assert evaluated == expected_evaluation


class TestUnbufferedExecutionProject(TestExecutionProject):
    def mock_project(self, project_class=None):
        project = super().mock_project(
            project_class=project_class,
            config_overrides={"flow": {"use_buffered_mode": "off"}},
        )
        return project


class TestExecutionDynamicProject(TestExecutionProject):
    project_class = _DynamicTestProject
    expected_number_of_steps = 10


class TestUnbufferedExecutionDynamicProject(
    TestUnbufferedExecutionProject, TestExecutionDynamicProject
):
    pass


class TestProjectMainInterface(TestProjectBase):
    project_class = _TestProject
    entrypoint = dict(
        path=os.path.realpath(
            os.path.join(os.path.dirname(__file__), "define_test_project.py")
        )
    )

    def switch_to_cwd(self):
        os.chdir(self.cwd)

    @pytest.fixture(autouse=True)
    def setup_main_interface(self, request):
        self.project = self.mock_project()
        self.cwd = os.getcwd()
        os.chdir(self._tmp_dir.name)
        request.addfinalizer(self.switch_to_cwd)

    def call_subcmd(self, subcmd):
        # Determine path to project module and construct command.
        fn_script = inspect.getsourcefile(type(self.project))
        _cmd = f"python {fn_script} {subcmd}"

        try:
            with add_path_to_environment_pythonpath(os.path.abspath(self.cwd)):
                with switch_to_directory(self.project.root_directory()):
                    return subprocess.check_output(
                        _cmd.split(), stderr=subprocess.DEVNULL
                    )
        except subprocess.CalledProcessError as error:
            print(error, file=sys.stderr)
            print(error.output, file=sys.stderr)
            raise

    def test_main_help(self):
        # This unit test mainly checks if the test setup works properly.
        self.call_subcmd("--help")

    def test_main_exec(self):
        assert len(self.project)
        for job in self.project:
            assert not job.doc.get("test", False)
        self.call_subcmd("exec op2")
        for job in self.project:
            assert job.doc.get("test", False)

    def test_main_run(self):
        assert len(self.project)
        for job in self.project:
            assert not job.isfile("world.txt")
        self.call_subcmd("run -o op1")
        even_jobs = [job for job in self.project if job.sp.b % 2 == 0]
        for job in self.project:
            if job in even_jobs:
                assert job.isfile("world.txt")
            else:
                assert not job.isfile("world.txt")

    def test_main_next(self):
        assert len(self.project)
        job_ids = set(self.call_subcmd("next op1").decode().split())
        even_jobs = [job.get_id() for job in self.project if job.sp.b % 2 == 0]
        assert job_ids == set(even_jobs)

    def test_main_status(self):
        assert len(self.project)
        status_output = (
            self.call_subcmd("--debug status --detailed").decode("utf-8").splitlines()
        )
        lines = iter(status_output)
        project = self.mock_project()
        num_ops = len(project.operations)
        for line in lines:
            for job in project:
                if job.get_id() in line:
                    op_lines = [line]
                    for i in range(num_ops - 1):
                        try:
                            op_lines.append(next(lines))
                        except StopIteration:
                            continue
                    for op in project._next_operations([(job,)]):
                        assert any(op.name in op_line for op_line in op_lines)

    def test_main_script(self):
        assert len(self.project)
        even_jobs = [job for job in self.project if job.sp.b % 2 == 0]
        for job in self.project:
            script_output = self.call_subcmd(f"script -j {job}").decode().splitlines()
            assert job.get_id() in "\n".join(script_output)
            if job in even_jobs:
                assert "run -o op1" in "\n".join(script_output)
            else:
                assert "run -o op1" not in "\n".join(script_output)


class TestDynamicProjectMainInterface(TestProjectMainInterface):
    project_class = _DynamicTestProject


class TestProjectDagDetection(TestProjectBase):
    """Tests of operation DAG detection."""

    project_class = DagTestProject

    def test_dag(self):
        project = self.mock_project()
        adj = project.detect_operation_graph()

        adj_correct = [
            [0, 1, 1, 0, 1, 0, 0],
            [0, 0, 0, 1, 0, 0, 0],
            [0, 0, 0, 1, 0, 1, 1],
            [0, 0, 0, 0, 0, 0, 1],
            [0, 0, 0, 0, 0, 1, 0],
            [0, 0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0, 0],
        ]

        assert adj == adj_correct


# Tests for multiple operation groups or groups with options
class TestGroupProject(TestProjectBase):
    project_class = _TestProject
    entrypoint = dict(
        path=os.path.realpath(
            os.path.join(os.path.dirname(__file__), "define_test_project.py")
        )
    )

    def test_instance(self):
        assert isinstance(self.project, FlowProject)

    def test_script(self):
        project = self.mock_project()
        # For run mode single operation groups
        for job in project:
            job_ops = project._get_submission_operations([(job,)], {})
            script = project._script(job_ops)
            if job.sp.b % 2 == 0:
                assert str(job) in script
                assert f"run -o op1 -j {job}" in script
                assert f"run -o op2 -j {job}" in script
            else:
                assert str(job) in script
                assert f"run -o op1 -j {job}" not in script
                assert f"run -o op2 -j {job}" in script

        # For multiple operation groups and options
        for job in project:
            job_op1 = project.groups["group1"]._create_submission_job_operation(
                project._entrypoint, project._get_default_directives(), (job,)
            )
            script1 = project._script([job_op1])
            assert f"run -o group1 -j {job}" in script1
            job_op2 = project.groups["group2"]._create_submission_job_operation(
                project._entrypoint, project._get_default_directives(), (job,)
            )
            script2 = project._script([job_op2])
            assert "--num-passes=2" in script2

    def test_directives_hierarchy(self):
        project = self.mock_project()
        for job in project:
            # Test submit JobOperations
            job_ops = project._get_submission_operations(
                (job,), project._get_default_directives(), names=["group2"]
            )
            assert all(
                [job_op.directives.get("omp_num_threads", 0) == 4 for job_op in job_ops]
            )
            job_ops = project._get_submission_operations(
                (job,), project._get_default_directives(), names=["op3"]
            )
            assert all(
                [job_op.directives.get("omp_num_threads", 0) == 1 for job_op in job_ops]
            )
            # Test run JobOperations
            job_ops = project.groups["group2"]._create_run_job_operations(
                project._entrypoint, project._get_default_directives(), (job,)
            )
            assert all(
                [job_op.directives.get("omp_num_threads", 0) == 4 for job_op in job_ops]
            )
            job_ops = project.groups["op3"]._create_run_job_operations(
                project._entrypoint, project._get_default_directives(), (job,)
            )
            assert all(
                [job_op.directives.get("omp_num_threads", 0) == 1 for job_op in job_ops]
            )

    def test_submission_aggregation(self):
        class A(flow.FlowProject):
            pass

        group = A.make_group("group")

        @group.with_directives(dict(ngpu=2, nranks=4))
        @A.operation
        def op1(job):
            pass

        @group.with_directives(dict(ngpu=2, nranks=4))
        @A.operation
        def op2(job):
            pass

        @group
        @A.operation
        @flow.directives(nranks=2)
        def op3(job):
            pass

        project = self.mock_project(A)
        group = project.groups["group"]
        job = [j for j in project][0]
        directives = group._get_submission_directives(
            project._get_default_directives(), (job,)
        )
        assert all(
            [directives["ngpu"] == 2, directives["nranks"] == 4, directives["np"] == 4]
        )

    def test_flowgroup_repr(self):
        class A(flow.FlowProject):
            pass

        group = A.make_group("group")

        @group.with_directives(dict(ngpu=2, nranks=4))
        @A.operation
        def op1(job):
            pass

        @group
        @A.operation
        def op2(job):
            pass

        @group
        @A.operation
        @flow.directives(nranks=2)
        def op3(job):
            pass

        project = self.mock_project(A)
        rep_string = repr(project.groups["group"])
        assert all(op in rep_string for op in ["op1", "op2", "op3"])
        assert "'nranks': 4" in rep_string
        assert "'ngpu': 2" in rep_string
        assert "options=''" in rep_string
        assert "name='group'" in rep_string


class TestGroupExecutionProject(TestProjectBase):
    project_class = _TestProject
    entrypoint = dict(
        path=os.path.realpath(
            os.path.join(os.path.dirname(__file__), "define_test_project.py")
        )
    )
    expected_number_of_steps = 4

    def test_run_with_operation_selection(self):
        project = self.mock_project()
        even_jobs = [job for job in project if job.sp.b % 2 == 0]
        with add_cwd_to_environment_pythonpath():
            with switch_to_directory(project.root_directory()):
                with pytest.raises(ValueError):
                    # The names argument must be a sequence of strings, not a string.
                    project.run(names="op1")
                project.run(names=["nonexistent-op"])
                assert not any(job.isfile("world.txt") for job in even_jobs)
                assert not any(job.doc.get("test") for job in project)
                project.run(names=["group1"])
                assert all(job.isfile("world.txt") for job in even_jobs)
                assert all(job.doc.get("test") for job in project)
                project.run(names=["group2"])
                assert all(job.isfile("world.txt") for job in even_jobs)
                assert all(job.doc.get("test3") for job in project)
                assert all("dynamic" not in job.doc for job in project)

    def test_run_parallel(self):
        project = self.mock_project()
        output = StringIO()
        with add_cwd_to_environment_pythonpath():
            with switch_to_directory(project.root_directory()):
                with redirect_stderr(output):
                    project.run(names=["group1"], np=2)
        output.seek(0)
        output.read()
        even_jobs = [job for job in project if job.sp.b % 2 == 0]
        for job in project:
            if job in even_jobs:
                assert job.isfile("world.txt")
            else:
                assert not job.isfile("world.txt")

    def test_submit_groups(self):
        MockScheduler.reset()
        project = self.mock_project()
        operations = [
            project.groups["group1"]._create_submission_job_operation(
                project._entrypoint, project._get_default_directives(), (job,)
            )
            for job in project
        ]
        assert len(list(MockScheduler.jobs())) == 0
        cluster_job_id = project._store_bundled(operations)
        with redirect_stderr(StringIO()):
            project._submit_operations(_id=cluster_job_id, operations=operations)
        assert len(list(MockScheduler.jobs())) == 1

    def test_submit_groups_invalid_char_with_error(self, monkeypatch):
        monkeypatch.setattr(MockScheduler, "_invalid_chars", ["/"])

        MockScheduler.reset()
        project = self.mock_project()
        operations = [
            project.groups["group1"]._create_submission_job_operation(
                project._entrypoint, {}, (job,)
            )
            for job in project
        ]
        assert len(list(MockScheduler.jobs())) == 0
        cluster_job_id = project._store_bundled(operations)
        with redirect_stderr(StringIO()):
            project._submit_operations(_id=cluster_job_id, operations=operations)
        with pytest.raises(RuntimeError):
            assert len(list(MockScheduler.jobs())) == 1
        MockScheduler.reset()

    def test_submit_groups_invalid_char_avoid_error(self, monkeypatch):
        monkeypatch.setattr(MockScheduler, "_invalid_chars", ["/"])
        monkeypatch.setattr(MockEnvironment, "JOB_ID_SEPARATOR", "-", raising=False)

        MockScheduler.reset()
        project = self.mock_project()
        operations = [
            project.groups["group1"]._create_submission_job_operation(
                project._entrypoint, {}, (job,)
            )
            for job in project
        ]
        assert len(list(MockScheduler.jobs())) == 0
        cluster_job_id = project._store_bundled(operations)
        with redirect_stderr(StringIO()):
            project.submit_operations(_id=cluster_job_id, operations=operations)
        assert len(list(MockScheduler.jobs())) == 1
        MockScheduler.reset()

    def test_submit(self):
        MockScheduler.reset()
        project = self.mock_project()
        assert len(list(MockScheduler.jobs())) == 0
        with redirect_stderr(StringIO()):
            project.submit(names=["group1", "group2"])
        num_jobs_submitted = 2 * len(project)
        assert len(list(MockScheduler.jobs())) == num_jobs_submitted
        MockScheduler.reset()

    def test_submit_invalid_char_with_error(self, monkeypatch):
        monkeypatch.setattr(MockScheduler, "_invalid_chars", ["/"])

        MockScheduler.reset()
        project = self.mock_project()
        assert len(list(MockScheduler.jobs())) == 0
        with redirect_stderr(StringIO()):
            project.submit(names=["group1", "group2"])
        num_jobs_submitted = 2 * len(project)
        with pytest.raises(RuntimeError):
            assert len(list(MockScheduler.jobs())) == num_jobs_submitted
        MockScheduler.reset()

    def test_submit_invalid_char_avoid_error(self, monkeypatch):
        monkeypatch.setattr(MockScheduler, "_invalid_chars", ["/"])
        monkeypatch.setattr(MockEnvironment, "JOB_ID_SEPARATOR", "-", raising=False)

        MockScheduler.reset()
        project = self.mock_project()
        assert len(list(MockScheduler.jobs())) == 0
        with redirect_stderr(StringIO()):
            project.submit(names=["group1", "group2"])
        num_jobs_submitted = 2 * len(project)
        assert len(list(MockScheduler.jobs())) == num_jobs_submitted
        MockScheduler.reset()

    def test_group_resubmit(self):
        MockScheduler.reset()
        project = self.mock_project()
        num_jobs_submitted = len(project)
        assert len(list(MockScheduler.jobs())) == 0
        with redirect_stderr(StringIO()):
            # Initial submission
            project.submit(names=["group1"])
            assert len(list(MockScheduler.jobs())) == num_jobs_submitted

            # Resubmit a bunch of times:
            for i in range(1, self.expected_number_of_steps + 3):
                MockScheduler.step()
                project.submit(names=["group1"])
                if len(list(MockScheduler.jobs())) == 0:
                    break  # break when there are no jobs left

        # Check that the actually required number of steps is equal to the expected number:
        assert i == self.expected_number_of_steps

    def test_operation_resubmit(self):
        MockScheduler.reset()
        project = self.mock_project()
        num_jobs_submitted = len(project)
        assert len(list(MockScheduler.jobs())) == 0
        with redirect_stderr(StringIO()):
            # Initial submission
            project.submit(names=["group1"])
            assert len(list(MockScheduler.jobs())) == num_jobs_submitted

            # Resubmit a bunch of times:
            for i in range(1, self.expected_number_of_steps + 3):
                MockScheduler.step()
                project.submit(names=["op1", "op2"])
                if len(list(MockScheduler.jobs())) == 0:
                    break  # break when there are no jobs left

        # Check that the actually required number of steps is equal to the expected number:
        assert i == self.expected_number_of_steps


class TestGroupUnbufferedExecutionProject(
    TestUnbufferedExecutionProject, TestGroupExecutionProject
):
    pass


class TestGroupExecutionDynamicProject(TestGroupExecutionProject):
    project_class = _DynamicTestProject
    expected_number_of_steps = 4


class TestGroupUnbufferedExecutionDynamicProject(
    TestGroupUnbufferedExecutionProject, TestGroupExecutionDynamicProject
):
    pass


class TestGroupProjectMainInterface(TestProjectBase):
    project_class = _TestProject
    entrypoint = dict(
        path=os.path.realpath(
            os.path.join(os.path.dirname(__file__), "define_test_project.py")
        )
    )

    def switch_to_cwd(self):
        os.chdir(self.cwd)

    @pytest.fixture(autouse=True)
    def setup_main_interface(self, request):
        self.project = self.mock_project()
        self.cwd = os.getcwd()
        os.chdir(self._tmp_dir.name)
        request.addfinalizer(self.switch_to_cwd)

    def call_subcmd(self, subcmd):
        # Determine path to project module and construct command.
        fn_script = inspect.getsourcefile(type(self.project))
        _cmd = f"python {fn_script} {subcmd}"

        try:
            with add_path_to_environment_pythonpath(os.path.abspath(self.cwd)):
                with switch_to_directory(self.project.root_directory()):
                    return subprocess.check_output(
                        _cmd.split(), stderr=subprocess.DEVNULL
                    )
        except subprocess.CalledProcessError as error:
            print(error, file=sys.stderr)
            print(error.output, file=sys.stderr)
            raise

    def test_main_run(self):
        assert len(self.project)
        for job in self.project:
            assert not job.isfile("world.txt")
        self.call_subcmd("run -o group1")
        even_jobs = [job for job in self.project if job.sp.b % 2 == 0]
        for job in self.project:
            assert job.doc["test"]
            if job in even_jobs:
                assert job.isfile("world.txt")
            else:
                assert not job.isfile("world.txt")

    def test_main_script(self):
        assert len(self.project)
        for job in self.project:
            script_output = (
                self.call_subcmd(f"script -j {job} -o group1").decode().splitlines()
            )
            assert job.get_id() in "\n".join(script_output)
            assert "-o group1" in "\n".join(script_output)
            script_output = (
                self.call_subcmd(f"script -j {job} -o group2").decode().splitlines()
            )
            assert "--num-passes=2" in "\n".join(script_output)

    def test_main_submit(self):
        project = self.mock_project()
        assert len(project)
        # Assert that correct output for group submission is given
        for job in project:
            submit_output = (
                self.call_subcmd(f"submit -j {job} -o group1 --pretend")
                .decode()
                .splitlines()
            )
            output_string = "\n".join(submit_output)
            assert f"run -o group1 -j {job}" in output_string
            submit_output = (
                self.call_subcmd(f"submit -j {job} -o group2 --pretend")
                .decode()
                .splitlines()
            )
            assert f"run -o group2 -j {job} --num-passes=2" in "\n".join(submit_output)


class TestGroupDynamicProjectMainInterface(TestProjectMainInterface):
    project_class = _DynamicTestProject
