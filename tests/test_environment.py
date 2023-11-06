# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.

import conftest
import pytest

import flow
from flow import get_environment
from flow.environment import (
    ComputeEnvironment,
    TestEnvironment,
    _NodeTypes,
    _PartitionConfig,
)
from flow.errors import SubmitError


class MockEnvironment(ComputeEnvironment):
    scheduler_type = conftest.MockScheduler
    _partition_config = _PartitionConfig(
        cpus_per_node={"cpu-shared": 100, "cpu-wholenode": 100, "default": 40},
        gpus_per_node={"gpu": 4},
        node_types={
            "cpu-shared": _NodeTypes.SHARED,
            "cpu-wholenode": _NodeTypes.WHOLENODE,
        },
    )

    @classmethod
    def is_present(cls):
        return True


class TestProject:
    def test_get_TestEnvironment(self):
        env = get_environment()
        assert issubclass(env, ComputeEnvironment)
        assert not issubclass(env, TestEnvironment)
        env = get_environment(test=True)
        assert issubclass(env, TestEnvironment)


class TestEnvironments(conftest.TestProjectBase):
    class Project(flow.FlowProject):
        pass

    @Project.operation(directives={"ngpu": 1})
    def gpu_op(job):
        pass

    @Project.operation(directives={"np": 1_000})
    def large_cpu_op(job):
        pass

    @Project.operation(directives={"np": 1})
    def small_cpu_op(job):
        pass

    project_class = Project

    def mock_project(self):
        project = self.project_class.get_project(path=self._tmp_dir.name)
        project.open_job({"i": 0}).init()
        project._environment = MockEnvironment
        return project

    def test_gpu_parttion_without_gpu(self):
        pr = self.mock_project()
        with pytest.raises(SubmitError):
            pr.submit(names=["small_cpu_op"], partition="gpu")

    def test_gpu_op_without_gpu_partition(self):
        pr = self.mock_project()
        with pytest.raises(SubmitError):
            pr.submit(names=["gpu_op"], partition="cpu-shared")

    def test_wholenode_submission_with_insufficient_resources(self):
        pr = self.mock_project()
        with pytest.raises(RuntimeError):
            pr.submit(names=["small_cpu_op"], partition="cpu-wholenode")

    def test_shared_submission_with_too_large_request(self):
        pr = self.mock_project()
        with pytest.raises(RuntimeError):
            pr.submit(names=["large_cpu_op"], partition="cpu-shared")

    def test_various_valid_submissions(self):
        pr = self.mock_project()
        pr.submit(names=["large_cpu_op"], partition="cpu-wholenode")
        pr.submit(names=["small_cpu_op"], partition="cpu-shared")
        pr.submit(names=["gpu_op"], partition="gpu")
