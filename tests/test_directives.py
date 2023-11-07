# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import datetime
import sys
from tempfile import TemporaryDirectory

import pytest

from flow import FlowProject
from flow.directives import (
    _GET_EXECUTABLE,
    _GPUS_PER_PROCESS,
    _LAUNCHER,
    _MEMORY_PER_CPU,
    _PROCESSES,
    _THREADS_PER_PROCESS,
    _WALLTIME,
    _Directive,
    _Directives,
)
from flow.errors import DirectivesError


@pytest.fixture()
def available_directives_list():
    return [
        _MEMORY_PER_CPU,
        _GPUS_PER_PROCESS,
        _PROCESSES,
        _THREADS_PER_PROCESS,
        _LAUNCHER,
        _GET_EXECUTABLE(),
        _WALLTIME,
    ]


@pytest.fixture()
def directives(available_directives_list):
    return _Directives(available_directives_list)


@pytest.fixture()
def product_directive():
    def val(v):
        if v < 10:
            raise ValueError("Price cannot be less than 10 units")
        return v

    product = _Directive(name="product", validator=val, default=10)
    return product


@pytest.fixture()
def non_default_directive_values():
    return [
        {
            "processes": 5,
            "threads_per_process": 20,
            "executable": "Non Default Path",
            "walltime": 64.0,
            "memory_per_cpu": 2,
            "launcher": "mpi",
        },
        {
            "processes": 4,
            "gpus_per_process": 1,
            "threads_per_process": 10,
            "executable": "PathFinder",
            "walltime": 20.0,
            "memory_per_cpu": 1,
        },
    ]


class TestItems:
    """Tests for _Directive class."""

    def test_default(self):
        assert _PROCESSES._default == 1
        assert _GPUS_PER_PROCESS._default == 0
        assert _THREADS_PER_PROCESS._default == 0
        assert _MEMORY_PER_CPU._default is None
        assert _GET_EXECUTABLE()._default == sys.executable
        assert _WALLTIME._default is None
        assert _LAUNCHER._default is None

    def test_invalid_values(self, available_directives_list):
        invalid_values = {
            "processes": [-1, "foo", {}, None],
            "gpus_per_process": [-1, "foo", {}, None],
            "threads_per_process": [-1, "foo", {}, None],
            "walltime": [-1, "foo", {}],
            "memory_per_cpu": [-1, "foo", {}],
        }

        for directive in available_directives_list:
            if directive._name in ("executable", "launcher"):
                # Executable and launcher expect a string, if not found, then it tries
                # to convert it into a string and becomes successful almost every time.
                # Hence the skipping.
                continue
            for i, value in enumerate(invalid_values[directive._name]):
                with pytest.raises((ValueError, TypeError)):
                    directive._validator(value)

    def test_defaults_are_valid(self, available_directives_list):
        for directive in available_directives_list:
            directive._validator(directive._default)

    def test_manual_item_default(self, product_directive):
        assert product_directive._default == 10

    def test_manual_item_validation(self, product_directive):
        val = product_directive._validator(product_directive._default)
        assert product_directive._default == val
        assert product_directive._validator(20) == 20
        with pytest.raises(ValueError):
            product_directive._validator(0)


class TestDirectives:
    """Tests for _Directives Class."""

    def test_get_directive(self, directives, available_directives_list):
        for item in available_directives_list:
            assert directives[item._name] == item._default

    def test_add_directive(self, available_directives_list):
        last_directive = available_directives_list.pop()
        directives = _Directives(available_directives_list)
        directives._add_directive(last_directive)
        assert directives[last_directive._name] == last_directive._default
        with pytest.raises(TypeError):
            directives._add_directive("Test")
        with pytest.raises(ValueError):
            directives._add_directive(last_directive)

    def test_set_defined_directive(self, directives):
        directives._set_defined_directive(_PROCESSES._name, 10)
        assert directives[_PROCESSES._name] == 10

    def test_set_defined_directive_invalid(self, directives):
        with pytest.raises(ValueError):
            directives._set_defined_directive(_PROCESSES._name, 0)

    def test_set_undefined_directive(self, directives):
        with pytest.raises(DirectivesError):
            directives._set_defined_directive("test", 0)

    def test_set_directives_item(self, directives):
        directives["test"] = True
        assert directives["test"]

    def test_del_directive(self, directives):
        directives["test"] = True
        directives._set_defined_directive(_PROCESSES._name, 100)
        assert directives[_PROCESSES._name] == 100
        assert directives["test"]
        del directives[_PROCESSES._name]
        assert directives[_PROCESSES._name] == _PROCESSES._default
        del directives["test"]
        with pytest.raises(KeyError):
            directives["test"]

    def test_update(self, directives, non_default_directive_values):
        new_directives = non_default_directive_values[1]
        directives.update(new_directives)
        for dir_ in new_directives:
            if dir_ == "walltime":
                assert directives[dir_] == datetime.timedelta(
                    hours=new_directives[dir_]
                )
            else:
                assert directives[dir_] == new_directives[dir_]

    def test_evaluate_directive_none_job(
        self, directives, non_default_directive_values
    ):
        directives.evaluate(None)
        valid_values = non_default_directive_values[0]
        valid_values["processes"] = lambda job: job.sp.i + 1
        directives.update(valid_values)
        with pytest.raises(RuntimeError):
            directives.evaluate(None)

    def test_evaluate_directive_valid_job(
        self, available_directives_list, non_default_directive_values
    ):
        _tmp_dir = TemporaryDirectory(prefix="flow-directives_")
        FlowProject.init_project(path=_tmp_dir.name)
        project = FlowProject.get_project(path=_tmp_dir.name)
        for i in range(5):
            project.open_job(dict(i=i)).init()

        for job in project:
            directives = _Directives(available_directives_list)
            directives.update({"processes": lambda job: job.sp.i + 1})
            directives.evaluate((job,))
            assert directives["processes"] == job.sp.i + 1
