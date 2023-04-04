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
    _MEMORY,
    _NGPU,
    _NP,
    _NRANKS,
    _OMP_NUM_THREADS,
    _PROCESSOR_FRACTION,
    _WALLTIME,
    _Directive,
    _Directives,
    _no_aggregation,
)
from flow.errors import DirectivesError


@pytest.fixture()
def available_directives_list():
    return [
        _NP,
        _NRANKS,
        _NGPU,
        _OMP_NUM_THREADS,
        _GET_EXECUTABLE(),
        _WALLTIME,
        _MEMORY,
        _PROCESSOR_FRACTION,
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

    def finalize(value, dict):
        discount = dict.get("discount", 0)
        free = dict.get("free", False)
        value = value - discount
        if value < 0 or free:
            return 0
        return value

    product = _Directive(
        name="product",
        validator=val,
        default=10,
        serial=_no_aggregation,
        parallel=_no_aggregation,
        finalize=finalize,
    )
    return product


@pytest.fixture()
def non_default_directive_values():
    return [
        {
            "np": 1,
            "ngpu": 10,
            "nranks": 5,
            "omp_num_threads": 20,
            "executable": "Non Default Path",
            "walltime": 64.0,
            "memory": 32,
            "processor_fraction": 0.5,
        },
        {
            "np": 4,
            "ngpu": 1,
            "nranks": 0,
            "omp_num_threads": 10,
            "executable": "PathFinder",
            "walltime": 20.0,
            "memory": 16,
            "processor_fraction": 0.5,
        },
    ]


class TestItems:
    """Tests for _Directive class."""

    def test_default(self):
        assert _NP._default == 1
        assert _NGPU._default == 0
        assert _NRANKS._default == 0
        assert _OMP_NUM_THREADS._default == 0
        assert _GET_EXECUTABLE()._default == sys.executable
        assert _WALLTIME._default is None
        assert _MEMORY._default is None
        assert _PROCESSOR_FRACTION._default == 1.0

    def test_invalid_values(self, available_directives_list):
        invalid_values = {
            "np": [-1, "foo", {}, None],
            "ngpu": [-1, "foo", {}, None],
            "nranks": [-1, "foo", {}, None],
            "omp_num_threads": [-1, "foo", {}, None],
            "walltime": [-1, "foo", {}],
            "memory": [-1, "foo", {}],
            "processor_fraction": [-0.5, 2.5, "foo", {}, None],
        }

        for directive in available_directives_list:
            if directive._name == "executable":
                # Executable expect a string, if not found, then it tries to convert
                # it into a string and becomes successful almost every time.
                # Hence skipping Executable.
                continue
            for i, value in enumerate(invalid_values[directive._name]):
                with pytest.raises((ValueError, TypeError)):
                    directive._validator(value)

    def test_defaults_are_valid(self, available_directives_list):
        for directive in available_directives_list:
            directive._validator(directive._default)

    def test_serial(self):
        assert _NP._serial(4, 2) == 4
        assert _NRANKS._serial(4, 2) == 4
        assert _NGPU._serial(4, 2) == 4
        assert _OMP_NUM_THREADS._serial(4, 2) == 4
        assert _GET_EXECUTABLE()._serial("Path1", "Path2") == "Path1"
        assert _WALLTIME._serial(4, 2) == 6
        assert _WALLTIME._serial(4, None) == 4
        assert _WALLTIME._serial(None, 4) == 4
        assert _WALLTIME._serial(None, None) is None
        assert _MEMORY._serial(4, 2) == 4
        assert _MEMORY._serial(4, None) == 4
        assert _MEMORY._serial(None, 4) == 4
        assert _MEMORY._serial(None, None) is None
        assert _PROCESSOR_FRACTION._serial(0.4, 0.2) == 0.4

    def test_parallel(self):
        assert _NP._parallel(4, 2) == 6
        assert _NRANKS._parallel(4, 2) == 6
        assert _NGPU._parallel(4, 2) == 6
        assert _OMP_NUM_THREADS._parallel(4, 2) == 6
        assert _GET_EXECUTABLE()._parallel("Path1", "Path2") == "Path1"
        assert _WALLTIME._parallel(4, 2) == 4
        assert _WALLTIME._parallel(4, None) == 4
        assert _WALLTIME._parallel(None, 4) == 4
        assert _WALLTIME._parallel(None, None) is None
        assert _MEMORY._parallel(4, 2) == 6
        assert _MEMORY._parallel(4, None) == 4
        assert _MEMORY._parallel(None, 4) == 4
        assert _MEMORY._parallel(None, None) is None
        assert _PROCESSOR_FRACTION._parallel(0.4, 0.2) == 0.4

    def test_finalize(self):
        dict_directives = {
            "nranks": _NRANKS._default,
            "omp_num_threads": _OMP_NUM_THREADS._default,
        }
        assert _NP._finalize(2, dict_directives) == 2
        dict_directives["nranks"] = 2
        dict_directives["omp_num_threads"] = 4
        assert _NP._finalize(2, dict_directives) == 2
        assert _NP._finalize(1, dict_directives) == 8

    def test_manual_item_default(self, product_directive):
        assert product_directive._default == 10

    def test_manual_item_validation(self, product_directive):
        val = product_directive._validator(product_directive._default)
        assert product_directive._default == val
        assert product_directive._validator(20) == 20
        with pytest.raises(ValueError):
            product_directive._validator(0)

    def test_manual_item_serial(self, product_directive):
        product_directive._serial(10, 20) == 10
        product_directive._serial(20, 10) == 20

    def test_manual_item_parallel(self, product_directive):
        product_directive._parallel(10, 20) == 10

    def test_manual_item_finalize(self, product_directive):
        asset_dict = {"free": False, "discount": 5}
        assert product_directive._finalize(50, asset_dict) == 45
        asset_dict["free"] = True
        assert product_directive._finalize(50, asset_dict) == 0


class TestDirectives:
    """Tests for _Directives Class."""

    def test_get_directive(self, directives, available_directives_list):
        for item in available_directives_list:
            assert directives[item._name] == item._default

    def test_add_directive(self, available_directives_list):
        directives = _Directives(available_directives_list[:-1])
        directives._add_directive(_PROCESSOR_FRACTION)
        assert directives[_PROCESSOR_FRACTION._name] == _PROCESSOR_FRACTION._default
        with pytest.raises(TypeError):
            directives._add_directive("Test")
        with pytest.raises(ValueError):
            directives._add_directive(_PROCESSOR_FRACTION)

    def test_set_defined_directive(self, directives):
        directives._set_defined_directive(_NP._name, 10)
        assert directives[_NP._name] == 10

    def test_set_defined_directive_invalid(self, directives):
        with pytest.raises(ValueError):
            directives._set_defined_directive(_NP._name, 0)

    def test_set_undefined_directive(self, directives):
        with pytest.raises(DirectivesError):
            directives._set_defined_directive("test", 0)

    def test_set_directives_item(self, directives):
        directives["test"] = True
        assert directives["test"]

    def test_del_directive(self, directives):
        directives["test"] = True
        directives._set_defined_directive(_NP._name, 100)
        assert directives[_NP._name] == 100
        assert directives["test"]
        del directives[_NP._name]
        assert directives[_NP._name] == _NP._default
        del directives["test"]
        with pytest.raises(KeyError):
            directives["test"]

    def test_update_directive_without_aggregate(
        self, directives, non_default_directive_values
    ):
        valid_values_1 = non_default_directive_values[1]
        expected_values = {
            "np": 4,
            "ngpu": 1,
            "nranks": 0,
            "omp_num_threads": 10,
            "executable": "PathFinder",
            "walltime": datetime.timedelta(hours=20.0),
            "memory": 16,
            "processor_fraction": 0.5,
        }
        directives.update(valid_values_1)
        for dirs in directives:
            assert directives[dirs] == expected_values[dirs]

    def test_update_directive_serial(
        self, available_directives_list, non_default_directive_values
    ):
        directives1 = _Directives(available_directives_list)
        directives2 = _Directives(available_directives_list)
        valid_values_0 = non_default_directive_values[0]
        valid_values_1 = non_default_directive_values[1]
        expected_values = {
            "np": 100,
            "ngpu": 10,
            "nranks": 5,
            "omp_num_threads": 20,
            "executable": "Non Default Path",
            "walltime": datetime.timedelta(hours=84.0),
            "memory": 32,
            "processor_fraction": 0.5,
        }
        directives1.update(valid_values_0)
        directives2.update(valid_values_1)
        directives1.update(directives2, aggregate=True)
        for dirs in directives1:
            assert directives1[dirs] == expected_values[dirs]

    def test_update_directive_parallel(
        self, available_directives_list, non_default_directive_values
    ):
        directives1 = _Directives(available_directives_list)
        directives2 = _Directives(available_directives_list)
        valid_values_0 = non_default_directive_values[0]
        valid_values_1 = non_default_directive_values[1]
        expected_values = {
            "np": 104,
            "ngpu": 11,
            "nranks": 5,
            "omp_num_threads": 30,
            "executable": "Non Default Path",
            "walltime": datetime.timedelta(hours=64.0),
            "memory": 48,
            "processor_fraction": 0.5,
        }
        directives1.update(valid_values_0)
        directives2.update(valid_values_1)
        directives1.update(directives2, aggregate=True, parallel=True)
        for dirs in directives1:
            assert directives1[dirs] == expected_values[dirs]

    def test_evaluate_directive_none_job(
        self, directives, non_default_directive_values
    ):
        directives.evaluate(None)
        valid_values = non_default_directive_values[0]
        valid_values["processor_fraction"] = lambda job: job.sp.i / 10
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

        valid_values = non_default_directive_values[0]
        valid_values["processor_fraction"] = lambda job: round(job.sp.i / 10, 1)

        for job in project:
            directives = _Directives(available_directives_list)
            directives.update(
                {"processor_fraction": lambda job: round(job.sp.i / 10, 1)}
            )
            directives.evaluate((job,))
            assert directives["processor_fraction"] == round(job.sp.i / 10, 1)
