# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import sys

from flow import Directives, _DirectivesItem, OnlyType, directive_module


def raise_if_too_small(value):
    def is_greater(v):
        try:
            if v < value:
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError("Expected a number greater than {}. Received {}"
                             "".format(value, v))
        return v


def _evaluate(value, job=None):
    if callable(value):
        if job is None:
            raise RuntimeError("job not specified when evaluating directive.")
        else:
            return value(job)
    else:
        return value


def finalize_np(value, directives):
    nranks = directives.get('nranks', 1)
    omp_num_threads = directives.get('omp_num_threads', 1)
    if callable(nranks) or callable(omp_num_threads):
        return value
    else:
        return max(value, max(1, nranks) * max(1, omp_num_threads))


def is_fraction(value):
    if 0 <= value <= 1:
        return value
    else:
        raise ValueError("Value must be beween 0 and 1.")


def no_aggregation(v, o):
    return v


nonnegative_int = OnlyType(int, postprocess=raise_if_too_small(-1))
natural_number = OnlyType(int, postprocess=raise_if_too_small(0))
nonnegative_real = OnlyType(float, postprocess=raise_if_too_small(0))


class TestDirectivesBase():
    project_class = _DirectivesItem

    def mock_project(self, project_class=None, *args, **kwargs):
        project_class = project_class if project_class else self.project_class
        if(project_class == _DirectivesItem):
            return project_class(**kwargs)
        return project_class(**kwargs)


class TestItems(TestDirectivesBase):
    """
    Tests for _DirectivesItem class
    """
    def test_serial_call(self):
        project = self.mock_project(name='np')
        assert project(project.serial) == project.serial

    def test_parallel_call(self):
        project = self.mock_project(name='np')
        assert project(project.parallel) == project.parallel

    def test_validation_call(self):
        def identity(v):
            if type(v) is int:
                return v**2
            return v
        project = self.mock_project(name='np', validation=identity)
        str_check = 'default'
        int_check = 2
        assert project(str_check) == str_check
        assert project(int_check) == identity(int_check)


class TestDirectives(TestDirectivesBase):
    """
    Tests for Directives Class
    """
    NP = _DirectivesItem('np', natural_number, 1, finalize_np)
    NGPU = _DirectivesItem('ngpu', nonnegative_int, 0)
    NRANKS = _DirectivesItem('nranks', nonnegative_int, 0)
    EXECUTABLE = _DirectivesItem('executable', OnlyType(str), sys.executable,
                                 no_aggregation, no_aggregation)
    OMP_NUM_THREADS = _DirectivesItem('omp_num_threads', nonnegative_int, 0)


    def test_single_directive(self):
        project = self.mock_project(Directives, available_directives_list=[self.NP])
        assert project['np'] == self.NP.validation(self.NP.default)

    def test_multiple_directive(self):
        directive_list = [self.NP, self.NGPU, self.NRANKS, self.EXECUTABLE, self.OMP_NUM_THREADS]
        project = self.mock_project(Directives, available_directives_list=directive_list)
        for idx, dir in enumerate(project):
            assert dir == directive_list[idx].name
