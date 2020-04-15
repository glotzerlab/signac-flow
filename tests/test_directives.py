# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest

from flow.directives import Directives, _DirectivesItem
from flow.directives import NP, NRANKS, NGPU, EXECUTABLE, OMP_NUM_THREADS

class TestDirectivesBase():
    test_class = _DirectivesItem

    @pytest.fixture(autouse=True)
    def setUp(self, test_class, **kwargs):
        return test_class(**kwargs)

class TestItems(TestDirectivesBase):
    """
    Tests for _DirectivesItem class
    """
    def test_serial_call(self.setUp):
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

    def test_single_directive(self):
        project = self.mock_project(Directives, available_directives_list=[NP])
        assert project['np'] == NP.validation(NP.default)

    def test_multiple_directive(self):
        directive_list = [NP, NGPU, NRANKS, EXECUTABLE, OMP_NUM_THREADS]
        project = self.mock_project(Directives, available_directives_list=directive_list)
        for idx, dir in enumerate(project):
            assert dir == directive_list[idx].name
