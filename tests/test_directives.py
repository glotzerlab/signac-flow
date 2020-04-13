# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest

from flow import Directives
from flow import _DirectivesItem
from tempfile import TemporaryDirectory

class TestDirectives():
    project_class = _DirectivesItem

    def mock_project(self, project_class=None, name=None, validation=None):
        project_class = project_class if project_class else self.project_class
        return project_class(name=name,validation=validation)


class TestItems(TestDirectives):

    def test_serial_call(self):
        project = self.mock_project()
        assert project(project.serial)==project.serial

    def test_parallel_call(self):
        project = self.mock_project()
        assert project(project.parallel)==project.parallel

    def test_validation_call(self):
        def identity(v):
            if type(v) is int:
                return v**2
            return v
        project = self.mock_project(validation=identity)
        str_check = 'default'
        int_check = 2
        assert project(str_check)==str_check
        assert project(int_check)==identity(int_check)