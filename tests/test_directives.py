# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
import sys
from copy import deepcopy

from flow.directives import Directives
from flow.directives import NP, NRANKS, NGPU, EXECUTABLE, OMP_NUM_THREADS


@pytest.fixture()
def setUp(test_class=Directives, **kwargs):
    def _setup(test_class=Directives, **kwargs):
        return test_class(**kwargs)
    return _setup


available_directives_list = [NP, NRANKS, NGPU, EXECUTABLE, OMP_NUM_THREADS]


class TestItems:
    """
    Tests for _DirectivesItem class
    """
    def test_serial(self):
        assert NP.serial(4, 2) == 4
        assert not NP.serial('4', '2') == 4

    def test_parallel(self):
        assert NP.parallel(4, 2) == 6

    def test_parallel_manual(self):
        directive_copy = deepcopy(NP)
        directive_copy.parallel = lambda x, y: x*y/(x+y)
        assert directive_copy.parallel(3, 2) == 1.2
        with pytest.raises(TypeError):
            directive_copy.parallel('4', '2')
        with pytest.raises(ZeroDivisionError):
            directive_copy.parallel(0, 0)

    def test_validation(self):
        for directive in available_directives_list:
            directive.validation(directive.default)

    def test_validation_manual(self):
        NP_copy = deepcopy(NP)
        NP_copy.default = 0
        with pytest.raises(ValueError):
            # As 0 is not a natural number this should raise an error
            NP_copy.validation(NP_copy.default)

        def perf_square(v):
            if (v**0.5) % 1 == 0:
                return True
            return False

        NP_copy.validation = perf_square
        assert NP_copy.validation(4)
        assert not NP_copy.validation(5)
        with pytest.raises(TypeError):
            NP_copy.validation('4')

    def test_default(self):
        assert NP.default == 1
        assert EXECUTABLE.default == sys.executable

    def test_finalize(self):
        # Yet to find an appropriate way to test this
        assert True


class TestDirectives:
    """
    Tests for Directives Class
    """
    def test_init_directives(self, setUp):
        directives = setUp(available_directives_list=available_directives_list)
        for item in available_directives_list:
            assert directives[item.name] == item.default

    def test_add_directives(self, setUp):
        directives = setUp(available_directives_list=available_directives_list[:-1])
        directives._add_directive(OMP_NUM_THREADS)
        assert directives[OMP_NUM_THREADS.name] == OMP_NUM_THREADS.default
        with pytest.raises(TypeError):
            directives._add_directive('Test')
        with pytest.raises(ValueError):
            directives._add_directive(NP)

    def test_define_directive(self, setUp):
        directives = setUp(available_directives_list=available_directives_list[:-1])
        valid_values = [100, 100, 100, 100]
        invalid_values = [0, -1, -1, -1]
        for idx, dir in enumerate(available_directives_list):
            directives._set_defined_directive(dir.name, valid_values[idx])
            assert directives[dir.name] == valid_values[idx]
        for dir in enumerate(available_directives_list):
            with pytest.raises(ValueError):
                directives._set_defined_directive(dir.name, invalid_values[idx])
