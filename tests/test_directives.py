# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
import sys
from copy import deepcopy

from flow.directives import Directives, _DirectivesItem
from flow.directives import NP, NRANKS, NGPU, EXECUTABLE, OMP_NUM_THREADS
from flow.directives import WALLTIME, MEMORY, PROCESS_FRACTION


@pytest.fixture()
def setUp(test_class=Directives, **kwargs):
    def _setup(test_class=Directives, **kwargs):
        return test_class(**kwargs)
    return _setup

@pytest.fixture()
def available_directives_list():
    return [NP, NRANKS, NGPU, OMP_NUM_THREADS,
            EXECUTABLE, WALLTIME, MEMORY,
            PROCESS_FRACTION]


class TestItems:
    """
    Tests for _DirectivesItem class
    """
    def test_default(self):
        assert NP.default == 1
        assert NGPU.default == 0
        assert NRANKS.default == 0
        assert OMP_NUM_THREADS.default == 0
        assert EXECUTABLE.default == sys.executable
        assert WALLTIME.default == 12.0
        assert MEMORY.default == 4
        assert PROCESS_FRACTION.default == 1.

    def test_validation(self, available_directives_list):
        for directive in available_directives_list:
            directive.validation(directive.default)

    # def test_validation_none(self):
    #     available_directives_list =
    #     for directive in available_directives_list:
    #         directive.validation(directive.default)

    def test_validation_manual(self):
        directive_copy = deepcopy(NP)

        def div_by_ten(v):
            if v % 10 == 0:
                return True
            return False

        directive_copy.validation = div_by_ten
        assert directive_copy.validation(10)
        assert not directive_copy.validation(5)
        with pytest.raises(TypeError):
            directive_copy.validation('4')

    def test_serial(self):
        assert NP.serial(4, 2) == 4
        assert NRANKS.serial(4, 2) == 4
        assert NGPU.serial(4, 2) == 4
        assert OMP_NUM_THREADS(4, 2) == 4
        assert EXECUTABLE.serial('Path1', 'Path2') == 'Path1'
        assert WALLTIME.serial(4, 2) == 6
        assert MEMORY.serial(4, 2) == 4
        assert PROCESS_FRACTION(4.0, 2.0) == 4.0

    def test_parallel(self):
        assert NP.parallel(4, 2) == 6
        assert NRANKS.parallel(4, 2) == 6
        assert NGPU.parallel(4, 2) == 6
        assert OMP_NUM_THREADS(4, 2) == 6
        assert EXECUTABLE.parallel('Path1', 'Path2') == 'Path1'
        assert WALLTIME.parallel(4, 2) == 4
        assert MEMORY.parallel(4, 2) == 6
        assert PROCESS_FRACTION.parallel(4.0, 2.0) == 4.0

    def test_parallel_manual(self):
        directive_copy = deepcopy(NP)
        directive_copy.parallel = lambda x, y: x*y
        parallel = directive_copy.parallel(3, 2)
        assert parallel == 6
        directive_copy.validation(parallel)
        with pytest.raises(TypeError):
            directive_copy.parallel('4', '2')

    def test_finalize(self):
        dict_directives = {'nranks': NRANKS.default, 'omp_num_threads': OMP_NUM_THREADS.default}
        assert NP.finalize(2, dict_directives) == 2
        dict_directives['nranks'] = 2
        dict_directives['omp_num_threads'] = 4
        assert NP.finalize(2, dict_directives) == 8
        dict_directives['nranks'] = NRANKS
        assert NP.finalize(2, dict_directives) == 2

    def test_manual_item(self, setUp):
        def val(v):
            if v < 10:
                raise ValueError("Price cannot be less than 10 units")
            return v

        def no_aggregation(v, o):
            return v

        def finalize(value, dict):
            discount = dict.get('discount', 0)
            free = dict.get('free', False)
            value = value - discount
            if value < 0 or free:
                return 0
            return value

        PRODUCT = setUp(_DirectivesItem, name='product', validation=val,
                        default=10, serial=no_aggregation, parallel=no_aggregation,
                        finalize=finalize)
        asset_dict = {'free': False, 'discount': 5}
        assert PRODUCT.default == PRODUCT.validation(PRODUCT.default)
        assert PRODUCT.default == PRODUCT.validation(20)
        assert PRODUCT.default == 10
        assert PRODUCT.serial(10, 20) == 10
        assert PRODUCT.parallel(10, 20) == 10
        assert PRODUCT.finalize(50, asset_dict) == 45
        asset_dict['free'] = True
        assert PRODUCT.finalize(50, asset_dict) == 0

        with pytest.raises(ValueError):
            PRODUCT.validation(0)


class TestDirectives:
    """
    Tests for Directives Class
    """
    def test_get_directives(self, setUp, available_directives_list):
        directives = setUp(available_directives_list=available_directives_list)
        for item in available_directives_list:
            assert directives[item.name] == item.default

    def test_add_directives(self, setUp, available_directives_list):
        directives = setUp(available_directives_list=available_directives_list[:-1])
        directives._add_directive(OMP_NUM_THREADS)
        assert directives[OMP_NUM_THREADS.name] == OMP_NUM_THREADS.default
        with pytest.raises(TypeError):
            directives._add_directive('Test')
        with pytest.raises(ValueError):
            directives._add_directive(NP)

    def test_set_directives(self, setUp, available_directives_list):
        directives = setUp(available_directives_list=available_directives_list[:-1])
        directives[OMP_NUM_THREADS.name] = OMP_NUM_THREADS.default
        assert directives[OMP_NUM_THREADS.name] == OMP_NUM_THREADS.default

    def test_define_directive(self, setUp, available_directives_list):
        directives = setUp(available_directives_list=available_directives_list[:-1])
        # The values below corresponds to NP, NRANKS, NGPU, EXECUTABLE.
        valid_values = [100, 100, 100, 'My String']
        invalid_values = [0, -1, -1, [0]]
        for idx, dir in enumerate(available_directives_list[:-1]):
            directives._set_defined_directive(dir.name, valid_values[idx])
            assert directives[dir.name] == valid_values[idx]
        for idx, dir in enumerate(available_directives_list[:-1]):
            with pytest.raises(ValueError):
                directives._set_defined_directive(dir.name, invalid_values[idx])

    def test_del_directive(self, setUp, available_directives_list):
        directives = setUp(available_directives_list=available_directives_list[:-1])
        directives[OMP_NUM_THREADS.name] = OMP_NUM_THREADS.default
        directives._set_defined_directive(NP.name, 100)
        assert directives[NP.name] == 100
        assert directives[OMP_NUM_THREADS.name] == OMP_NUM_THREADS.default
        del directives[NP.name]
        assert directives[NP.name] == NP.default
        del directives[OMP_NUM_THREADS.name]
        with pytest.raises(KeyError):
            directives[OMP_NUM_THREADS.name]
