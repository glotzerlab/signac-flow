# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
import sys

from flow.directives import Directives, _DirectivesItem
from flow.directives import NP, NRANKS, NGPU, EXECUTABLE, OMP_NUM_THREADS
from flow.directives import WALLTIME, MEMORY, PROCESS_FRACTION
from flow.directives import no_aggregation


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


@pytest.fixture()
def product_directive():
    def val(v):
        if v < 10:
            raise ValueError("Price cannot be less than 10 units")
        return v

    def finalize(value, dict):
        discount = dict.get('discount', 0)
        free = dict.get('free', False)
        value = value - discount
        if value < 0 or free:
            return 0
        return value

    product = _DirectivesItem(name='product', validation=val,
                              default=10, serial=no_aggregation,
                              parallel=no_aggregation, finalize=finalize)
    return product


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

    def test_defaults_are_valid(self, available_directives_list):
        for directive in available_directives_list:
            directive.validation(directive.default)

    def test_serial(self):
        assert NP.serial(4, 2) == 4
        assert NRANKS.serial(4, 2) == 4
        assert NGPU.serial(4, 2) == 4
        assert OMP_NUM_THREADS.serial(4, 2) == 4
        assert EXECUTABLE.serial('Path1', 'Path2') == 'Path1'
        assert WALLTIME.serial(4, 2) == 6
        assert MEMORY.serial(4, 2) == 4
        assert PROCESS_FRACTION.serial(0.4, 0.2) == 0.4

    def test_parallel(self):
        assert NP.parallel(4, 2) == 6
        assert NRANKS.parallel(4, 2) == 6
        assert NGPU.parallel(4, 2) == 6
        assert OMP_NUM_THREADS.parallel(4, 2) == 6
        assert EXECUTABLE.parallel('Path1', 'Path2') == 'Path1'
        assert WALLTIME.parallel(4, 2) == 4
        assert MEMORY.parallel(4, 2) == 6
        assert PROCESS_FRACTION.parallel(0.4, 0.2) == 0.4

    def test_finalize(self):
        dict_directives = {'nranks': NRANKS.default, 'omp_num_threads': OMP_NUM_THREADS.default}
        assert NP.finalize(2, dict_directives) == 2
        dict_directives['nranks'] = 2
        dict_directives['omp_num_threads'] = 4
        assert NP.finalize(2, dict_directives) == 8
        dict_directives['nranks'] = NRANKS
        assert NP.finalize(2, dict_directives) == 2

    def test_manual_item_default(self, product_directive):
        assert product_directive.default == 10

    def test_manual_item_validation(self, product_directive):
        try:
            val = product_directive.validation(product_directive.default)
            assert product_directive.default == val
        except Exception:
            raise ValueError("Invalid default value for product_directive")
        assert product_directive.default == product_directive.validation(product_directive.default)
        assert product_directive.validation(20) == 20
        with pytest.raises(ValueError):
            product_directive.validation(0)

    def test_manual_item_serial(self, product_directive):
        product_directive.serial(10, 20) == 10

    def test_manual_item_parallel(self, product_directive):
        product_directive.parallel(10, 20) == 10

    def test_manual_item_finalize(self, product_directive):
        asset_dict = {'free': False, 'discount': 5}
        assert product_directive.finalize(50, asset_dict) == 45
        asset_dict['free'] = True
        assert product_directive.finalize(50, asset_dict) == 0


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
