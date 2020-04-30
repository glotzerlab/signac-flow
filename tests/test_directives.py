# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
import sys
from copy import deepcopy

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
    def test_get_directive(self, setUp, available_directives_list):
        directives = setUp(available_directives_list=available_directives_list)
        for item in available_directives_list:
            assert directives[item.name] == item.default

    def test_add_directive(self, setUp, available_directives_list):
        directives = setUp(available_directives_list=available_directives_list[:-1])
        directives._add_directive(PROCESS_FRACTION)
        assert directives[PROCESS_FRACTION.name] == PROCESS_FRACTION.default
        with pytest.raises(TypeError):
            directives._add_directive('Test')
        with pytest.raises(ValueError):
            directives._add_directive(PROCESS_FRACTION)

    def test_set_defined_directive(self, setUp, available_directives_list):
        directives = setUp(available_directives_list=available_directives_list)
        directives._set_defined_directive(NP.name, 10)
        assert directives[NP.name] == 10

    def test_set_defined_directive_invalid(self, setUp, available_directives_list):
        directives = setUp(available_directives_list=available_directives_list)
        directives._set_defined_directive(NP.name, 0)
        assert directives[NP.name] == NP.default

    def test_set_undefined_directive(self, setUp, available_directives_list, product_directive):
        directives = setUp(available_directives_list=available_directives_list)
        with pytest.raises(Exception):
            directives._set_defined_directive(product_directive.name, 0)

    def test_set_directives_item(self, setUp, available_directives_list, product_directive):
        directives = setUp(available_directives_list=available_directives_list)
        directives[product_directive.name] = product_directive.default
        assert directives[product_directive.name] == product_directive.default

    def test_del_directive(self, setUp, available_directives_list, product_directive):
        directives = setUp(available_directives_list=available_directives_list)
        directives[product_directive.name] = 100
        directives._set_defined_directive(NP.name, 100)
        assert directives[NP.name] == 100
        assert directives[product_directive.name] == 100
        del directives[NP.name]
        assert directives[NP.name] == NP.default
        del directives[product_directive.name]
        with pytest.raises(KeyError):
            directives[product_directive.name]

    def test_update_directive_without_aggregate(self, setUp, available_directives_list):
        directives1 = setUp(available_directives_list=available_directives_list)
        directives2 = deepcopy(directives1)
        valid_values = {'np': 4, 'ngpu': 1, 'nranks': 0,
                        'omp_num_threads': 10, 'executable': 'PathFinder',
                        'walltime': 20., 'memory': 16, 'processor_fraction': 0.5}
        for dirs in directives2:
            directives2[dirs] = valid_values[dirs]
        directives1.update(directives2)
        for dirs in directives1:
            assert directives1[dirs] == directives2[dirs]

    def test_update_directive_serial(self, setUp, available_directives_list):
        directives1 = setUp(available_directives_list=available_directives_list)
        directives2 = deepcopy(directives1)
        valid_values = {'np': 4, 'ngpu': 1, 'nranks': 0,
                        'omp_num_threads': 10, 'executable': 'PathFinder',
                        'walltime': 20., 'memory': 16, 'processor_fraction': 0.5}
        expected_values = {'np': 10, 'ngpu': 1, 'nranks': 0,
                           'omp_num_threads': 10, 'executable': sys.executable,
                           'walltime': 32., 'memory': 16, 'processor_fraction': 1.}
        for dirs in directives2:
            directives2[dirs] = valid_values[dirs]
        directives1.update(directives2, aggregate=True)
        for dirs in directives1:
            assert directives1[dirs] == expected_values[dirs]

    def test_update_directive_parallel(self, setUp, available_directives_list):
        directives1 = setUp(available_directives_list=available_directives_list)
        directives2 = deepcopy(directives1)
        valid_values = {'np': 4, 'ngpu': 1, 'nranks': 0,
                        'omp_num_threads': 10, 'executable': 'PathFinder',
                        'walltime': 20., 'memory': 16, 'processor_fraction': 0.5}
        expected_values = {'np': 11, 'ngpu': 1, 'nranks': 0,
                           'omp_num_threads': 10, 'executable': sys.executable,
                           'walltime': 20., 'memory': 20, 'processor_fraction': 1.}
        for dirs in directives2:
            directives2[dirs] = valid_values[dirs]
        directives1.update(directives2, aggregate=True, parallel=True)
        for dirs in directives1:
            assert directives1[dirs] == expected_values[dirs]
