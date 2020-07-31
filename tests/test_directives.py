# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest

from flow.directives import Directives, _DirectivesItem, _no_aggregation
from flow.directives import (NP, NRANKS, NGPU, EXECUTABLE, OMP_NUM_THREADS,
                             WALLTIME, MEMORY, PROCESS_FRACTION)
from flow import FlowProject
from tempfile import TemporaryDirectory


@pytest.fixture()
def available_directives_list():
    return [NP, NRANKS, NGPU, OMP_NUM_THREADS,
            EXECUTABLE, WALLTIME, MEMORY,
            PROCESS_FRACTION]


@pytest.fixture()
def directives(available_directives_list):
    return Directives(available_directives_list)


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
                              default=10, serial=_no_aggregation,
                              parallel=_no_aggregation, finalize=finalize)
    return product


@pytest.fixture()
def non_default_directive_values():
    return [{'np': 1, 'ngpu': 10, 'nranks': 5,
             'omp_num_threads': 20, 'executable': 'Non Default Path',
             'walltime': 64., 'memory': 32, 'processor_fraction': 0.5},
            {'np': 4, 'ngpu': 1, 'nranks': 0,
             'omp_num_threads': 10, 'executable': 'PathFinder',
             'walltime': 20., 'memory': 16, 'processor_fraction': 0.5}]


class TestItems:
    """
    Tests for _DirectivesItem class
    """
    def test_default(self):
        assert NP.default == 1
        assert NGPU.default == 0
        assert NRANKS.default == 0
        assert OMP_NUM_THREADS.default == 0
        assert EXECUTABLE.default is None
        assert WALLTIME.default == 12.0
        assert MEMORY.default == 4
        assert PROCESS_FRACTION.default == 1.

    def test_invalid_values(self, available_directives_list):
        invalid_values = {'np': [-1, 'foo', {}, None],
                          'ngpu': [-1, 'foo', {}, None],
                          'nranks': [-1, 'foo', {}, None],
                          'omp_num_threads': [-1, 'foo', {}, None],
                          'walltime': ['foo', {}, None],
                          'memory': [-1, 'foo', {}, None],
                          'processor_fraction': [-0.5, 2.5, 'foo', {}, None]
                          }

        for directive in available_directives_list:
            if directive.name == 'executable':
                # Executable expect a string, if not found, then it tries to convert
                # it into a string and becomes successful almost every time.
                # Hence skipping Executable.
                continue
            for i, value in enumerate(invalid_values[directive.name]):
                with pytest.raises((ValueError, TypeError)):
                    directive.validation(value)

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
        assert NP.finalize(2, dict_directives) == 2
        assert NP.finalize(1, dict_directives) == 8
        dict_directives['nranks'] = lambda x: x**2
        assert NP.finalize(2, dict_directives) == 2

    def test_manual_item_default(self, product_directive):
        assert product_directive.default == 10

    def test_manual_item_validation(self, product_directive):
        val = product_directive.validation(product_directive.default)
        assert product_directive.default == val
        assert product_directive.validation(20) == 20
        with pytest.raises(ValueError):
            product_directive.validation(0)

    def test_manual_item_serial(self, product_directive):
        product_directive.serial(10, 20) == 10
        product_directive.serial(20, 10) == 20

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
    def test_get_directive(self, directives, available_directives_list):
        for item in available_directives_list:
            assert directives[item.name] == item.default

    def test_add_directive(self, available_directives_list):
        directives = Directives(available_directives_list[:-1])
        directives._add_directive(PROCESS_FRACTION)
        assert directives[PROCESS_FRACTION.name] == PROCESS_FRACTION.default
        with pytest.raises(TypeError):
            directives._add_directive('Test')
        with pytest.raises(ValueError):
            directives._add_directive(PROCESS_FRACTION)

    def test_set_defined_directive(self, directives):
        directives._set_defined_directive(NP.name, 10)
        assert directives[NP.name] == 10

    def test_set_defined_directive_invalid(self, directives):
        with pytest.raises(ValueError):
            directives._set_defined_directive(NP.name, 0)

    def test_set_undefined_directive(self, directives):
        with pytest.raises(KeyError):
            directives._set_defined_directive('test', 0)

    def test_set_directives_item(self, directives):
        directives['test'] = True
        assert directives['test']

    def test_del_directive(self, directives):
        directives['test'] = True
        directives._set_defined_directive(NP.name, 100)
        assert directives[NP.name] == 100
        assert directives['test']
        del directives[NP.name]
        assert directives[NP.name] == NP.default
        del directives['test']
        with pytest.raises(KeyError):
            directives['test']

    def test_update_directive_without_aggregate(
        self, directives, non_default_directive_values
    ):
        valid_values_1 = non_default_directive_values[1]
        expected_values = {'np': 4, 'ngpu': 1, 'nranks': 0,
                           'omp_num_threads': 10, 'executable': 'PathFinder',
                           'walltime': 20.0, 'memory': 16, 'processor_fraction': 0.5}
        directives.update(valid_values_1)
        for dirs in directives:
            assert directives[dirs] == expected_values[dirs]

    def test_update_directive_serial(
        self, available_directives_list, non_default_directive_values
    ):
        directives1 = Directives(available_directives_list)
        directives2 = Directives(available_directives_list)
        valid_values_0 = non_default_directive_values[0]
        valid_values_1 = non_default_directive_values[1]
        expected_values = {'np': 100, 'ngpu': 10, 'nranks': 5,
                           'omp_num_threads': 20, 'executable': 'Non Default Path',
                           'walltime': 84.0, 'memory': 32, 'processor_fraction': 0.5}
        directives1.update(valid_values_0)
        directives2.update(valid_values_1)
        directives1.update(directives2, aggregate=True)
        for dirs in directives1:
            assert directives1[dirs] == expected_values[dirs]

    def test_update_directive_parallel(
        self, available_directives_list, non_default_directive_values
    ):
        directives1 = Directives(available_directives_list)
        directives2 = Directives(available_directives_list)
        valid_values_0 = non_default_directive_values[0]
        valid_values_1 = non_default_directive_values[1]
        expected_values = {'np': 104, 'ngpu': 11, 'nranks': 5,
                           'omp_num_threads': 30, 'executable': 'Non Default Path',
                           'walltime': 64.0, 'memory': 48, 'processor_fraction': 0.5}
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
        valid_values['processor_fraction'] = lambda job: job.sp.i/10
        directives.update(valid_values)
        with pytest.raises(RuntimeError):
            directives.evaluate(None)

    def test_evaluate_directive_valid_job(
        self, available_directives_list, non_default_directive_values
    ):
        _tmp_dir = TemporaryDirectory(prefix='flow-directives_')
        FlowProject.init_project(name='DirectivesTest',
                                 root=_tmp_dir.name)
        project = FlowProject.get_project(root=_tmp_dir.name)
        for i in range(5):
            project.open_job(dict(i=i)).init()

        valid_values = non_default_directive_values[0]
        valid_values['processor_fraction'] = lambda job: round(job.sp.i/10, 1)

        for job in project:
            directives = Directives(available_directives_list)
            directives.update({'processor_fraction': lambda job: round(job.sp.i/10, 1)})
            directives.evaluate(job)
            assert directives['processor_fraction'] == round(job.sp.i/10, 1)
