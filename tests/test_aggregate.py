import pytest

from functools import partial
from itertools import zip_longest
from tempfile import TemporaryDirectory

import signac
from flow.aggregate import _aggregate, _select


class AggregateProjectSetup:
    project_class = signac.Project
    entrypoint = dict(path='')

    @pytest.fixture
    def setUp(self, request):
        self._tmp_dir = TemporaryDirectory(prefix='flow-aggregate_')
        request.addfinalizer(self._tmp_dir.cleanup)
        self.project = self.project_class.init_project(
            name='AggregateTestProject',
            root=self._tmp_dir.name)

    def mock_project(self):
        project = self.project_class.get_project(root=self._tmp_dir.name)
        for i in range(10):
            even = i % 2 == 0
            if even:
                project.open_job(dict(i=i, half=i/2, even=even)).init()
            else:
                project.open_job(dict(i=i, even=even)).init()
        return project

    @pytest.fixture
    def project(self):
        return self.mock_project()


# Tests for _aggregate class
class TestAggregate(AggregateProjectSetup):

    def test_default_init(self):
        aggregate = _aggregate()
        test_list = [1, 2, 3, 4, 5]
        assert aggregate._sort is None
        assert aggregate._aggregator(test_list) == test_list

    def test_invalid_aggregator(self):
        aggregators = ['str', 1, {}]
        for aggregator in aggregators:
            with pytest.raises(TypeError):
                _aggregate(aggregator)

    def test_invalid_sort(self):
        sort_list = [1, {}, lambda x: x]
        for sort in sort_list:
            with pytest.raises(TypeError):
                _aggregate(sort=sort)

    def test_invalid_reverse(self):
        reverse_list = ['str', 1, {}, lambda x: x]
        for reverse in reverse_list:
            with pytest.raises(TypeError):
                _aggregate(reverse=reverse)

    def test_call_without_function(self):
        aggregate = _aggregate()
        call_result = aggregate()
        assert len(call_result) == 2
        assert call_result[1] is None
        assert call_result[0]([1, 2, 3, 4, 5]) == [1, 2, 3, 4, 5]

    def test_call_with_function(self):
        aggregate = _aggregate()

        def test_function(x):
            return x

        assert not getattr(test_function, '_flow_aggregate', False)
        test_function = aggregate(test_function)
        assert getattr(test_function, '_flow_aggregate', False)

    def test_with_decorator_with_pre_initialization(self):
        aggregate = _aggregate()

        @aggregate
        def test_function(x):
            return x

        assert getattr(test_function, '_flow_aggregate', False)

    def test_with_decorator_without_pre_initialization(self):
        @_aggregate()
        def test_function(x):
            return x

        assert getattr(test_function, '_flow_aggregate', False)

    def test_valid_aggregator_non_partial(self, setUp, project):
        # Return groups of 1
        def helper_aggregator(jobs):
            yield from jobs

        aggregate = _aggregate(helper_aggregator)

        aggregate_job_manual = helper_aggregator(project)
        aggregate_job_via_aggregator = aggregate()[0](project)

        assert [jobs for jobs in aggregate_job_manual] == \
               [jobs for jobs in aggregate_job_via_aggregator]

    def test_valid_aggregator_partial(self, setUp, project):
        aggregate = _aggregate(lambda jobs: jobs)
        aggregate_job_via_aggregator = aggregate()[0](project)

        assert [jobs for jobs in project] == \
               [jobs for jobs in aggregate_job_via_aggregator]

    def test_valid_sort(self, setUp, project):
        helper_sort = partial(sorted, key=lambda job: job.sp.i)
        aggregate = _aggregate(sort='i')

        assert(helper_sort(project) == aggregate()[1](project))

    def test_valid_reversed_sort(self, setUp, project):
        helper_sort = partial(sorted, key=lambda job: job.sp.i, reverse=True)
        aggregate = _aggregate(sort='i', reverse=True)

        assert(helper_sort(project) == aggregate()[1](project))

    def test_groups_of_invalid_num(self):
        invalid_values = [{}, 'str', -1, -1.5]
        for invalid_value in invalid_values:
            with pytest.raises((TypeError, ValueError)):
                _aggregate.groupsof(invalid_value)

    def test_groups_of_valid_num(self, setUp, project):
        valid_values = [1, 2, 3, 6]

        def helper_aggregator(jobs, num):
            args = [iter(jobs)] * num
            return zip_longest(*args)

        for valid_value in valid_values:
            aggregate = _aggregate.groupsof(valid_value)
            aggregate_job_manual = helper_aggregator(project, valid_value)
            aggregate_job_via_aggregator = aggregate()[0](project)
            assert [jobs for jobs in aggregate_job_manual] == \
                   [jobs for jobs in aggregate_job_via_aggregator]

    def test_group_by_invalid_key(self):
        with pytest.raises(TypeError):
            _aggregate.groupby(1)

    def test_groupby_with_valid_string_key(self, setUp, project):
        aggregate = _aggregate.groupby('even')
        aggregates = 0
        for agg in aggregate()[0](project):
            aggregates += 1
        assert aggregates == 2

    def test_groupby_with_invalid_string_key(self, setUp, project):
        aggregate = _aggregate.groupby('invalid_key')
        with pytest.raises(KeyError):
            for agg in aggregate()[0](project):
                pass

    def test_groupby_with_default_key_for_string(self, setUp, project):
        aggregate = _aggregate.groupby('half', default=-1)
        aggregates = 0
        for agg in aggregate()[0](project):
            aggregates += 1
        assert aggregates == 6

    def test_groupby_with_Iterable_key(self, setUp, project):
        aggregate = _aggregate.groupby(['i', 'even'])
        aggregates = 0
        for agg in aggregate()[0](project):
            aggregates += 1
        assert aggregates == 10

    def test_groupby_with_invalid_Iterable_key(self, setUp, project):
        aggregate = _aggregate.groupby(['half', 'even'])
        with pytest.raises(KeyError):
            for agg in aggregate()[0](project):
                pass

    def test_groupby_with_default_key_for_Iterable(self, setUp, project):
        aggregate = _aggregate.groupby(['half', 'even'], default=-1)
        aggregates = 0
        for agg in aggregate()[0](project):
            aggregates += 1
        assert aggregates == 6

    def test_groupby_with_callable_key(self, setUp, project):
        def keyfunction(job):
            return job.sp['even']

        aggregate = _aggregate.groupby(keyfunction)
        aggregates = 0
        for agg in aggregate()[0](project):
            aggregates += 1
        assert aggregates == 2

    def test_groupby_with_invalid_callable_key(self, setUp, project):
        def keyfunction(job):
            return job.sp['half']
        aggregate = _aggregate.groupby(keyfunction)
        with pytest.raises(KeyError):
            for agg in aggregate()[0](project):
                pass


# Tests for _select class
class TestSelect(AggregateProjectSetup):

    def test_default_init(self):
        select = _select()
        test_list = [1, 2, 3, 4, 5]
        assert [item for item in select._filter(test_list)] == \
               test_list

    def test_invalid_filter(self):
        filters = ['str', 1, []]
        for filter in filters:
            with pytest.raises(TypeError):
                _select(filter)

    def test_call_without_function(self):
        select = _select()
        call_result = select()
        assert [item for item in call_result([1, 2, 3, 4, 5])] == \
               [1, 2, 3, 4, 5]

    def test_call_with_function(self):
        select = _select()

        def test_function(x):
            return x

        assert not getattr(test_function, '_flow_select', False)
        test_function = select(test_function)
        assert getattr(test_function, '_flow_select', False)

    def test_with_decorator_with_pre_initialization(self):
        select = _select()

        @select
        def test_function(x):
            return x

        assert getattr(test_function, '_flow_select', False)

    def test_with_decorator_without_pre_initialization(self):
        @_select()
        def test_function(x):
            return x

        assert getattr(test_function, '_flow_select', False)

    def test_valid_filter(self, setUp, project):
        def filter(job):
            return job.sp.i > 5

        select = _select(filter)
        filtered_jobs = []
        for job in project:
            if filter(job):
                filtered_jobs.append(job)
        assert [job for job in select()(project)] == \
               filtered_jobs
