import pytest

from functools import partial
from tempfile import TemporaryDirectory

import signac
from flow.aggregate import aggregate


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
                project.open_job(dict(i=i, half=i / 2, even=even)).init()
            else:
                project.open_job(dict(i=i, even=even)).init()
        return project

    @pytest.fixture
    def project(self):
        return self.mock_project()


class TestAggregate(AggregateProjectSetup):

    def test_default_init(self):
        aggregate_instance = aggregate()
        test_list = [1, 2, 3, 4, 5]
        assert aggregate_instance._sort is None
        assert aggregate_instance._aggregator(test_list) == test_list
        assert aggregate_instance._select is None

    def test_invalid_aggregator(self, setUp, project):
        aggregators = ['str', 1, {}]
        for aggregator in aggregators:
            with pytest.raises(TypeError):
                aggregate(aggregator)

        def invalid_aggregator_function(jobs):
            _aggregate = []
            for job in jobs:
                if job.sp.i > 5:
                    _aggregate.append(job)
                else:
                    _aggregate.append([job])
            return _aggregate

        with pytest.raises(ValueError):
            aggregate(invalid_aggregator_function)(project)

    def test_invalid_sort(self):
        sort_list = [1, {}, lambda x: x]
        for sort in sort_list:
            with pytest.raises(TypeError):
                aggregate(sort=sort)

    def test_invalid_reverse(self):
        reverse_list = ['str', 1, {}, lambda x: x]
        for reverse in reverse_list:
            with pytest.raises(TypeError):
                aggregate(reverse=reverse)

    def test_invalid_select(self):
        selectors = ['str', 1, []]
        for _select in selectors:
            with pytest.raises(TypeError):
                aggregate(select=_select)

    def test_invalid_call(self):
        call_params = ['str', 1, None]
        for param in call_params:
            with pytest.raises(TypeError):
                aggregate()(param)

    def test_call_without_function(self):
        aggregate_instance = aggregate()
        with pytest.raises(TypeError):
            aggregate_instance()

    def test_call_with_function(self):
        aggregate_instance = aggregate()

        def test_function(x):
            return x

        assert not getattr(test_function, '_flow_aggregate', False)
        test_function = aggregate_instance(test_function)
        assert getattr(test_function, '_flow_aggregate', False)

    def test_with_decorator_with_pre_initialization(self):
        aggregate_instance = aggregate()

        @aggregate_instance
        def test_function(x):
            return x

        assert getattr(test_function, '_flow_aggregate', False)

    def test_with_decorator_without_pre_initialization(self):
        @aggregate()
        def test_function(x):
            return x

        assert getattr(test_function, '_flow_aggregate', False)

    def test_valid_aggregator_non_partial(self, setUp, project):
        # Return groups of 1
        def helper_aggregator(jobs):
            yield from jobs

        aggregate_instance = aggregate(helper_aggregator)

        aggregate_job_manual = helper_aggregator(project)
        aggregate_job_via_aggregator = aggregate_instance(project)

        assert [[jobs for jobs in aggregate_job_manual]] == \
               aggregate_job_via_aggregator

    def test_valid_aggregator_partial(self, setUp, project):
        aggregate_instance = aggregate(lambda jobs: jobs)
        aggregate_job_via_aggregator = aggregate_instance(project)

        assert [[jobs for jobs in project]] == \
               aggregate_job_via_aggregator

    def test_valid_sort(self, setUp, project):
        helper_sort = partial(sorted, key=lambda job: job.sp.i)
        aggregate_instance = aggregate(sort='i')

        assert([helper_sort(project)] == aggregate_instance(project))

    def test_valid_reversed_sort(self, setUp, project):
        helper_sort = partial(sorted, key=lambda job: job.sp.i, reverse=True)
        aggregate_instance = aggregate(sort='i', reverse=True)

        assert([helper_sort(project)] == aggregate_instance(project))

    def test_groups_of_invalid_num(self):
        invalid_values = [{}, 'str', -1, -1.5]
        for invalid_value in invalid_values:
            with pytest.raises((TypeError, ValueError)):
                aggregate.groupsof(invalid_value)

    def test_groups_of_valid_num(self, setUp, project):
        valid_values = [1, 2, 3, 6]

        total_jobs = len(project)

        for valid_value in valid_values:
            aggregate_instance = aggregate.groupsof(valid_value)
            aggregate_job_via_aggregator = aggregate_instance(project)
            if total_jobs % valid_value == 0:
                length_of_aggregate = total_jobs/valid_value
            else:
                length_of_aggregate = int(total_jobs/valid_value) + 1
            assert len(aggregate_job_via_aggregator) == length_of_aggregate

    def test_group_by_invalid_key(self):
        with pytest.raises(TypeError):
            aggregate.groupby(1)

    def test_groupby_with_valid_string_key(self, setUp, project):
        aggregate_instance = aggregate.groupby('even')
        aggregates = 0
        for agg in aggregate_instance(project):
            aggregates += 1
        assert aggregates == 2

    def test_groupby_with_invalid_string_key(self, setUp, project):
        aggregate_instance = aggregate.groupby('invalid_key')
        with pytest.raises(KeyError):
            for agg in aggregate_instance(project):
                pass

    def test_groupby_with_default_key_for_string(self, setUp, project):
        aggregate_instance = aggregate.groupby('half', default=-1)
        aggregates = 0
        for agg in aggregate_instance(project):
            aggregates += 1
        assert aggregates == 6

    def test_groupby_with_Iterable_key(self, setUp, project):
        aggregate_instance = aggregate.groupby(['i', 'even'])
        aggregates = 0
        for agg in aggregate_instance(project):
            aggregates += 1
        assert aggregates == 10

    def test_groupby_with_invalid_Iterable_key(self, setUp, project):
        aggregate_instance = aggregate.groupby(['half', 'even'])
        with pytest.raises(KeyError):
            for agg in aggregate_instance(project):
                pass

    def test_groupby_with_default_key_for_Iterable(self, setUp, project):
        aggregate_instance = aggregate.groupby(['half', 'even'], default=-1)
        aggregates = 0
        for agg in aggregate_instance(project):
            aggregates += 1
        assert aggregates == 6

    def test_groupby_with_callable_key(self, setUp, project):
        def keyfunction(job):
            return job.sp['even']

        aggregate_instance = aggregate.groupby(keyfunction)
        aggregates = 0
        for agg in aggregate_instance(project):
            aggregates += 1
        assert aggregates == 2

    def test_groupby_with_invalid_callable_key(self, setUp, project):
        def keyfunction(job):
            return job.sp['half']
        aggregate_instance = aggregate.groupby(keyfunction)
        with pytest.raises(KeyError):
            for agg in aggregate_instance(project):
                pass

    def test_valid_select(self, setUp, project):
        def _select(job):
            return job.sp.i > 5

        aggregate_instance = aggregate.groupsof(1, select=_select)
        selected_jobs = []
        for job in project:
            if _select(job):
                selected_jobs.append([job])
        assert aggregate_instance(project) == selected_jobs
