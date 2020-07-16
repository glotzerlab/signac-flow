import pytest

from functools import partial
from tempfile import TemporaryDirectory

import signac
from flow.aggregate import Aggregate, _MakeAggregate


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
        aggregate_instance = Aggregate()
        test_list = [1, 2, 3, 4, 5]
        assert aggregate_instance._sort is None
        assert aggregate_instance._aggregator(test_list) == [test_list]
        assert aggregate_instance._select is None

    def test_invalid_aggregator(self, setUp, project):
        aggregators = ['str', 1, {}]
        for aggregator in aggregators:
            with pytest.raises(TypeError):
                Aggregate(aggregator)

    def test_invalid_sort(self):
        sort_list = [1, {}, lambda x: x]
        for sort in sort_list:
            with pytest.raises(TypeError):
                Aggregate(sort=sort)

    def test_invalid_select(self):
        selectors = ['str', 1, []]
        for _select in selectors:
            with pytest.raises(TypeError):
                Aggregate(select=_select)

    def test_invalid_call(self):
        call_params = ['str', 1, None]
        for param in call_params:
            with pytest.raises(TypeError):
                Aggregate()(param)

    def test_call_without_function(self):
        aggregate_instance = Aggregate()
        with pytest.raises(TypeError):
            aggregate_instance()

    def test_call_with_function(self):
        aggregate_instance = Aggregate()

        def test_function(x):
            return x

        assert not getattr(test_function, '_flow_aggregate', False)
        test_function = aggregate_instance(test_function)
        assert getattr(test_function, '_flow_aggregate', False)

    def test_with_decorator_with_pre_initialization(self):
        aggregate_instance = Aggregate()

        @aggregate_instance
        def test_function(x):
            return x

        assert getattr(test_function, '_flow_aggregate', False)

    def test_with_decorator_without_pre_initialization(self):
        @Aggregate()
        def test_function(x):
            return x

        assert getattr(test_function, '_flow_aggregate', False)

    def test_groups_of_invalid_num(self):
        invalid_values = [{}, 'str', -1, -1.5]
        for invalid_value in invalid_values:
            with pytest.raises((TypeError, ValueError)):
                Aggregate.groupsof(invalid_value)

    def test_group_by_invalid_key(self):
        with pytest.raises(TypeError):
            Aggregate.groupby(1)

    def test_groupby_with_invalid_type_default_key_for_Iterable(self, setUp, project):
        with pytest.raises(TypeError):
            Aggregate.groupby(['half', 'even'], default=-1)

    def test_groupby_with_invalid_length_default_key_for_Iterable(self, setUp, project):
        with pytest.raises(ValueError):
            Aggregate.groupby(['half', 'even'], default=[-1, -1, -1])


class TestMakeAggregate(AggregateProjectSetup):

    def test_valid_aggregator_non_partial(self, setUp, project):
        # Return groups of 1
        def helper_aggregator(jobs):
            for job in jobs:
                yield [job]

        aggregate_instance = Aggregate(helper_aggregator)

        aggregate_instance = _MakeAggregate(aggregate_instance)
        aggregate_job_manual = helper_aggregator(project)
        aggregate_job_via_aggregator = aggregate_instance(project)

        assert [aggregate for aggregate in aggregate_job_manual] == \
               aggregate_job_via_aggregator

    def test_valid_aggregator_partial(self, setUp, project):
        aggregate_instance = Aggregate(lambda jobs: [jobs])
        aggregate_instance = _MakeAggregate(aggregate_instance)
        aggregate_job_via_aggregator = aggregate_instance(project)

        assert [[job for job in project]] == \
               aggregate_job_via_aggregator

    def test_valid_sort(self, setUp, project):
        helper_sort = partial(sorted, key=lambda job: job.sp.i)
        aggregate_instance = Aggregate(sort='i')
        aggregate_instance = _MakeAggregate(aggregate_instance)

        assert([helper_sort(project)] == aggregate_instance(project))

    def test_valid_reversed_sort(self, setUp, project):
        helper_sort = partial(sorted, key=lambda job: job.sp.i, reverse=True)
        aggregate_instance = Aggregate(sort='i', reverse=True)
        aggregate_instance = _MakeAggregate(aggregate_instance)

        assert([helper_sort(project)] == aggregate_instance(project))

    def test_groups_of_valid_num(self, setUp, project):
        valid_values = [1, 2, 3, 6]

        total_jobs = len(project)

        for valid_value in valid_values:
            aggregate_instance = Aggregate.groupsof(valid_value)
            aggregate_instance = _MakeAggregate(aggregate_instance)
            aggregate_job_via_aggregator = aggregate_instance(project)
            if total_jobs % valid_value == 0:
                length_of_aggregate = total_jobs/valid_value
            else:
                length_of_aggregate = int(total_jobs/valid_value) + 1
            assert len(aggregate_job_via_aggregator) == length_of_aggregate

    def test_groupby_with_valid_string_key(self, setUp, project):
        aggregate_instance = Aggregate.groupby('even')
        aggregate_instance = _MakeAggregate(aggregate_instance)
        aggregates = 0
        for agg in aggregate_instance(project):
            aggregates += 1
        assert aggregates == 2

    def test_groupby_with_invalid_string_key(self, setUp, project):
        aggregate_instance = Aggregate.groupby('invalid_key')
        aggregate_instance = _MakeAggregate(aggregate_instance)
        with pytest.raises(KeyError):
            for agg in aggregate_instance(project):
                pass

    def test_groupby_with_default_key_for_string(self, setUp, project):
        aggregate_instance = Aggregate.groupby('half', default=-1)
        aggregate_instance = _MakeAggregate(aggregate_instance)
        aggregates = 0
        for agg in aggregate_instance(project):
            aggregates += 1
        assert aggregates == 6

    def test_groupby_with_Iterable_key(self, setUp, project):
        aggregate_instance = Aggregate.groupby(['i', 'even'])
        aggregate_instance = _MakeAggregate(aggregate_instance)
        aggregates = 0
        for agg in aggregate_instance(project):
            aggregates += 1
        assert aggregates == 10

    def test_groupby_with_invalid_Iterable_key(self, setUp, project):
        aggregate_instance = Aggregate.groupby(['half', 'even'])
        aggregate_instance = _MakeAggregate(aggregate_instance)
        with pytest.raises(KeyError):
            for agg in aggregate_instance(project):
                pass

    def test_groupby_with_valid_default_key_for_Iterable(self, setUp, project):
        aggregate_instance = Aggregate.groupby(['half', 'even'], default=[-1, -1])
        aggregate_instance = _MakeAggregate(aggregate_instance)
        aggregates = 0
        for agg in aggregate_instance(project):
            aggregates += 1
        assert aggregates == 6

    def test_groupby_with_callable_key(self, setUp, project):
        def keyfunction(job):
            return job.sp['even']

        aggregate_instance = Aggregate.groupby(keyfunction)
        aggregate_instance = _MakeAggregate(aggregate_instance)
        aggregates = 0
        for agg in aggregate_instance(project):
            aggregates += 1
        assert aggregates == 2

    def test_groupby_with_invalid_callable_key(self, setUp, project):
        def keyfunction(job):
            return job.sp['half']
        aggregate_instance = Aggregate.groupby(keyfunction)
        aggregate_instance = _MakeAggregate(aggregate_instance)
        with pytest.raises(KeyError):
            for agg in aggregate_instance(project):
                pass

    def test_valid_select(self, setUp, project):
        def _select(job):
            return job.sp.i > 5

        aggregate_instance = Aggregate.groupsof(1, select=_select)
        aggregate_instance = _MakeAggregate(aggregate_instance)
        selected_jobs = []
        for job in project:
            if _select(job):
                selected_jobs.append([job])
        assert aggregate_instance(project) == selected_jobs
