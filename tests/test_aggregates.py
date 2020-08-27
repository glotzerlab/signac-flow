import pytest

from functools import partial
from tempfile import TemporaryDirectory

import signac
from flow.aggregates import Aggregate


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


# Test the decorator class Aggregate
class TestAggregate(AggregateProjectSetup):

    def test_default_init(self):
        aggregate_instance = Aggregate()
        test_list = (1, 2, 3, 4, 5)
        assert aggregate_instance._sort_by is None
        assert aggregate_instance._aggregator(test_list) == [test_list]
        assert aggregate_instance._select is None

    def test_invalid_aggregator(self, setUp, project):
        aggregators = ['str', 1, {}]
        for aggregator in aggregators:
            with pytest.raises(TypeError):
                Aggregate(aggregator)

    def test_invalid_sort_by(self):
        sort_list = [1, {}, lambda x: x]
        for sort in sort_list:
            with pytest.raises(TypeError):
                Aggregate(sort_by=sort)

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

    def test_call_without_decorator(self):
        aggregate_instance = Aggregate()
        with pytest.raises(TypeError):
            aggregate_instance()

    def test_call_with_decorator(self):
        @Aggregate()
        def test_function(x):
            return x

        assert hasattr(test_function, '_flow_aggregate')

    def test_groups_of_invalid_num(self):
        invalid_values = [{}, 'str', -1, -1.5]
        for invalid_value in invalid_values:
            with pytest.raises((TypeError, ValueError)):
                Aggregate.groupsof(invalid_value)

    def test_group_by_invalid_key(self):
        with pytest.raises(TypeError):
            Aggregate.groupby(1)

    def test_groupby_with_valid_type_default_for_Iterable(self):
        Aggregate.groupby(['half', 'even'], default=[-1, -1])

    def test_groupby_with_invalid_type_default_key_for_Iterable(self):
        with pytest.raises(TypeError):
            Aggregate.groupby(['half', 'even'], default=-1)

    def test_groupby_with_invalid_length_default_key_for_Iterable(self):
        with pytest.raises(ValueError):
            Aggregate.groupby(['half', 'even'], default=[-1, -1, -1])

    def test_aggregate_hashing(self):
        # Since we need to store groups on a per aggregate basis in the project,
        # we need to be sure that the aggregates are hashing and compared correctly.
        # This test ensures this feature.
        def helper_default_aggregator(jobs):
            return [jobs]

        def helper_non_default_aggregator(jobs):
            for job in jobs:
                yield (job,)

        list_of_aggregates = [
            Aggregate(), Aggregate(), Aggregate(helper_default_aggregator),
            Aggregate(helper_non_default_aggregator), Aggregate(helper_non_default_aggregator),
            Aggregate.groupsof(1), Aggregate.groupsof(1),
            Aggregate.groupsof(2), Aggregate.groupsof(3), Aggregate.groupsof(4),
            Aggregate.groupby('half'), Aggregate.groupby('half'), Aggregate.groupby('even'),
            Aggregate.groupby('half', -1), Aggregate.groupby('even', -1),
            Aggregate.groupby(['half', 'even'], default=[-1, -1])
        ]

        assert len(set(list_of_aggregates)) == 11


# Test the _StoreAggregate and _StoreDefaultAggregate classes
class TestStoreAggregate(AggregateProjectSetup):

    def test_valid_aggregator(self, setUp, project):
        # Return groups of 1
        def helper_aggregator(jobs):
            for job in jobs:
                yield (job,)

        aggregate_instance = Aggregate(helper_aggregator)
        aggregate_instance = aggregate_instance._create_AggregatesStore(project)

        aggregate_job_manual = helper_aggregator(project)

        assert [aggregate for aggregate in aggregate_job_manual] == \
               list(aggregate_instance)

    def test_valid_lambda_aggregator(self, setUp, project):
        aggregate_instance = Aggregate(lambda jobs: [jobs])
        aggregate_instance = aggregate_instance._create_AggregatesStore(project)

        assert [tuple(project)] == list(aggregate_instance)

    def test_valid_sort_by(self, setUp, project):
        helper_sort = partial(sorted, key=lambda job: job.sp.i)
        aggregate_instance = Aggregate(sort_by='i')
        aggregate_instance = aggregate_instance._create_AggregatesStore(project)

        assert [tuple(helper_sort(project))] == list(aggregate_instance)

    def test_valid_reversed_sort(self, setUp, project):
        helper_sort = partial(sorted, key=lambda job: job.sp.i, reverse=True)
        aggregate_instance = Aggregate(sort_by='i', reverse_order=True)
        aggregate_instance = aggregate_instance._create_AggregatesStore(project)
        assert [tuple(helper_sort(project))] == list(aggregate_instance)

    def test_groups_of_valid_num(self, setUp, project):
        valid_values = [1, 2, 3, 6, 10]
        expected_aggregates = [10, 5, 4, 2, 1]

        for i, valid_value in enumerate(valid_values):
            aggregate_instance = Aggregate.groupsof(valid_value)
            aggregate_instance = aggregate_instance._create_AggregatesStore(project)
            assert len(aggregate_instance) == expected_aggregates[i]

    def test_groupby_with_valid_string_key(self, setUp, project):
        aggregate_instance = Aggregate.groupby('even')
        aggregate_instance = aggregate_instance._create_AggregatesStore(project)
        for aggregate in aggregate_instance:
            even = aggregate[0].sp.even
            for job in aggregate:
                assert even == job.sp.even
        assert len(aggregate_instance) == 2

    def test_groupby_with_invalid_string_key(self, setUp, project):
        aggregate_instance = Aggregate.groupby('invalid_key')
        with pytest.raises(KeyError):
            # We will attempt to generate aggregates but will fail in
            # doing so due to the invalid key
            aggregate_instance._create_AggregatesStore(project)

    def test_groupby_with_default_key_for_string(self, setUp, project):
        aggregate_instance = Aggregate.groupby('half', default=-1)
        aggregate_instance = aggregate_instance._create_AggregatesStore(project)
        for aggregate in aggregate_instance:
            half = aggregate[0].sp.get('half', -1)
            for job in aggregate:
                assert half == job.sp.get('half', -1)
        assert len(aggregate_instance) == 6

    def test_groupby_with_Iterable_key(self, setUp, project):
        aggregate_instance = Aggregate.groupby(['i', 'even'])
        aggregate_instance = aggregate_instance._create_AggregatesStore(project)
        # No aggregation takes place
        assert len(aggregate_instance) == 10

    def test_groupby_with_invalid_Iterable_key(self, setUp, project):
        aggregate_instance = Aggregate.groupby(['half', 'even'])
        with pytest.raises(KeyError):
            # We will attempt to generate aggregates but will fail in
            # doing so due to the invalid keys
            aggregate_instance._create_AggregatesStore(project)

    def test_groupby_with_valid_default_key_for_Iterable(self, setUp, project):
        aggregate_instance = Aggregate.groupby(['half', 'even'], default=[-1, -1])
        aggregate_instance = aggregate_instance._create_AggregatesStore(project)
        for aggregate in aggregate_instance:
            half = aggregate[0].sp.get('half', -1)
            even = aggregate[0].sp.get('even', -1)
            for job in aggregate:
                assert half == job.sp.get('half', -1) and even == job.sp.get('even', -1)
        assert len(aggregate_instance) == 6

    def test_groupby_with_callable_key(self, setUp, project):
        def keyfunction(job):
            return job.sp['even']

        aggregate_instance = Aggregate.groupby(keyfunction)
        aggregate_instance = aggregate_instance._create_AggregatesStore(project)
        for aggregate in aggregate_instance:
            even = aggregate[0].sp.even
            for job in aggregate:
                assert even == job.sp.even
        assert len(aggregate_instance) == 2

    def test_groupby_with_invalid_callable_key(self, setUp, project):
        def keyfunction(job):
            return job.sp['half']
        aggregate_instance = Aggregate.groupby(keyfunction)
        with pytest.raises(KeyError):
            # We will attempt to generate aggregates but will fail in
            # doing so due to the invalid key
            aggregate_instance._create_AggregatesStore(project)

    def test_valid_select(self, setUp, project):
        def _select(job):
            return job.sp.i > 5

        aggregate_instance = Aggregate.groupsof(1, select=_select)
        aggregate_instance = aggregate_instance._create_AggregatesStore(project)
        selected_jobs = []
        for job in project:
            if _select(job):
                selected_jobs.append((job,))
        assert list(aggregate_instance) == selected_jobs
