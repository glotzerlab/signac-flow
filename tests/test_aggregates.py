from functools import partial
from tempfile import TemporaryDirectory

import pytest
import signac

from flow.aggregates import _DefaultAggregateStore, aggregator, get_aggregate_id
from flow.errors import FlowProjectDefinitionError


@pytest.fixture
def list_of_aggregators():
    def helper_default_aggregator_function(jobs):
        yield tuple(jobs)

    def helper_non_default_aggregator_function(jobs):
        for job in jobs:
            yield (job,)

    # The below list contains 14 distinct aggregator objects and some duplicates.
    return [
        aggregator(),
        aggregator(),
        aggregator(helper_default_aggregator_function),
        aggregator(helper_non_default_aggregator_function),
        aggregator(helper_non_default_aggregator_function),
        aggregator.groupsof(1),
        aggregator.groupsof(1),
        aggregator.groupsof(2),
        aggregator.groupsof(3),
        aggregator.groupsof(4),
        aggregator.groupby("even"),
        aggregator.groupby("even"),
        aggregator.groupby("half", -1),
        aggregator.groupby("half", -1),
        aggregator.groupby(["half", "even"], default=[-1, -1]),
    ]


class AggregateProjectSetup:
    project_class = signac.Project
    entrypoint = dict(path="")

    @pytest.fixture
    def setUp(self, request):
        self._tmp_dir = TemporaryDirectory(prefix="flow-aggregate_")
        request.addfinalizer(self._tmp_dir.cleanup)
        self.project = self.project_class.init_project(
            name="AggregateTestProject", root=self._tmp_dir.name
        )

    def mock_project(self):
        project = self.project_class.get_project(root=self._tmp_dir.name)
        for i in range(10):
            even = (i % 2) == 0
            if even:
                project.open_job(dict(i=i, half=i / 2, even=even)).init()
            else:
                project.open_job(dict(i=i, even=even)).init()
        return project

    @pytest.fixture
    def project(self):
        return self.mock_project()


# Test the decorator class aggregator
class TestAggregate(AggregateProjectSetup):
    def test_default_init(self):
        aggregate_instance = aggregator()
        # Ensure that all values are converted to tuples
        test_values = [(1, 2, 3, 4, 5), (), [1, 2, 3, 4, 5], []]
        assert not aggregate_instance._is_default_aggregator
        assert aggregate_instance._sort_by is None
        assert aggregate_instance._sort_ascending
        assert aggregate_instance._select is None
        for value in test_values:
            assert list(aggregate_instance._aggregator_function(value)) == [
                tuple(value)
            ]

    def test_invalid_aggregator_function(self, setUp, project):
        aggregator_functions = ["str", 1, {}]
        for aggregator_function in aggregator_functions:
            with pytest.raises(TypeError):
                aggregator(aggregator_function)

    def test_invalid_sort_by(self):
        sort_list = [1, {}]
        for sort in sort_list:
            with pytest.raises(TypeError):
                aggregator(sort_by=sort)

    def test_invalid_select(self):
        selectors = ["str", 1, []]
        for _select in selectors:
            with pytest.raises(TypeError):
                aggregator(select=_select)

    def test_invalid_call(self):
        call_params = ["str", 1, None]
        for param in call_params:
            with pytest.raises(FlowProjectDefinitionError):
                aggregator()(param)

    def test_call_without_decorator(self):
        aggregate_instance = aggregator()
        with pytest.raises(FlowProjectDefinitionError):
            aggregate_instance()

    def test_call_with_decorator(self):
        @aggregator()
        def test_function(x):
            return x

        assert hasattr(test_function, "_flow_aggregate")

    def test_groups_of_invalid_num(self):
        invalid_values = [{}, "str", -1, -1.5]
        for invalid_value in invalid_values:
            with pytest.raises((TypeError, ValueError)):
                aggregator.groupsof(invalid_value)

    def test_group_by_invalid_key(self):
        with pytest.raises(TypeError):
            aggregator.groupby(1)

    def test_groupby_with_valid_type_default_for_Iterable(self):
        aggregator.groupby(["half", "even"], default=[-1, -1])

    def test_groupby_with_invalid_type_default_key_for_Iterable(self):
        with pytest.raises(TypeError):
            aggregator.groupby(["half", "even"], default=-1)

    def test_groupby_with_invalid_length_default_key_for_Iterable(self):
        with pytest.raises(ValueError):
            aggregator.groupby(["half", "even"], default=[-1, -1, -1])

    def test_aggregate_hashing(self, list_of_aggregators):
        # Since we need to store groups on a per aggregate basis in the project,
        # we need to be sure that the aggregates are hashing and compared correctly.
        # This test ensures this feature.
        # list_of_aggregators contains 14 distinct store objects (because an
        # aggregator object is differentiated on the basis of the `_is_aggregate` attribute).
        # When this list is converted to set, then these objects are hashed first
        # and then compared. Since sets don't carry duplicate values, we test
        # whether the length of the set obtained from the list is equal to 14 or not.
        assert len(set(list_of_aggregators)) == 14
        # Ensure that equality implies hash equality.
        for agg1 in list_of_aggregators:
            for agg2 in list_of_aggregators:
                if agg1 == agg2:
                    assert hash(agg1) == hash(agg2)


# Test the _AggregateStore and _DefaultAggregateStore classes
class TestAggregateStore(AggregateProjectSetup):
    def test_custom_aggregator_function(self, setUp, project):
        # Testing aggregator function returning aggregates of 1
        def helper_aggregator_function(jobs):
            for job in jobs:
                yield (job,)

        aggregate_instance = aggregator(helper_aggregator_function)
        aggregate_store = aggregate_instance._create_AggregateStore(project)
        aggregate_job_manual = helper_aggregator_function(project)
        assert tuple(aggregate_job_manual) == tuple(aggregate_store.values())

        # Testing aggregator function returning aggregates of all the jobs
        aggregate_instance = aggregator(lambda jobs: [jobs])
        aggregate_store = aggregate_instance._create_AggregateStore(project)
        assert (tuple(project),) == tuple(aggregate_store.values())

    def test_sort_by(self, setUp, project):
        helper_sort = partial(sorted, key=lambda job: job.sp.i)
        aggregate_instance = aggregator(sort_by="i")
        aggregate_store = aggregate_instance._create_AggregateStore(project)
        assert (tuple(helper_sort(project)),) == tuple(aggregate_store.values())

    def test_sort_by_callable(self, setUp, project):
        def keyfunction(job):
            return job.sp.i

        helper_sort = partial(sorted, key=keyfunction)
        aggregate_instance = aggregator(sort_by=keyfunction)
        aggregate_store = aggregate_instance._create_AggregateStore(project)
        assert (tuple(helper_sort(project)),) == tuple(aggregate_store.values())

    def test_sort_descending(self, setUp, project):
        helper_sort = partial(sorted, key=lambda job: job.sp.i, reverse=True)
        aggregate_instance = aggregator(sort_by="i", sort_ascending=False)
        aggregate_store = aggregate_instance._create_AggregateStore(project)
        assert (tuple(helper_sort(project)),) == tuple(aggregate_store.values())

    def test_groups_of_valid_num(self, setUp, project):
        valid_values = [1, 2, 3, 6, 10]
        # Expected length of aggregates which are made using the above values.
        expected_length_of_aggregators = [10, 5, 4, 2, 1]
        # Expect length of each aggregate which are made using the above
        # values. The zeroth index of the nested list denotes the length of all
        # the aggregates expect the last one. The first index denotes the
        # length of the last aggregate formed.
        expected_length_per_aggregate = [[1, 1], [2, 2], [3, 1], [6, 4], [10, 10]]
        for i, valid_value in enumerate(valid_values):
            aggregate_instance = aggregator.groupsof(valid_value)
            aggregate_store = aggregate_instance._create_AggregateStore(project)
            expected_len = expected_length_of_aggregators[i]
            assert len(aggregate_store) == expected_len

            # We also check the length of every aggregate in order to ensure
            # proper aggregation.
            for j, aggregate in enumerate(aggregate_store.values()):
                if j == expected_len - 1:  # Checking for the last aggregate
                    assert len(aggregate) == expected_length_per_aggregate[i][1]
                else:
                    assert len(aggregate) == expected_length_per_aggregate[i][0]

    def test_groupby_with_valid_string_key(self, setUp, project):
        aggregate_instance = aggregator.groupby("even")
        aggregate_store = aggregate_instance._create_AggregateStore(project)
        for aggregate in aggregate_store.values():
            even = aggregate[0].sp.even
            assert all(even == job.sp.even for job in aggregate)
        assert len(aggregate_store) == 2

    def test_groupby_with_invalid_string_key(self, setUp, project):
        aggregate_instance = aggregator.groupby("invalid_key")
        with pytest.raises(KeyError):
            # We will attempt to generate aggregates but will fail in
            # doing so due to the invalid key
            aggregate_instance._create_AggregateStore(project)

    def test_groupby_with_default_key_for_string(self, setUp, project):
        aggregate_instance = aggregator.groupby("half", default=-1)
        aggregate_store = aggregate_instance._create_AggregateStore(project)
        for aggregate in aggregate_store.values():
            half = aggregate[0].sp.get("half", -1)
            assert all(half == job.sp.get("half", -1) for job in aggregate)
        assert len(aggregate_store) == 6

    def test_groupby_with_Iterable_key(self, setUp, project):
        aggregate_instance = aggregator.groupby(["i", "even"])
        aggregate_store = aggregate_instance._create_AggregateStore(project)
        # No aggregation takes place hence this means we don't need to check
        # whether all the aggregate members are equivalent.
        assert len(aggregate_store) == 10

    def test_groupby_with_invalid_Iterable_key(self, setUp, project):
        aggregate_instance = aggregator.groupby(["half", "even"])
        with pytest.raises(KeyError):
            # We will attempt to generate aggregates but will fail in
            # doing so due to the invalid keys
            aggregate_instance._create_AggregateStore(project)

    def test_groupby_with_valid_default_key_for_Iterable(self, setUp, project):
        aggregate_instance = aggregator.groupby(["half", "even"], default=[-1, -1])
        aggregate_store = aggregate_instance._create_AggregateStore(project)
        for aggregate in aggregate_store.values():
            half = aggregate[0].sp.get("half", -1)
            even = aggregate[0].sp.get("even", -1)
            assert all(
                half == job.sp.get("half", -1) and even == job.sp.get("even", -1)
                for job in aggregate
            )
        assert len(aggregate_store) == 6

    def test_groupby_with_callable_key(self, setUp, project):
        def keyfunction(job):
            return job.sp["even"]

        aggregate_instance = aggregator.groupby(keyfunction)
        aggregate_store = aggregate_instance._create_AggregateStore(project)
        for aggregate in aggregate_store.values():
            even = aggregate[0].sp.even
            assert all(even == job.sp.even for job in aggregate)
        assert len(aggregate_store) == 2

    def test_groupby_with_invalid_callable_key(self, setUp, project):
        def keyfunction(job):
            return job.sp["half"]

        aggregate_instance = aggregator.groupby(keyfunction)
        with pytest.raises(KeyError):
            # We will attempt to generate aggregates but will fail in
            # doing so due to the invalid key
            aggregate_instance._create_AggregateStore(project)

    def test_valid_select(self, setUp, project):
        def _select(job):
            return job.sp.i > 5

        aggregate_instance = aggregator.groupsof(1, select=_select)
        aggregate_store = aggregate_instance._create_AggregateStore(project)
        selected_jobs = []
        for job in project:
            if _select(job):
                selected_jobs.append((job,))
        assert list(aggregate_store.values()) == selected_jobs

    def test_store_hashing(self, setUp, project, list_of_aggregators):
        # Since we need to store groups on a per aggregate basis in the project,
        # we need to be sure that the aggregates are hashing and compared correctly.
        # This test ensures this feature.
        list_of_stores = [
            aggregator._create_AggregateStore(project)
            for aggregator in list_of_aggregators
        ]
        assert len(list_of_stores) == len(list_of_aggregators)
        # The above list contains 14 distinct store objects (because a
        # store object is differentiated on the basis of the
        # ``_is_default_aggregate`` attribute of the aggregator). When this
        # list is converted to a set, then these objects are hashed first and
        # then compared. Since sets don't carry duplicate values, we test
        # whether the length of the set obtained from the list is equal to 14
        # or not.
        assert len(set(list_of_stores)) == 14

    def test_aggregates_are_tuples(self, setUp, project, list_of_aggregators):
        # This test ensures that all aggregator functions return tuples. All
        # aggregate stores are expected to return tuples for their values, but
        # this also tests that the aggregator functions (groupsof, groupby) are
        # generating tuples internally.
        for aggregator_instance in list_of_aggregators:
            aggregate_store = aggregator_instance._create_AggregateStore(project)
            if not isinstance(aggregate_store, _DefaultAggregateStore):
                for aggregate in aggregate_store._generate_aggregates():
                    assert isinstance(aggregate, tuple)
                    assert all(
                        isinstance(job, signac.contrib.job.Job) for job in aggregate
                    )
            for aggregate in aggregate_store.values():
                assert isinstance(aggregate, tuple)
                assert all(isinstance(job, signac.contrib.job.Job) for job in aggregate)

    def test_get_by_id(self, setUp, project, list_of_aggregators):
        # Ensure that all aggregates can be fetched by id.
        for aggregator_instance in list_of_aggregators:
            aggregate_store = aggregator_instance._create_AggregateStore(project)
            for aggregate in aggregate_store.values():
                assert aggregate == aggregate_store[get_aggregate_id(aggregate)]

    def test_get_invalid_id(self, setUp, project):
        jobs = tuple(project)
        aggregator_instance = aggregator()._create_AggregateStore(project)
        default_aggregator = aggregator.groupsof(1)._create_AggregateStore(project)
        # Test for an aggregate of single job for aggregator instance
        with pytest.raises(LookupError):
            aggregator_instance[get_aggregate_id((jobs[0],))]
        # Test for an aggregate of all jobs for default aggregator
        with pytest.raises(LookupError):
            default_aggregator[get_aggregate_id(jobs)]

    def test_contains(self, setUp, project):
        jobs = tuple(project)
        aggregator_instance = aggregator()._create_AggregateStore(project)
        default_aggregator = aggregator.groupsof(1)._create_AggregateStore(project)
        # Test for an aggregate of all jobs
        assert get_aggregate_id(jobs) in aggregator_instance
        assert get_aggregate_id(jobs) not in default_aggregator
        # Test for an aggregate of single job
        assert not jobs[0].get_id() in aggregator_instance
        assert jobs[0].get_id() in default_aggregator
