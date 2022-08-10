from functools import partial
from math import ceil

import pytest
import signac
from conftest import TestProjectBase

from flow.aggregates import _DefaultAggregateStore, aggregator, get_aggregate_id
from flow.errors import FlowProjectDefinitionError


class AggregateProjectSetup(TestProjectBase):
    project_name = "AggregateTestProject"

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
    def mocked_project(self):
        return self.mock_project()


class AggregateFixtures:
    @classmethod
    def _get_single_job_aggregate(cls, jobs):
        for job in jobs:
            yield (job,)

    @classmethod
    def _get_all_job_aggregate(cls, jobs):
        return (tuple(jobs),)

    @pytest.fixture
    def get_single_job_aggregate(self):
        return AggregateFixtures._get_single_job_aggregate

    @pytest.fixture
    def get_all_job_aggregate(self):
        return AggregateFixtures._get_all_job_aggregate

    @classmethod
    def list_of_aggregators(cls):
        # The below list contains 14 distinct aggregator objects and some duplicates.
        return [
            aggregator(),
            aggregator(),
            aggregator(cls._get_all_job_aggregate),
            aggregator(cls._get_single_job_aggregate),
            aggregator(cls._get_single_job_aggregate),
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

    def create_aggregate_store(self, aggregator_instance, mocked_project):
        return aggregator_instance._create_AggregateStore(mocked_project)

    def get_aggregates_from_store(self, aggregate_store):
        return tuple(aggregate_store.values())


# Test the _AggregateStore and _DefaultAggregateStore classes
class TestAggregateStore(AggregateProjectSetup, AggregateFixtures):
    def test_custom_aggregator_function(
        self, mocked_project, get_single_job_aggregate, get_all_job_aggregate
    ):
        # Testing aggregator function returning aggregates of 1
        aggregate_store = self.create_aggregate_store(
            aggregator(get_single_job_aggregate), mocked_project
        )
        aggregate_job_manual = tuple(get_single_job_aggregate(mocked_project))
        assert self.get_aggregates_from_store(aggregate_store) == aggregate_job_manual

        # Testing aggregator function returning aggregates of all the jobs
        aggregate_store = self.create_aggregate_store(
            aggregator(get_all_job_aggregate), mocked_project
        )
        assert self.get_aggregates_from_store(aggregate_store) == get_all_job_aggregate(
            mocked_project
        )

    def test_sort_by(self, mocked_project):
        helper_sort = partial(sorted, key=lambda job: job.sp.i)
        aggregate_store = self.create_aggregate_store(
            aggregator(sort_by="i"), mocked_project
        )
        assert self.get_aggregates_from_store(aggregate_store) == (
            tuple(helper_sort(mocked_project)),
        )

    def test_sort_by_callable(self, mocked_project):
        def keyfunction(job):
            return job.sp.i

        helper_sort = partial(sorted, key=keyfunction)
        aggregate_store = self.create_aggregate_store(
            aggregator(sort_by=keyfunction), mocked_project
        )
        assert self.get_aggregates_from_store(aggregate_store) == (
            tuple(helper_sort(mocked_project)),
        )

    def test_sort_descending(self, mocked_project):
        helper_sort = partial(sorted, key=lambda job: job.sp.i, reverse=True)
        aggregate_store = self.create_aggregate_store(
            aggregator(sort_by="i", sort_ascending=False), mocked_project
        )
        assert self.get_aggregates_from_store(aggregate_store) == (
            tuple(helper_sort(mocked_project)),
        )

    @pytest.mark.parametrize("aggregate_length", [1, 2, 3, 6, 10])
    def test_groups_of_valid_num(self, mocked_project, aggregate_length):
        aggregate_store = self.create_aggregate_store(
            aggregator.groupsof(aggregate_length), mocked_project
        )
        expected_len = ceil(10 / aggregate_length)
        assert len(aggregate_store) == expected_len

        # We also check the length of every aggregate in order to ensure
        # proper aggregation.
        last_agg_len = (
            10 % aggregate_length if (10 % aggregate_length != 0) else aggregate_length
        )
        for j, aggregate in enumerate(aggregate_store.values()):
            if j == expected_len - 1:  # Checking for the last aggregate
                assert len(aggregate) == last_agg_len
            else:
                assert len(aggregate) == aggregate_length

    def test_groupby_with_valid_string_key(self, mocked_project):
        aggregate_store = self.create_aggregate_store(
            aggregator.groupby("even"), mocked_project
        )
        assert len(aggregate_store) == 2
        for aggregate in aggregate_store.values():
            even = aggregate[0].sp.even
            assert all(even == job.sp.even for job in aggregate)

    def test_groupby_with_invalid_string_key(self, mocked_project):
        with pytest.raises(KeyError):
            # We will attempt to generate aggregates but will fail in
            # doing so due to the invalid key
            self.create_aggregate_store(
                aggregator.groupby("invalid_key"), mocked_project
            )

    def test_groupby_with_default_key_for_string(self, mocked_project):
        aggregate_store = self.create_aggregate_store(
            aggregator.groupby("half", default=-1), mocked_project
        )
        assert len(aggregate_store) == 6
        for aggregate in aggregate_store.values():
            half = aggregate[0].sp.get("half", -1)
            assert all(half == job.sp.get("half", -1) for job in aggregate)

    def test_groupby_with_Iterable_key(self, mocked_project):
        aggregate_store = self.create_aggregate_store(
            aggregator.groupby(["i", "even"]), mocked_project
        )
        # No aggregation takes place hence this means we don't need to check
        # whether all the aggregate members are equivalent.
        assert len(aggregate_store) == 10

    def test_groupby_with_invalid_Iterable_key(self, mocked_project):
        with pytest.raises(KeyError):
            # We will attempt to generate aggregates but will fail in
            # doing so due to the invalid keys
            self.create_aggregate_store(
                aggregator.groupby(["half", "even"]), mocked_project
            )

    def test_groupby_with_valid_default_key_for_Iterable(self, mocked_project):
        aggregate_store = self.create_aggregate_store(
            aggregator.groupby(["half", "even"], default=[-1, -1]), mocked_project
        )
        assert len(aggregate_store) == 6
        for aggregate in aggregate_store.values():
            half = aggregate[0].sp.get("half", -1)
            even = aggregate[0].sp.get("even", -1)
            assert all(
                half == job.sp.get("half", -1) and even == job.sp.get("even", -1)
                for job in aggregate
            )

    def test_groupby_with_callable_key(self, mocked_project):
        def keyfunction(job):
            return job.sp["even"]

        aggregate_store = self.create_aggregate_store(
            aggregator.groupby(keyfunction), mocked_project
        )
        assert len(aggregate_store) == 2
        for aggregate in aggregate_store.values():
            even = aggregate[0].sp.even
            assert all(even == job.sp.even for job in aggregate)

    def test_groupby_with_invalid_callable_key(self, mocked_project):
        def keyfunction(job):
            return job.sp["half"]

        with pytest.raises(KeyError):
            # We will attempt to generate aggregates but will fail in
            # doing so due to the invalid key
            self.create_aggregate_store(aggregator.groupby(keyfunction), mocked_project)

    def test_valid_select(self, mocked_project):
        def _select(job):
            return job.sp.i > 5

        aggregate_store = self.create_aggregate_store(
            aggregator.groupsof(1, select=_select), mocked_project
        )
        selected_jobs = []
        for job in mocked_project:
            if _select(job):
                selected_jobs.append((job,))
        assert self.get_aggregates_from_store(aggregate_store) == tuple(selected_jobs)

    def test_store_hashing(self, mocked_project):
        # Since we need to store groups on a per aggregate basis in the mocked_project,
        # we need to be sure that the aggregates are hashing and compared correctly.
        # This test ensures this feature.
        total_aggregators = AggregateFixtures.list_of_aggregators()
        list_of_stores = [
            self.create_aggregate_store(aggregator, mocked_project)
            for aggregator in total_aggregators
        ]
        assert len(list_of_stores) == len(total_aggregators)
        # The above list contains 14 distinct store objects (because a
        # store object is differentiated on the basis of the
        # ``_is_default_aggregate`` attribute of the aggregator). When this
        # list is converted to a set, then these objects are hashed first and
        # then compared. Since sets don't carry duplicate values, we test
        # whether the length of the set obtained from the list is equal to 14
        # or not.
        assert len(set(list_of_stores)) == 14

    @pytest.mark.parametrize(
        "aggregator_instance", AggregateFixtures.list_of_aggregators()
    )
    def test_aggregates_are_tuples(self, mocked_project, aggregator_instance):
        # This test ensures that all aggregator functions return tuples. All
        # aggregate stores are expected to return tuples for their values, but
        # this also tests that the aggregator functions (groupsof, groupby) are
        # generating tuples internally.
        aggregate_store = self.create_aggregate_store(
            aggregator_instance, mocked_project
        )
        if not isinstance(aggregate_store, _DefaultAggregateStore):
            for aggregate in aggregate_store._generate_aggregates():
                assert isinstance(aggregate, tuple)
                assert all(isinstance(job, signac.contrib.job.Job) for job in aggregate)
        for aggregate in aggregate_store.values():
            assert isinstance(aggregate, tuple)
            assert all(isinstance(job, signac.contrib.job.Job) for job in aggregate)

    @pytest.mark.parametrize(
        "aggregator_instance", AggregateFixtures.list_of_aggregators()
    )
    def test_get_by_id(self, mocked_project, aggregator_instance):
        # Ensure that all aggregates can be fetched by id.
        aggregate_store = self.create_aggregate_store(
            aggregator_instance, mocked_project
        )
        for aggregate in aggregate_store.values():
            assert aggregate == aggregate_store[get_aggregate_id(aggregate)]

    def test_get_invalid_id(self, mocked_project):
        jobs = tuple(mocked_project)
        full_aggregate_store = self.create_aggregate_store(aggregator(), mocked_project)
        default_aggregate_store = self.create_aggregate_store(
            aggregator.groupsof(1), mocked_project
        )
        # Test for an aggregate of single job for aggregator instance
        with pytest.raises(LookupError):
            full_aggregate_store[get_aggregate_id((jobs[0],))]
        # Test for an aggregate of all jobs for default aggregator
        with pytest.raises(LookupError):
            default_aggregate_store[get_aggregate_id(jobs)]

    def test_contains(self, mocked_project):
        jobs = tuple(mocked_project)
        full_aggregate_store = self.create_aggregate_store(aggregator(), mocked_project)
        default_aggregate_store = aggregator.groupsof(1)._create_AggregateStore(
            mocked_project
        )
        # Test for an aggregate of all jobs
        assert get_aggregate_id(jobs) in full_aggregate_store
        assert get_aggregate_id(jobs) not in default_aggregate_store
        # Test for an aggregate of single job
        assert not jobs[0].id in full_aggregate_store
        assert jobs[0].id in default_aggregate_store


# Test the decorator class aggregator
class TestAggregate(AggregateProjectSetup, AggregateFixtures):
    @pytest.mark.parametrize("agg_value", [(1, 2, 3, 4, 5), (), [1, 2, 3, 4, 5], []])
    def test_default_init(self, agg_value):
        aggregate_instance = aggregator()
        assert not aggregate_instance._is_default_aggregator
        assert aggregate_instance._sort_by is None
        assert aggregate_instance._sort_ascending
        assert aggregate_instance._select is None
        assert list(aggregate_instance._aggregator_function(agg_value)) == [
            tuple(agg_value)
        ]

    @pytest.mark.parametrize("aggregator_function", ["str", 1, {}])
    def test_invalid_aggregator_function(self, aggregator_function):
        with pytest.raises(TypeError):
            aggregator(aggregator_function)

    @pytest.mark.parametrize("sort_by", [1, {}])
    def test_invalid_sort_by(self, sort_by):
        with pytest.raises(TypeError):
            aggregator(sort_by=sort_by)

    @pytest.mark.parametrize("select", ["str", 1, []])
    def test_invalid_select(self, select):
        with pytest.raises(TypeError):
            aggregator(select=select)

    @pytest.mark.parametrize("param", ["str", 1, None])
    def test_invalid_call(self, param):
        aggregator_instance = aggregator()
        with pytest.raises(FlowProjectDefinitionError):
            aggregator_instance(param)

    def test_call_without_decorator(self):
        aggregate_instance = aggregator()
        with pytest.raises(FlowProjectDefinitionError):
            aggregate_instance()

    def test_call_with_decorator(self):
        @aggregator()
        def test_function(x):
            return x

        assert hasattr(test_function, "_flow_aggregate")

    @pytest.mark.parametrize("invalid_value", [{}, "str", -1, -1.5])
    def test_groups_of_invalid_num(self, invalid_value):
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

    def test_aggregate_hashing(self):
        # Since we need to store groups on a per aggregate basis in the project,
        # we need to be sure that the aggregates are hashing and compared correctly.
        # This test ensures this feature.
        # list_of_aggregators contains 14 distinct store objects (because an
        # aggregator object is differentiated on the basis of the `_is_aggregate` attribute).
        # When this list is converted to set, then these objects are hashed first
        # and then compared. Since sets don't carry duplicate values, we test
        # whether the length of the set obtained from the list is equal to 14 or not.
        total_aggregators = AggregateFixtures.list_of_aggregators()
        assert len(set(total_aggregators)) == 14
        # Ensure that equality implies hash equality.
        for agg1 in total_aggregators:
            for agg2 in total_aggregators:
                if agg1 == agg2:
                    assert hash(agg1) == hash(agg2)
