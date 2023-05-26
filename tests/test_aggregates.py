from functools import partial
from math import ceil

import pytest
from conftest import TestProjectBase

import flow
from flow.aggregates import (
    _AggregateStoresCursor,
    _JobAggregateCursor,
    aggregator,
    get_aggregate_id,
)


def generate_flow_project():
    class SimpleAggregateProject(flow.FlowProject):
        pass

    @SimpleAggregateProject.operation(aggregator=aggregator.groupby("is_even"))
    def op1(job):
        pass

    @SimpleAggregateProject.operation(aggregator=aggregator.groupby("is_even"))
    def op2(job):
        pass

    @SimpleAggregateProject.operation(aggregator=aggregator.groupsof(2))
    def op3(job):
        pass

    @SimpleAggregateProject.operation(aggregator=aggregator())
    def op4(job):
        pass

    @SimpleAggregateProject.operation
    def op5(job):
        pass

    return SimpleAggregateProject


class AggregateProjectSetup(TestProjectBase):
    project_class = generate_flow_project()

    def mock_project(self):
        project = self.project_class.get_project(path=self._tmp_dir.name)
        for i in range(10):
            is_even = (i % 2) == 0
            if is_even:
                project.open_job(dict(i=i, half=i / 2, is_even=is_even)).init()
            else:
                project.open_job(dict(i=i, is_even=is_even)).init()
        project._reregister_aggregates()
        return project

    @pytest.fixture
    def mocked_project(self):
        return self.mock_project()


class AggregateFixtures:
    @pytest.fixture
    def get_single_job_aggregate(self):
        def generator(jobs):
            yield from ((job,) for job in jobs)

        return generator

    @pytest.fixture
    def get_all_job_aggregate(self):
        def generator(jobs):
            return (tuple(jobs),)

        return generator

    @pytest.fixture
    def list_of_aggregators(self, get_single_job_aggregate, get_all_job_aggregate):
        # The below list contains 14 distinct aggregator objects and some duplicates.
        return [
            aggregator(),
            aggregator(),
            aggregator(get_all_job_aggregate),
            aggregator(get_single_job_aggregate),
            aggregator(get_single_job_aggregate),
            aggregator.groupsof(1),
            aggregator.groupsof(1),
            aggregator.groupsof(2),
            aggregator.groupsof(3),
            aggregator.groupsof(4),
            aggregator.groupby("is_even"),
            aggregator.groupby("is_even"),
            aggregator.groupby("half", -1),
            aggregator.groupby("half", -1),
            aggregator.groupby(["half", "is_even"], default=[-1, -1]),
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
        expected_length = ceil(10 / aggregate_length)
        assert len(aggregate_store) == expected_length

        # We also check the length of every aggregate in order to ensure
        # proper aggregation.
        last_agg_length = (
            10 % aggregate_length if (10 % aggregate_length != 0) else aggregate_length
        )
        aggregates = list(aggregate_store.values())
        assert all(len(agg) == aggregate_length for agg in aggregates[:-1])
        assert len(aggregates[-1]) == last_agg_length

    def test_groupby_with_valid_string_key(self, mocked_project):
        aggregate_store = self.create_aggregate_store(
            aggregator.groupby("is_even"), mocked_project
        )
        assert len(aggregate_store) == 2
        for aggregate in aggregate_store.values():
            even = aggregate[0].sp.is_even
            assert all(even == job.sp.is_even for job in aggregate)

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
            aggregator.groupby(["i", "is_even"]), mocked_project
        )
        # No aggregation takes place hence this means we don't need to check
        # whether all the aggregate members are equivalent.
        assert len(aggregate_store) == len(mocked_project)

    def test_groupby_with_invalid_Iterable_key(self, mocked_project):
        with pytest.raises(KeyError):
            # We will attempt to generate aggregates but will fail in
            # doing so due to the invalid keys
            self.create_aggregate_store(
                aggregator.groupby(["half", "is_even"]), mocked_project
            )

    def test_groupby_with_valid_default_key_for_Iterable(self, mocked_project):
        aggregate_store = self.create_aggregate_store(
            aggregator.groupby(["half", "is_even"], default=[-1, -1]), mocked_project
        )
        assert len(aggregate_store) == 6
        for aggregate in aggregate_store.values():
            half = aggregate[0].sp.get("half", -1)
            even = aggregate[0].sp["is_even"]
            assert all(
                half == job.sp.get("half", -1) and even == job.sp["is_even"]
                for job in aggregate
            )

    def test_groupby_with_callable_key(self, mocked_project):
        def keyfunction(job):
            return job.sp["is_even"]

        aggregate_store = self.create_aggregate_store(
            aggregator.groupby(keyfunction), mocked_project
        )
        assert len(aggregate_store) == 2
        for aggregate in aggregate_store.values():
            even = aggregate[0].sp.is_even
            assert all(even == job.sp.is_even for job in aggregate)

    def test_groupby_with_invalid_callable_key(self, mocked_project):
        def keyfunction(job):
            return job.sp["half"]

        with pytest.raises(KeyError):
            # We will attempt to generate aggregates but will fail in
            # doing so due to the invalid key
            self.create_aggregate_store(aggregator.groupby(keyfunction), mocked_project)

    def test_valid_select(self, mocked_project):
        def select(job):
            return job.sp.i > 5

        aggregate_store = self.create_aggregate_store(
            aggregator.groupsof(1, select=select), mocked_project
        )
        selected_jobs = tuple((job,) for job in mocked_project if select(job))
        assert self.get_aggregates_from_store(aggregate_store) == selected_jobs

    def test_store_hashing(self, mocked_project, list_of_aggregators):
        # Since we need to store groups on a per aggregate basis in the mocked_project,
        # we need to be sure that the aggregates are hashing and compared correctly.
        list_of_stores = [
            self.create_aggregate_store(aggregator, mocked_project)
            for aggregator in list_of_aggregators
        ]
        # The above list contains 14 distinct store objects (because a
        # store object is differentiated on the basis of the
        # ``_is_default_aggregate`` attribute of the aggregator). When this
        # list is converted to a set, then these objects are hashed first and
        # then compared. Since sets don't carry duplicate values, we test
        # whether the length of the set obtained from the list is equal to 14
        # or not.
        assert len(set(list_of_stores)) == 14

    @pytest.mark.parametrize(
        "aggregator_instance",
        [
            aggregator(),
            aggregator.groupsof(1),
            aggregator.groupsof(2),
            aggregator.groupsof(3),
            aggregator.groupsof(4),
            aggregator.groupby("is_even"),
            aggregator.groupby("half", -1),
            aggregator.groupby(["half", "is_even"], default=[-1, -1]),
        ],
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
    def test_default_init(self):
        aggregate_instance = aggregator()
        assert not aggregate_instance._is_default_aggregator
        assert aggregate_instance._sort_by is None
        assert aggregate_instance._sort_ascending
        assert aggregate_instance._select is None
        assert next(aggregate_instance._aggregator_function((1, 2, 3))) == (1, 2, 3)

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

    @pytest.mark.parametrize("invalid_value", [{}, "str", -1, -1.5])
    def test_groups_of_invalid_num(self, invalid_value):
        with pytest.raises((TypeError, ValueError)):
            aggregator.groupsof(invalid_value)

    def test_group_by_invalid_key(self):
        with pytest.raises(TypeError):
            aggregator.groupby(1)

    def test_groupby_with_valid_type_default_for_Iterable(self):
        aggregator.groupby(["half", "is_even"], default=[-1, -1])

    def test_groupby_with_invalid_type_default_key_for_Iterable(self):
        with pytest.raises(TypeError):
            aggregator.groupby(["half", "is_even"], default=-1)

    def test_groupby_with_invalid_length_default_key_for_Iterable(self):
        with pytest.raises(ValueError):
            aggregator.groupby(["half", "is_even"], default=[-1, -1, -1])

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


class TestAggregateUtilities(AggregateProjectSetup):
    def test_skips_duplicate_aggregates(self, mocked_project):
        # Check that length and actual iteration are the same and correct.
        # If this did not skip duplicates the length would be 19.
        stores = _AggregateStoresCursor(mocked_project)
        assert len(stores) == 18
        assert len(list(stores)) == 18
        # Ensure that some known aggregates are in the cursor
        assert tuple(mocked_project) in stores
        assert all((job,) in stores for job in mocked_project)

    def test_filters(self, mocked_project):
        agg_cursor = _JobAggregateCursor(
            project=mocked_project, filter={"is_even": True}
        )
        assert len(agg_cursor) == 5

    def test_reregister_aggregates(self, mocked_project):
        agg_cursor = _AggregateStoresCursor(project=mocked_project)
        NUM_BEFORE_REREGISTRATION = 18
        assert len(agg_cursor) == NUM_BEFORE_REREGISTRATION
        new_jobs = [
            mocked_project.open_job(dict(i=10, is_even=True, half=5)),
            mocked_project.open_job(dict(i=11, is_even=False)),
        ]
        assert not any(j in mocked_project for j in new_jobs)
        for job in new_jobs:
            job.init()
        # Default aggregate store doesn't need to be re-registered.
        assert len(agg_cursor) == NUM_BEFORE_REREGISTRATION + 2
        mocked_project._reregister_aggregates()
        # The operation op3 adds another aggregate in the mocked_project.
        assert len(agg_cursor) == NUM_BEFORE_REREGISTRATION + 3
