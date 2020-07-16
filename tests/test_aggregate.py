import pytest

from tempfile import TemporaryDirectory

import signac
from flow.aggregate import Aggregate


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
        assert aggregate_instance._aggregator(test_list) == [test_list]

    def test_invalid_aggregator(self, setUp, project):
        aggregators = ['str', 1, {}]
        for aggregator in aggregators:
            with pytest.raises(TypeError):
                Aggregate(aggregator)

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
