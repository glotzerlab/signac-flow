import pytest
from conftest import TestProjectBase

from flow import FlowProject
from flow.errors import ConfigKeyError
from flow.util.config import get_config_value


class _ConfigProject(FlowProject):
    pass


class TestConfigFilter(TestProjectBase):
    project_class = _ConfigProject

    def test_base(self):
        project = self.mock_project(config_overrides={"flow": {"foo": 4}})
        assert get_config_value(project, "foo") == 4

    def test_missing_key(self):
        project = self.mock_project()
        with pytest.raises(ConfigKeyError):
            get_config_value(project, "foo")

    def test_missing_key_with_default(self):
        project = self.mock_project()
        default = 52
        assert get_config_value(project, "foo", default=default) == default

    def test_set_key_with_default(self):
        default = 52
        set_value = 4
        project = self.mock_project(config_overrides={"flow": {"foo": set_value}})
        assert get_config_value(project, "foo", default=default) == set_value

    def test_with_namespace(self):
        project = self.mock_project(config_overrides={"flow": {"bar": {"foo": 4}}})
        assert get_config_value(project, "foo", ns="bar") == 4

    def test_missing_key_with_default_with_namespace(self):
        default = 52
        project = self.mock_project()
        assert get_config_value(project, "foo", ns="bar", default=default) == default

    def test_set_key_with_default_with_namespace(self):
        default = 52
        set_value = 4
        project = self.mock_project(
            config_overrides={"flow": {"bar": {"foo": set_value}}}
        )
        assert get_config_value(project, "foo", ns="bar", default=default) == set_value
