import os
from tempfile import TemporaryDirectory

import pytest
import signac


class FlowProjectSetup:
    project_class = signac.Project
    entrypoint = dict(path="")
    project_name = None

    @pytest.fixture(autouse=True)
    def _setup(self, request):
        self._tmp_dir = TemporaryDirectory(prefix="signac-flow_")
        request.addfinalizer(self._tmp_dir.cleanup)
        self.project = self.project_class.init_project(
            name=self.project_name, root=self._tmp_dir.name
        )
        self.cwd = os.getcwd()

    def mock_project(self):
        pass

    @pytest.fixture
    def project(self):
        return self.mock_project()
