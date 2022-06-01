from io import StringIO

from flow import FlowProject
from flow.project import _AggregateStoresCursor

from .. import FlowProjectSetup


class TestStatusPerformance(FlowProjectSetup):
    class Project(FlowProject):
        pass

    @Project.operation
    @Project.post.isfile("DOES_NOT_EXIST")
    def foo(job):
        pass

    project_class = Project
    project_name = "FlowTestProject"

    def mock_project(self):
        project = self.project_class.get_project(root=self._tmp_dir.name)
        for i in range(1000):
            project.open_job(dict(i=i)).init()
        return project

    def test_status_performance(self, project):
        import timeit

        time = timeit.timeit(
            lambda: project._fetch_status(
                aggregates=_AggregateStoresCursor(project),
                err=StringIO(),
                ignore_errors=False,
            ),
            number=10,
        )
        assert time < 10
