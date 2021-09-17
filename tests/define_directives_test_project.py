import datetime

from flow import FlowProject


class _DirectivesTestProject(FlowProject):
    pass


group = _DirectivesTestProject.make_group(name="walltimegroup")


@group
@_DirectivesTestProject.operation.with_directives({"walltime": 1.0})
def op_walltime(job):
    pass


@group
@_DirectivesTestProject.operation.with_directives({"walltime": None})
def op_walltime_2(job):
    pass


@group
@_DirectivesTestProject.operation.with_directives(
    {"walltime": datetime.timedelta(hours=2)}
)
def op_walltime_3(job):
    pass


if __name__ == "__main__":
    _DirectivesTestProject().main()
