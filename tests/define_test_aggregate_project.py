from flow import FlowProject, aggregator


class _TestAggregateProject(FlowProject):
    pass


@aggregator.groupsof(1)
@_TestAggregateProject.operation
def agg_op1(*jobs):
    pass


@aggregator.groupsof(2)
@_TestAggregateProject.operation
def agg_op2(*jobs):
    pass


@aggregator.groupsof(2)
@_TestAggregateProject.operation
def agg_op3(*jobs):
    pass
