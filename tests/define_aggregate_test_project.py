from flow import FlowProject, aggregator


class _AggregateTestProject(FlowProject):
    pass


group1 = _AggregateTestProject.make_group('group1', aggregator_obj=aggregator.groupsof(3))


@_AggregateTestProject.operation
@aggregator()
def agg_op1(*jobs):
    sum = 0
    for job in jobs:
        sum += job.sp.i
    for job in jobs:
        job.document.sum = sum


@_AggregateTestProject.operation
@group1
@aggregator.groupsof(4)
def agg_op2(*jobs):
    for job in jobs:
        job.document.test2 = job.document.get('test2', 0) + 1


@_AggregateTestProject.operation
@group1
@aggregator.groupby('i')
def agg_op3(*jobs):
    for job in jobs:
        job.document.test3 = job.document.get('test3', 0) + 1


if __name__ == '__main__':
    _AggregateTestProject().main()
