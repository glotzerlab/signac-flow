from flow import FlowProject, aggregator, cmd, with_job


class _TestAggregateProject(FlowProject):
    pass


def custom_aggregator(jobs):
    even = [job for job in jobs if job.sp.i % 2 == 0]
    odd = [job for job in jobs if job.sp.i % 2 != 0]
    return [even, odd]


group1 = _TestAggregateProject.make_group(name="group_agg", aggregator_obj=aggregator())


@_TestAggregateProject.operation
def op1(job):
    pass


@_TestAggregateProject.operation
@aggregator.groupby("even")
def agg_op1(*jobs):
    total = sum([job.sp.i for job in jobs])
    for job in jobs:
        job.document.sum = total


@_TestAggregateProject.operation
@aggregator.groupby(lambda job: job.sp.i % 2)
def agg_op1_different(*jobs):
    sum_other = sum([job.sp.i for job in jobs])
    for job in jobs:
        job.document.sum_other = sum_other


@_TestAggregateProject.operation
@aggregator(custom_aggregator)
def agg_op1_custom(*jobs):
    sum_custom = sum([job.sp.i for job in jobs])
    for job in jobs:
        job.document.sum_custom = sum_custom


@_TestAggregateProject.operation
@group1
@aggregator.groupsof(30)
def agg_op2(*jobs):
    for job in jobs:
        job.document.op2 = True


@_TestAggregateProject.operation
@group1
@aggregator()
def agg_op3(*jobs):
    for job in jobs:
        job.document.op3 = True


@_TestAggregateProject.operation
@cmd
@aggregator(sort_by="i", select=lambda job: job.sp.i <= 2)
def agg_op4(*jobs):
    return "echo '{jobs[0].sp.i} and {jobs[1].sp.i}'"


@_TestAggregateProject.operation
@with_job
@aggregator()
def agg_op5(*jobs):
    pass


if __name__ == "__main__":
    _TestAggregateProject().main()
