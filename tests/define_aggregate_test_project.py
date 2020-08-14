from flow import FlowProject, Aggregate


class _AggregateTestProject(FlowProject):
    pass


group1 = _AggregateTestProject.make_group(name="group_agg", aggregate=Aggregate())


@_AggregateTestProject.label
def aggregate_doc_condition(job):
    try:
        return job._project.document['average']
    except KeyError:
        return False


@_AggregateTestProject.operation
@Aggregate()
@_AggregateTestProject.post.true('average')
def agg_op1(*jobs):
    sum = 0
    for job in jobs:
        sum += job.sp.i
    for job in jobs:
        job.document.sum = sum


@_AggregateTestProject.operation
@group1
@_AggregateTestProject.post.true('average')
def agg_op2(*jobs):
    average = 0
    for job in jobs:
        average += job.sp.i
    average /= len(jobs)
    for job in jobs:
        job.document.average = average


@_AggregateTestProject.operation
@group1
@_AggregateTestProject.post.true('test3')
def agg_op3(*jobs):
    for job in jobs:
        job.document.test3 = True


if __name__ == '__main__':
    _AggregateTestProject().main()
