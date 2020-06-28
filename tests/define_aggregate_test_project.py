from flow import FlowProject, aggregate


class _AggregateTestProject(FlowProject):
    pass


group1 = _AggregateTestProject.make_group(name="group_agg", aggregator=aggregate()()[0])


@_AggregateTestProject.label
def aggregate_doc_condition(*jobs):
    try:
        return all([job._project.document['average'] for job in jobs])
    except KeyError:
        return False


@_AggregateTestProject.operation
@group1
@_AggregateTestProject.post.true('average')
def agg_op1(*jobs):
    print('asfsdg')
    average = 0
    for job in jobs:
        average += job.sp.i
    average /= len(jobs)
    for job in jobs:
        job.document.average = average


@_AggregateTestProject.operation
@group1
@_AggregateTestProject.post.true('test3')
def agg_op2(*jobs):
    for job in jobs:
        job.document.test3 = True


if __name__ == '__main__':
    _AggregateTestProject().main()
