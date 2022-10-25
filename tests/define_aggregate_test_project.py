from flow import FlowProject, aggregator


class _AggregateTestProject(FlowProject):
    pass


def statepoint_i_even_odd_aggregator(jobs):
    even = tuple(job for job in jobs if job.sp.i % 2 == 0)
    odd = tuple(job for job in jobs if job.sp.i % 2 != 0)
    return [even, odd]


def set_all_job_docs(jobs, key, value):
    for job in jobs:
        if job.document.get(key) == value:
            raise RuntimeError(f"Key {key} is already set to {value}!")
        job.document[key] = value


group1 = _AggregateTestProject.make_group(
    name="group_agg", group_aggregator=aggregator()
)


@_AggregateTestProject.operation
def op1(job):
    pass


@_AggregateTestProject.operation(cmd=True)
@aggregator.groupby("even")
def agg_op_parallel(*jobs):
    # This is used to test parallel execution of aggregation operations
    return f"echo '{len(jobs)}'"


@_AggregateTestProject.operation
@aggregator.groupby("even")
def agg_op1(*jobs):
    total = sum(job.sp.i for job in jobs)
    set_all_job_docs(jobs, "sum", total)


@_AggregateTestProject.operation
@aggregator.groupby(lambda job: job.sp.i % 2)
def agg_op1_different(*jobs):
    sum_other = sum(job.sp.i for job in jobs)
    set_all_job_docs(jobs, "sum_other", sum_other)


@_AggregateTestProject.operation
@aggregator(statepoint_i_even_odd_aggregator)
def agg_op1_custom(*jobs):
    sum_custom = sum(job.sp.i for job in jobs)
    set_all_job_docs(jobs, "sum_custom", sum_custom)


@group1
@_AggregateTestProject.operation
@aggregator.groupsof(30)
def agg_op2(*jobs):
    set_all_job_docs(jobs, "op2", True)


@group1
@_AggregateTestProject.operation
@aggregator()
def agg_op3(*jobs):
    set_all_job_docs(jobs, "op3", True)


@_AggregateTestProject.operation(cmd=True)
@aggregator(sort_by="i", select=lambda job: job.sp.i <= 2)
def agg_op4(*jobs):
    return f"echo '{jobs[0].sp.i} and {jobs[1].sp.i}'"


if __name__ == "__main__":
    _AggregateTestProject().main()
