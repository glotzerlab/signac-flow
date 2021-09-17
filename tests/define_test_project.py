import os

import flow
from flow import FlowProject


class _TestProject(FlowProject):
    pass


group1 = _TestProject.make_group(name="group1")
group2 = _TestProject.make_group(name="group2", options="--num-passes=2")


@_TestProject.label
def default_label(job):
    return True


@_TestProject.label
def negative_default_label(job):
    return False


@_TestProject.label("named_label")
def anonymous_label(job):
    return True


@_TestProject.label
def b_is_even(job):
    try:
        return job.sp.b % 2 == 0
    except AttributeError:
        return False


@group1
@_TestProject.operation.with_directives(
    {
        # Explicitly set a "bad" directive that is unused by the template.
        # The submit interface should warn about unused directives.
        "bad_directive": 0,
        # But not this one:
        "np": 1,
    }
)
@flow.cmd
@_TestProject.pre(b_is_even)
@_TestProject.post.isfile("world.txt")
def op1(job):
    return 'echo "hello" > {job.ws}/world.txt'


def _need_to_fork(job):
    return job.doc.get("fork")


@group1
@_TestProject.operation.with_directives({"fork": _need_to_fork})
@_TestProject.post.true("test")
def op2(job):
    job.document.test = os.getpid()


@group2.with_directives(dict(omp_num_threads=4))
@_TestProject.operation.with_directives({"ngpu": 1, "omp_num_threads": 1})
@_TestProject.post.true("test3_true")
@_TestProject.post.false("test3_false")
@_TestProject.post.not_(lambda job: job.doc.test3_false)
def op3(job):
    job.document.test3_true = True
    job.document.test3_false = False


class _DynamicTestProject(_TestProject):
    pass


group3 = _DynamicTestProject.make_group(name="group3")


@group3
@_DynamicTestProject.operation
@_DynamicTestProject.pre.after(op1)
@_DynamicTestProject.post(lambda job: job.sp.get("dynamic", False))
def op4(job):
    job.sp.dynamic = True  # migration during execution


if __name__ == "__main__":
    _DynamicTestProject().main()
