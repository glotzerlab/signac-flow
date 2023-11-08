import os

from flow import FlowProject


class _TestProject(FlowProject):
    pass


group1 = _TestProject.make_group(name="group1")
group2 = _TestProject.make_group(name="group2", submit_options="--num-passes=2")


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
@_TestProject.pre(b_is_even)
@_TestProject.post.isfile("world.txt")
@_TestProject.operation(
    cmd=True,
    directives={
        # Explicitly set a "bad" directive that is unused by the template.
        # The submit interface should warn about unused directives.
        "bad_directive": 0,
        # But not this one:
        "np": 1,
    },
)
def op1(job):
    return f'echo "hello" > {job.path}/world.txt'


@group1
@_TestProject.post.true("test")
@_TestProject.operation
def op2(job):
    job.document.test = os.getpid()


@group2(directives={"omp_num_threads": 4})
@_TestProject.post.true("test3_true")
@_TestProject.post.false("test3_false")
@_TestProject.post.not_(lambda job: job.doc.test3_false)
@_TestProject.operation(directives={"ngpu": 1, "omp_num_threads": 1})
def op3(job):
    job.document.test3_true = True
    job.document.test3_false = False


class _DynamicTestProject(_TestProject):
    pass


group3 = _DynamicTestProject.make_group(name="group3")


@group3
@_DynamicTestProject.pre.after(op1)
@_DynamicTestProject.post(lambda job: job.sp.get("dynamic", False))
@_DynamicTestProject.operation
def op4(job):
    job.sp.dynamic = True  # migration during execution


if __name__ == "__main__":
    _DynamicTestProject().main()
