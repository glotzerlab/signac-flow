import flow
from flow import FlowProject


class TestProject(FlowProject):
    pass


group1 = TestProject.make_group(name="group1")

group2 = TestProject.make_group(name="group2",
                                options="--num-passes=2")


@TestProject.label
def default_label(job):
    return True


@TestProject.label
def negative_default_label(job):
    return False


@TestProject.label
def b_is_even(job):
    try:
        return job.sp.b % 2 == 0
    except AttributeError:
        return False


@group1
@TestProject.operation
@flow.cmd
@TestProject.pre(b_is_even)
@TestProject.post.isfile('world.txt')
# The submit interface should warn about this explicitly set directive,
# because it would not be used by the template script:
# Explicitly set "good" directive.
@flow.directives(bad_directive=0)
# But not this one:
@flow.directives(np=1)
def op1(job):
    return 'echo "hello" > {job.ws}/world.txt'


@group1
@TestProject.operation
@TestProject.post.true('test')
def op2(job):
    job.document.test = True


@group2
@TestProject.operation
@TestProject.post.true('test3')
def op3(job):
    job.document.test3 = True


class TestDynamicProject(TestProject):
    pass


group3 = TestDynamicProject.make_group(name="group3")


@group3
@TestDynamicProject.operation
@TestDynamicProject.pre.after(op1)
@TestDynamicProject.post.true('dynamic')
def op4(job):
    job.sp.dynamic = True   # migration during execution


@group3
@TestDynamicProject.operation
@TestDynamicProject.pre.after(op1)
@TestDynamicProject.post.true('group_dynamic')
def op5(job):
    job.sp.group_dynamic = True   # migration during execution


if __name__ == '__main__':
    TestDynamicProject().main()
