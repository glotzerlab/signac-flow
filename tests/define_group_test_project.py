import flow
from flow import FlowProject


class GroupTestProject(FlowProject):
    pass


group1 = GroupTestProject.make_group(name="group1")

group2 = GroupTestProject.make_group(name="group2",
                                     options="--num-passes=2")


@GroupTestProject.label
def default_label(job):
    return True


@GroupTestProject.label
def negative_default_label(job):
    return False


@GroupTestProject.label
def b_is_even(job):
    try:
        return job.sp.b % 2 == 0
    except AttributeError:
        return False


@group1
@GroupTestProject.operation
@flow.cmd
@GroupTestProject.pre(b_is_even)
@GroupTestProject.post.isfile('world.txt')
# The submit interface should warn about this explicitly set directive,
# because it would not be used by the template script:
# Explicitly set "good" directive.
@flow.directives(bad_directive=0)
# But not this one:
@flow.directives(np=1)
def op1(job):
    return 'echo "hello" > {job.ws}/world.txt'


@group1
@GroupTestProject.operation
@GroupTestProject.post.true('test')
def op2(job):
    job.document.test = True


@group2.with_directives(ngpu=2)
@GroupTestProject.operation
@GroupTestProject.post.true('test3')
@flow.directives(ngpu=1)
def op3(job):
    job.document.test3 = True


class GroupTestDynamicProject(GroupTestProject):
    pass


group3 = GroupTestDynamicProject.make_group(name="group3")


@group3
@GroupTestDynamicProject.operation
@GroupTestDynamicProject.pre.after(op1)
@GroupTestDynamicProject.post.true('dynamic')
def op4(job):
    job.sp.dynamic = True   # migration during execution


@group3
@GroupTestDynamicProject.operation
@GroupTestDynamicProject.pre.after(op1)
@GroupTestDynamicProject.post.true('group_dynamic')
def op5(job):
    job.sp.group_dynamic = True   # migration during execution


if __name__ == '__main__':
    GroupTestDynamicProject().main()
