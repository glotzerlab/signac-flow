import flow
from flow import FlowProject


class TestProject(FlowProject):
    pass


@TestProject.label
def default_label(job):
    return True


@TestProject.label
def negative_default_label(job):
    return False


@TestProject.label
def b_is_even(job):
    return job.sp.b % 2 == 0


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


@TestProject.operation
@TestProject.post.true('test')
def op2(job):
    job.document.test = True


class TestDynamicProject(TestProject):
    pass


@TestDynamicProject.operation
@TestDynamicProject.pre.after(op1)
@TestDynamicProject.post.true('dynamic')
def op3(job):
    job.sp.dynamic = True   # migration during execution


if __name__ == '__main__':
    TestProject().main()
