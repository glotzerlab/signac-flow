import os
import flow
from flow import FlowProject


class _TestProject(FlowProject):
    pass


@_TestProject.label
def default_label(job):
    return True


@_TestProject.label
def negative_default_label(job):
    return False


@_TestProject.label
def b_is_even(job):
    try:
        return job.sp.b % 2 == 0
    except AttributeError:
        return False


@_TestProject.operation
@flow.cmd
@_TestProject.pre(b_is_even)
@_TestProject.post.isfile('world.txt')
# Explicitly set a "bad" directive that is unused by the template.
# The submit interface should warn about unused directives.
@flow.directives(bad_directive=0)
# But not this one:
@flow.directives(np=1)
def op1(job):
    return 'echo "hello" > {job.ws}/world.txt'


def _need_to_fork(job):
    return job.doc.get('fork')


@_TestProject.operation
@flow.directives(fork=_need_to_fork)
@_TestProject.post.true('test')
def op2(job):
    job.document.test = os.getpid()


class DynamicProjectTest(_TestProject):
    pass


@DynamicProjectTest.operation
@DynamicProjectTest.pre.after(op1)
@DynamicProjectTest.post.true('dynamic')
def op3(job):
    job.sp.dynamic = True   # migration during execution


if __name__ == '__main__':
    DynamicProjectTest().main()
