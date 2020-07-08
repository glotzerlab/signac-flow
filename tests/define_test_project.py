import os
import flow
from flow import FlowProject


class _TestProject(FlowProject):
    pass


group1 = _TestProject.make_group(name="group1")
group2 = _TestProject.make_group(name="group2",
                                 options="--num-passes=2")


@_TestProject.label
def default_label(job):
    return True


@_TestProject.label
def negative_default_label(job):
    return False


@_TestProject.label('named_label')
def anonymous_label(job):
    return True


@_TestProject.label
def b_is_even(job):
    try:
        return job.sp.b % 2 == 0
    except AttributeError:
        return False


@_TestProject.operation
@flow.cmd
@_TestProject.pre(b_is_even)
@group1
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
@group1
@_TestProject.post.true('test')
def op2(job):
    job.document.test = os.getpid()


@_TestProject.operation
@group2.with_directives(dict(omp_num_threads=4))
@_TestProject.post.true('test3')
@flow.directives(ngpu=1, omp_num_threads=1)
def op3(job):
    job.document.test3 = True


class _DynamicTestProject(_TestProject):
    pass


group3 = _DynamicTestProject.make_group(name="group3")


@_DynamicTestProject.operation
@group3
@_DynamicTestProject.pre.after(op1)
@_DynamicTestProject.post.true('dynamic')
def op4(job):
    job.sp.dynamic = True   # migration during execution


if __name__ == '__main__':
    _DynamicTestProject().main()
