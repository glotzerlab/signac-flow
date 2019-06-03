import signac
import flow
from flow import FlowProject, directives

eg_group = FlowProject.make_group(name='eg_group', directives={'np': 2})
new_group = FlowProject.make_group(name='new_group')

@FlowProject.operation
@eg_group
def foo(job):
    print('foo')


@FlowProject.operation
@eg_group
def bar(job):
    print('bar')


@new_group
@FlowProject.operation
def num(job):
    print('num')


@FlowProject.operation
@eg_group
def who(job):
    print('who')


if __name__ == '__main__':
    FlowProject().main()
