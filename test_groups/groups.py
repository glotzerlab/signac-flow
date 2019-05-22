import signac
import flow
from flow import FlowProject, directives

eg_group = FlowProject.make_group(name='eg_group')


@FlowProject.operation
@eg_group
def foo(job):
    print('foo')


@FlowProject.operation
@eg_group
def bar(job):
    print('bar')


if __name__ == '__main__':
    fp = FlowProject()
    print(fp._groups['eg_group'].operations)
    print(fp._operations)
