from flow import FlowProject

eg_group = FlowProject.make_group(name='eg_group', directives={'np': 2,
                                                               'walltime': 0.5})
new_group = FlowProject.make_group(name='new_group', options='--num-passes=3')
a_group = FlowProject.make_group(name='a', options='--num-passes=5')


@FlowProject.operation
@eg_group
@a_group
@FlowProject.post(lambda job: job.isfile("eg.txt"))
def foo(job):
    print('foo')
    with open(job.fn('eg.txt'), 'w') as fp:
        fp.write("Hello World!")


@FlowProject.operation
@eg_group
def bar(job):
    print('bar')


@new_group
@FlowProject.pre.after(eg_group)
@FlowProject.operation
def num(job):
    print('num')


@FlowProject.operation
@eg_group
def who(job):
    print('who')


if __name__ == '__main__':
    f = FlowProject()
    f.main()
