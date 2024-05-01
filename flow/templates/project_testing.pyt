import datetime
from flow import FlowProject
import signac


class {{ project_class_name }}(FlowProject):
    pass


@{{ project_class_name }}.label
def setup_done(job):
    if job.isfile('job.txt'):
        return 'setup'


@{{ project_class_name }}.operation
@{{ project_class_name }}.post(setup_done)
def setup(job):
    with open(job.fn('job.txt'), 'w') as f:
        f.write(job._id)


@{{ project_class_name }}.operation
@{{ project_class_name }}.pre(lambda job: job.sp.get('a'))
@{{ project_class_name }}.pre.after(setup)
@{{ project_class_name }}.post.isfile('op1.txt')
def op1(job):
    with open(job.fn('op1.txt'), 'w') as f:
        f.write('b: False')


@{{ project_class_name }}.label
def op2_count(job):
    return 'op2({}x)'.format(job.doc.get('n_op2', 0))


@{{ project_class_name }}.operation
@{{ project_class_name }}.pre.after(setup)
@{{ project_class_name }}.post(lambda job: job.doc.get('n_op2', 0) >= 3)
def op2(job):
    with open(job.fn('op2.txt'), 'a') as f:
        f.write(str(datetime.datetime.now()) + '\n')
    job.doc.setdefault('n_op2', 0)
    job.doc.n_op2 += 1


if __name__ == '__main__':
    project = signac.get_project()
    signac.testing.init_jobs(project)
    {{ project_class_name }}().main()
