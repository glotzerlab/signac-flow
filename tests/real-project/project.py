import datetime
import itertools
from flow import FlowProject
import signac


class Project(FlowProject):
    pass


def setup_done(job):
    return job.isfile('job.txt')


def false_b(job):
    return job.sp.get('b', None) is False


def op1_done(job):
    return job.isfile('op1.txt')


def op2_three_times(job):
    return job.doc.get('n_op2', 0) >= 3


@Project.label
def setup_done_label(job):
    if setup_done(job):
        return 'setup'


@Project.label
def op2_count(job):
    return 'op2 {}x'.format(job.doc.get('n_op2', 0))


@Project.operation
@Project.post(setup_done)
def setup(job):
    with open(job.fn('job.txt'), 'w') as f:
        f.write(job._id)


@Project.operation
@Project.pre(false_b)
@Project.pre.after(setup)
@Project.post(op1_done)
def op1(job):
    with open(job.fn('op1.txt'), 'w') as f:
        f.write('b: False')


@Project.operation
@Project.post(op2_three_times)
def op2(job):
    with open(job.fn('op2.txt'), 'a') as f:
        f.write(str(datetime.datetime.now()) + '\n')
    try:
        job.doc.n_op2 += 1
    except AttributeError:
        job.doc.n_op2 = 1


if __name__ == '__main__':
    project = signac.init_project('MyProject')

    def _dict_product(dd):
        keys = dd.keys()
        for element in itertools.product(*dd.values()):
            yield dict(zip(keys, element))

    sp_params = {
            'a': [1, 2, [3, 4, 5], 6],
            'b': ['signac', 18.0, False],
            'c': [{'foo': 'bar', 'baz': 'thud'}, 3, 2, 1],
            'fish': [1, 2, 'red', 'blue', {'green': ['eggs', 'ham']}]
        }
    for sp in _dict_product(sp_params):
        job = project.open_job(sp)
        job.init()

    Project().main()
