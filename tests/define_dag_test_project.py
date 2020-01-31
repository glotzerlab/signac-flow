from flow import FlowProject


class TestDagProject(FlowProject):
    pass


def first_complete(job):
    return job.doc.get('first') is not None


@TestDagProject.operation
@TestDagProject.post(first_complete)
def first(job):
    job.doc.first = True


@TestDagProject.operation
@TestDagProject.pre(first_complete)
@TestDagProject.post.true('second')
@TestDagProject.post.false('other_condition')
def second(job):
    job.doc.second = True


@TestDagProject.operation
@TestDagProject.pre.after(first)
@TestDagProject.post.true('third')
def third(job):
    job.doc.third = True


@TestDagProject.operation
@TestDagProject.pre.true('second')
@TestDagProject.pre.after(third)
@TestDagProject.post.true('fourth')
def fourth(job):
    job.doc.fourth = True


@TestDagProject.operation
@TestDagProject.pre.copy_from(third)
@TestDagProject.post.isfile('fifth.txt')
def fifth(job):
    import os

    def touch(fname, times=None):
        with open(fname, 'a'):
            os.utime(fname, times)
    with job:
        touch('fifth.txt')


@TestDagProject.operation
@TestDagProject.pre.after(third)
@TestDagProject.pre.isfile('fifth.txt')
@TestDagProject.post.true('sixth')
def sixth(job):
    job.doc.sixth = True


@TestDagProject.operation
@TestDagProject.pre.after(third)
@TestDagProject.pre.after(fourth)
@TestDagProject.post.true('seventh')
def seventh(job):
    job.doc.seventh = True


if __name__ == '__main__':
    TestDagProject().main()
