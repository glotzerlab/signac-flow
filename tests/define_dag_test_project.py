from flow import FlowProject


class DagTestProject(FlowProject):
    pass


def first_complete(job):
    return job.doc.get("first") is not None


@DagTestProject.operation
@DagTestProject.post(first_complete)
def first(job):
    job.doc.first = True


@DagTestProject.operation
@DagTestProject.pre(first_complete)
@DagTestProject.post.true("second")
@DagTestProject.post.false("other_condition")
def second(job):
    job.doc.second = True


@DagTestProject.operation
@DagTestProject.pre.after(first)
@DagTestProject.post.true("third")
def third(job):
    job.doc.third = True


@DagTestProject.operation
@DagTestProject.pre.true("second")
@DagTestProject.pre.after(third)
@DagTestProject.post.true("fourth")
def fourth(job):
    job.doc.fourth = True


@DagTestProject.operation
@DagTestProject.pre.copy_from(third)
@DagTestProject.post.isfile("fifth.txt")
def fifth(job):
    import os

    def touch(fname, times=None):
        with open(fname, "a"):
            os.utime(fname, times)

    with job:
        touch("fifth.txt")


@DagTestProject.operation
@DagTestProject.pre.after(third)
@DagTestProject.pre.isfile("fifth.txt")
@DagTestProject.post.true("sixth")
def sixth(job):
    job.doc.sixth = True


@DagTestProject.operation
@DagTestProject.pre.after(third)
@DagTestProject.pre.after(fourth)
@DagTestProject.post.true("seventh")
def seventh(job):
    job.doc.seventh = True


if __name__ == "__main__":
    DagTestProject().main()
