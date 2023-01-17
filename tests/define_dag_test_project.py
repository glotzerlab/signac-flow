from flow import FlowProject


class DagTestProject(FlowProject):
    pass


def first_complete(job):
    return job.doc.get("first") is not None


@DagTestProject.post(first_complete)
@DagTestProject.operation
def first(job):
    job.doc.first = True


@DagTestProject.pre(first_complete)
@DagTestProject.post.true("second")
@DagTestProject.post.false("other_condition")
@DagTestProject.operation
def second(job):
    job.doc.second = True


@DagTestProject.pre.after(first)
@DagTestProject.post.true("third")
@DagTestProject.operation
def third(job):
    job.doc.third = True


@DagTestProject.pre.true("second")
@DagTestProject.pre.after(third)
@DagTestProject.post.true("fourth")
@DagTestProject.operation
def fourth(job):
    job.doc.fourth = True


@DagTestProject.pre.copy_from(third)
@DagTestProject.post.isfile("fifth.txt")
@DagTestProject.operation
def fifth(job):
    import os

    def touch(fname, times=None):
        with open(fname, "a"):
            os.utime(fname, times)

    with job:
        touch("fifth.txt")


@DagTestProject.pre.after(third)
@DagTestProject.pre.isfile("fifth.txt")
@DagTestProject.post.true("sixth")
@DagTestProject.operation
def sixth(job):
    job.doc.sixth = True


@DagTestProject.pre.after(third)
@DagTestProject.pre.after(fourth)
@DagTestProject.post.true("seventh")
@DagTestProject.operation
def seventh(job):
    job.doc.seventh = True


if __name__ == "__main__":
    DagTestProject().main()
