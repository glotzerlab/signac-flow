import flow


class TestProject(flow.FlowProject):
    ngpu = 2
    np = 3
    omp_num_threads = 4
    nranks = 5
    walltime = 1
    memory = "512m"


group1 = TestProject.make_group(name="group1")


@group1
@TestProject.operation
def serial_op(job):
    pass


@group1
@TestProject.operation(directives={"np": TestProject.np})
def parallel_op(job):
    pass


@TestProject.operation(directives={"nranks": TestProject.nranks})
def mpi_op(job):
    pass


@TestProject.operation(directives={"omp_num_threads": TestProject.omp_num_threads})
def omp_op(job):
    pass


@TestProject.operation(
    directives={
        "nranks": TestProject.nranks,
        "omp_num_threads": TestProject.omp_num_threads,
    }
)
def hybrid_op(job):
    pass


@TestProject.operation(
    directives={"ngpu": TestProject.ngpu, "nranks": TestProject.ngpu}
)
def gpu_op(job):
    pass


@TestProject.operation(
    directives={"ngpu": TestProject.ngpu, "nranks": TestProject.nranks}
)
def mpi_gpu_op(job):
    pass


@group1
@TestProject.operation(directives={"memory": TestProject.memory})
def memory_op(job):
    pass


@group1
@TestProject.operation(directives={"walltime": TestProject.walltime})
def walltime_op(job):
    pass


@TestProject.operation(cmd=True)
def multiline_cmd(job):
    return 'echo "First line"\necho "Second line"'
