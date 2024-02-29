import flow


class TestProject(flow.FlowProject):
    gpus_per_process = 1
    processes = 3
    threads_per_process = 4
    launcher = "mpi"
    walltime = 1
    memory_per_cpu = "512m"


group1 = TestProject.make_group(name="group1")


@group1
@TestProject.operation
def serial_op(job):
    pass


@group1
@TestProject.operation(directives={"processes": TestProject.processes})
def parallel_op(job):
    pass


@TestProject.operation(
    directives={"processes": TestProject.processes, "launcher": TestProject.launcher}
)
def mpi_op(job):
    pass


@TestProject.operation(
    directives={"threads_per_process": TestProject.threads_per_process}
)
def omp_op(job):
    pass


@TestProject.operation(
    directives={
        "processes": TestProject.processes,
        "threads_per_process": TestProject.threads_per_process,
        "launcher": "mpi",
    }
)
def hybrid_op(job):
    pass


@TestProject.operation(
    directives={
        "gpus_per_process": TestProject.gpus_per_process,
        "processes": TestProject.gpus_per_process,
        "launcher": TestProject.launcher,
    }
)
def gpu_op(job):
    pass


@TestProject.operation(
    directives={
        "gpus_per_process": TestProject.gpus_per_process,
        "processes": TestProject.processes,
        "launcher": "mpi",
    }
)
def mpi_gpu_op(job):
    pass


@group1
@TestProject.operation(directives={"memory_per_cpu": TestProject.memory_per_cpu})
def memory_op(job):
    pass


@group1
@TestProject.operation(directives={"walltime": TestProject.walltime})
def walltime_op(job):
    pass


@TestProject.operation(cmd=True)
def multiline_cmd(job):
    return 'echo "First line"\necho "Second line"'
