import flow


class TestProject(flow.FlowProject):
    ngpu = 2
    np = 3
    omp_num_threads = 4
    nranks = 5


group1 = TestProject.make_group(name="group1")


@TestProject.operation
@group1
def serial_op(job):
    pass


@TestProject.operation
@flow.directives(np=TestProject.np)
@group1
def parallel_op(job):
    pass


@TestProject.operation
@flow.directives(nranks=TestProject.nranks)
def mpi_op(job):
    pass


@TestProject.operation
@flow.directives(omp_num_threads=TestProject.omp_num_threads)
def omp_op(job):
    pass


@TestProject.operation
@flow.directives(nranks=TestProject.nranks, omp_num_threads=TestProject.omp_num_threads)
def hybrid_op(job):
    pass


@TestProject.operation
@flow.directives(ngpu=TestProject.ngpu, nranks=TestProject.ngpu)
def gpu_op(job):
    pass


@TestProject.operation
@flow.directives(ngpu=TestProject.ngpu, nranks=TestProject.nranks)
def mpi_gpu_op(job):
    pass
