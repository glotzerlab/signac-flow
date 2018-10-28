from flow import FlowProject
from flow import directives

class TestProject(FlowProject):
    pass

N = 2

@TestProject.operation
def serial_op(job):
    pass

@TestProject.operation
@directives(np=N)
def parallel_op(job):
    pass

@TestProject.operation
@directives(nranks=N)
def mpi_op(job):
    pass

@TestProject.operation
@directives(omp_num_threads=N)
def omp_op(job):
    pass

@TestProject.operation
@directives(nranks=N, omp_num_threads=N)
def hybrid_op(job):
    pass
