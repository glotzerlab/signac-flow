#! /usr/bin/env python3

import signac

import flow

CPUS_PER_NODE = {{num_cpus}}
GPUS_PER_NODE = {{num_gpus}}


class Project(flow.FlowProject):
    pass


@Project.post.true("base_op")
@Project.operation(directives={"np": 1})
def base_op(job):
    """Operation with no parallelization."""
    job.doc["base_op"] = True


@Project.post.true("intra_node_np")
@Project.operation(directives={"np": CPUS_PER_NODE})
def intra_node_np(job):
    import multiprocessing

    job.doc["intra_node_np"] = multiprocessing.cpu_count() == max(
        8, CPUS_PER_NODE)


@Project.post.true("intra_node_mpi")
@Project.operation(directives={"nranks": CPUS_PER_NODE})
def intra_node_mpi(job):
    from mpi4py import MPI

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    if rank == 0:
        job.doc["intra_node_mpi"] = size == CPUS_PER_NODE


@Project.post.true("inter_node_mpi")
@Project.operation(directives={"nranks": 2 * CPUS_PER_NODE})
def inter_node_mpi(job):
    from mpi4py import MPI

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    if rank == 0:
        job.doc["inter_node_mpi"] = size == 2 * CPUS_PER_NODE


{% if num_gpus > 0 %}
@Project.post.true("gpu_op")
@Project.operation(directives={"ngpu": 1})
def gpu_op(job):
    import cupy as cp

    # Will only return true if GPU can be accessed
    mean_correct = cp.asnumpy(cp.mean(cp.arange(101)) == 50).item()
    print(mean_correct, type(mean_correct), flush=True)
    job.doc["gpu_op"] = mean_correct


@Project.post.true("intra_gpu_op")
@Project.operation(directives={"ngpu": GPUS_PER_NODE, "nranks": GPUS_PER_NODE})
def node_gpu_op(job):
    import cupy as cp
    from mpi4py import MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    assert comm.Get_size() == GPUS_PER_NODE

    try:
        with cp.cuda.Device(rank):
            assert cp.asnumpy(cp.mean(cp.arange(101)) == 50).item()
    except Exception as err:
        raised = True
        error = err
    else:
        raised = False
        error = None
    raised = comm.gather(raised, 0)
    errors = comm.gather(error, 0)
    if rank == 0:
        if any(raised):
            print(list(filter(lambda x: x is not None, errors)), flush=True)
        else:
            job.doc["intra_gpu_op"] = True


@Project.post.true("inter_gpu_op")
@Project.operation(
    directives={
        "nranks": 2 * GPUS_PER_NODE, "ngpu": 2 * GPUS_PER_NODE})
def inter_node_gpu_op(job):
    import cupy as cp
    from mpi4py import MPI

    # TODO: check the functioning of each GPU.
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    assert comm.Get_size() == 2 * GPUS_PER_NODE
    try:
        with cp.cuda.Device(rank % GPUS_PER_NODE):
            assert cp.asnumpy(cp.mean(cp.arange(101)) == 50).item()
    except Exception as err:
        raised = True
        error = err
    else:
        raised = False
        error = None
    raised = comm.gather(raised, 0)
    errors = comm.gather(error, 0)
    if rank == 0:
        print(raised, flush=True)
        if any(raised):
            print(list(filter(lambda x: x is not None, errors)), flush=True)
        else:
            job.doc["inter_gpu_op"] = True


{% endif %}
if __name__ == "__main__":
    Project().main()
