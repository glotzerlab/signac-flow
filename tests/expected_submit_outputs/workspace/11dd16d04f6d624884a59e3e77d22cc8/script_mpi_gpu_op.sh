#PBS -N SubmissionTe/11dd16d0/mpi_gpu_op/0000/c052724af69b472b5287cec9b6296546
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(11dd16d04f6d624884a59e3e77d22cc8)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op 11dd16d04f6d624884a59e3e77d22cc8

