#PBS -N SubmissionTe/11dd16d0/mpi_op/0000/2748738b31325b2c157239c85f44236e
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(11dd16d04f6d624884a59e3e77d22cc8)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 11dd16d04f6d624884a59e3e77d22cc8

