#PBS -N SubmissionTe/fd5f1576/mpi_op/0000/1c1d057fd040340a893600f82f96afa9
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(fd5f15761bc3017ad4069d08ab9dc3d2)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op fd5f15761bc3017ad4069d08ab9dc3d2 &
wait

