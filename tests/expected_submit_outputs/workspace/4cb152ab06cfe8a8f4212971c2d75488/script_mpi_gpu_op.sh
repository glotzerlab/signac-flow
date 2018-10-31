#PBS -N SubmissionTe/4cb152ab/mpi_gpu_op/0000/ce7e222aea200c85618077c3662e76f8
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(4cb152ab06cfe8a8f4212971c2d75488)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op 4cb152ab06cfe8a8f4212971c2d75488

