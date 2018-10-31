#PBS -N SubmissionTe/c6eca32b/mpi_gpu_op/0000/fbf17a83c6f85668f094c57fecc1efb4
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(c6eca32bee7206857127ee07e4bb8a4e)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op c6eca32bee7206857127ee07e4bb8a4e

