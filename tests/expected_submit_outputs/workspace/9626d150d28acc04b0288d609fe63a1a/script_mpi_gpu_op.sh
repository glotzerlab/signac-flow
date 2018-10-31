#PBS -N SubmissionTe/9626d150/mpi_gpu_op/0000/42de2b221e8c848afd7ea5a68ce6514f
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(9626d150d28acc04b0288d609fe63a1a)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op 9626d150d28acc04b0288d609fe63a1a

