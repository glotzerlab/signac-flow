#PBS -N SubmissionTe/9626d150/mpi_op/0000/7280fa4c88582530d566670b552006f2
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(9626d150d28acc04b0288d609fe63a1a)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 9626d150d28acc04b0288d609fe63a1a

