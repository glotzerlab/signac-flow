#PBS -N SubmissionTe/9626d150/hybrid_op/0000/ad8771fd81f6900185ff8506ad3aba93
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(9626d150d28acc04b0288d609fe63a1a)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 9626d150d28acc04b0288d609fe63a1a

