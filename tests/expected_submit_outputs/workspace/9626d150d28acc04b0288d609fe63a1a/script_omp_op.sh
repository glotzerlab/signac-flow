#PBS -N SubmissionTe/9626d150/omp_op/0000/023647e5e7a1333ff9b2ef39cc93add7
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(9626d150d28acc04b0288d609fe63a1a)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 9626d150d28acc04b0288d609fe63a1a

