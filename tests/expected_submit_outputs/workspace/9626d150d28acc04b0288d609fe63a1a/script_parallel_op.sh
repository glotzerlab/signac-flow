#PBS -N SubmissionTe/9626d150/parallel_op/0000/e4dfed99c93862ae148e8d34785d12d5
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(9626d150d28acc04b0288d609fe63a1a)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 9626d150d28acc04b0288d609fe63a1a

