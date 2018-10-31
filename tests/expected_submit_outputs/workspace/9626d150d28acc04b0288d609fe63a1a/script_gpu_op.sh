#PBS -N SubmissionTe/9626d150/gpu_op/0000/57eaebb014a2f37e23354d6a293b8432
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(9626d150d28acc04b0288d609fe63a1a)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op 9626d150d28acc04b0288d609fe63a1a

