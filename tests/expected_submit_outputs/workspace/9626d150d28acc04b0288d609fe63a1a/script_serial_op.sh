#PBS -N SubmissionTe/9626d150/serial_op/0000/37e8c667eaad60d7f10eb1dbbcb8e611
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(9626d150d28acc04b0288d609fe63a1a)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 9626d150d28acc04b0288d609fe63a1a

