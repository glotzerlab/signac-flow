#PBS -N SubmissionTe/38dbccd7/serial_op/0000/35ee2f0517e5bb9831e9fdc28180378d
#PBS -V
#PBS -l nodes=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(38dbccd79e1d32d69930dd227fd250ae)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 38dbccd79e1d32d69930dd227fd250ae

