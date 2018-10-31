#PBS -N SubmissionTe/50d0bba0/serial_op/0000/698648b3157f4d8fb810fcbd255ad6d0
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(50d0bba019db759bcdbddb9aed4cd204)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 50d0bba019db759bcdbddb9aed4cd204

