#PBS -N SubmissionTe/3a4af2fe/serial_op/0000/929ee07483223d440f3c985ccb719886
#PBS -V
#PBS -l nodes=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(3a4af2feee66159a285a2d2de5f37293)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 3a4af2feee66159a285a2d2de5f37293

