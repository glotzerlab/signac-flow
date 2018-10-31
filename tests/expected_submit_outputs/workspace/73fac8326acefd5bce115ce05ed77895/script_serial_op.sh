#PBS -N SubmissionTe/73fac832/serial_op/0000/c4c08076af3b585bd788388bd8fa3b94
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(73fac8326acefd5bce115ce05ed77895)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 73fac8326acefd5bce115ce05ed77895

