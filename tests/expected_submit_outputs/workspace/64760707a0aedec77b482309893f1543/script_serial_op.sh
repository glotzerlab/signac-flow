#PBS -N SubmissionTe/64760707/serial_op/0000/c76891d6c432da3f490e686f8cef37f4
#PBS -l walltime=01:00:00
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(64760707a0aedec77b482309893f1543)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 64760707a0aedec77b482309893f1543

