#PBS -N SubmissionTe/c6eca32b/serial_op/0000/c5f020ef7e2698bea53d087970bd2bc8
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(c6eca32bee7206857127ee07e4bb8a4e)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op c6eca32bee7206857127ee07e4bb8a4e

