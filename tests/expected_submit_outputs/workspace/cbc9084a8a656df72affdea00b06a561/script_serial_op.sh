#PBS -N SubmissionTe/cbc9084a/serial_op/0000/dba494842ff82d04752933a3d0894a8b
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(cbc9084a8a656df72affdea00b06a561)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op cbc9084a8a656df72affdea00b06a561 &
wait

