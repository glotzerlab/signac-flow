#PBS -N SubmissionTe/cbc9084a/gpu_op/0000/8c04897474f74a4968d5d0b5a9301620
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(cbc9084a8a656df72affdea00b06a561)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op cbc9084a8a656df72affdea00b06a561 &
wait

