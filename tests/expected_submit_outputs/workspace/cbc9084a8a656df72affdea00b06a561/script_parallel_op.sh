#PBS -N SubmissionTe/cbc9084a/parallel_op/0000/37fa851bebf6e60ec904b0979a642317
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(cbc9084a8a656df72affdea00b06a561)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op cbc9084a8a656df72affdea00b06a561 &
wait

