#PBS -N SubmissionTe/c6eca32b/parallel_op/0000/69621e36278e35980d245e36645434b1
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(c6eca32bee7206857127ee07e4bb8a4e)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op c6eca32bee7206857127ee07e4bb8a4e

