#PBS -N SubmissionTe/c6eca32b/gpu_op/0000/479cdcb2c218d14db46809fdc6a7b6e5
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(c6eca32bee7206857127ee07e4bb8a4e)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op c6eca32bee7206857127ee07e4bb8a4e

