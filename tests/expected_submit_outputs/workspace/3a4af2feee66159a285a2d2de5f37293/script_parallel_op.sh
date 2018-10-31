#PBS -N SubmissionTe/3a4af2fe/parallel_op/0000/ea5a71ea13398f9e79e38ac1d258d7a7
#PBS -V
#PBS -l nodes=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(3a4af2feee66159a285a2d2de5f37293)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 3a4af2feee66159a285a2d2de5f37293

