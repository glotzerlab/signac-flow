#PBS -N SubmissionTe/3a4af2fe/gpu_op/0000/e37027251709b1c448bf288b295e8fc7
#PBS -V
#PBS -l nodes=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(3a4af2feee66159a285a2d2de5f37293)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op 3a4af2feee66159a285a2d2de5f37293

