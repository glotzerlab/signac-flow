#PBS -N SubmissionTe/118f4f3c/gpu_op/0000/c5ecdc863c99f6538e04c178a2df8503
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(118f4f3ccfae13a187ade1247f33dd3c)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op 118f4f3ccfae13a187ade1247f33dd3c

