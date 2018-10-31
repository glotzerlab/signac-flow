#PBS -N SubmissionTe/50d0bba0/gpu_op/0000/4c5bf3f77f8c1a6019ee780c600d45f1
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(50d0bba019db759bcdbddb9aed4cd204)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op 50d0bba019db759bcdbddb9aed4cd204

