#PBS -N SubmissionTe/50d0bba0/parallel_op/0000/6d27414d2ef41b2c97e312c4707ac1cc
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(50d0bba019db759bcdbddb9aed4cd204)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 50d0bba019db759bcdbddb9aed4cd204

