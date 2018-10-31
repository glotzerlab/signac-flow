#PBS -N SubmissionTe/118f4f3c/parallel_op/0000/5cf5bd6e6d98e925eea2db2c17867e7f
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(118f4f3ccfae13a187ade1247f33dd3c)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 118f4f3ccfae13a187ade1247f33dd3c

