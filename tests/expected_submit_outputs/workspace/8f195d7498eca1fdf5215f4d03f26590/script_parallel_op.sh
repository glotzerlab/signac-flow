#PBS -N SubmissionTe/8f195d74/parallel_op/0000/281344e0ebce3394a14af7c60e6f4299
#PBS -V
#PBS -l nodes=1
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(8f195d7498eca1fdf5215f4d03f26590)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 8f195d7498eca1fdf5215f4d03f26590

