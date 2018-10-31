#PBS -N SubmissionTe/3fa0a26f/parallel_op/0000/d54d1dcf5727151ee1ef8bd9e2e8710c
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(3fa0a26faa0ceb2d07430b99af5a0e2b)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 3fa0a26faa0ceb2d07430b99af5a0e2b

