#PBS -N SubmissionTe/11dd16d0/parallel_op/0000/7be0d226f857da139794d4db85da5810
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(11dd16d04f6d624884a59e3e77d22cc8)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 11dd16d04f6d624884a59e3e77d22cc8

