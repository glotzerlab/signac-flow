#PBS -N SubmissionTe/64760707/parallel_op/0000/85ad077b532444233d03a452e9b7e682
#PBS -l walltime=01:00:00
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(64760707a0aedec77b482309893f1543)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 64760707a0aedec77b482309893f1543

