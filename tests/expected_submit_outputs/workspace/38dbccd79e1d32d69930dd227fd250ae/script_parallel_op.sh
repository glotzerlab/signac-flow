#PBS -N SubmissionTe/38dbccd7/parallel_op/0000/c79210803b9c496125f78d80d0d878bf
#PBS -V
#PBS -l nodes=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(38dbccd79e1d32d69930dd227fd250ae)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 38dbccd79e1d32d69930dd227fd250ae

