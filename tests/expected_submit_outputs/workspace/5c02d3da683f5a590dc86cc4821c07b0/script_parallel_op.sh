#PBS -N SubmissionTe/5c02d3da/parallel_op/0000/a55c118a813e1837719b7f2e86665225
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(5c02d3da683f5a590dc86cc4821c07b0)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 5c02d3da683f5a590dc86cc4821c07b0

