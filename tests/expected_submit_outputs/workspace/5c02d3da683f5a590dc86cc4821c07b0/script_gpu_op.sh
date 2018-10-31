#PBS -N SubmissionTe/5c02d3da/gpu_op/0000/e18f73bd84430c27d8ea94f0dba7d7c8
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(5c02d3da683f5a590dc86cc4821c07b0)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op 5c02d3da683f5a590dc86cc4821c07b0

