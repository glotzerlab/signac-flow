#PBS -N SubmissionTe/73fac832/gpu_op/0000/d5ad367272f856215aed1581339583a8
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(73fac8326acefd5bce115ce05ed77895)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op 73fac8326acefd5bce115ce05ed77895

