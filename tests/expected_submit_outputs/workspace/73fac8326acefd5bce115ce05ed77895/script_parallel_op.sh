#PBS -N SubmissionTe/73fac832/parallel_op/0000/b3fce710ea2c1c5a9dbd7e10b00d90fa
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(73fac8326acefd5bce115ce05ed77895)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 73fac8326acefd5bce115ce05ed77895

