#PBS -N SubmissionTe/73fac832/hybrid_op/0000/7522b95535f0c07070135e3842e507d3
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(73fac8326acefd5bce115ce05ed77895)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 73fac8326acefd5bce115ce05ed77895

