#PBS -N SubmissionTe/73fac832/omp_op/0000/54a42efef9c7cab4c60e3c6210bdef6b
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(73fac8326acefd5bce115ce05ed77895)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 73fac8326acefd5bce115ce05ed77895

