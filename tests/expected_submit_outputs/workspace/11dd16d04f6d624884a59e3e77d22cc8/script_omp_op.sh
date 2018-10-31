#PBS -N SubmissionTe/11dd16d0/omp_op/0000/5c79e4a4816b8ebf36bceea758c29d19
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(11dd16d04f6d624884a59e3e77d22cc8)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 11dd16d04f6d624884a59e3e77d22cc8

