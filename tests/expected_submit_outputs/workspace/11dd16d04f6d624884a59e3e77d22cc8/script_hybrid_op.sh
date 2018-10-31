#PBS -N SubmissionTe/11dd16d0/hybrid_op/0000/691a74fa1a515b521e9644b89eedf5ab
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(11dd16d04f6d624884a59e3e77d22cc8)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 11dd16d04f6d624884a59e3e77d22cc8

