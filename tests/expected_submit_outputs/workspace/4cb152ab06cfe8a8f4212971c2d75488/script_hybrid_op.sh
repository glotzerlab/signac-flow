#PBS -N SubmissionTe/4cb152ab/hybrid_op/0000/26a44a2d39b1a56aaf9c1291f4c50bb3
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(4cb152ab06cfe8a8f4212971c2d75488)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 4cb152ab06cfe8a8f4212971c2d75488

