#PBS -N SubmissionTe/118f4f3c/hybrid_op/0000/4aad98d392f516ab2f6aff48ecc87324
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(118f4f3ccfae13a187ade1247f33dd3c)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 118f4f3ccfae13a187ade1247f33dd3c

