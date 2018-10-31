#PBS -N SubmissionTe/38dbccd7/hybrid_op/0000/f915eef18f59d976d63f6575d45d0481
#PBS -V
#PBS -l nodes=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(38dbccd79e1d32d69930dd227fd250ae)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 38dbccd79e1d32d69930dd227fd250ae

