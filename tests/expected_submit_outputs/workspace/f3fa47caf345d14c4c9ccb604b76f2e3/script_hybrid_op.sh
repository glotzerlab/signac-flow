#!/bin/bash
#SBATCH --job-name="SubmissionTe/f3fa47ca/hybrid_op/0000/7808a3a470a528c78fd389be56a6b1ad"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(f3fa47caf345d14c4c9ccb604b76f2e3)
export OMP_NUM_THREADS=2
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op f3fa47caf345d14c4c9ccb604b76f2e3

