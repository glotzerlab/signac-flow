#!/bin/bash
#SBATCH --job-name="SubmissionTe/8aef86cd/hybrid_op/0000/5fd380f86341e6ef7e70a1eb0392bdf5"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(8aef86cd5a5dbb175d555864a7c91eed)
export OMP_NUM_THREADS=2
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 8aef86cd5a5dbb175d555864a7c91eed

