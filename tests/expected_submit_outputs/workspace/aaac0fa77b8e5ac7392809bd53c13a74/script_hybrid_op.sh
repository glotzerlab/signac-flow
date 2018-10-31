#!/bin/bash
#SBATCH --job-name="SubmissionTe/aaac0fa7/hybrid_op/0000/a9ea01620aa8db955a6b2acc1bd9f126"
#SBATCH --partition=RM
#SBATCH -N 2
#SBATCH --ntasks-per-node 4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(aaac0fa77b8e5ac7392809bd53c13a74)
export OMP_NUM_THREADS=2
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op aaac0fa77b8e5ac7392809bd53c13a74

