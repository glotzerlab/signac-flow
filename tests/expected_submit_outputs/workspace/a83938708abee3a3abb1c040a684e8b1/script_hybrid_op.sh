#!/bin/bash
#SBATCH --job-name="SubmissionTe/a8393870/hybrid_op/0000/2b8a8745b031285b70326fe5e9a1e82c"
#SBATCH --partition=RM
#SBATCH -t 01:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node 4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(a83938708abee3a3abb1c040a684e8b1)
export OMP_NUM_THREADS=2
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op a83938708abee3a3abb1c040a684e8b1

