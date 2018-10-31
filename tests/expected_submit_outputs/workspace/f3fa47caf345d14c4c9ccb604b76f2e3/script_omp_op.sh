#!/bin/bash
#SBATCH --job-name="SubmissionTe/f3fa47ca/omp_op/0000/9072ee1636543e2d02e1c6d907b284b1"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(f3fa47caf345d14c4c9ccb604b76f2e3)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op f3fa47caf345d14c4c9ccb604b76f2e3

