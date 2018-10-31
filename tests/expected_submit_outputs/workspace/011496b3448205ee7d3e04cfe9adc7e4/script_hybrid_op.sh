#!/bin/bash
#SBATCH --job-name="SubmissionTe/011496b3/hybrid_op/0000/31a293bf535b43f04a6f8dc06b5a3124"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(011496b3448205ee7d3e04cfe9adc7e4)
export OMP_NUM_THREADS=2
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 011496b3448205ee7d3e04cfe9adc7e4

