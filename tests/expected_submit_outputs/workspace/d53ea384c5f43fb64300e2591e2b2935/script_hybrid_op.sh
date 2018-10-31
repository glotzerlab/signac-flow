#!/bin/bash
#SBATCH --job-name="SubmissionTe/d53ea384/hybrid_op/0000/37853d7388fe36f69da921990d9611a9"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(d53ea384c5f43fb64300e2591e2b2935)
export OMP_NUM_THREADS=2
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op d53ea384c5f43fb64300e2591e2b2935

