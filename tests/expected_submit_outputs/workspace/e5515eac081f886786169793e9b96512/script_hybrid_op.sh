#!/bin/bash
#SBATCH --job-name="SubmissionTe/e5515eac/hybrid_op/0000/a3aad3e6ca705729114a15a3cfca227b"
#SBATCH --partition=compute
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(e5515eac081f886786169793e9b96512)
export OMP_NUM_THREADS=2
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op e5515eac081f886786169793e9b96512

