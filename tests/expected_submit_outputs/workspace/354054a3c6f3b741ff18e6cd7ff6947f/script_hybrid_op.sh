#!/bin/bash
#SBATCH --job-name="SubmissionTe/354054a3/hybrid_op/0000/96238652a6740009d4cee0c4ae0c5b0e"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(354054a3c6f3b741ff18e6cd7ff6947f)
export OMP_NUM_THREADS=2
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 354054a3c6f3b741ff18e6cd7ff6947f &
wait

