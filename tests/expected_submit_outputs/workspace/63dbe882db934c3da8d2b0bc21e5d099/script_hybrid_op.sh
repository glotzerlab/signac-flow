#!/bin/bash
#SBATCH --job-name="SubmissionTe/63dbe882/hybrid_op/0000/1a1d61c63aed958e76c9e25b126842f9"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(63dbe882db934c3da8d2b0bc21e5d099)
export OMP_NUM_THREADS=2
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 63dbe882db934c3da8d2b0bc21e5d099

