#!/bin/bash
#SBATCH --job-name="SubmissionTe/eff95441/hybrid_op/0000/f428cf0eb744ef57cde31f3392597b40"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(eff95441c00916227c0838e29ab01084)
export OMP_NUM_THREADS=2
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op eff95441c00916227c0838e29ab01084

