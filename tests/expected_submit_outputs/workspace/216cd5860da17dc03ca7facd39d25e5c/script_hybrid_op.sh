#!/bin/bash
#SBATCH --job-name="SubmissionTe/216cd586/hybrid_op/0000/608d0106d2587248cfde49f9ac1586c4"
#SBATCH --partition=shared
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(216cd5860da17dc03ca7facd39d25e5c)
export OMP_NUM_THREADS=2
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 216cd5860da17dc03ca7facd39d25e5c

