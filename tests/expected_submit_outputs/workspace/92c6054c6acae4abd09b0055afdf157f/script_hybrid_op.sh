#!/bin/bash
#SBATCH --job-name="SubmissionTe/92c6054c/hybrid_op/0000/9d29f8f2fdde71a44fa329c135382499"
#SBATCH --partition=RM-Shared
#SBATCH -N 1
#SBATCH --ntasks-per-node 4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(92c6054c6acae4abd09b0055afdf157f)
export OMP_NUM_THREADS=2
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 92c6054c6acae4abd09b0055afdf157f

