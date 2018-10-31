#!/bin/bash
#SBATCH --job-name="SubmissionTe/92c6054c/omp_op/0000/a2b16b88648912af4ff8232467d046c8"
#SBATCH --partition=RM-Shared
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(92c6054c6acae4abd09b0055afdf157f)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 92c6054c6acae4abd09b0055afdf157f

