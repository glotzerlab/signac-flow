#!/bin/bash
#SBATCH --job-name="SubmissionTe/354054a3/omp_op/0000/65c139cad82c7502a35d9211edef50fd"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(354054a3c6f3b741ff18e6cd7ff6947f)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 354054a3c6f3b741ff18e6cd7ff6947f &
wait

