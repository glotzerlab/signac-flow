#!/bin/bash
#SBATCH --job-name="SubmissionTe/216cd586/omp_op/0000/104bba1fa641caf218cbdf053d85b164"
#SBATCH --partition=shared
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(216cd5860da17dc03ca7facd39d25e5c)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 216cd5860da17dc03ca7facd39d25e5c

