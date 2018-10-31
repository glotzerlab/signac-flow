#!/bin/bash
#SBATCH --job-name="SubmissionTe/011496b3/omp_op/0000/c3e999aaa8a1b2eedb67bae966959b2f"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(011496b3448205ee7d3e04cfe9adc7e4)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 011496b3448205ee7d3e04cfe9adc7e4

