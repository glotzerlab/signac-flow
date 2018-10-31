#!/bin/bash
#SBATCH --job-name="SubmissionTe/a8393870/omp_op/0000/d750dd33345c79d360b597698bf86698"
#SBATCH --partition=RM
#SBATCH -t 01:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(a83938708abee3a3abb1c040a684e8b1)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op a83938708abee3a3abb1c040a684e8b1

