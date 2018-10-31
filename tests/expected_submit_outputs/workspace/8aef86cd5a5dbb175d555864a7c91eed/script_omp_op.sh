#!/bin/bash
#SBATCH --job-name="SubmissionTe/8aef86cd/omp_op/0000/d673b433c65ea1dbd4af570c25fc0486"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(8aef86cd5a5dbb175d555864a7c91eed)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 8aef86cd5a5dbb175d555864a7c91eed

