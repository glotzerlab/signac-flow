#!/bin/bash
#SBATCH --job-name="SubmissionTe/1fe75367/omp_op/0000/4edac970ae6bfaab3963f61438ec3861"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(1fe753679fef722e7adca78b4ae843f2)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 1fe753679fef722e7adca78b4ae843f2

